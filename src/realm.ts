import type { Generation, Graph, StoreKey } from "./contract/types.ts";
import { Fault } from "./fault.ts";
import { __internal as graphInternal } from "./contract/engine.ts";
import { __internal as reactiveInternal } from "./reactive.ts";

/**
 * The branded identifier of a snapshot value encoding.
 *
 * Encoding identifiers are part of the snapshot envelope. A consumer should
 * pass an identifier produced by Aeolia, such as
 * {@link AEOLIA_TAGGED_ENCODING}, rather than inventing one. The brand keeps
 * arbitrary strings from being used where an encoding identifier is required
 * in TypeScript; it does not change the runtime representation.
 */
export type EncodingId = string & { readonly __encodingId: unique symbol };

/** One committed store value carried by a {@link Snapshot}. */
export interface SnapshotEntry {
  /**
   * The value in Aeolia's tagged, realm-safe representation.
   *
   * Primitive values, `undefined`, non-finite numbers, `bigint`, `Date`,
   * `Map`, `Set`, arrays, and plain objects are represented directly or with
   * a single-key tag. Object aliases and cycles use JSON-pointer-like `$ref`
   * tags. Functions, symbols, promises, weak collections, custom class
   * instances, and other unsupported values make {@link snapshot} throw an
   * encoding fault for the containing store.
   */
  readonly value: unknown;

  /**
   * The current store generation at the time the value was captured.
   *
   * Starting a request advances this ordering token before that request lands,
   * so it can be newer than the encoded committed value. Predictions are not
   * part of a snapshot.
   */
  readonly generation: Generation;

  /**
   * Milliseconds elapsed since the value's last committed landing when the
   * snapshot was created.
   *
   * Adoption uses this age to reconstruct freshness timing in the target
   * graph instead of treating every adopted value as newly landed.
   */
  readonly age: number;
}

type ValidatedSnapshotEntry = Omit<SnapshotEntry, "generation"> & {
  readonly generation: number;
};

/**
 * The portable envelope produced by {@link snapshot} and consumed by
 * {@link adopt}.
 *
 * The envelope carries selected committed values and timing metadata. It does
 * not carry a contract, callbacks, signal identities, active requests,
 * timers, optimistic predictions, streams, or projections. To reconstruct
 * state in another graph, create that graph with the same contract namespace
 * and adopt this envelope into it.
 */
export interface Snapshot {
  /** The snapshot format version. Aeolia currently accepts only version `1`. */
  readonly version: 1;

  /**
   * The contract namespace that owns the captured keys.
   *
   * {@link adopt} requires this to equal the target graph's contract
   * namespace before it processes any entries.
   */
  readonly namespace: string;

  /** The value encoding used by every entry in this envelope. */
  readonly encoding: EncodingId;

  /**
   * Snapshot entries keyed by their stringified {@link StoreKey}.
   *
   * Only stores whose effective snapshot option is enabled and that have a
   * committed value are included. Stores are traversed in code-unit key order
   * before JavaScript object-property ordering is applied, so store creation
   * order does not affect the envelope.
   */
  readonly entries: Readonly<Record<string, SnapshotEntry>>;
}

/**
 * The result of attempting to adopt a snapshot.
 *
 * Entries are decoded independently. One malformed entry is reported in
 * `skipped` while other valid entries can still be adopted.
 */
export interface AdoptionReport {
  /** Store keys whose entries were validated, decoded, and adopted. */
  readonly adopted: readonly StoreKey[];

  /** Non-empty store keys whose entries could not be validated or decoded. */
  readonly skipped: readonly StoreKey[];
}

/** The tagged value encoding implemented by {@link snapshot} and {@link adopt}. */
export const AEOLIA_TAGGED_ENCODING = "aeolia-tagged/1" as EncodingId;

const hasOwn = (value: object, key: string): boolean =>
  Object.prototype.hasOwnProperty.call(value, key);

function pointerSegment(value: string): string {
  return value.replaceAll("~", "~0").replaceAll("/", "~1");
}

function childPointer(pointer: string, segment: string | number): string {
  return `${pointer}/${pointerSegment(String(segment))}`;
}

function isPlainObject(value: object): boolean {
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function isRuntimeTag(value: object, name: "Date" | "Map" | "Set" | "Promise"): boolean {
  // `instanceof` is the normative discriminator for the tagged types.  The
  // toString fallback is needed for values supplied by another JS realm,
  // where that realm's intrinsic constructor is not this realm's constructor.
  try {
    if (name === "Date" && value instanceof Date) return true;
    if (name === "Map" && value instanceof Map) return true;
    if (name === "Set" && value instanceof Set) return true;
    if (name === "Promise" && value instanceof Promise) return true;
  } catch {
    // A hostile Symbol.hasInstance should still produce an encoding fault.
  }
  try {
    return Object.prototype.toString.call(value) === `[object ${name}]`;
  } catch {
    return false;
  }
}

function noEncodedForm(): never {
  throw new TypeError("value has no aeolia-tagged encoding");
}

function encodePlainContents(
  value: object,
  pointer: string,
  seen: Map<object, string>,
): Record<string, unknown> {
  const result: Record<string, unknown> = {};
  const keys = Object.keys(value);
  for (const key of keys) {
    // The read is deliberately performed exactly once.  If an accessor
    // throws, snapshot() turns that boundary failure into an encoding fault
    // naming the store rather than letting a raw accessor error escape.
    const member = (value as Record<string, unknown>)[key];
    defineEnumerableDataProperty(
      result,
      key,
      encodeValue(member, childPointer(pointer, key), seen, false),
    );
  }
  return result;
}

function defineEnumerableDataProperty(
  target: Record<string, unknown>,
  key: string,
  value: unknown,
): void {
  Object.defineProperty(target, key, {
    value,
    enumerable: true,
    configurable: true,
    writable: true,
  });
}

function encodeValue(
  value: unknown,
  pointer: string,
  seen: Map<object, string>,
  payloadContext: boolean,
): unknown {
  if (value === undefined) return { $undefined: true };
  if (value === null || typeof value === "string" || typeof value === "boolean") return value;
  if (typeof value === "number") {
    if (Number.isNaN(value)) return { $num: "NaN" };
    if (value === Number.POSITIVE_INFINITY) return { $num: "Inf" };
    if (value === Number.NEGATIVE_INFINITY) return { $num: "-Inf" };
    if (Object.is(value, -0)) return { $num: "-0" };
    return value;
  }
  if (typeof value === "bigint") return { $bigint: value.toString(10) };
  if (typeof value === "symbol" || typeof value === "function") return noEncodedForm();

  const object = value as object;
  const previous = seen.get(object);
  if (previous != null) return { $ref: previous };

  if (isRuntimeTag(object, "Date")) {
    seen.set(object, pointer);
    const milliseconds = Date.prototype.getTime.call(object);
    return { $date: Number.isNaN(milliseconds) ? null : new Date(milliseconds).toISOString() };
  }

  if (isRuntimeTag(object, "Map")) {
    seen.set(object, pointer);
    const entries: unknown[] = [];
    let index = 0;
    for (const [key, member] of (object as Map<unknown, unknown>).entries()) {
      const entryPointer = childPointer(childPointer(pointer, "$map"), index);
      entries.push([
        encodeValue(key, childPointer(entryPointer, 0), seen, false),
        encodeValue(member, childPointer(entryPointer, 1), seen, false),
      ]);
      index += 1;
    }
    return { $map: entries };
  }

  if (isRuntimeTag(object, "Set")) {
    seen.set(object, pointer);
    const members: unknown[] = [];
    let index = 0;
    for (const member of (object as Set<unknown>).values()) {
      members.push(
        encodeValue(member, childPointer(childPointer(pointer, "$set"), index), seen, false),
      );
      index += 1;
    }
    return { $set: members };
  }

  if (Array.isArray(object)) {
    seen.set(object, pointer);
    const result: unknown[] = Array.from({ length: object.length });
    for (let index = 0; index < object.length; index += 1) {
      // Holes intentionally become the explicit undefined tag.
      result[index] = encodeValue(object[index], childPointer(pointer, index), seen, false);
    }
    return result;
  }

  if (!isPlainObject(object)) return noEncodedForm();

  // Register before inspecting members: a self-reference in a $plain value
  // must point to the wrapper position, not recurse through the payload.
  seen.set(object, pointer);
  const keys = Object.keys(object);
  if (!payloadContext && keys.length === 1 && keys[0]!.startsWith("$")) {
    // A plain object with one $-prefixed member would otherwise be
    // indistinguishable from a tagged wrapper. Encode its member in an
    // explicit plain-object payload, and allow that payload one level through
    // without wrapping it again.
    return { $plain: encodePlainContents(object, childPointer(pointer, "$plain"), seen) };
  }
  return encodePlainContents(object, pointer, seen);
}

function validPointer(pointer: string): boolean {
  if (pointer === "") return true;
  if (!pointer.startsWith("/")) return false;
  return !/(^|[^~])~(?![01])/.test(pointer);
}

class DecodeFailure extends Error {}

function decodeValue(
  encoded: unknown,
  pointer: string,
  references: Map<string, unknown>,
  activeSources: WeakSet<object>,
): unknown {
  if (
    encoded === null ||
    typeof encoded === "string" ||
    typeof encoded === "boolean" ||
    typeof encoded === "number"
  ) {
    return encoded;
  }
  if (typeof encoded !== "object") throw new DecodeFailure("encoded value is not JSON data");
  if (activeSources.has(encoded)) throw new DecodeFailure("encoded value is cyclic");
  activeSources.add(encoded);
  try {
    if (Array.isArray(encoded)) {
      const result: unknown[] = Array.from({ length: encoded.length });
      references.set(pointer, result);
      for (let index = 0; index < encoded.length; index += 1) {
        result[index] = decodeValue(
          encoded[index],
          childPointer(pointer, index),
          references,
          activeSources,
        );
      }
      return result;
    }

    const object = encoded as Record<string, unknown>;
    const keys = Object.keys(object);
    if (keys.length === 1 && keys[0]!.startsWith("$")) {
      const tag = keys[0]!;
      const payload = object[tag];
      switch (tag) {
        case "$ref": {
          if (typeof payload !== "string" || !validPointer(payload) || !references.has(payload)) {
            throw new DecodeFailure("invalid $ref");
          }
          return references.get(payload);
        }
        case "$undefined":
          if (payload !== true) throw new DecodeFailure("invalid $undefined payload");
          return undefined;
        case "$num":
          if (payload === "NaN") return Number.NaN;
          if (payload === "Inf") return Number.POSITIVE_INFINITY;
          if (payload === "-Inf") return Number.NEGATIVE_INFINITY;
          if (payload === "-0") return -0;
          throw new DecodeFailure("invalid $num payload");
        case "$bigint":
          if (typeof payload !== "string" || !/^-?\d+$/.test(payload))
            throw new DecodeFailure("invalid $bigint payload");
          try {
            return BigInt(payload);
          } catch {
            throw new DecodeFailure("invalid $bigint payload");
          }
        case "$date": {
          if (payload === null) {
            const result = new Date(Number.NaN);
            references.set(pointer, result);
            return result;
          }
          if (
            typeof payload !== "string" ||
            !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$/.test(payload)
          ) {
            throw new DecodeFailure("invalid $date payload");
          }
          const result = new Date(payload);
          if (Number.isNaN(result.getTime())) throw new DecodeFailure("invalid $date payload");
          references.set(pointer, result);
          return result;
        }
        case "$map": {
          if (!Array.isArray(payload)) throw new DecodeFailure("invalid $map payload");
          const result = new Map<unknown, unknown>();
          references.set(pointer, result);
          for (let index = 0; index < payload.length; index += 1) {
            const entry = payload[index];
            if (!Array.isArray(entry) || entry.length !== 2)
              throw new DecodeFailure("invalid $map entry");
            const base = childPointer(pointer, "$map");
            const key = decodeValue(
              entry[0],
              childPointer(childPointer(base, index), 0),
              references,
              activeSources,
            );
            const member = decodeValue(
              entry[1],
              childPointer(childPointer(base, index), 1),
              references,
              activeSources,
            );
            result.set(key, member);
          }
          return result;
        }
        case "$set": {
          if (!Array.isArray(payload)) throw new DecodeFailure("invalid $set payload");
          const result = new Set<unknown>();
          references.set(pointer, result);
          for (let index = 0; index < payload.length; index += 1) {
            result.add(
              decodeValue(
                payload[index],
                childPointer(childPointer(pointer, "$set"), index),
                references,
                activeSources,
              ),
            );
          }
          return result;
        }
        case "$plain": {
          if (
            payload === null ||
            typeof payload !== "object" ||
            Array.isArray(payload) ||
            !isPlainObject(payload)
          ) {
            throw new DecodeFailure("invalid $plain payload");
          }
          const result: Record<string, unknown> = {};
          references.set(pointer, result);
          const base = childPointer(pointer, "$plain");
          for (const key of Object.keys(payload)) {
            const member = decodeValue(
              (payload as Record<string, unknown>)[key],
              childPointer(base, key),
              references,
              activeSources,
            );
            defineEnumerableDataProperty(result, key, member);
          }
          return result;
        }
        default:
          throw new DecodeFailure(`unknown tag ${tag}`);
      }
    }

    const result: Record<string, unknown> = {};
    references.set(pointer, result);
    for (const key of keys) {
      const member = decodeValue(
        object[key],
        childPointer(pointer, key),
        references,
        activeSources,
      );
      defineEnumerableDataProperty(result, key, member);
    }
    return result;
  } finally {
    activeSources.delete(encoded);
  }
}

function snapshotFault(involved: readonly string[]): Fault {
  return new Fault("snapshot", involved);
}

function assertSnapshotEnvelope(graph: Graph, value: unknown): asserts value is Snapshot {
  // This call also makes snapshot/adopt on a disposed graph terminal, like
  // every other graph-owned operation.
  const namespace = graphInternal.realmNamespace(graph);
  if (value === null || typeof value !== "object" || Array.isArray(value)) throw snapshotFault([]);
  const candidate = value as Record<string, unknown>;
  if (!hasOwn(candidate, "version") || candidate.version !== 1) throw snapshotFault(["version"]);
  if (!hasOwn(candidate, "encoding") || candidate.encoding !== AEOLIA_TAGGED_ENCODING)
    throw snapshotFault(["encoding"]);
  if (!hasOwn(candidate, "namespace") || candidate.namespace !== namespace)
    throw snapshotFault(["namespace"]);
  if (
    !hasOwn(candidate, "entries") ||
    candidate.entries === null ||
    typeof candidate.entries !== "object" ||
    Array.isArray(candidate.entries)
  ) {
    throw snapshotFault(["entries"]);
  }
}

function ownSnapshotEntry(value: unknown): ValidatedSnapshotEntry {
  if (value === null || typeof value !== "object" || Array.isArray(value))
    throw new DecodeFailure("entry is not an object");
  const entry = value as Record<string, unknown>;
  if (!hasOwn(entry, "value") || !hasOwn(entry, "generation") || !hasOwn(entry, "age"))
    throw new DecodeFailure("entry is incomplete");
  if (!Number.isSafeInteger(entry.generation) || (entry.generation as number) < 0)
    throw new DecodeFailure("invalid generation");
  if (typeof entry.age !== "number" || !Number.isFinite(entry.age) || entry.age < 0)
    throw new DecodeFailure("invalid age");
  return { value: entry.value, generation: entry.generation as number, age: entry.age };
}

/**
 * Captures selected committed values from a graph in a portable envelope.
 *
 * Snapshot selection follows each store's effective `snapshot` setting. The
 * captured value is the committed base, not any optimistic predictions, and
 * active requests, streams, projections, callbacks, and reactive identities
 * remain graph-local. Stores are traversed in JavaScript code-unit key order
 * before entries are assigned to the object envelope. The returned envelope,
 * entry map, and entry records are frozen.
 *
 * Values are encoded with {@link AEOLIA_TAGGED_ENCODING}. The encoder
 * preserves supported special values, aliases, and cycles within each store
 * entry. Sparse-array holes become explicit `undefined` values, and identity
 * is not preserved between separate store entries. The encoder reads
 * enumerable own string properties once; an accessor that throws is reported
 * as an encoding fault for its store.
 *
 * @param graph - The open graph whose selected committed values are captured.
 * @returns A deterministic snapshot envelope suitable for {@link adopt} on a
 * graph with the same contract namespace.
 * @throws {@link Fault} With kind `encoding` when a selected value has no
 * supported representation, or kind `disposed` when the graph is closed.
 *
 * @example
 * ```ts
 * import { adopt, snapshot, type Graph } from "aeolia";
 *
 * declare const sourceGraph: Graph;
 * declare const targetGraph: Graph;
 *
 * const state = snapshot(sourceGraph);
 * adopt(targetGraph, state);
 * ```
 */
export function snapshot(graph: Graph): Snapshot {
  const stores = [...graphInternal.realmStores(graph)].sort((left, right) => {
    const leftKey = String(left.key);
    const rightKey = String(right.key);
    return leftKey < rightKey ? -1 : leftKey > rightKey ? 1 : 0;
  });
  const now = Date.now();
  const entries: Record<string, SnapshotEntry> = Object.create(null) as Record<
    string,
    SnapshotEntry
  >;
  for (const store of stores) {
    if (!store.snapshot || !store.hasCommitted) continue;
    const key = String(store.key);
    let encoded: unknown;
    try {
      encoded = encodeValue(store.committedValue(), "", new Map<object, string>(), false);
    } catch (error) {
      if (error instanceof Fault && error.kind === "encoding") throw error;
      throw new Fault("encoding", [key]);
    }
    const landing = store.lastLandingAt;
    const age = landing === undefined ? 0 : Math.max(0, now - landing);
    const entry: SnapshotEntry = Object.freeze({
      value: encoded,
      generation: store.generation as Generation,
      age,
    });
    Object.defineProperty(entries, key, {
      value: entry,
      enumerable: true,
      configurable: false,
      writable: false,
    });
  }
  return Object.freeze({
    version: 1 as const,
    namespace: graphInternal.realmNamespace(graph),
    encoding: AEOLIA_TAGGED_ENCODING,
    entries: Object.freeze(entries),
  });
}

/**
 * Decodes and commits a snapshot into a graph.
 *
 * The envelope version, encoding, and namespace are validated before any
 * entry is processed. A mismatch throws a `snapshot` fault and leaves the
 * graph unchanged. Entries are then validated and decoded independently;
 * valid entries are adopted in one reactive batch, while malformed entries
 * are returned in `AdoptionReport.skipped` and do not prevent other entries
 * from landing.
 *
 * Adoption creates or configures keyed store state as needed, restores each
 * entry's landing age for freshness, and issues a fresh local generation
 * newer than both the local and incoming generation. An older in-flight
 * response therefore cannot overwrite an adopted value. Adopted values are
 * committed values, not optimistic predictions.
 *
 * @param graph - The open graph that receives the decoded committed values.
 * @param value - A snapshot envelope. Its runtime metadata is validated even
 * when the TypeScript value was obtained from an untrusted boundary.
 * @returns A frozen report listing adopted and skipped store keys.
 * @throws {@link Fault} With kind `snapshot` for an unsupported envelope or
 * kind `disposed` when the graph is closed. Store configuration and reactive
 * propagation failures raised while committing valid entries are rethrown.
 *
 * @example
 * ```ts
 * import { adopt, snapshot, type Graph } from "aeolia";
 *
 * declare const sourceGraph: Graph;
 * declare const targetGraph: Graph;
 *
 * const report = adopt(targetGraph, snapshot(sourceGraph));
 * if (report.skipped.length > 0) {
 *   console.warn("Some state entries were not restored", report.skipped);
 * }
 * ```
 */
export function adopt(graph: Graph, value: Snapshot): AdoptionReport {
  assertSnapshotEnvelope(graph, value);
  const entries = (value as Snapshot).entries as Readonly<Record<string, unknown>>;
  const adopted: StoreKey[] = [];
  const skipped: StoreKey[] = [];
  const pending: Array<
    ValidatedSnapshotEntry & {
      readonly key: string;
      readonly storeKey: StoreKey;
    }
  > = [];
  const now = Date.now();

  for (const key of Object.keys(entries)) {
    let storeKeyValue: StoreKey | undefined;
    try {
      storeKeyValue = key.length === 0 ? undefined : (key as StoreKey);
      if (storeKeyValue === undefined) throw new DecodeFailure("empty store key");
      const entry = ownSnapshotEntry(entries[key]);
      const decoded = decodeValue(
        entry.value,
        "",
        new Map<string, unknown>(),
        new WeakSet<object>(),
      );
      pending.push({
        key,
        storeKey: storeKeyValue,
        value: decoded,
        generation: entry.generation,
        age: entry.age,
      });
    } catch {
      if (storeKeyValue != null) skipped.push(storeKeyValue);
    }
  }

  reactiveInternal.batch(() => {
    for (const entry of pending) {
      graphInternal.adoptRealmEntry(
        graph,
        entry.key,
        entry.value,
        entry.generation,
        now - entry.age,
      );
      adopted.push(entry.storeKey);
    }
  });
  return Object.freeze({ adopted: Object.freeze(adopted), skipped: Object.freeze(skipped) });
}
