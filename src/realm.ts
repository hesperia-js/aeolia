import type {
  Generation,
  Graph,
  StoreKey,
  StoreIdentity,
  ValueRevision,
} from "./contract/types.ts";
import type { QueryBindingHandle } from "./contract/query-binding.ts";
import type { StreamBindingHandle } from "./contract/stream-binding.ts";
import type { StoreBindingRuntime } from "./contract/runtime.ts";
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
  /** The original raw-key snapshot format version. */
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

/** Accepted freshness is distinct from a base published only by observation. */
export type SnapshotLanding =
  | {
      /** Presence without accepted request/write freshness. */
      readonly kind: "none";
    }
  | {
      /** An accepted native landing supplies freshness timing. */
      readonly kind: "accepted";
      /** Finite nonnegative elapsed milliseconds since that landing. */
      readonly age: number;
    };

/** Portable raw or completed-query address; references use the tagged codec. */
export type RawOrQuerySnapshotAddressV2 =
  | {
      /** Uses the graph's ordinary raw-key namespace. */
      readonly kind: "raw";
      /** Nonempty authored logical key, without normalization. */
      readonly key: string;
    }
  | {
      /** Resolves only through a genuine completed-query binding. */
      readonly kind: "binding";
      /** Canonically tagged qualified description supplied by the caller's mapping. */
      readonly reference: unknown;
      /** Nonempty authored logical key within that binding. */
      readonly key: string;
    };

/** A stream address resolves only through a genuine reconstructed stream binding. */
export interface StreamSnapshotAddressV2 {
  /** Distinguishes stream bindings from query bindings with similar reference data. */
  readonly kind: "stream-binding";
  /** Canonically tagged qualified description, independent of local graph identities. */
  readonly reference: unknown;
  /** Already-authored logical key; adoption never computes an input or key. */
  readonly key: string;
}

/** Full portable address; sibling bindings can share the same logical key. */
export type SnapshotAddress = RawOrQuerySnapshotAddressV2 | StreamSnapshotAddressV2;

/** Committed-base fields common to all v2 address kinds. */
export interface SnapshotEntryFieldsV2 {
  /** Existing aeolia-tagged encoded data, excluding predictions. */
  readonly value: unknown;
  /** Source request/write ordering, validated before local authority is issued. */
  readonly generation: Generation;
  /** Source-local provenance; never compared as a cross-graph clock. */
  readonly valueRevision: ValueRevision;
}

/** Streams have accepted landings; raw/query bases may have observation-only presence. */
export type SnapshotEntryV2 = SnapshotEntryFieldsV2 &
  (
    | {
        /** Full raw or completed-query address. */
        readonly address: RawOrQuerySnapshotAddressV2;
        /** Actual base freshness, including observation-only presence. */
        readonly landing: SnapshotLanding;
      }
    | {
        /** Full stream address, never interchangeable with a query address. */
        readonly address: StreamSnapshotAddressV2;
        /** Native stream bases always originate in accepted writes. */
        readonly landing: Extract<SnapshotLanding, { readonly kind: "accepted" }>;
      }
  );

/** Explicit v2 envelope containing full addresses rather than a raw-key record. */
export interface SnapshotV2 {
  /** Selected only by snapshot's explicit version option. */
  readonly version: 2;
  /** Must equal the receiving contract namespace. */
  readonly namespace: string;
  /** Tagged encoding shared by values and qualified references. */
  readonly encoding: EncodingId;
  /** Independent committed bases; duplicate physical addresses are invalid. */
  readonly entries: readonly SnapshotEntryV2[];
}

/** Associates supported qualified transport data with an actual active local binding. */
export interface SnapshotBindingReference {
  /** Genuine binding on the exact graph passed to snapshot or adopt. */
  readonly binding: QueryBindingHandle | StreamBindingHandle;
  /** Supported description data, never executable graph/context/profile objects. */
  readonly reference: unknown;
}

/** Explicit writer selection, including raw-only and observation-only snapshots. */
export interface SnapshotOptionsV2 {
  /** Selects the full-address format; mappings alone never select v2. */
  readonly version: 2;
  /** Unique genuine handles and canonically encoded references across both kinds. */
  readonly bindings?: readonly SnapshotBindingReference[];
}

/** Already reconstructed target bindings; no resolver or acquisition callback is invoked. */
export interface AdoptOptionsV2 {
  /** Unique genuine handles paired with their qualified reference descriptions. */
  readonly bindings?: readonly SnapshotBindingReference[];
}

/** Actual local write stamp; a stream caller checks its revision before releasing owned I/O. */
export type AdoptedEntryV2 =
  | {
      /** Adopted raw or completed-query address. */
      readonly address: RawOrQuerySnapshotAddressV2;
      /** Actual destination physical store identity. */
      readonly identity: StoreIdentity;
    }
  | {
      /** Adopted stream address. */
      readonly address: StreamSnapshotAddressV2;
      /** Actual destination physical store identity. */
      readonly identity: StoreIdentity;
      /** Local revision reserved by this successful adoption write. */
      readonly valueRevision: ValueRevision;
    };

/** Per-entry adoption results; occupation is a stream skip, never overwrite authority. */
export interface AdoptionReportV2 {
  /** Identifies this report's full-address representation. */
  readonly version: 2;
  /** Successfully committed entries with their actual native write identities. */
  readonly adopted: readonly AdoptedEntryV2[];
  /** Malformed or occupied independent entries retain their original address. */
  readonly skipped: readonly {
    /** Original address of a malformed entry, or the full occupied stream address. */
    readonly address: unknown;
  }[];
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
  canonical = false,
  check: () => void = () => {},
): Record<string, unknown> {
  const result: Record<string, unknown> = {};
  const keys = Object.keys(value);
  check();
  if (canonical) keys.sort();
  for (const key of keys) {
    // The read is deliberately performed exactly once.  If an accessor
    // throws, snapshot() turns that boundary failure into an encoding fault
    // naming the store rather than letting a raw accessor error escape.
    const member = (value as Record<string, unknown>)[key];
    check();
    defineEnumerableDataProperty(
      result,
      key,
      encodeValue(member, childPointer(pointer, key), seen, false, canonical, check),
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
  canonical = false,
  check: () => void = () => {},
): unknown {
  check();
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

  const tagged = (name: "Date" | "Map" | "Set"): boolean => {
    const result = isRuntimeTag(object, name);
    check();
    return result;
  };
  if (tagged("Date")) {
    seen.set(object, pointer);
    const milliseconds = Date.prototype.getTime.call(object);
    return { $date: Number.isNaN(milliseconds) ? null : new Date(milliseconds).toISOString() };
  }

  if (tagged("Map")) {
    seen.set(object, pointer);
    const entries: unknown[] = [];
    let index = 0;
    for (const [key, member] of (object as Map<unknown, unknown>).entries()) {
      const entryPointer = childPointer(childPointer(pointer, "$map"), index);
      entries.push([
        encodeValue(key, childPointer(entryPointer, 0), seen, false, canonical, check),
        encodeValue(member, childPointer(entryPointer, 1), seen, false, canonical, check),
      ]);
      index += 1;
    }
    return { $map: entries };
  }

  if (tagged("Set")) {
    seen.set(object, pointer);
    const members: unknown[] = [];
    let index = 0;
    for (const member of (object as Set<unknown>).values()) {
      members.push(
        encodeValue(
          member,
          childPointer(childPointer(pointer, "$set"), index),
          seen,
          false,
          canonical,
          check,
        ),
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
      result[index] = encodeValue(
        object[index],
        childPointer(pointer, index),
        seen,
        false,
        canonical,
        check,
      );
    }
    return result;
  }

  const plain = isPlainObject(object);
  check();
  if (!plain) return noEncodedForm();

  // Register before inspecting members: a self-reference in a $plain value
  // must point to the wrapper position, not recurse through the payload.
  seen.set(object, pointer);
  const keys = Object.keys(object);
  if (!payloadContext && keys.length === 1 && keys[0]!.startsWith("$")) {
    // A plain object with one $-prefixed member would otherwise be
    // indistinguishable from a tagged wrapper. Encode its member in an
    // explicit plain-object payload, and allow that payload one level through
    // without wrapping it again.
    return {
      $plain: encodePlainContents(object, childPointer(pointer, "$plain"), seen, canonical, check),
    };
  }
  return encodePlainContents(object, pointer, seen, canonical, check);
}

function validPointer(pointer: string): boolean {
  if (pointer === "") return true;
  if (!pointer.startsWith("/")) return false;
  return !/(^|[^~])~(?![01])/.test(pointer);
}

class DecodeFailure extends Error {}
class TransferAmbiguity extends Error {}

function decodeValue(
  encoded: unknown,
  pointer: string,
  references: Map<string, unknown>,
  activeSources: WeakSet<object>,
  check: () => void = () => {},
): unknown {
  check();
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
          check,
        );
      }
      return result;
    }

    const object = encoded as Record<string, unknown>;
    const keys = Object.keys(object);
    check();
    if (keys.length === 1 && keys[0]!.startsWith("$")) {
      const tag = keys[0]!;
      const payload = object[tag];
      check();
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
            check();
            if (!Array.isArray(entry) || entry.length !== 2)
              throw new DecodeFailure("invalid $map entry");
            const base = childPointer(pointer, "$map");
            const key = decodeValue(
              entry[0],
              childPointer(childPointer(base, index), 0),
              references,
              activeSources,
              check,
            );
            const member = decodeValue(
              entry[1],
              childPointer(childPointer(base, index), 1),
              references,
              activeSources,
              check,
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
                check,
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
              check,
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
        check,
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

function assertSnapshotEnvelope(
  graph: Graph,
  value: unknown,
  version: unknown,
): asserts value is Snapshot {
  // This call also makes snapshot/adopt on a disposed graph terminal, like
  // every other graph-owned operation.
  const namespace = graphInternal.realmNamespace(graph);
  if (value === null || typeof value !== "object" || Array.isArray(value)) throw snapshotFault([]);
  const candidate = value as Record<string, unknown>;
  if (version !== 1) throw snapshotFault(["version"]);
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
export function snapshot(graph: Graph): Snapshot;
/**
 * Captures full addresses through genuine qualified bindings, without acquisition.
 * @param graph - Open source graph containing the selected committed bases.
 * @param options - Explicit format selection and genuine qualified binding mappings.
 * @returns Frozen v2 envelope; predictions and native operation state are excluded.
 * @throws Fault for unsupported encoding, stale native authority, or a disposed lifetime.
 */
export function snapshot(graph: Graph, options: SnapshotOptionsV2): SnapshotV2;
export function snapshot(graph: Graph, options?: SnapshotOptionsV2): Snapshot | SnapshotV2 {
  if (options !== undefined) return snapshotV2(graph, options);
  const stores = [...graphInternal.realmStores(graph)].sort((left, right) => {
    const leftKey = String(left.key);
    const rightKey = String(right.key);
    return leftKey < rightKey ? -1 : leftKey > rightKey ? 1 : 0;
  });
  const now = Date.now();
  const captured: typeof stores = [];
  const entries: Record<string, SnapshotEntry> = Object.create(null) as Record<
    string,
    SnapshotEntry
  >;
  for (const store of stores) {
    if (!store.snapshot || !store.baseAvailable || !store.transferEligible) continue;
    captured.push(store);
    const key = String(store.key);
    if (store.bound || !store.hasAcceptedLanding) throw new Fault("encoding", [key]);
    let encoded: unknown;
    try {
      encoded = encodeValue(
        store.committedValue(),
        "",
        new Map<object, string>(),
        false,
        false,
        () => {
          if (!store.current()) throw new Fault("encoding", [key]);
        },
      );
    } catch (error) {
      if (error instanceof Fault && error.kind === "encoding") throw error;
      throw new Fault("encoding", [key]);
    }
    if (!store.current()) throw new Fault("encoding", [key]);
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
  for (const store of captured)
    if (!store.current()) throw new Fault("encoding", [String(store.key)]);
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
export function adopt(graph: Graph, value: Snapshot): AdoptionReport;
/**
 * Adopts already reconstructed full addresses; occupied streams are skipped.
 * @param graph - Open destination with genuine reconstructed binding contexts.
 * @param value - Explicit v2 envelope, validated before native writes.
 * @param options - Unique qualified mappings to the destination's existing bindings.
 * @returns Frozen per-entry report; stream records include their actual local write revision.
 * @throws Fault for malformed envelopes, mapping ambiguity, or changed preparation authority.
 * Native configuration and propagation failures retain their original thrown channel.
 */
export function adopt(graph: Graph, value: SnapshotV2, options: AdoptOptionsV2): AdoptionReportV2;
export function adopt(
  graph: Graph,
  value: Snapshot | SnapshotV2,
  options?: AdoptOptionsV2,
): AdoptionReport | AdoptionReportV2 {
  const preparationCurrent = graphInternal.realmPreparationCurrent(graph);
  const version =
    value !== null && typeof value === "object" && hasOwn(value, "version")
      ? value.version
      : undefined;
  if (version === 2) return adoptV2(graph, value, options ?? {}, preparationCurrent);
  assertSnapshotEnvelope(graph, value, version);
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

interface CapturedBinding {
  readonly native: StoreBindingRuntime;
  readonly reference: unknown;
  readonly bytes: string;
  readonly current: () => boolean;
}

function recordFields(value: unknown, fields: readonly string[], check: () => void) {
  if (value === null || typeof value !== "object" || Array.isArray(value))
    throw new DecodeFailure("expected a record");
  const keys = Object.keys(value);
  check();
  if (keys.some((key) => !fields.includes(key))) throw new DecodeFailure("unsupported field");
  return value as Record<string, unknown>;
}

function readOwn(value: Record<string, unknown>, key: string, check: () => void): unknown {
  const own = hasOwn(value, key);
  check();
  if (!own) throw new DecodeFailure(`missing ${key}`);
  const result = value[key];
  check();
  return result;
}

function canonicalReference(value: unknown, check: () => void) {
  const encoded = encodeValue(value, "", new Map(), false, true, check);
  check();
  const bytes = JSON.stringify(encoded);
  if (typeof bytes !== "string") return noEncodedForm();
  return { encoded, bytes };
}

function captureBindings(graph: Graph, list: unknown, check: () => void): CapturedBinding[] {
  if (list === undefined) return [];
  if (!Array.isArray(list)) throw new DecodeFailure("bindings must be an array");
  const rows = Array.from(list as unknown[]);
  check();
  const handles = new Set<object>();
  const pending: Array<{
    readonly native: StoreBindingRuntime;
    reference?: unknown;
    readonly current: () => boolean;
  }> = [];
  const current = (): void => {
    check();
    if (pending.some((row) => !row.current())) throw snapshotFault(["bindings"]);
  };
  for (const row of rows) {
    const record = recordFields(row, ["binding", "reference"], current);
    const handle = readOwn(record, "binding", current);
    if (handle === null || (typeof handle !== "object" && typeof handle !== "function"))
      throw new DecodeFailure("invalid binding handle");
    const native = graphInternal.realmBinding(graph, handle);
    current();
    if (handles.has(handle)) throw new DecodeFailure("duplicate binding handle");
    handles.add(handle);
    const epoch = native.context.state.resetEpoch;
    const captured = {
      native,
      reference: undefined as unknown,
      current: () =>
        !native.context.disposed &&
        !native.context.state.disposed &&
        native.context.state.resetEpoch === epoch,
    };
    pending.push(captured);
    captured.reference = readOwn(record, "reference", current);
  }
  const references = new Set<string>();
  const captured: CapturedBinding[] = [];
  for (const row of pending) {
    const reference = canonicalReference(row.reference, current);
    if (references.has(reference.bytes)) throw new DecodeFailure("duplicate binding reference");
    references.add(reference.bytes);
    captured.push({
      native: row.native,
      reference: reference.encoded,
      bytes: reference.bytes,
      current: row.current,
    });
  }
  current();
  return captured;
}

function snapshotV2(graph: Graph, options: SnapshotOptionsV2): SnapshotV2 {
  const graphCurrent = graphInternal.realmCurrent(graph);
  const namespace = graphInternal.realmNamespace(graph);
  const stores = graphInternal
    .realmStores(graph)
    .filter((store) => store.snapshot && store.baseAvailable && store.transferEligible);
  let mappings: CapturedBinding[] = [];
  const check = (): void => {
    const changed = stores.find((store) => !store.current());
    if (changed !== undefined) throw new Fault("encoding", [String(changed.key)]);
    if (!graphCurrent() || mappings.some((row) => !row.current())) throw new Fault("encoding", []);
  };
  try {
    const settings = recordFields(options, ["version", "bindings"], check);
    if (readOwn(settings, "version", check) !== 2) throw new DecodeFailure("unsupported version");
    const list = settings.bindings;
    check();
    mappings = captureBindings(graph, list, check);
    const byBinding = new Map(mappings.map((row) => [row.native, row]));
    const selected = stores
      .map((store) => {
        let address: SnapshotAddress;
        if (store.binding === undefined)
          address = Object.freeze({ kind: "raw", key: String(store.key) });
        else {
          const mapping = byBinding.get(store.binding);
          if (mapping === undefined) throw new DecodeFailure("missing binding reference");
          address =
            store.binding.kind === "query"
              ? Object.freeze({
                  kind: "binding",
                  reference: mapping.reference,
                  key: String(store.key),
                })
              : Object.freeze({
                  kind: "stream-binding",
                  reference: mapping.reference,
                  key: String(store.key),
                });
        }
        return { store, address, order: JSON.stringify(address)! };
      })
      .sort((left, right) => (left.order < right.order ? -1 : left.order > right.order ? 1 : 0));
    const now = Date.now();
    const entries: SnapshotEntryV2[] = [];
    for (const { store, address } of selected) {
      check();
      if (
        !Number.isSafeInteger(store.generation) ||
        store.generation < 0 ||
        !Number.isSafeInteger(store.valueRevision) ||
        store.valueRevision < 0
      )
        throw new DecodeFailure("unsafe native ordering");
      let value: unknown;
      try {
        value = encodeValue(store.committedValue(), "", new Map(), false, false, check);
      } catch (error) {
        if (error instanceof Fault && error.kind === "encoding") throw error;
        throw new Fault("encoding", [String(store.key)]);
      }
      check();
      const landing: SnapshotLanding = store.hasAcceptedLanding
        ? Object.freeze({ kind: "accepted", age: Math.max(0, now - store.lastLandingAt!) })
        : Object.freeze({ kind: "none" });
      if (
        landing.kind === "accepted" &&
        (!Number.isFinite(landing.age) || landing.age > Number.MAX_SAFE_INTEGER)
      )
        throw new DecodeFailure("invalid native landing");
      const fields = {
        value,
        generation: store.generation as Generation,
        valueRevision: store.valueRevision,
      };
      if (address.kind === "stream-binding") {
        if (landing.kind !== "accepted") throw new DecodeFailure("stream has no accepted landing");
        entries.push(Object.freeze({ ...fields, address, landing }));
      } else entries.push(Object.freeze({ ...fields, address, landing }));
    }
    check();
    return Object.freeze({
      version: 2,
      namespace,
      encoding: AEOLIA_TAGGED_ENCODING,
      entries: Object.freeze(entries),
    });
  } catch (error) {
    if (error instanceof Fault && (error.kind === "encoding" || error.kind === "disposed"))
      throw error;
    throw new Fault("encoding", []);
  }
}

function adoptV2(
  graph: Graph,
  input: unknown,
  options: AdoptOptionsV2,
  preparationCurrent: () => boolean,
): AdoptionReportV2 {
  const namespace = graphInternal.realmNamespace(graph);
  let mappings: CapturedBinding[] = [];
  const check = (): void => {
    if (!preparationCurrent() || mappings.some((row) => !row.current())) throw snapshotFault([]);
  };
  const adopted: AdoptedEntryV2[] = [];
  const skipped: { readonly address: unknown }[] = [];
  const pending: Array<{
    readonly address: SnapshotAddress;
    readonly originalAddress: unknown;
    readonly target: ReturnType<typeof graphInternal.prepareRealmAdoption>;
    readonly record: Record<string, unknown>;
    value?: unknown;
    generation?: number;
    landingAt?: number;
    valid: boolean;
  }> = [];
  try {
    const envelope = recordFields(input, ["version", "namespace", "encoding", "entries"], check);
    if (
      readOwn(envelope, "namespace", check) !== namespace ||
      readOwn(envelope, "encoding", check) !== AEOLIA_TAGGED_ENCODING
    )
      throw new DecodeFailure("unsupported envelope");
    const entries = readOwn(envelope, "entries", check);
    if (!Array.isArray(entries)) throw new DecodeFailure("entries must be an array");
    const rows = Array.from(entries as unknown[]);
    check();
    const settings = recordFields(options, ["bindings"], check);
    const list = settings.bindings;
    check();
    mappings = captureBindings(graph, list, check);
    const byReference = new Map(mappings.map((row) => [row.bytes, row]));
    const physical = new Map<StoreBindingRuntime | undefined, Set<string>>();
    for (const row of rows) {
      let originalAddress: unknown;
      let record: Record<string, unknown>;
      let address: SnapshotAddress;
      let binding: StoreBindingRuntime | undefined;
      try {
        if (row === null || typeof row !== "object" || Array.isArray(row))
          throw new DecodeFailure("expected an entry record");
        originalAddress = readOwn(row as Record<string, unknown>, "address", check);
        record = row as Record<string, unknown>;
        const wire = recordFields(originalAddress, ["kind", "key", "reference"], check);
        const kind = readOwn(wire, "kind", check);
        const key = readOwn(wire, "key", check);
        if (typeof key !== "string" || key.length === 0) throw new DecodeFailure("invalid key");
        if (kind === "raw") {
          if (hasOwn(wire, "reference")) throw new DecodeFailure("raw reference");
          check();
          address = Object.freeze({ kind, key });
        } else if (kind === "binding" || kind === "stream-binding") {
          const encoded = readOwn(wire, "reference", check);
          const decoded = decodeValue(encoded, "", new Map(), new WeakSet(), check);
          const reference = canonicalReference(decoded, check);
          const mapping = byReference.get(reference.bytes);
          if (
            mapping === undefined ||
            mapping.native.kind !== (kind === "binding" ? "query" : "stream")
          )
            throw new TransferAmbiguity("missing or wrong-kind binding reference");
          binding = mapping.native;
          address =
            kind === "binding"
              ? Object.freeze({ kind: "binding", key, reference: mapping.reference })
              : Object.freeze({ kind: "stream-binding", key, reference: mapping.reference });
        } else throw new DecodeFailure("unsupported address kind");
      } catch (error) {
        check();
        if (error instanceof TransferAmbiguity) throw error;
        skipped.push(Object.freeze({ address: originalAddress }));
        continue;
      }
      let keys = physical.get(binding);
      if (keys?.has(address.key)) throw snapshotFault([address.key]);
      if (keys === undefined) {
        keys = new Set();
        physical.set(binding, keys);
      }
      keys.add(address.key);
      try {
        recordFields(record, ["address", "value", "generation", "valueRevision", "landing"], check);
      } catch {
        check();
        skipped.push(Object.freeze({ address: originalAddress }));
        continue;
      }
      pending.push({
        address,
        originalAddress,
        record,
        target: graphInternal.prepareRealmAdoption(graph, address.key, binding),
        valid: false,
      });
    }
    const now = Date.now();
    for (const entry of pending) {
      try {
        const generation = readOwn(entry.record, "generation", check);
        const revision = readOwn(entry.record, "valueRevision", check);
        if (
          !Number.isSafeInteger(generation) ||
          (generation as number) < 0 ||
          !Number.isSafeInteger(revision) ||
          (revision as number) < 0
        )
          throw new DecodeFailure("invalid ordering");
        const landing = recordFields(
          readOwn(entry.record, "landing", check),
          ["kind", "age"],
          check,
        );
        const kind = readOwn(landing, "kind", check);
        let landingAt: number | undefined;
        if (kind === "accepted") {
          const age = readOwn(landing, "age", check);
          if (
            typeof age !== "number" ||
            !Number.isFinite(age) ||
            age < 0 ||
            age > Number.MAX_SAFE_INTEGER
          )
            throw new DecodeFailure("invalid age");
          landingAt = now - age;
        } else if (
          kind !== "none" ||
          entry.address.kind === "stream-binding" ||
          hasOwn(landing, "age")
        )
          throw new DecodeFailure("invalid landing");
        const value = decodeValue(
          readOwn(entry.record, "value", check),
          "",
          new Map(),
          new WeakSet(),
          check,
        );
        check();
        entry.value = value;
        entry.generation = generation as number;
        entry.landingAt = landingAt;
        entry.valid = true;
      } catch {
        check();
        skipped.push(Object.freeze({ address: entry.originalAddress }));
      }
    }
    check();
    if (pending.some((entry) => !entry.target.current())) throw snapshotFault([]);
  } catch (error) {
    if (error instanceof Fault) throw error;
    throw snapshotFault([]);
  }
  for (const entry of pending) if (entry.valid) entry.target.validate(entry.generation!);
  reactiveInternal.batch(() => {
    for (const entry of pending) {
      if (!entry.valid) continue;
      const stamp = entry.target.adopt(entry.value, entry.generation!, entry.landingAt);
      if (stamp === undefined) skipped.push(Object.freeze({ address: entry.address }));
      else if (entry.address.kind === "stream-binding")
        adopted.push(
          Object.freeze({
            address: entry.address,
            identity: stamp.identity,
            valueRevision: stamp.valueRevision,
          }),
        );
      else adopted.push(Object.freeze({ address: entry.address, identity: stamp.identity }));
    }
  });
  return Object.freeze({
    version: 2,
    adopted: Object.freeze(adopted),
    skipped: Object.freeze(skipped),
  });
}
