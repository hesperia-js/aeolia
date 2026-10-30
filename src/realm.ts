import type {
  Generation,
  Graph,
  StoreKey,
  StoreIdentity,
  ValueRevision,
} from "./contract/types.ts";
import type { QueryBindingHandle } from "./contract/query-binding.ts";
import type { StreamBindingHandle } from "./contract/stream-binding.ts";
import type { StoreBindingRuntime, StoreRuntime } from "./contract/runtime.ts";
import type { Readable, WritableSignal, Computed } from "./reactive.ts";
import type { Store, QueryStore, StreamStore } from "./contract/types.ts";
import { prepareTransferPublication, transferGraph, transferEpochCurrent, transferHasAdmission, transferStoreAdmissible, transferStore, transferStoreEligible, transferStoreCurrent, transferStoreSemanticCurrent, transferReadableStore, transferReadableRole, transferProjection, transferAuthority, transferReadableCaller, transferCaller } from "./contract/query-policy.ts";
import type { TransferPublication } from "./contract/query-policy.ts";
import { getObservationTransferFacet } from "./contract/observation-state.ts";
import { transferPositionedCheckpoint, type TransferPositionedCheckpoint } from "./contract/projection.ts";
import type { PositionedStreamAccess, ProjectionPolicy } from "./contract/projection.ts";
import type { PositionedStoreCheckpoint, PositionedStoreRestoreSeed, PositionedStoreRestoration, PositionedStorePolicyValues, ValueOptions } from "./contract/types.ts";
import { readerLifetimeNow } from "./contract/retirement.ts";
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


/** Stable native constructor association; copied fields cannot authenticate it. */
export type ReactiveConstruction<T> = import("./reactive/engine.ts").ReactiveConstruction<T>;
/** Transport-local slot identity; it grants no native authority. */
export type TransferSlot = string;
/** Closed response-codec output, copied and frozen before a candidate can seal. */
export type ClosedEncodedValue = null | undefined | boolean | number | string | readonly ClosedEncodedValue[] | { readonly [key: string]: ClosedEncodedValue };
/** Selected response codec's projected failure representation. */
export type ClosedEncodedFailure = ClosedEncodedValue;
/** Exact native cached source presented to the selected codec. */
export interface TransferSourceWitness {
  /** Stable slot in this image. */
  readonly slot: TransferSlot;
  /** Authentic cached source; encoding does not read it. */
  readonly readable: Readable<unknown>;
  /** Actual physical Store when this slot denotes a native source. */
  readonly store?: Store<unknown>;
  /** Physical source slot for an exact caller witness. */
  readonly physical?: TransferSlot;
}
/** Exact native receiver target presented to the selected decoder. */
export interface TransferTargetWitness {
  /** Slot being restored. */
  readonly slot: TransferSlot;
  /** Genuine receiver readable. */
  readonly readable: Readable<unknown>;
  /** Actual receiver Store, when applicable. */
  readonly store?: Store<unknown>;
}
/** One codec session selected by the actual program/response owner. */
export interface RequiredTransferCodec {
  /** Encodes a successful cached value, including present undefined. */
  encodeValue(value: unknown, source: TransferSourceWitness): ClosedEncodedValue;
  /** Encodes only an admitted native cached failure, never Error-valued successful data. */
  encodeFailure(cause: unknown, source: TransferSourceWitness): ClosedEncodedFailure;
  /** Decodes a selected successful value for its genuine receiver. */
  decodeValue(value: ClosedEncodedValue, target: TransferTargetWitness): unknown;
  /** Decodes the projected failure representation for its genuine receiver. */
  decodeFailure(value: ClosedEncodedFailure, target: TransferTargetWitness): unknown;
}
/** Genuine native member or native query/stream binding. */
export type NativeTransferSource = ((...args: any[]) => Store<any>) | QueryBindingHandle | StreamBindingHandle;
/** Native value policy accepted during passive source preparation. */
type NativeTransferValue<Source> = NativeStoreFor<Source> extends Store<infer Value> ? Value : unknown;
/** Existing value options, with actual query caller freshness for query sources. */
export type ExistingNativeValueOptions<Source = NativeTransferSource> = Source extends import("./contract/query-policy.ts").QueryTransferSource
  ? import("./contract/types.ts").StoreOptions<NativeTransferValue<Source>>
  : import("./contract/types.ts").ValueOptions<NativeTransferValue<Source>>;
/** Public Store shape retained from the genuine source's declared value type. */
export type NativeStoreFor<Source> = Source extends (...args: any[]) => infer Result
  ? Result
  : Source extends import("./contract/query-binding.ts").QueryBinding<any, infer Value>
    ? import("./contract/types.ts").QueryStore<Value, any>
    : Source extends { readonly member: (...args: any[]) => infer MemberResult }
      ? MemberResult
      : Store<unknown>;

/** Genuine observation participant owned by one passive query claim. */
export interface PreparedPassiveObservation<Value> {
  /** Exact native lease; pending publication grants no publishing authority. */
  readonly lease:import("./contract/observation.ts").ObservationLease<Value>;
  /** The owning claim's exact registration-cleanup Promise, not provider or graph retirement. */
  readonly retired:Promise<void>;
  /** Discards the owning pending claim; an existing shared publisher is preserved. */
  discard():void;
}

declare const passiveTransferClaimBrand: unique symbol;
/** Detached native acquisition; metadata is safe while body reads remain gated. */
export interface PassiveTransferClaim<Source = NativeTransferSource> {
  /** Genuine native handle; activation and body access begin only after publication. */
  readonly store: NativeStoreFor<Source>;
  /** Prepares genuine query observation metadata; an exact existing lease may be reused. */
  readonly prepareObservation: Source extends import("./contract/query-policy.ts").QueryTransferSource
    ? (existingLease?:import("./contract/observation.ts").ObservationLease<NativeTransferValue<Source>>) => PreparedPassiveObservation<NativeTransferValue<Source>>
    : never;
  /** Discards this pending claim without disposing a shared existing physical Store. */
  discard(): void;
  /** This claim's actual pending-registration cleanup completion. */
  readonly retired: Promise<void>;
  /** Private nominal identity; copying public fields cannot issue a claim. */
  readonly [passiveTransferClaimBrand]: true;
}
/** Serialized cached node; native facets carry addresses instead of private State values. */
export interface RequiredTransferNode {
  /** Transport-local node identity. */
  readonly slot: TransferSlot;
  /** Actual cached role. */
  readonly kind: "state" | "computed" | "native" | "authority";
  /** Immediate cached dependency slots in native order. */
  readonly sources: readonly TransferSlot[];
  /** Owning source slot for a native facet. */
  readonly store?: TransferSlot;
  /** Native facet or authority role. */
  readonly facet?: string;
  /** Exact per-caller source when this native facet belongs to one acquisition. */
  readonly caller?: TransferSlot;
  /** Exact positioned group whose native facet this row names. */
  readonly positioned?: TransferSlot;
  /** Actual target fold tuple ordinal within that group. */
  readonly projection?: number;
  /** Graph index for a native graph authority. */
  readonly graph?: number;
  /** Cached value presence, including present undefined. */
  readonly hasValue: boolean;
  /** Cached failure presence. */
  readonly hasError: boolean;
  /** Selected encoded value; omitted for native private facets. */
  readonly value?: ClosedEncodedValue;
  /** Selected encoded projected failure. */
  readonly error?: ClosedEncodedFailure;
}
/** One authenticated source's transport descriptor; local native authority is never serialized. */
export interface RequiredTransferStore {
  /** Source slot matched to a genuine receiver claim. */
  readonly slot: TransferSlot;
  /** Graph index in the image's namespace sequence. */
  readonly graph: number;
  /** Authored logical address. */
  readonly key: string;
  /** Raw and bound sources remain distinct. */
  readonly bound: boolean;
  /** Genuine native source role. */
  readonly kind: "query" | "stream" | "store";
  /** Whether this image contains an eligible committed base. */
  readonly hasValue: boolean;
  /** Selected encoding of that base, when present. */
  readonly value?: ClosedEncodedValue;
  /** Native ordering at the source cut. */
  readonly generation: number;
  /** Elapsed landing age; absent when no accepted landing exists. */
  readonly age?: number;
}
/** Native normalized freshness, including the omitted-options infinite default. */
export type TransferCallerWindow =
  | { /** Native infinite default when caller and declaration omit a window. */ readonly kind:"infinite" }
  | { /** Finite native freshness window. */ readonly kind:"finite"; /** Actual normalized milliseconds, including fractional values. */ readonly milliseconds:number };
/** One genuine query caller; its physical base and operation coverage remain shared. */
export interface RequiredTransferCaller {
  /** Slot mapped to this exact caller's passive claim. */
  readonly slot:TransferSlot;
  /** Shared physical Store slot. */
  readonly store:TransferSlot;
  /** Actual normalized native freshness semantics, never a callback or policy token. */
  readonly window:TransferCallerWindow;
  /** Actual settled caller metadata at the source cut. */
  readonly status:import("./contract/types.ts").StoreStatus;
}
/** Closed immutable native cached image; contains no callbacks, native handles, or policy objects. */
export interface EncodedRequiredTransfer {
  /** Native required-transfer format identifier. */
  readonly format: "aeolia-required-transfer";
  /** Format version. */
  readonly version: 1 | 2;
  /** Native contract namespaces in caller-supplied graph order. */
  readonly graphs: readonly string[];
  /** Exact requested roots. */
  readonly roots: readonly TransferSlot[];
  /** Cached reactive nodes. */
  readonly nodes: readonly RequiredTransferNode[];
  /** Native source descriptors and optional selected committed bases. */
  readonly stores: readonly RequiredTransferStore[];
  /** Distinct native callers and their exact physical source relationships. */
  readonly callers:readonly RequiredTransferCaller[];
  /** Genuine positioned groups; present only in version two. */
  readonly positioned?: readonly {
    /** Transport-local group matched to one actual reader restoration. */
    readonly slot: TransferSlot;
    /** Actual physical source descriptor. */
    readonly store: TransferSlot;
    /** One aggregate boundary/base/fold seed in the selected codec arena. */
    readonly seed: ClosedEncodedValue;
    /** Number of actual required native fold participants. */
    readonly projections: number;
  }[];
}
/** Required cached capture arguments. */
export interface RequiredTransferCaptureOptions {
  /** Exact native Store checkpoints supplied by the real required source closure. */
  readonly positioned?: readonly PositionedStoreCheckpoint<any, readonly unknown[]>[];
  /** Actual graphs owning every reached native source. */
  readonly graphs: readonly Graph[];
  /** Authentic required roots and their original native construction associations. */
  readonly roots: readonly { readonly source: Readable<unknown>; readonly construction: ReactiveConstruction<unknown> }[];
  /** Actual selected response codec session. */
  readonly codec: RequiredTransferCodec;
}
/** Required receiver mapping and prospective live State writes. */
export interface RequiredTransferAdoptionOptions {
  /** Exact native positioned claims and same-arena decoded seeds for group slots. */
  readonly positioned?: readonly { readonly slot: TransferSlot; readonly claim: PassiveTransferClaim; readonly seed: PositionedStoreRestoreSeed<any, any, readonly unknown[]> }[];
  /** Actual receiver graphs, in matching namespace order. */
  readonly graphs: readonly Graph[];
  /** Genuine passive claims for source slots. */
  readonly sources: readonly { readonly slot: TransferSlot; readonly claim: PassiveTransferClaim }[];
  /** Actual reconstructed targets and their native construction associations. */
  readonly reactive: readonly { readonly slot: TransferSlot; readonly target: Readable<unknown>; readonly construction: ReactiveConstruction<unknown> }[];
  /** Actual selected response codec session. */
  readonly codec: RequiredTransferCodec;
  /** Existing native State writes published with the cache adoption. */
  readonly mappedStateWrites: readonly { readonly target: WritableSignal<unknown>; readonly construction: ReactiveConstruction<unknown>; readonly value: unknown }[];
}

const requiredConstructions = new WeakSet<object>();
type NativePassiveClaim = ReturnType<typeof graphInternal.prepareNativePassiveTransferClaim>;
const requiredClaims = new WeakMap<object, NativePassiveClaim>();

/**
 * Retains the actual installed native constructor association without evaluating the source.
 * @param source - Genuine State or Computed returned by its constructor.
 * @returns Stable opaque native association with safe own source/kind metadata.
 * @throws TypeError for fabricated or unsupported readables.
 */
export function captureReactiveConstruction<T>(source: WritableSignal<T> | Computed<T>): ReactiveConstruction<T> {
  const construction = reactiveInternal.transferConstruction(source);
  requiredConstructions.add(construction);
  return construction;
}
function requiredConstruction(source: Readable<unknown>, construction: ReactiveConstruction<unknown>): void {
  if (!requiredConstructions.has(construction) || reactiveInternal.transferConstruction(source) !== construction)
    throw new TypeError("Required transfer needs the actual native construction association");
}
function closedRequiredCopy<T>(input: T): T {
  const copies = new WeakMap<object, object>();
  const pending: { readonly input: object; readonly output: object }[] = [];
  const copy = (value: unknown): unknown => {
    if (value === null || value === undefined || typeof value === "string" || typeof value === "boolean") return value;
    if (typeof value === "number" && Number.isFinite(value)) return value;
    if (typeof value !== "object") throw new TypeError("Required transfer codec must return closed encoded data");
    const prior = copies.get(value); if (prior !== undefined) return prior;
    const array = Array.isArray(value);
    const prototype = Object.getPrototypeOf(value);
    if (!array && prototype !== null && prototype !== Object.prototype) throw new TypeError("Native handles and class instances cannot enter required transfer encoding");
    const output = array ? new Array(value.length) : Object.create(null);
    copies.set(value,output); pending.push({input:value,output});
    return output;
  };
  const result=copy(input);
  for(let index=0;index<pending.length;index+=1){
    const row=pending[index]!;
    for(const key of Reflect.ownKeys(row.input)){
      if(Array.isArray(row.input)&&key==="length")continue;
      if(typeof key!=="string")throw new TypeError("Encoded data cannot contain symbol fields");
      const descriptor=Object.getOwnPropertyDescriptor(row.input,key);
      if(descriptor===undefined||!("value"in descriptor))throw new TypeError("Encoded data cannot contain accessors");
      Object.defineProperty(row.output,key,{value:copy(descriptor.value),enumerable:true,writable:false,configurable:false});
    }
    Object.freeze(row.output);
  }
  return result as T;
}
function requiredGraphStates(graphs: readonly Graph[]) {
  const seen = new Set<Graph>();
  return graphs.map((graph) => {
    const state = transferGraph(graph);
    if (state === undefined || state.disposed || seen.has(graph)) throw new TypeError("Required transfer needs distinct current native graphs");
    seen.add(graph); return state;
  });
}

/**
 * Captures the actual cached closure without evaluating roots or excluded native bases.
 * @param options - Actual graphs, original construction receipts, roots and selected codec.
 * @returns A pending image publication and genuine local source witnesses.
 * @throws TypeError for dirty/uninitialized caches, unsupported facets, or ineligible native sources.
 */
export function prepareRequiredTransferCapture(options: RequiredTransferCaptureOptions): TransferPublication & { readonly image: EncodedRequiredTransfer; readonly sources: readonly TransferSourceWitness[] } {
  const graphHandles=Array.from(options.graphs);
  const positionedInputs=Array.from(options.positioned??[]);
  const roots=Array.from(options.roots,(root)=>({source:root.source,construction:root.construction}));
  const codec=options.codec;const encodeValue=codec.encodeValue.bind(codec);const encodeFailure=codec.encodeFailure.bind(codec);
  const graphStates = requiredGraphStates(graphHandles);
  for (const root of roots) requiredConstruction(root.source,root.construction);
  const cached = reactiveInternal.transferCapture(roots.map((root) => root.source));
  if (!cached.current()) throw new TypeError("Required cached closure is not current");
  const guards: (() => boolean)[] = graphStates.map((state) => transferEpochCurrent(state));
  const nodeSlots = new Map<Readable<unknown>,string>();
  const sourceSlots = new Map<StoreRuntime<any>,string>();
  const witnesses: TransferSourceWitness[] = [];
  const stores: RequiredTransferStore[] = [];
  const callers:RequiredTransferCaller[]=[];
  const callerSlots=new Map<object,string>();
  const nodes: RequiredTransferNode[] = [];
  const now = Date.now();
  const positionedRecords: {slot:string;capture:TransferPositionedCheckpoint}[]=[];
  const positionedByRuntime=new Map<StoreRuntime<any>,{slot:string;capture:TransferPositionedCheckpoint}>();
  for(const checkpoint of positionedInputs){const capture=transferPositionedCheckpoint(checkpoint);if(positionedByRuntime.has(capture.runtime))throw new TypeError("Required positioned session must be enrolled once");const record={slot:"p"+positionedRecords.length,capture};positionedRecords.push(record);positionedByRuntime.set(capture.runtime,record);}
  for (let index=0;index<cached.nodes.length;index+=1) nodeSlots.set(cached.nodes[index]!.source,"n"+index);
  const sourceSlot = (runtime: StoreRuntime<any>): string => {
    let slot = sourceSlots.get(runtime); if (slot !== undefined) return slot;
    const graph = graphHandles.indexOf(runtime.graph);
    if (graph < 0 || !transferStoreAdmissible(runtime) || (runtime.baseAvailable && !runtime.transferEligible && !runtime.failing)) throw new TypeError("Required native source is ineligible");
    slot = "s"+sourceSlots.size; sourceSlots.set(runtime,slot);
    let sourceStore:Store<unknown>=positionedByRuntime.get(runtime)?.capture.source??runtime.store;
    for(let index=runtime.callers.length-1;index>=0;index-=1){const actual=transferCaller(runtime.callers[index]!);if(actual!==undefined){sourceStore=actual.store;break;}}
    const witness: TransferSourceWitness = Object.freeze({slot,readable:runtime.value,store:sourceStore});
    witnesses.push(witness); guards.push(transferStoreCurrent(runtime,cached.nodes.some((node)=>node.hasError)));
    if(runtime.readerScope!==undefined&&!positionedByRuntime.has(runtime)&&runtime.snapshot&&runtime.baseAvailable&&runtime.transferEligible)throw new Fault("encoding",[String(runtime.key)]);
    const kind = runtime.binding?.kind ?? (runtime.streamSource !== undefined ? "stream" : runtime.source !== undefined ? "query" : "store");
    const hasValue = !positionedByRuntime.has(runtime) && runtime.snapshot && runtime.baseAvailable && transferStoreEligible(runtime);
    stores.push({slot,graph,key:String(runtime.key),bound:runtime.binding!==undefined,kind,hasValue,
      ...(hasValue ? {value:encodeValue(reactiveInternal.signalValue(runtime.committed),witness)} : {}),
      generation:runtime.generation,...(runtime.lastLandingAt===undefined?{}:{age:Math.max(0,now-runtime.lastLandingAt)})});
    return slot;
  };
  const callerSlot=(readable:Readable<unknown>):string|undefined=>{
    const record=transferReadableCaller(readable);if(record===undefined)return undefined;
    transferStore(record.store);
    let slot=callerSlots.get(record.caller);if(slot!==undefined)return slot;
    const physical=sourceSlot(record.runtime);
    slot="c"+callerSlots.size;callerSlots.set(record.caller,slot);
    const window=record.caller.windowMs;
    if(window!==Number.POSITIVE_INFINITY&&(!Number.isFinite(window)||window<0))throw new TypeError("Native caller has an invalid normalized freshness window");
    const status=reactiveInternal.signalValue(record.caller.statusCell);
    if(status==="fetching"||status==="revalidating")throw new TypeError("Required native caller has unresolved request metadata");
    callers.push({slot,store:physical,window:window===Number.POSITIVE_INFINITY?{kind:"infinite"}:{kind:"finite",milliseconds:window},status});
    witnesses.push(Object.freeze({slot,physical,readable:record.caller.status,store:record.store}));
    guards.push(transferEpochCurrent(record.runtime));
    return slot;
  };
  for (const node of cached.nodes) {
    const slot=nodeSlots.get(node.source)!;
    const runtime=transferReadableStore(node.source);
    const dependencies=node.sources.map((source)=>nodeSlots.get(source)!);
    if (runtime !== undefined) {
      const facet=transferReadableRole(node.source);
      if(facet?.startsWith("observation-")&&getObservationTransferFacet(runtime,facet)!==node.source)throw new TypeError("Required observation metadata is not owned by the current native publisher");
      if (facet===undefined) throw new TypeError("Required native facet has no authenticated receiver role");
      const caller=callerSlot(node.source);
      const group=positionedByRuntime.get(runtime);
      nodes.push({slot,kind:"native",store:sourceSlot(runtime),...(group===undefined?{}:{positioned:group.slot}),...(caller===undefined?{}:{caller}),facet,sources:dependencies,hasValue:false,hasError:false});
      continue;
    }
    let authority: RequiredTransferNode | undefined;
    for(let index=0;index<graphStates.length;index+=1) if(node.source===graphStates[index]!.resetVersion)
      authority={slot,kind:"authority",graph:index,facet:"graph-reset",sources:[],hasValue:false,hasError:false};
    const nativeAuthority=transferAuthority(node.source);
    if(authority===undefined&&nativeAuthority!==undefined){
      if(!graphHandles.includes(nativeAuthority.graph))throw new TypeError("Required context authority belongs to an unlisted graph");
      const owned=cached.nodes.find((candidate)=>transferReadableStore(candidate.source)?.binding?.context===nativeAuthority.owner);
      if(owned===undefined)throw new TypeError("Required context authority lacks a corresponding acquired native source");
      authority={slot,kind:"authority",store:sourceSlot(transferReadableStore(owned.source)!),facet:nativeAuthority.kind,sources:dependencies,hasValue:false,hasError:false};
    }
    if(authority!==undefined){nodes.push(authority);continue;}
    if(transferProjection(node.source)!==undefined){
      let found=false;
      for(const record of positionedRecords){const facet=record.capture.facet(node.source as object);if(facet===undefined)continue;nodes.push({slot,kind:"native",store:sourceSlot(record.capture.runtime),positioned:record.slot,projection:facet.index,facet:facet.role,sources:dependencies,hasValue:false,hasError:false});found=true;break;}
      if(!found)throw new TypeError("Required projection needs its enrolled authentic positioned group");
      continue;
    }
    if(!node.hasRun || (!node.hasValue && !node.hasError)) throw new TypeError("Required cache is uninitialized");
    const witness:TransferSourceWitness=Object.freeze({slot,readable:node.source}); witnesses.push(witness);
    nodes.push({slot,kind:node.kind,sources:dependencies,hasValue:node.hasValue,hasError:node.hasError,
      ...(node.hasValue?{value:encodeValue(node.value,witness)}:{}),
      ...(node.hasError?{error:encodeFailure(node.error,witness)}:{})});
  }
  const positioned=positionedRecords.map((record)=>{const store=sourceSlot(record.capture.runtime);const witness:TransferSourceWitness=Object.freeze({slot:record.slot,readable:record.capture.runtime.value,store:record.capture.source});witnesses.push(witness);return {slot:record.slot,store,seed:encodeValue(record.capture.seed,witness),projections:record.capture.projections};});
  const image=closedRequiredCopy<EncodedRequiredTransfer>({format:"aeolia-required-transfer",version:positioned.length?2:1,graphs:graphStates.map((state)=>state.namespace),roots:roots.map((root)=>nodeSlots.get(root.source)!),nodes,stores,callers,...(positioned.length?{positioned}:{} )});
  const current=():boolean=>{
    if(!cached.current())return false;
    const positionedNow=readerLifetimeNow();
    for(let index=0;index<positionedRecords.length;index++)if(!positionedRecords[index]!.capture.currentAt(positionedNow))return false;
    for(let index=0;index<graphHandles.length;index+=1)if(graphStates[index]!.disposed||transferHasAdmission(graphHandles[index]!))return false;
    for(let index=0;index<guards.length;index+=1)if(!guards[index]!())return false;
    return true;
  };
  if(!current())throw new TypeError("Required capture changed during codec preparation");
  return Object.freeze({...prepareTransferPublication(current,()=>{}),image,sources:Object.freeze(witnesses)});
}

/**
 * Prepares a genuine detached source without invoking its public acquisition member.
 * @param graph - Actual receiving native graph.
 * @param source - Genuine raw member or native binding.
 * @param input - Already-prepared native input, used once for native address derivation.
 * @param options - Existing native value policy.
 * @param positioned - Admitted reader, exact decoded seed, and actual target fold policies for a positioned source.
 * @returns Opaque pending claim with safe metadata and publication-gated body access.
 * @throws TypeError for unsupported, fabricated, crossed or stale source preparation.
 */
export function preparePassiveTransferClaim<Input, Event, Position, Reader, const Policies extends readonly ProjectionPolicy<NoInfer<Event>, any>[]>(
  graph: Graph, source: PositionedStreamAccess<Input, Event, Position, Reader>, input: Input,
  options: ValueOptions<NoInfer<Event>> | undefined,
  positioned: {
    /** Actual target policies in the required native tuple order. */ readonly policies: Policies;
    /** Same decoded object authenticated by the admitted provider reader. */ readonly seed: PositionedStoreRestoreSeed<NoInfer<Event>, NoInfer<Position>, NoInfer<PositionedStorePolicyValues<Event, Policies>>>;
    /** Genuine already-admitted reader for that exact seed. */ readonly reader: NoInfer<Reader>;
  },
): PassiveTransferClaim<PositionedStreamAccess<Input, Event, Position, Reader>> & {
  /** Actual reader-owned Store shared by the returned native folds. */ readonly store: StreamStore<Event>;
  /** Genuine one-shot group; its retired completion is physical resource cleanup. */ readonly restoration: PositionedStoreRestoration<Event, PositionedStorePolicyValues<Event, Policies>>;
};
export function preparePassiveTransferClaim<Source extends NativeTransferSource>(graph:Graph,source:Source,input:unknown,options?:ExistingNativeValueOptions<Source>):PassiveTransferClaim<Source>;
export function preparePassiveTransferClaim<Source extends NativeTransferSource>(graph:Graph,source:Source,input:unknown,options?:ExistingNativeValueOptions<Source>,positioned?:{readonly policies:readonly ProjectionPolicy<any,any>[];readonly seed:PositionedStoreRestoreSeed<any,any,readonly unknown[]>;readonly reader:unknown}):PassiveTransferClaim<Source>{
  const native=graphInternal.prepareNativePassiveTransferClaim(graph,source,input,options as import("./contract/types.ts").StoreOptions<unknown>|undefined,positioned);
  try {
  let observation:PreparedPassiveObservation<NativeTransferValue<Source>>|undefined;
  const prepareObservation=(existingLease?:import("./contract/observation.ts").ObservationLease<NativeTransferValue<Source>>):PreparedPassiveObservation<NativeTransferValue<Source>>=>{
    const participant=native.prepareObservation(existingLease as import("./contract/observation.ts").ObservationLease<unknown>|undefined);
    if(observation===undefined)observation=Object.freeze({lease:participant.lease as import("./contract/observation.ts").ObservationLease<NativeTransferValue<Source>>,retired:native.retired,discard:native.discard});
    return observation;
  };
  const claim=Object.freeze({store:native.store,discard:native.discard,retired:native.retired,...(native.restoration===undefined?{}:{restoration:native.restoration}),...(native.kind==="query"?{prepareObservation}:{})}) as PassiveTransferClaim<Source>;
  requiredClaims.set(claim,native);
  native.positioned?.issue();
  return claim;
  } catch (cause) {
    try { native.discard(); } catch (cleanupCause) { graphInternal.reportContinuationError(graph, cleanupCause); }
    throw cause;
  }
}

function requiredNativeFacet(native:NativePassiveClaim,facet:string):Readable<unknown>{
  const runtime=native.runtime;
  if(native.positioned?.session.bound!==undefined){
    const store=native.store as StreamStore<unknown>;
    if(facet==="stream-value")return store.value;
    if(facet==="stream-available")return store.available;
    if(facet==="stream-status")return store.status;
    if(facet==="stream-pending")return store.pending;
    if(facet==="stream-error")return store.error;
    if(facet==="stream-lifecycle")return native.positioned.session.bound.lifecycle;
  }
  if(facet.startsWith("passive-")){
    switch(facet.slice(8)){
      case "value":case "stream-value":return native.store.value;
      case "available":case "stream-available":return native.store.available;
      case "error":case "stream-error":return(native.store as QueryStore<unknown,unknown>).error;
      case "query-status":return(native.store as QueryStore<unknown,unknown>).status;
      case "query-pending":return(native.store as QueryStore<unknown,unknown>).pending;
      case "query-progress":return(native.store as QueryStore<unknown,unknown>).progress;
      case "stream-status":return(native.store as StreamStore<unknown>).status;
      case "stream-pending":return(native.store as StreamStore<unknown>).pending;
    }
    throw new TypeError("Unknown genuine passive native facet");
  }
  switch(facet){
    case "committed":return runtime.committed;
    case "predictions":return runtime.predictionStack;
    case "available":case "stream-available":return runtime.store.available;
    case "value":case "stream-value":return runtime.value;
    case "lifecycle":case "stream-lifecycle":return runtime.lifecycle;
    case "error":case "stream-error":return runtime.error;
    case "error-cell":return runtime.errorCell;
    case "stream-status-cell":return runtime.streamStatusCell;
    case "stream-status":return runtime.streamStatus;
    case "query-status":if(native.caller!==undefined)return native.caller.status;break;
    case "query-status-cell":if(native.caller!==undefined)return native.caller.statusCell;break;
    case "query-pending":if(native.caller!==undefined)return native.caller.pending;break;
    case "query-progress":if(native.progressCell!==undefined)return native.progressCell;break;
    case "stream-pending":return(native.store as StreamStore<unknown>).pending;
    case "observation-issue":case "observation-activity":case "observation-issue-cell":case "observation-live":case "observation-activity-cell":{
      const readable=native.observation()?.getFacet(facet)??getObservationTransferFacet(runtime,facet);if(readable!==undefined)return readable;break;
    }
  }
  throw new TypeError("Required native facet has no genuine passive counterpart");
}

/**
 * Prepares all required receiver caches, eligible bases and mapped State writes together.
 * @param input - Closed required-transfer image from the selected program/response.
 * @param options - Genuine receiver graphs, source claims, constructions and selected codec.
 * @returns One all-or-nothing native publication with deferred notifications.
 * @throws TypeError for malformed/crossed/incomplete mappings or stale native preparation.
 */
export function prepareRequiredTransferAdoption(input:unknown,options:RequiredTransferAdoptionOptions):TransferPublication{
  const graphHandles=Array.from(options.graphs);
  const sourceEntries=Array.from(options.sources,(entry)=>({slot:entry.slot,claim:entry.claim}));
  const positionedEntries=Array.from(options.positioned??[],(entry)=>({slot:entry.slot,claim:entry.claim,seed:entry.seed}));
  const reactiveEntries=Array.from(options.reactive,(entry)=>({slot:entry.slot,target:entry.target,construction:entry.construction}));
  const mappedWrites=Array.from(options.mappedStateWrites,(entry)=>({target:entry.target,construction:entry.construction,value:entry.value}));
  const codec=options.codec;const decodeValue=codec.decodeValue.bind(codec);const decodeFailure=codec.decodeFailure.bind(codec);
  const image=closedRequiredCopy(input) as EncodedRequiredTransfer;
  if(image===null||typeof image!=="object"||image.format!=="aeolia-required-transfer"||(image.version!==1&&image.version!==2)||!Array.isArray(image.graphs)||!Array.isArray(image.nodes)||!Array.isArray(image.stores)||!Array.isArray(image.callers)||!Array.isArray(image.roots))throw new TypeError("Invalid required-transfer image");
  if((image.version===1&&image.positioned!==undefined)||(image.version===2&&(!Array.isArray(image.positioned)||image.positioned.length===0)))throw new TypeError("Invalid positioned image version");
  const states=requiredGraphStates(graphHandles);
  if(image.graphs.length!==states.length||image.graphs.some((namespace,index)=>namespace!==states[index]!.namespace))throw new TypeError("Required-transfer graph namespaces differ");
  const guards:(()=>boolean)[]=states.map((state)=>transferEpochCurrent(state));
  const claimsBySlot=new Map<string,NativePassiveClaim>();
  const nativeBySlot=new Map<string,NativePassiveClaim>();
  const callerBySlot=new Map<string,NativePassiveClaim>();
  const callerRows=new Map<string,RequiredTransferCaller>();
  const natives:NativePassiveClaim[]=[];
  const uniqueNatives=new Set<NativePassiveClaim>();
  for(const source of sourceEntries){
    const native=requiredClaims.get(source.claim);
    if(native===undefined||claimsBySlot.has(source.slot)||!native.current())throw new TypeError("Required transfer needs distinct current genuine passive claims");
    claimsBySlot.set(source.slot,native);if(!uniqueNatives.has(native)){uniqueNatives.add(native);natives.push(native);}
  }
  if(claimsBySlot.size!==image.stores.length+image.callers.length)throw new TypeError("Required source mapping is incomplete");
  const storeRows=new Map<string,RequiredTransferStore>();
  for(const row of image.stores){
    const native=claimsBySlot.get(row.slot);
    if(native===undefined||storeRows.has(row.slot)||!Number.isInteger(row.graph)||row.graph<0||row.graph>=states.length||native.runtime.graph!==graphHandles[row.graph]||String(native.runtime.key)!==row.key||(native.runtime.binding!==undefined)!==row.bound||!Number.isSafeInteger(row.generation)||row.generation<0||typeof row.hasValue!=="boolean"||(row.age!==undefined&&(!Number.isFinite(row.age)||row.age<0)))throw new TypeError("Required source descriptor does not match its native claim");
    const kind=native.kind;
    if(kind!==row.kind)throw new TypeError("Required source role differs");
    storeRows.set(row.slot,row);nativeBySlot.set(row.slot,native);
  }
  const usedCallers=new Set<object>();
  for(const row of image.callers){
    const native=claimsBySlot.get(row.slot);const physical=nativeBySlot.get(row.store);
    if(typeof row.slot!=="string"||storeRows.has(row.slot)||callerRows.has(row.slot)||native?.caller===undefined||physical===undefined||native.runtime!==physical.runtime||usedCallers.has(native.caller))throw new TypeError("Required caller mapping is incomplete, crossed, or collapsed");
    const window=row.window;
    if(window===null||typeof window!=="object"||(window.kind!=="finite"&&window.kind!=="infinite")||(window.kind==="finite"&&(!Number.isFinite(window.milliseconds)||window.milliseconds<0)))throw new TypeError("Invalid normalized caller freshness descriptor");
    const expectedWindow=window.kind==="infinite"?Number.POSITIVE_INFINITY:window.milliseconds;
    if(native.caller.windowMs!==expectedWindow)throw new TypeError("Required receiver caller freshness differs");
    if(row.status!=="empty"&&row.status!=="ready"&&row.status!=="stale"&&row.status!=="failed")throw new TypeError("Required caller metadata is not settled");
    usedCallers.add(native.caller);callerBySlot.set(row.slot,native);callerRows.set(row.slot,row);
  }
  const positionedBySlot=new Map<string,NativePassiveClaim>();
  const positionedSeedByNative=new Map<NativePassiveClaim,PositionedStoreRestoreSeed<any,any,readonly unknown[]>>();
  const positionedRows=image.positioned??[];
  const groupInputs=new Map(positionedEntries.map((entry)=>[entry.slot,entry]));
  if(groupInputs.size!==positionedEntries.length||groupInputs.size!==positionedRows.length)throw new TypeError("Required positioned group mapping is incomplete");
  for(const row of positionedRows){
    const entry=groupInputs.get(row.slot);
    const native=entry===undefined?undefined:requiredClaims.get(entry.claim);
    const physical=nativeBySlot.get(row.store);
    if(typeof row.slot!=="string"||storeRows.has(row.slot)||callerRows.has(row.slot)||positionedBySlot.has(row.slot)||
       native===undefined||native!==physical||native.positioned===undefined||positionedSeedByNative.has(native)||
       !Number.isSafeInteger(row.projections)||row.projections<0||native.positioned.projections.length!==row.projections||
       storeRows.get(row.store)?.hasValue!==false)throw new TypeError("Positioned source, reader or projection tuple mapping differs");
    const witness:TransferTargetWitness=Object.freeze({slot:row.slot,readable:native.store.value,store:native.store});
    const seed=decodeValue(row.seed,witness);
    if(!Object.is(seed,entry!.seed)||!Object.is(seed,native.positioned.seed))throw new TypeError("Positioned reader did not receive the same decoded seed object");
    const data=seed as {base?:{present?:boolean;value?:unknown}};
    if(data.base?.present===true&&!native.policy.snapshot)throw new TypeError("Positioned seed cannot import an excluded source base");
    positionedBySlot.set(row.slot,native);positionedSeedByNative.set(native,entry!.seed);
  }
  for(const native of natives)if(native.positioned!==undefined&&!positionedSeedByNative.has(native))throw new TypeError("Positioned claim lacks its exact required group row");
  const observations:NonNullable<ReturnType<NativePassiveClaim["observation"]>>[]=[];
  const observationByRuntime=new Map<StoreRuntime<unknown>,NonNullable<ReturnType<NativePassiveClaim["observation"]>>>();
  for(const native of natives){
    const participant=native.observation();if(participant===undefined)continue;
    const previous=observationByRuntime.get(native.runtime);
    if(previous!==undefined&&previous.state!==participant.state)throw new TypeError("Competing native observation states");
    if(!participant.current()||participant.state.incarnation!==native.incarnation)throw new TypeError("Required observation participant is stale");
    if(previous===undefined){observationByRuntime.set(native.runtime,participant);observations.push(participant);}
  }
  const explicit=new Map<string,RequiredTransferAdoptionOptions["reactive"][number]>();
  for(const entry of reactiveEntries){
    requiredConstruction(entry.target,entry.construction);
    if(explicit.has(entry.slot))throw new TypeError("Repeated required reactive slot");
    explicit.set(entry.slot,entry);
  }
  const targets=new Map<string,Readable<unknown>>();
  const nodeRows=new Map<string,RequiredTransferNode>();
  for(const row of image.nodes){
    if(typeof row.slot!=="string"||nodeRows.has(row.slot)||storeRows.has(row.slot)||callerRows.has(row.slot)||positionedBySlot.has(row.slot)||!Array.isArray(row.sources)||typeof row.hasValue!=="boolean"||typeof row.hasError!=="boolean")throw new TypeError("Malformed required cached node");
    nodeRows.set(row.slot,row);
    if((row.kind==="native"||row.kind==="authority")&&(row.hasValue||row.hasError||Object.hasOwn(row,"value")||Object.hasOwn(row,"error")))throw new TypeError("Native private facets cannot carry authored State payloads");
    if(row.kind==="native"){
      const native=row.caller===undefined?(row.store===undefined?undefined:nativeBySlot.get(row.store)):callerBySlot.get(row.caller);
      if(row.caller!==undefined&&(callerRows.get(row.caller)?.store!==row.store||native?.runtime!==nativeBySlot.get(row.store!)?.runtime))throw new TypeError("Native facet belongs to a crossed caller");
      if(native===undefined||typeof row.facet!=="string")throw new TypeError("Native cached node lacks its genuine source claim");
      const observation=observationByRuntime.get(native.runtime);
      const group=row.positioned===undefined?undefined:positionedBySlot.get(row.positioned);
      if(row.positioned!==undefined&&(group!==native||group.positioned===undefined))throw new TypeError("Native facet crossed its actual positioned group");
      if(row.projection!==undefined){
        if(group?.positioned===undefined||!Number.isSafeInteger(row.projection)||row.projection<0)throw new TypeError("Missing actual positioned fold ordinal");
        const facet=group.positioned.facet(row.projection,row.facet);if(facet===undefined)throw new TypeError("Positioned fold facet has no genuine native counterpart");targets.set(row.slot,facet);
      }else targets.set(row.slot,row.facet.startsWith("observation-")?(observation?.getFacet(row.facet)??requiredNativeFacet(native,row.facet)):requiredNativeFacet(native,row.facet));
      if((row.facet==="observation-issue"||row.facet==="observation-issue-cell")&&reactiveInternal.signalValue(observation?.state.issue??native.runtime.observation!.issue)!==undefined)throw new TypeError("Required observation issue has no compatible closed payload");
    }else if(row.kind==="authority"){
      if(row.facet==="graph-reset"&&row.graph!==undefined&&states[row.graph]!==undefined)targets.set(row.slot,states[row.graph]!.resetVersion);
      else if((row.facet==="context-active"||row.facet==="context-active-cell")&&row.store!==undefined){const runtime=nativeBySlot.get(row.store)?.runtime;if(runtime?.binding===undefined)throw new TypeError("Missing genuine receiver context");targets.set(row.slot,row.facet==="context-active-cell"?runtime.binding.context.activeCell:runtime.binding.context.handle.active);}
      else throw new TypeError("Unknown required native authority");
    }else if(row.kind==="state"||row.kind==="computed"){
      if(row.positioned!==undefined||row.projection!==undefined)throw new TypeError("Authored State cannot impersonate a positioned native facet");
      const entry=explicit.get(row.slot);
      if(entry===undefined||entry.construction.kind!==row.kind)throw new TypeError("Required construction mapping is incomplete or crossed");
      if(transferReadableStore(entry.target)!==undefined||transferProjection(entry.target)!==undefined)throw new TypeError("Private native State cannot be mapped as authored reactive state");
      targets.set(row.slot,entry.target);
    }else throw new TypeError("Unknown required cached role");
  }
  for(const slot of explicit.keys())if(!nodeRows.has(slot)||nodeRows.get(slot)!.kind==="native"||nodeRows.get(slot)!.kind==="authority")throw new TypeError("Unexpected reactive target mapping");
  for(const row of image.nodes)for(const slot of row.sources)if(!targets.has(slot))throw new TypeError("Required dependency mapping is incomplete");
  for(const root of image.roots)if(!targets.has(root))throw new TypeError("Required root mapping is incomplete");
  const writes: {target:WritableSignal<unknown>;value:unknown;equals?:(previous:unknown,proposed:unknown)=>boolean}[]=[];
  const nativeComparators=new Map(natives.filter((native)=>native.compare!==undefined).map((native)=>[native.runtime.committed,native.compare!]));
  const writeValues=new Map<WritableSignal<unknown>,unknown>();
  const stageWrite=(target:WritableSignal<unknown>,value:unknown):void=>{
    if(writeValues.has(target)){if(!Object.is(writeValues.get(target),value))throw new TypeError("Conflicting required State writes");return;}
    writeValues.set(target,value);const equals=nativeComparators.get(target);writes.push({target,value,...(equals===undefined?{}:{equals})});
  };
  for(const participant of observations)for(const write of participant.writes)stageWrite(write.target,write.value);
  const metadata: {runtime:StoreRuntime<any>;value:unknown;generation:number;revision:ValueRevision;incarnation:object;landingAt:number|undefined}[]=[];
  const now=Date.now();
  for(const row of image.stores){
    const native=nativeBySlot.get(row.slot)!;const runtime=native.runtime;
    if(!transferStoreAdmissible(runtime))throw new TypeError("Required receiver address is ineligible");
    if(native.positioned!==undefined||!row.hasValue||!native.policy.snapshot||(native.existing&&runtime.baseAvailable))continue;
    const witness:TransferTargetWitness=Object.freeze({slot:row.slot,readable:native.store.value,store:native.store});
    const value=decodeValue(row.value,witness);
    if(runtime.generation>=Number.MAX_SAFE_INTEGER||row.generation>=Number.MAX_SAFE_INTEGER||runtime.valueRevision>=Number.MAX_SAFE_INTEGER)throw new RangeError("Required native ordering exhausted");
    const generation=Math.max(runtime.generation,row.generation)+1;
    const revision=(runtime.valueRevision+1)as ValueRevision;
    const incarnation=native.incarnation;
    const landingAt=row.age===undefined?undefined:now-row.age;
    metadata.push({runtime,value,generation,revision,incarnation,landingAt});
    stageWrite(runtime.committed,value);stageWrite(runtime.presence,true);stageWrite(runtime.errorCell,undefined);
    const callers=[...runtime.callers,...natives.flatMap((candidate)=>candidate.runtime===runtime&&candidate.caller!==undefined?[candidate.caller]:[])];
    for(const caller of callers)stageWrite(caller.statusCell,landingAt===undefined?"empty":now-landingAt<caller.windowMs?"ready":"stale");
  }
  for(const [slot,native]of callerBySlot){
    if(!metadata.some((landing)=>landing.runtime===native.runtime))continue;
    const caller=native.caller!;const status=callerRows.get(slot)!.status;
    if(status==="ready"||status==="stale"){
      const write=writes.find((candidate)=>candidate.target===caller.statusCell);if(write!==undefined){write.value=status;writeValues.set(caller.statusCell,status);}else stageWrite(caller.statusCell,status);
    }
  }
  let nativePublished=false;
  const priorSemanticGuards=natives.map((native)=>transferStoreSemanticCurrent(native.runtime));
  const semanticGuards=natives.map((native)=>{const landing=metadata.find((row)=>row.runtime===native.runtime);return transferStoreSemanticCurrent(native.runtime,landing===undefined?{generation:native.runtime.generation,revision:native.runtime.valueRevision,incarnation:native.incarnation,base:native.runtime.baseAvailable,failing:native.runtime.failing,invalidated:native.runtime.invalidated,committed:reactiveInternal.signalValue(native.runtime.committed),error:reactiveInternal.signalValue(native.runtime.errorCell)}:{generation:landing.generation,revision:landing.revision,incarnation:landing.incarnation,base:true,failing:false,invalidated:false,committed:landing.value,error:undefined});});
  const nativeIndex=new Map(natives.map((native,index)=>[native,index]));
  const semanticCells:{cell:WritableSignal<unknown>;before:unknown;after:unknown}[]=[];
  function cacheSemanticCurrent(slot:string):()=>boolean{
    const guards=new Set<number>();const cells=new Map<Readable<unknown>,{cell:WritableSignal<unknown>;before:unknown;after:unknown}>();
    const seen=new Set<string>();const pending=[...nodeRows.get(slot)!.sources];
    for(let index=0;index<pending.length;index+=1){const key=pending[index]!;if(seen.has(key))continue;seen.add(key);const row=nodeRows.get(key)!;
      if(row.store!==undefined){const native=nativeBySlot.get(row.store);if(native!==undefined)guards.add(nativeIndex.get(native)!);}
      if(row.kind==="native"||row.kind==="authority"){const target=targets.get(key)!;if(reactiveInternal.transferConstruction(target).kind==="state"&&!cells.has(target)){
        const cell=target as WritableSignal<unknown>;const value=reactiveInternal.signalValue(cell);const record={cell,before:value,after:value};cells.set(target,record);semanticCells.push(record);
      }}
      for(let sourceIndex=0;sourceIndex<row.sources.length;sourceIndex+=1)pending.push(row.sources[sourceIndex]!);
    }
    const indexes=[...guards];const cellRows=[...cells.values()];
    return()=>{const rows=nativePublished?semanticGuards:priorSemanticGuards;for(let index=0;index<indexes.length;index+=1)if(!rows[indexes[index]!]!())return false;
      for(let index=0;index<cellRows.length;index+=1){const row=cellRows[index]!;if(!Object.is(reactiveInternal.signalValue(row.cell),nativePublished?row.after:row.before))return false;}return true;};
  }
  const adoptions: {target:Computed<unknown>;sources:readonly Readable<unknown>[];value:unknown;error:unknown;hasValue:boolean;hasError:boolean;semanticCurrent:()=>boolean}[]=[];
  const adoptionSlots:string[]=[];
  const expectedStates=new Map<string,unknown>();
  const semanticMismatches=new Set<string>();
  const targetCapture=reactiveInternal.transferCapture([...explicit.values()].map((entry)=>entry.target));
  const targetCaches=new Map(targetCapture.nodes.map((node)=>[node.source,node]));
  guards.push(targetCapture.current);
  for(const row of image.nodes){
    if(row.kind!=="state"&&row.kind!=="computed")continue;
    if(!row.hasValue&&!row.hasError)throw new TypeError("Required cached node has no admitted result");
    const entry=explicit.get(row.slot)!;const target=entry.target;
    const witness:TransferTargetWitness=Object.freeze({slot:row.slot,readable:target});
    const cache=targetCaches.get(target)!;
    const value=row.hasValue?decodeValue(row.value,witness):undefined;
    const error=row.hasError?decodeFailure(row.error,witness):undefined;
    if(row.kind==="computed"&&cache.hasRun){if(cache.hasValue!==row.hasValue||cache.hasError!==row.hasError||(row.hasValue&&!Object.is(cache.value,value))||(row.hasError&&!Object.is(cache.error,error)))semanticMismatches.add(row.slot);continue;}
    if(row.kind==="state"){expectedStates.set(row.slot,value);if(!reactiveInternal.transferConstructionInitial(target))continue;}
    if(row.kind==="state"){
      if(row.hasError)throw new TypeError("Native State cannot contain a cached failure role");
      stageWrite(target as WritableSignal<unknown>,value);
    }else {adoptionSlots.push(row.slot);adoptions.push({target:target as Computed<unknown>,sources:row.sources.map((slot)=>targets.get(slot)!),value,error,hasValue:row.hasValue,hasError:row.hasError,semanticCurrent:cacheSemanticCurrent(row.slot)});}
  }
  for(const entry of mappedWrites){
    requiredConstruction(entry.target,entry.construction);
    if(entry.construction.kind!=="state"||transferReadableStore(entry.target)!==undefined||transferProjection(entry.target)!==undefined)throw new TypeError("Mapped live-slot writes require genuine authored native State");
    stageWrite(entry.target,entry.value);
  }
  const importedByRuntime=new Map<StoreRuntime<any>,{value:unknown}>(metadata.map((row)=>[row.runtime,row]));
  for(const [native,seed]of positionedSeedByNative){const base=(seed as {base?:{present:boolean;value?:unknown}}).base;if(base?.present&&Object.is(reactiveInternal.signalValue(native.runtime.committed),base.value))importedByRuntime.set(native.runtime,{value:base.value});}
  function compatibleCache(slot:string,valueFor:(target:WritableSignal<unknown>)=>unknown):boolean{
    const seen=new Set<string>();const pending=[...nodeRows.get(slot)!.sources];
    for(let index=0;index<pending.length;index+=1){
      const dependency=pending[index]!;if(seen.has(dependency))continue;seen.add(dependency);
      if(semanticMismatches.has(dependency))return false;
      const row=nodeRows.get(dependency)!;
      if(row.kind==="state"){
        const target=targets.get(dependency)! as WritableSignal<unknown>;
        const actual=writeValues.has(target)?valueFor(target):reactiveInternal.signalValue(target);
        if(!Object.is(actual,expectedStates.get(dependency)))return false;
      }else if(row.kind==="native"&&row.store!==undefined){
        if(row.projection!==undefined){
          const native=positionedBySlot.get(row.positioned!)!;const seed=positionedSeedByNative.get(native)!;
          const expected=row.facet==="projection-value"?seed.values[row.projection]:row.facet==="projection-status"?"open":undefined;
          if(!Object.is(valueFor(targets.get(dependency)as WritableSignal<unknown>),expected))return false;
          for(let sourceIndex=0;sourceIndex<row.sources.length;sourceIndex++)pending.push(row.sources[sourceIndex]!);
          continue;
        }
        const runtime=nativeBySlot.get(row.store)!.runtime;
        if(runtime.baseAvailable&&!importedByRuntime.has(runtime))return false;
        const imported=importedByRuntime.get(runtime);
        if(imported!==undefined&&!Object.is(valueFor(runtime.committed),imported.value))return false;
      }
      for(let sourceIndex=0;sourceIndex<row.sources.length;sourceIndex+=1)pending.push(row.sources[sourceIndex]!);
    }
    return true;
  }
  const linkByTarget=new Map<Computed<unknown>,readonly Readable<unknown>[]>();
  const linkTargetCurrent=new Map<Readable<unknown>,boolean>();
  const unrunNative=(target:Readable<unknown>):target is Computed<unknown>=>{
    let unrun=linkTargetCurrent.get(target);
    if(unrun===undefined){const node=reactiveInternal.transferCapture([target]).nodes[0]!;unrun=node.kind==="computed"&&!node.hasRun;linkTargetCurrent.set(target,unrun);}
    return unrun;
  };
  for(const native of natives)for(const facet of native.facets)if(unrunNative(facet.target))linkByTarget.set(facet.target,[facet.source]);
  for(const row of image.nodes){
    if(row.kind!=="native"&&row.kind!=="authority")continue;
    const target=targets.get(row.slot)!;
    if(!unrunNative(target)||linkByTarget.has(target))continue;
    const dependencies=row.sources.map((slot)=>targets.get(slot)!);
    const native=row.store===undefined?undefined:nativeBySlot.get(row.store);
    if(native!==undefined&&(row.facet==="value"||row.facet==="stream-value")&&!native.runtime.baseAvailable&&!dependencies.includes(native.runtime.presence))dependencies.push(native.runtime.presence);
    linkByTarget.set(target,dependencies);
  }
  const links=[...linkByTarget].map(([target,sources])=>({target,sources}));
  const reactivePlan=reactiveInternal.transferPrepare(writes,adoptions,links);
  for(const row of semanticCells)row.after=writeValues.has(row.cell)?reactivePlan.valueFor(row.cell):row.before;
  for(const slot of adoptionSlots)if(!compatibleCache(slot,reactivePlan.valueFor))throw new TypeError("Required server cache conflicts with a browser-winning dependency");
  const claimPlan=graphInternal.prepareNativePassiveTransferClaims(natives);
  const current=():boolean=>{
    const positionedNow=readerLifetimeNow();
    if(!claimPlan.current(positionedNow)||!reactivePlan.current())return false;
    for(let index=0;index<natives.length;index+=1){const native=natives[index]!;if(!(native.currentAt?native.currentAt(positionedNow):native.current())||!transferStoreAdmissible(native.runtime))return false;}
    for(let index=0;index<observations.length;index+=1)if(!observations[index]!.current())return false;
    for(let index=0;index<graphHandles.length;index+=1)if(states[index]!.disposed||transferHasAdmission(graphHandles[index]!))return false;
    for(let index=0;index<guards.length;index+=1)if(!guards[index]!())return false;
    return true;
  };
  if(!current())throw new TypeError("Required adoption changed during preparation");
  return prepareTransferPublication(current,()=>{
    claimPlan.publish();
    reactivePlan.publish();
    for(let index=0;index<metadata.length;index+=1){const row=metadata[index]!;const runtime=row.runtime;
      runtime.generation=row.generation;runtime.valueRevision=row.revision;runtime.incarnation=row.incarnation;
      runtime.baseAvailable=true;runtime.hasAcceptedLanding=row.landingAt!==undefined;runtime.lastLandingAt=row.landingAt;
      runtime.transferEligible=true;runtime.transitioning=false;runtime.dropped=false;runtime.failing=false;runtime.invalidated=false;
    }
    nativePublished=true;
  },()=>{try{claimPlan.deliver();}finally{reactivePlan.notify();}},()=>{for(const native of natives)native.discard();});
}
