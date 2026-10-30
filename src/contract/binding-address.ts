import type { StoreKey, ValueRevision } from "./types.ts";
import type { QueryBindingHandle } from "./query-binding.ts";
import type { StreamBindingHandle } from "./stream-binding.ts";
import type {
  BindingAddressRuntime,
  StoreBindingRuntime,
  StoreRuntime,
  StreamBindingRuntime,
} from "./runtime.ts";
import { storeKey } from "./identity.ts";
import { noteTransferChange } from "./query-policy.ts";
import { __internal as reactiveInternal } from "../reactive.ts";
import {
  assertBindingContextOpen,
  beginBindingAddress,
  bindingAddress,
  captureStoreRuntime,
  queryBindingByHandle,
  streamBindingByHandle,
} from "./registry.ts";

declare const boundAddressStampBrand: unique symbol;
declare const boundAddressEmptyBrand: unique symbol;
/** Opaque address-local facts, permanently stale after their native authority changes. */
export interface BoundAddressStamp {
  /** @internal Nominal stamp; facts belong to an exact native binding/key. */
  readonly [boundAddressStampBrand]: true;
}
/**
 * Non-reserving condition for the exact inspected binding and logical key.
 * Only the same stream binding can consume a stream-origin condition, once,
 * through its member's expectEmpty option. A query-origin condition has no query
 * consumer and cannot authorize stream admission.
 */
export interface BoundAddressEmpty {
  /** @internal Nominal condition; structural imitation grants no admission. */
  readonly [boundAddressEmptyBrand]: true;
}
/** Coherent native presence and occupation; no payload or fabricated store identity is returned. */
export type BoundAddressInspection =
  | {
      /** Native authority changed; this result grants no presence or admission facts. */
      readonly kind: "changed";
    }
  | {
      /** The address facts remained coherent throughout this read. */
      readonly kind: "stable";
      /** No native visible/base value or operation occupies the address. */
      readonly state: "empty";
      /** No visible value, including a prediction, is present. */
      readonly present: false;
      /** No committed value is present. */
      readonly basePresent: false;
      /** Address-local facts for a later expected-stamp check. */
      readonly stamp: BoundAddressStamp;
      /** Binding-local empty condition; query-origin conditions grant no stream admission. */
      readonly condition: BoundAddressEmpty;
    }
  | {
      /** The address facts remained coherent throughout this read. */
      readonly kind: "stable";
      /** Data or native work occupies the address, even if no value is visible. */
      readonly state: "occupied";
      /** Whether the native visible fold has a value, including undefined. */
      readonly present: boolean;
      /** Whether committed native state has a value, including undefined. */
      readonly basePresent: boolean;
      /** Address-local facts for a later expected-stamp check. */
      readonly stamp: BoundAddressStamp;
    };

interface ReaderAddressSnapshot {
  readonly runtime: StoreRuntime<unknown>;
  readonly incarnation?: object;
  readonly revision: ValueRevision;
  readonly generation: number;
  readonly stream?: object;
  readonly transitioning: boolean;
  readonly disposed: boolean;
  readonly basePresent: boolean;
  readonly requests: number;
  readonly activeRequest?: object;
  readonly predictions: readonly { readonly id: number; readonly dead: boolean }[];
}

function readerStoreRuntimes(binding: StoreBindingRuntime): readonly StoreRuntime<unknown>[] {
  return binding.kind === "stream" ? [...(binding.readerStores ?? [])] : [];
}

function readerAddressSnapshots(
  binding: StoreBindingRuntime,
  key: string,
): readonly ReaderAddressSnapshot[] {
  return readerStoreRuntimes(binding)
    .filter((runtime) => String(runtime.key) === key)
    .map((runtime) => ({
      runtime,
      incarnation: runtime.incarnation,
      revision: runtime.valueRevision,
      generation: runtime.generation,
      stream: runtime.stream,
      transitioning: runtime.transitioning,
      disposed: runtime.disposed,
      basePresent: runtime.baseAvailable,
      requests: runtime.requests.size,
      activeRequest: runtime.activeRequest,
      predictions: reactiveInternal.signalValue(runtime.predictionStack).map((prediction) => ({
        id: prediction.id,
        dead: prediction.dead,
      })),
    }));
}

function readerAddressSnapshotCurrent(snapshot: ReaderAddressSnapshot): boolean {
  const { runtime } = snapshot;
  const predictions = reactiveInternal.signalValue(runtime.predictionStack);
  return (
    runtime.incarnation === snapshot.incarnation &&
    runtime.valueRevision === snapshot.revision &&
    runtime.generation === snapshot.generation &&
    runtime.stream === snapshot.stream &&
    runtime.transitioning === snapshot.transitioning &&
    runtime.disposed === snapshot.disposed &&
    runtime.baseAvailable === snapshot.basePresent &&
    runtime.requests.size === snapshot.requests &&
    runtime.activeRequest === snapshot.activeRequest &&
    predictions.length === snapshot.predictions.length &&
    predictions.every(
      (prediction, index) =>
        prediction.id === snapshot.predictions[index]?.id &&
        prediction.dead === snapshot.predictions[index]?.dead,
    )
  );
}

interface AddressFacts {
  readonly binding: StoreBindingRuntime;
  readonly key: string;
  readonly address: BindingAddressRuntime;
  readonly guard: object;
  readonly epoch: number;
  readonly runtime?: StoreRuntime<unknown>;
  readonly readerStores: readonly StoreRuntime<unknown>[];
  readonly readerSnapshots: readonly ReaderAddressSnapshot[];
  readonly incarnation?: object;
  readonly revision?: ValueRevision;
  readonly generation?: number;
  readonly stream?: object;
  readonly retirement?: object;
  readonly request?: object;
  readonly requests: number;
  readonly operations: number;
  readonly empty: boolean;
}
const stamps = new WeakMap<object, AddressFacts>();
const emptyConditions = new WeakMap<object, { readonly facts: AddressFacts; used: boolean }>();

/** @internal Import authority belongs to the actual base, without retaining its lifetime. */
export function importedBaselineCurrent(runtime: StoreRuntime<unknown>): boolean {
  const seed = runtime.importedBaseline;
  return (
    seed !== undefined &&
    runtime.binding?.kind === "stream" &&
    !runtime.binding.context.disposed &&
    !runtime.graph.__runtime.disposed &&
    !runtime.disposed &&
    !runtime.transitioning &&
    runtime.baseAvailable &&
    runtime.hasAcceptedLanding &&
    runtime.incarnation === seed.incarnation &&
    runtime.graph.__runtime.resetEpoch === seed.resetEpoch &&
    runtime.generation === seed.generation &&
    runtime.valueRevision === seed.valueRevision
  );
}

/** @internal Used by inspection and guarded stream import; empty shells grant no occupation. */
export function boundAddressOccupied(binding: StoreBindingRuntime, key: string): boolean {
  const runtime = binding.stores.get(key);
  const readerOccupied = readerStoreRuntimes(binding).some(
    (reader) =>
      String(reader.key) === key &&
      !reader.disposed &&
      (reader.transitioning ||
        reader.baseAvailable ||
        reactiveInternal
          .signalValue(reader.predictionStack)
          .some((prediction) => !prediction.dead) ||
        reader.requests.size > 0 ||
        (reader.activeRequest !== undefined && !reader.activeRequest.settled) ||
        (reader.stream !== undefined && !reader.stream.ended) ||
        reader.boundRetirement?.phase === "retiring"),
  );
  return (
    readerOccupied ||
    (binding.addresses.get(key)?.operations.size ?? 0) > 0 ||
    (runtime !== undefined &&
      (runtime.transitioning ||
        runtime.baseAvailable ||
        reactiveInternal
          .signalValue(runtime.predictionStack)
          .some((prediction) => !prediction.dead) ||
        runtime.requests.size > 0 ||
        (runtime.activeRequest !== undefined && !runtime.activeRequest.settled) ||
        (runtime.stream !== undefined && !runtime.stream.ended) ||
        runtime.boundRetirement?.phase === "retiring" ||
        importedBaselineCurrent(runtime)))
  );
}

function factsCurrent(facts: AddressFacts): boolean {
  const { binding, runtime, address } = facts;
  const state = binding.context.state;
  const readerStores = readerStoreRuntimes(binding).filter(
    (reader) => String(reader.key) === facts.key,
  );
  const readerSnapshotsCurrent = facts.readerSnapshots.every(readerAddressSnapshotCurrent);
  return (
    !state.disposed &&
    !binding.context.disposed &&
    state.resetEpoch === facts.epoch &&
    binding.addresses.get(facts.key) === address &&
    address.guard === facts.guard &&
    address.operations.size === facts.operations &&
    binding.stores.get(facts.key) === runtime &&
    readerStores.length === facts.readerStores.length &&
    readerStores.every((reader, index) => reader === facts.readerStores[index]) &&
    readerSnapshotsCurrent &&
    (runtime === undefined ||
      (!runtime.disposed &&
        !runtime.transitioning &&
        runtime.incarnation === facts.incarnation &&
        runtime.valueRevision === facts.revision &&
        runtime.generation === facts.generation &&
        runtime.stream === facts.stream &&
        runtime.boundRetirement === facts.retirement &&
        runtime.activeRequest === facts.request &&
        runtime.requests.size === facts.requests))
  );
}

/**
 * Inspects a genuine existing bound address without acquisition, key computation or idle activity.
 * @param handle - Actual active query or stream binding on its native graph/context.
 * @param key - Nonempty already-authored logical key; no coercion or callback is used.
 * @param options - Optional genuine prior stamp for the same exact binding and key.
 * @returns Changed for stale facts; otherwise native visible/base presence and address occupation.
 * Empty additionally supplies a condition for that exact binding/key. Only the same
 * stream binding's ordinary member can consume a stream-origin condition, once.
 * Query-origin conditions have no query consumer and cannot authorize stream admission.
 * @throws TypeError for fabricated/crossed handles, stamps, keys or options. Native disposed/read faults preserve their channel.
 */
export function inspectBoundAddress(
  handle: QueryBindingHandle | StreamBindingHandle,
  key: StoreKey,
  options?: {
    /** Prior stamp issued by this exact binding/key; stale facts return changed. */
    readonly expected?: BoundAddressStamp;
  },
): BoundAddressInspection {
  const binding = queryBindingByHandle.get(handle) ?? streamBindingByHandle.get(handle);
  if (binding === undefined) throw new TypeError("Address inspection requires a genuine binding");
  assertBindingContextOpen(binding.context);
  const checkedKey = storeKey(key);
  if (
    options !== undefined &&
    (options === null || typeof options !== "object" || Array.isArray(options))
  )
    throw new TypeError("Invalid bound address inspection options");
  const expected: unknown = options === undefined ? undefined : Reflect.get(options, "expected");
  assertBindingContextOpen(binding.context);
  if (expected !== undefined) {
    const previous = stamps.get(expected as object);
    if (previous === undefined || previous.binding !== binding || previous.key !== checkedKey)
      throw new TypeError("Address stamp does not belong to this binding and key");
    if (!factsCurrent(previous)) return Object.freeze({ kind: "changed" });
  }
  const address = bindingAddress(binding, checkedKey);
  const runtime = binding.stores.get(checkedKey);
  const state = binding.context.state;
  const facts: AddressFacts = {
    binding,
    key: checkedKey,
    address,
    guard: address.guard,
    epoch: state.resetEpoch,
    runtime,
    readerStores: readerStoreRuntimes(binding).filter(
      (reader) => String(reader.key) === checkedKey,
    ),
    readerSnapshots: readerAddressSnapshots(binding, checkedKey),
    incarnation: runtime?.incarnation,
    revision: runtime?.valueRevision,
    generation: runtime?.generation,
    stream: runtime?.stream,
    retirement: runtime?.boundRetirement,
    request: runtime?.activeRequest,
    requests: runtime?.requests.size ?? 0,
    operations: address.operations.size,
    empty: false,
  };
  if (!factsCurrent(facts)) return Object.freeze({ kind: "changed" });
  const capture = runtime === undefined ? undefined : captureStoreRuntime(runtime);
  if (capture?.kind === "changed" || !factsCurrent(facts))
    return Object.freeze({ kind: "changed" });
  const readerCaptures = facts.readerSnapshots.map((snapshot) =>
    captureStoreRuntime(snapshot.runtime),
  );
  if (
    readerCaptures.some((readerCapture) => readerCapture.kind === "changed") ||
    !factsCurrent(facts)
  )
    return Object.freeze({ kind: "changed" });
  const present =
    (capture?.kind === "stable" && capture.present) ||
    readerCaptures.some(
      (readerCapture) => readerCapture.kind === "stable" && readerCapture.present,
    );
  const basePresent =
    (runtime?.baseAvailable ?? false) ||
    facts.readerSnapshots.some((snapshot) => snapshot.basePresent);
  const occupied = present || boundAddressOccupied(binding, checkedKey);
  const completed = { ...facts, empty: !occupied };
  const stamp = Object.freeze({}) as BoundAddressStamp;
  stamps.set(stamp, completed);
  if (occupied)
    return Object.freeze({ kind: "stable", state: "occupied", present, basePresent, stamp });
  const condition = Object.freeze({}) as BoundAddressEmpty;
  emptyConditions.set(condition, { facts: completed, used: false });
  return Object.freeze({
    kind: "stable",
    state: "empty",
    present: false,
    basePresent: false,
    stamp,
    condition,
  });
}

/** @internal Consumes only a current empty condition immediately before this invocation's first mutation. */
export function reserveBoundStreamAddress(
  binding: StreamBindingRuntime,
  key: StoreKey,
  condition: unknown,
): object {
  assertBindingContextOpen(binding.context);
  if (condition !== undefined) {
    const record = emptyConditions.get(condition as object);
    if (
      record === undefined ||
      record.facts.binding !== binding ||
      record.facts.key !== String(key)
    )
      throw new TypeError("Empty condition does not belong to this stream address");
    if (record.used || !record.facts.empty || !factsCurrent(record.facts))
      throw new TypeError("The inspected stream address is no longer empty");
    const address = bindingAddress(binding, String(key));
    noteTransferChange(binding);
    noteTransferChange(binding.handle);
    noteTransferChange(address);
    const token = beginBindingAddress(binding, String(key))!;
    noteTransferChange(condition as object);
    record.used = true;
    return token;
  }
  const address = bindingAddress(binding, String(key));
  if (address.operations.size !== 0)
    throw new TypeError("The bound stream address is transitioning");
  noteTransferChange(binding);
  noteTransferChange(binding.handle);
  noteTransferChange(address);
  return beginBindingAddress(binding, String(key))!;
}
