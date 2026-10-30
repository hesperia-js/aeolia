import { __internal as reactiveInternal } from "../reactive.ts";
import type { GraphId } from "./identity.ts";
import type {
  Graph,
  MutationDefinition,
  QueryDefinition,
  StoreCapture,
  StreamDefinition,
  PositionedStreamDefinition,
  PositionedStreamSource,
} from "./types.ts";
import { Fault } from "../fault.ts";
import type { QueryBinding, TypedTarget } from "./query-binding.ts";
import type { Unsubscribe } from "../fault.ts";
import { assertGraphOpen, assertStoreOpen, reportContinuationError } from "./faults.ts";
import { noteTransferChange } from "./query-policy.ts";
import type {
  BindingAddressRuntime,
  BindingContextRuntime,
  BoundProducerRuntime,
  BoundStreamSessionRuntime,
  EffectInstructionRuntime,
  GraphProjection,
  GraphRuntimeState,
  ProfileEffectRuntime,
  QueryBindingRuntime,
  QueryTargetRuntime,
  StoreBindingRuntime,
  StoreRuntime,
  StreamBindingRuntime,
} from "./runtime.ts";

let nextGraphId = 0;

export function allocateGraphId(): GraphId {
  return `graph-${nextGraphId++}` as GraphId;
}

/** Runtime lookup for public store shells and their prototype chain. */
export const runtimeByStore = new WeakMap<object, StoreRuntime<unknown>>();

/** Exact query wrappers; inheriting from a genuine wrapper grants no ticket. */
export const queryRuntimeByStore = new WeakMap<object, StoreRuntime<unknown>>();

interface DormantStoreHandle {
  readonly runtime: StoreRuntime<any>;
  readonly isPublished: () => boolean;
  readonly query: boolean;
}
const dormantStoreHandles = new WeakMap<object, DormantStoreHandle>();

/** @internal Registers a private Store wrapper before the atomic receiver publication. */
export function registerDormantStoreHandle<T>(
  handle: object,
  runtime: StoreRuntime<T>,
  isPublished: () => boolean,
  query = true,
): void {
  if (handle === runtime.store || runtimeByStore.has(handle) || queryRuntimeByStore.has(handle))
    throw new TypeError("A passive claim must use a fresh Store wrapper");
  if (typeof isPublished !== "function") throw new TypeError("Expected a native publication predicate");
  if (typeof query !== "boolean") throw new TypeError("Expected a native Store role");
  noteTransferChange(handle);
  dormantStoreHandles.set(handle, { runtime, isPublished, query });
  runtimeByStore.set(handle, runtime as StoreRuntime<unknown>);
  if (query) queryRuntimeByStore.set(handle, runtime as StoreRuntime<unknown>);
}

/** @internal Refuses a pre-registered passive Store until its transfer publishes. */
export function assertStoreHandlePublished(handle: object): void {
  const claim = dormantStoreHandles.get(handle);
  if (claim === undefined) return;
  if (
    runtimeByStore.get(handle) !== claim.runtime ||
    (claim.query && queryRuntimeByStore.get(handle) !== claim.runtime) ||
    (!claim.query && queryRuntimeByStore.has(handle))
  )
    throw new TypeError("The passive Store handle has changed");
  if (!claim.isPublished()) throw new TypeError("The passive Store handle is still dormant");
}

/** Runtime lookup for graph objects used by internal adapters. */
export const graphRuntimeByGraph = new WeakMap<object, GraphRuntimeState<any>>();

export function registerProjection(graph: Graph, projection: GraphProjection): Unsubscribe {
  const state = graphRuntimeByGraph.get(graph as object);
  if (state === undefined) throw new TypeError("The value is not an Aeolia graph");
  assertGraphOpen(state);
  noteTransferChange(state);
  noteTransferChange(projection);
  state.projections.add(projection);
  let active = true;
  return () => {
    if (!active) return;
    noteTransferChange(state);
    noteTransferChange(projection);
    active = false;
    state.projections.delete(projection);
  };
}

export function reportGraphContinuationError(graph: Graph, error: unknown): void {
  const state = graphRuntimeByGraph.get(graph as object);
  if (state === undefined || state.disposed) return;
  reportContinuationError(state, error);
}

export function runtimeForStore(value: unknown): StoreRuntime<unknown> | undefined {
  if (value === null || (typeof value !== "object" && typeof value !== "function"))
    return undefined;
  assertStoreHandlePublished(value as object);
  let current: object | null = value;
  while (current !== null) {
    const runtime = runtimeByStore.get(current);
    if (runtime !== undefined) {
      assertStoreHandlePublished(current);
      return runtime;
    }
    current = Object.getPrototypeOf(current) as object | null;
  }
  return undefined;
}

/** Exact native capability provenance, never exported by a package entry. */
export const bindingContextByHandle = new WeakMap<object, BindingContextRuntime>();
export const queryBindingByHandle = new WeakMap<object, QueryBindingRuntime>();
export const queryTargetByHandle = new WeakMap<object, QueryTargetRuntime>();
export const profileEffectByHandle = new WeakMap<object, ProfileEffectRuntime>();
export const effectInstructionByHandle = new WeakMap<object, EffectInstructionRuntime>();
export const targetPredictionByHandle = new WeakMap<object, BoundProducerRuntime>();

export function assertBindingContextOpen(context: BindingContextRuntime): void {
  assertGraphOpen(context.state);
  if (context.disposed) throw new Fault("disposed", [String(context.state.graph.id)]);
}

export function isQueryTargetCurrent(target: QueryTargetRuntime): boolean {
  const { runtime, binding, incarnation } = target;
  return (
    !binding.context.disposed &&
    !binding.context.state.disposed &&
    !runtime.disposed &&
    !runtime.dropped &&
    runtime.incarnation === incarnation
  );
}

export function captureQueryTarget<Q extends QueryDefinition<any, any, any, any>, V>(
  binding: QueryBindingRuntime,
  runtime: StoreRuntime<unknown>,
): TypedTarget<Q, V> | undefined {
  assertBindingContextOpen(binding.context);
  const incarnation = runtime.incarnation;
  if (runtime.disposed) throw new Fault("disposed", [String(runtime.key)]);
  if (runtime.dropped || incarnation === undefined) return undefined;
  const record: QueryTargetRuntime = { binding, runtime, incarnation };
  const target = Object.freeze({
    key: runtime.key,
    identity: runtime.identity,
    profile: binding.profile,
    forBinding<W>(candidate: QueryBinding<Q, W>): TypedTarget<Q, W> | undefined {
      const checked = queryBindingByHandle.get(candidate);
      if (checked === undefined) throw new TypeError("The value is not an Aeolia query binding");
      assertBindingContextOpen(checked.context);
      if (checked !== binding || !isQueryTargetCurrent(record)) return undefined;
      return target as unknown as TypedTarget<Q, W>;
    },
  }) as TypedTarget<Q, V>;
  queryTargetByHandle.set(target, record);
  return target;
}

export function captureBoundProducer<
  M extends MutationDefinition<any, any>,
  Q extends QueryDefinition<any, any, any, any>,
  V,
>(
  mutation: M,
  target: TypedTarget<Q, V>,
  producer: (
    value: NoInfer<V> | undefined,
    input: M extends MutationDefinition<infer I, any> ? I : never,
  ) => NoInfer<V>,
): BoundProducerRuntime {
  const record = queryTargetByHandle.get(target);
  if (record === undefined || typeof producer !== "function")
    throw new TypeError("Prediction requires a genuine target and a producer");
  assertBindingContextOpen(record.binding.context);
  const state = record.binding.context.state;
  if (
    !state.mutationDefinitions.has(mutation) ||
    !mutation.affects.some((effect) => effect.query === record.binding.definition)
  )
    throw new TypeError("Prediction target is not affected by this mutation");
  return {
    mutation,
    target: record,
    // Exact mutation and invariant ticket associations were checked before erasure.
    producer: (value, input) =>
      producer(
        value as V | undefined,
        input as M extends MutationDefinition<infer I, any> ? I : never,
      ),
  };
}

/** Exact associations created by the native raw-member and bound-wrapper owners. */
export const rawStreamMemberByFunction = new WeakMap<
  object,
  {
    readonly state: GraphRuntimeState<any>;
    readonly definition:
      | StreamDefinition<any, any, any>
      | PositionedStreamDefinition<any, any, any, any, any>;
  }
>();
export const streamBindingByHandle = new WeakMap<object, StreamBindingRuntime>();
export const boundStreamByStore = new WeakMap<object, BoundStreamSessionRuntime<unknown>>();

/** Provider callbacks captured by the genuine declaration factory. */
export const positionedStreamSourceByDefinition = new WeakMap<
  object,
  PositionedStreamSource<any, any, any, any>
>();

/** Native event-position writer attached only to one active source-open call. */
export const positionedPositionWriterByOpenOptions = new WeakMap<
  object,
  (position: unknown) => void
>();

/** One exact native request for a live or admitted positioned opening. */
export const positionedOpenRequestByOpenOptions = new WeakMap<
  object,
  { readonly after?: unknown; readonly reader?: unknown }
>();

export function bindingAddress(binding: StoreBindingRuntime, key: string): BindingAddressRuntime {
  let address = binding.addresses.get(key);
  if (address === undefined) {
    address = { binding, key, operations: new Set(), guard: {} };
    noteTransferChange(binding);
    noteTransferChange(binding.handle);
    noteTransferChange(address);
    binding.addresses.set(key, address);
  }
  return address;
}

export function changeBoundAddress<T>(runtime: StoreRuntime<T>): void {
  const address = storeBindingAddress(runtime);
  if (address !== undefined) {
    noteTransferChange(runtime);
    noteTransferChange(address.binding);
    noteTransferChange(address.binding.handle);
    noteTransferChange(address);
    address.guard = {};
  }
  if (runtime.readerScope !== undefined && runtime.binding !== undefined) {
    const aggregate = bindingAddress(runtime.binding, String(runtime.key));
    noteTransferChange(runtime);
    noteTransferChange(runtime.binding);
    noteTransferChange(runtime.binding.handle);
    noteTransferChange(aggregate);
    aggregate.guard = {};
  }
}

interface StoreAddressOperationRecord {
  readonly runtime: StoreRuntime<unknown>;
  readonly address: BindingAddressRuntime;
  readonly aggregate?: BindingAddressRuntime;
}

const storeAddressOperations = new WeakMap<object, StoreAddressOperationRecord>();

/** Exact native entry address; reader-scoped Stores do not replace the keyed slot. */
export function storeBindingAddress<T>(
  runtime: StoreRuntime<T>,
): BindingAddressRuntime | undefined {
  return (
    runtime.readerScope?.address ??
    (runtime.binding === undefined
      ? undefined
      : bindingAddress(runtime.binding, String(runtime.key)))
  );
}

export function beginStoreAddress<T>(runtime: StoreRuntime<T>): object | undefined {
  const address = storeBindingAddress(runtime);
  if (address === undefined) return undefined;
  const aggregate =
    runtime.readerScope !== undefined && runtime.binding !== undefined
      ? bindingAddress(runtime.binding, String(runtime.key))
      : undefined;
  const token = {};
  noteTransferChange(runtime);
  noteTransferChange(address.binding);
  noteTransferChange(address.binding.handle);
  noteTransferChange(address);
  address.operations.add(token);
  address.guard = {};
  if (aggregate !== undefined && aggregate !== address) {
    noteTransferChange(aggregate);
    noteTransferChange(aggregate.binding.handle);
    aggregate.operations.add(token);
    aggregate.guard = {};
  }
  storeAddressOperations.set(token, {
    runtime: runtime as StoreRuntime<unknown>,
    address,
    ...(aggregate === undefined || aggregate === address ? {} : { aggregate }),
  });
  return token;
}

export function endStoreAddress<T>(runtime: StoreRuntime<T>, token: object | undefined): void {
  if (token === undefined) return;
  const operation = storeAddressOperations.get(token);
  if (operation === undefined || operation.runtime !== runtime) return;
  noteTransferChange(runtime);
  noteTransferChange(operation.address.binding);
  noteTransferChange(operation.address.binding.handle);
  noteTransferChange(operation.address);
  storeAddressOperations.delete(token);
  if (operation.address.operations.delete(token)) operation.address.guard = {};
  if (operation.aggregate?.operations.has(token)) {
    noteTransferChange(operation.aggregate);
    noteTransferChange(operation.aggregate.binding.handle);
    operation.aggregate.operations.delete(token);
    operation.aggregate.guard = {};
  }
}

export function beginBindingAddress(
  binding: StoreBindingRuntime | undefined,
  key: string,
): object | undefined {
  if (binding === undefined) return undefined;
  const address = bindingAddress(binding, key);
  const token = {};
  noteTransferChange(binding);
  noteTransferChange(binding.handle);
  noteTransferChange(address);
  address.operations.add(token);
  address.guard = {};
  return token;
}

export function endBindingAddress(
  binding: StoreBindingRuntime | undefined,
  key: string,
  token: object | undefined,
): void {
  if (binding === undefined || token === undefined) return;
  const address = binding.addresses.get(key);
  if (address?.operations.has(token)) {
    noteTransferChange(binding);
    noteTransferChange(binding.handle);
    noteTransferChange(address);
    address.operations.delete(token);
    address.guard = {};
  }
}

/** @internal One visible-fold kernel; retirement authority is a genuine native session record. */
export function captureStoreRuntime<T>(
  runtime: StoreRuntime<T>,
  retirement?: BoundStreamSessionRuntime<T>,
): StoreCapture<T> {
  assertStoreOpen(runtime);
  if (runtime.binding !== undefined) assertBindingContextOpen(runtime.binding.context);
  if (
    retirement !== undefined &&
    (retirement.runtime !== runtime ||
      retirement.phase !== "retiring" ||
      runtime.boundRetirement !== retirement ||
      retirement.retirement === undefined)
  )
    throw new TypeError("Invalid native retirement capture authority");
  if (retirement === undefined && (runtime.transitioning || runtime.boundRetirement !== undefined))
    return Object.freeze({ kind: "changed" });
  const incarnation = runtime.incarnation;
  const revision = runtime.valueRevision;
  const epoch = runtime.graph.__runtime.resetEpoch;
  const retirementToken = retirement?.retirement?.token;
  const current = (): boolean =>
    !runtime.disposed &&
    !runtime.graph.__runtime.disposed &&
    runtime.binding?.context.disposed !== true &&
    runtime.incarnation === incarnation &&
    runtime.valueRevision === revision &&
    runtime.graph.__runtime.resetEpoch === epoch &&
    (retirement === undefined
      ? !runtime.transitioning && runtime.boundRetirement === undefined
      : runtime.boundRetirement === retirement &&
        retirement.phase === "retiring" &&
        retirement.retirement?.token === retirementToken);
  const value = reactiveInternal.nativePeek(runtime.value, current);
  if (!current()) return Object.freeze({ kind: "changed" });
  const present =
    runtime.baseAvailable ||
    reactiveInternal.signalValue(runtime.predictionStack).some((prediction) => !prediction.dead);
  const capture: StoreCapture<T> = present
    ? {
        kind: "stable",
        identity: runtime.identity,
        valueRevision: revision,
        present: true,
        value: value as T,
      }
    : { kind: "stable", identity: runtime.identity, valueRevision: revision, present: false };
  return Object.freeze(capture);
}
