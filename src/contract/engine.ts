import type {
  Contract,
  ContractApi,
  Graph,
  GraphOptions,
  MutationDefinition,
  MutationOptions,
  OperationTree,
  QueryDefinition,
  Store,
  StoreAt,
  StoreDefinition,
  StoreOptions,
  StreamDefinition,
  ValueOptions,
} from "./types.ts";
import { storeKey } from "./identity.ts";
import { Fault, __registerFaultChannel, __unregisterFaultChannel } from "../fault.ts";
import type { Unsubscribe } from "../fault.ts";
import { __internal as reactiveInternal } from "../reactive.ts";
import {
  allocateGraphId,
  graphRuntimeByGraph,
  registerProjection,
  reportGraphContinuationError,
  runtimeForStore,
} from "./registry.ts";
import type { GraphRuntime, GraphRuntimeState, RealmStoreState, StoreRuntime } from "./runtime.ts";
import {
  collectDeclaredStores,
  finiteBound,
  graphIdleMs,
  isOperation,
  isStore,
} from "./runtime-validation.ts";
import { assertGraphOpen, reportContinuationError } from "./faults.ts";
import { collectExpired, touch } from "./collection.ts";
import { getOrCreateStore } from "./store.ts";
import { makeQueryStore } from "./query.ts";
import { runMutation } from "./mutation.ts";
import { streamMember } from "./stream.ts";
import { armStoreTimer } from "./query-request.ts";
import { issue } from "./store-state.ts";
import { writeAllStatuses } from "./status.ts";

/** @internal Default keyed-store retention period, in milliseconds. */
export const DEFAULT_IDLE_MS = 300_000;

/** @internal Default maximum number of simultaneous predictions on one store. */
export const DEFAULT_MAX_PREDICTIONS = 64;

function realmStores(graph: Graph): readonly RealmStoreState[] {
  const state = (graph as GraphRuntime<any>).__runtime;
  assertGraphOpen(state);
  return [...state.stores.values()].map((runtime) => ({
    key: runtime.key,
    snapshot: runtime.snapshot,
    hasCommitted: runtime.hasCommitted,
    generation: runtime.generation,
    ...(runtime.lastLandingAt === undefined ? {} : { lastLandingAt: runtime.lastLandingAt }),
    committedValue: () => reactiveInternal.signalValue(runtime.committed),
  }));
}

function realmNamespace(graph: Graph): string {
  const state = (graph as GraphRuntime<any>).__runtime;
  assertGraphOpen(state);
  return state.namespace;
}

function adoptRealmEntry(
  graph: Graph,
  key: string,
  value: unknown,
  foreignGeneration: number,
  landingAt: number,
): void {
  const state = (graph as GraphRuntime<any>).__runtime;
  assertGraphOpen(state);
  const definition = state.declaredStores.get(key) as StoreDefinition<unknown> | undefined;
  const runtime = getOrCreateStore(state, storeKey(key), definition);
  runtime.generation = Math.max(runtime.generation, foreignGeneration);
  issue(runtime);
  runtime.lastLandingAt = landingAt;
  runtime.failing = false;
  runtime.invalidated = false;
  runtime.errorCell.set(undefined);
  runtime.committed.set(value);
  runtime.hasCommitted = true;
  runtime.presence.set(true);
  runtime.dropped = false;
  touch(runtime);
  writeAllStatuses(runtime);
  armStoreTimer(runtime);
}

function buildApi<C extends Contract>(state: GraphRuntimeState<C>, tree: OperationTree): unknown {
  const api: Record<string, unknown> = {};
  for (const [property, value] of Object.entries(tree)) {
    if (isStore(value)) {
      api[property] = () => getOrCreateStore(state, storeKey(value.name), value).store;
    } else if (isOperation(value)) {
      if (value.kind === "query")
        api[property] = (input: unknown, options?: StoreOptions<unknown>) =>
          makeQueryStore(state, value as QueryDefinition<unknown, unknown, any>, input, options);
      else if (value.kind === "mutation")
        api[property] = (input: unknown, options?: MutationOptions) =>
          runMutation(state, value as MutationDefinition<unknown, unknown>, input, options);
      else
        api[property] = (input: unknown, options?: ValueOptions<unknown>) =>
          streamMember(state, value as StreamDefinition<unknown, unknown, any>, input, options);
    } else if (value !== null && typeof value === "object")
      api[property] = buildApi(state, value as OperationTree);
  }
  return Object.freeze(api);
}

function disposeGraph<C extends Contract>(state: GraphRuntimeState<C>): void {
  if (state.disposed) return;
  state.disposed = true;
  state.stopAbort?.();
  state.stopAbort = undefined;
  if (state.collectionTimer != null) clearTimeout(state.collectionTimer);
  state.collectionTimer = undefined;
  state.collectionHeap.length = 0;
  for (const close of Array.from(state.projections)) {
    try {
      close();
    } catch (error) {
      reportContinuationError(state, error);
    }
  }
  state.projections.clear();
  for (const runtime of state.stores.values()) {
    if (runtime.timer != null) clearTimeout(runtime.timer);
    runtime.timer = undefined;
    for (const stop of runtime.livenessStops) stop();
    for (const stop of runtime.readStops) stop();
    runtime.livenessStops.length = 0;
    runtime.readStops.length = 0;
    const stream = runtime.stream;
    if (stream != null && !stream.ended) {
      stream.ended = true;
      runtime.stream = undefined;
      state.activeControllers.delete(stream.controller);
      stream.controller.abort();
      if (stream.iterator != null && typeof stream.iterator.return === "function") {
        try {
          void Promise.resolve(stream.iterator.return()).catch(() => undefined);
        } catch {
          /* disposal is terminal */
        }
      }
    }
    runtime.presence.set(false);
    runtime.disposed = true;
    runtime.collectionIndex = -1;
    try {
      runtime.lifecycle.update((value) => value + 1);
    } catch (error) {
      reportContinuationError(state, error);
    }
  }
  for (const controller of Array.from(state.activeControllers)) controller.abort();
  state.activeControllers.clear();
  __unregisterFaultChannel(state.graph);
  state.faultObservers.clear();
}

/**
 * Creates an isolated state graph for a contract.
 *
 * @param options - Contract, lifetime signal, and resource bounds for the graph.
 * @returns A graph whose typed API is derived from `options.contract`.
 * @throws A {@link Fault} If a numeric resource bound is invalid.
 */
export function createGraph<C extends Contract>(options: GraphOptions<C>): Graph<C> {
  const idleMs = graphIdleMs(options.idleMs ?? DEFAULT_IDLE_MS);
  const maxPredictions = finiteBound(
    options.maxPredictions ?? DEFAULT_MAX_PREDICTIONS,
    "maxPredictions",
  );
  const id = allocateGraphId();
  const graph = {} as GraphRuntime<C>;
  const state: GraphRuntimeState<C> = {
    graph,
    namespace: options.contract.namespace,
    idleMs,
    maxPredictions,
    ...(options.onUnobservedFault === undefined
      ? {}
      : { onUnobservedFault: options.onUnobservedFault }),
    stores: new Map(),
    declaredStores: new Map(),
    faultObservers: new Set(),
    activeControllers: new Set(),
    projections: new Set(),
    collectionHeap: [],
    sweepScheduled: false,
    nextPredictionId: 0,
    disposed: false,
  };
  collectDeclaredStores(options.contract.operations, state.declaredStores);
  const graphStore = <T, N extends string>(definition: StoreDefinition<T, N>): Store<T> =>
    getOrCreateStore(state, storeKey(definition.name), definition).store;
  const graphAt = <K extends string>(key: K): StoreAt<C, K> => {
    assertGraphOpen(state);
    const branded = storeKey(key);
    const definition = state.declaredStores.get(key) as StoreDefinition<unknown> | undefined;
    return getOrCreateStore(state, branded, definition).store as StoreAt<C, K>;
  };
  const onFault = (observer: (fault: Fault) => void): Unsubscribe => {
    assertGraphOpen(state);
    state.faultObservers.add(observer);
    let active = true;
    return () => {
      if (!active) return;
      active = false;
      state.faultObservers.delete(observer);
    };
  };
  Object.assign(graph, {
    __runtime: state,
    id,
    api: buildApi(state, options.contract.operations) as ContractApi<C>,
    store: graphStore,
    at: graphAt,
    dispose: () => disposeGraph(state),
  });
  graphRuntimeByGraph.set(graph, state);
  __registerFaultChannel(graph, onFault);
  const abortSignal = options.abortSignal;
  if (abortSignal != null) {
    if (abortSignal.aborted) disposeGraph(state);
    else {
      const abort = (): void => disposeGraph(state);
      abortSignal.addEventListener("abort", abort, { once: true });
      state.stopAbort = () => abortSignal.removeEventListener("abort", abort);
    }
  }
  return graph;
}

/** @internal Integration surface shared by Aeolia's graph-adjacent modules. */
export const __internal = Object.freeze({
  runtimeOf(store: Store<unknown>): StoreRuntime<unknown> {
    const runtime = runtimeForStore(store);
    if (runtime !== undefined) return runtime;
    throw new TypeError("The value is not an Aeolia graph store");
  },
  issueGeneration: issue,
  collectExpired,
  touch,
  registerProjection,
  reportContinuationError: reportGraphContinuationError,
  realmStores,
  realmNamespace,
  adoptRealmEntry,
});
