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
import {
  hasMutationOptionFields,
  hasStoreOptionFields,
  isMutationOptions,
  isStoreOptions,
  isCancellationOptions,
  isStoreDefinition,
  isOperationDefinition,
} from "./types.ts";
import { validateContract } from "./validation.ts";
import { isObject } from "../utils.ts";
import { storeKey } from "./identity.ts";
import { Fault, __registerFaultChannel, __unregisterFaultChannel } from "../fault.ts";
import type { Unsubscribe } from "../fault.ts";
import { __internal as reactiveInternal, computed, signal } from "../reactive.ts";
import {
  allocateGraphId,
  graphRuntimeByGraph,
  registerProjection,
  reportGraphContinuationError,
  runtimeForStore,
} from "./registry.ts";
import type { GraphRuntime, GraphRuntimeState, RealmStoreState, StoreRuntime } from "./runtime.ts";
import { collectDeclaredStores, finiteBound, graphIdleMs } from "./runtime-validation.ts";
import { AbortError, assertGraphOpen, reportContinuationError } from "./faults.ts";
import { collectExpired, endStream, touch } from "./collection.ts";
import { getOrCreateStore } from "./store.ts";
import { makeQueryStore } from "./query.ts";
import { runMutation } from "./mutation.ts";
import { streamMember } from "./stream.ts";
import { armStoreTimer } from "./query-request.ts";
import { hasQueryOptionsBrand } from "./query-options.ts";
import { issue } from "./store-state.ts";
import { writeAllStatuses } from "./status.ts";
import { drainResetCleanups, resetGraph } from "./reset.ts";
import {
  beginPhysicalWork,
  createRetirementOwner,
  finishPhysicalWork,
  physicalCleanup,
  recordCleanupFailure,
  retireOwner,
} from "./retirement.ts";
import type { PhysicalWork } from "./retirement.ts";

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

function graphAcquisition<T>(state: GraphRuntimeState<any>, action: () => T): T {
  assertGraphOpen(state);
  const work = beginPhysicalWork(state.retirement, "acquisition");
  try {
    return action();
  } finally {
    finishPhysicalWork(work);
  }
}

function buildApi<C extends Contract>(state: GraphRuntimeState<C>, tree: OperationTree): unknown {
  const api: Record<string, unknown> = {};
  for (const [property, value] of Object.entries(tree)) {
    if (isStoreDefinition(value)) {
      api[property] = () => getOrCreateStore(state, storeKey(value.name), value).store;
    } else if (isOperationDefinition(value)) {
      if (value.kind === "query")
        api[property] = (...args: [input?: unknown, options?: StoreOptions<unknown>]) =>
          graphAcquisition(state, () => {
            const [input, options] = args;
            const call = normalizeQueryCall(input, options, args.length > 1);
            return makeQueryStore(
              state,
              value as QueryDefinition<unknown, unknown, any>,
              call.input,
              call.options,
            );
          });
      else if (value.kind === "mutation")
        api[property] = (...args: [input?: unknown, options?: MutationOptions]) =>
          graphAcquisition(state, () => {
            const [input, options] = args;
            const call = normalizeMutationCall(input, options, args.length > 1);
            return runMutation(
              state,
              value as MutationDefinition<unknown, unknown>,
              call.input,
              call.options,
            );
          });
      else
        api[property] = (input: unknown, options?: ValueOptions<unknown>) =>
          graphAcquisition(state, () =>
            streamMember(state, value as StreamDefinition<unknown, unknown, any>, input, options),
          );
    } else if (value !== null && typeof value === "object")
      api[property] = buildApi(state, value as OperationTree);
  }
  return Object.freeze(api);
}

function normalizeQueryCall(
  input: unknown,
  options: StoreOptions<unknown> | undefined,
  hasSecondArgument: boolean,
): {
  readonly input: unknown;
  readonly options?: StoreOptions<unknown>;
} {
  let callInput = input;
  let callOptions: unknown = options;
  if (hasQueryOptionsBrand(input)) {
    if (
      options !== undefined &&
      (!isObject(options) || typeof options === "function" || Array.isArray(options))
    )
      throw new TypeError("Invalid query options.");
    callInput = undefined;
    callOptions = { ...input, ...options };
  } else if (!hasSecondArgument && hasStoreOptionFields(input)) {
    callInput = undefined;
    callOptions = input;
  }
  if (callOptions === undefined) return { input: callInput };
  if (!isStoreOptions(callOptions)) throw new TypeError("Invalid query options.");
  return { input: callInput, options: callOptions };
}

function normalizeMutationCall(
  input: unknown,
  options: MutationOptions | undefined,
  hasSecondArgument: boolean,
): {
  readonly input: unknown;
  readonly options?: MutationOptions;
} {
  let callInput = input;
  let callOptions: unknown = options;
  if (hasQueryOptionsBrand(input)) {
    if (
      options !== undefined &&
      (!isObject(options) || typeof options === "function" || Array.isArray(options))
    )
      throw new TypeError("Invalid mutation options.");
    callInput = undefined;
    callOptions = { ...input, ...options };
  } else if (!hasSecondArgument && hasMutationOptionFields(input)) {
    callInput = undefined;
    callOptions = input;
  }
  if (callOptions === undefined) return { input: callInput };
  if (!isMutationOptions(callOptions)) throw new TypeError("Invalid mutation options.");
  return { input: callInput, options: callOptions };
}

function cancelPending<C extends Contract>(state: GraphRuntimeState<C>, reason?: unknown): void {
  if (state.disposed) return;
  const cancellation = reason === undefined ? new AbortError("Graph work was canceled.") : reason;
  const operation = state.resetOperation;
  const controllers = [...state.activeControllers];
  const controllerSet = new Set(controllers);
  const resetRequests =
    operation === undefined
      ? []
      : [...state.stores.values()].flatMap((runtime) =>
          [...runtime.requests].filter(
            (request) =>
              request.callerAbortSignal === operation.signal &&
              controllerSet.has(request.controller),
          ),
        );
  const streams = [...state.stores.values()].flatMap((runtime) => {
    const session = runtime.stream;
    return session !== undefined && !session.ended ? [{ runtime, session }] : [];
  });
  const projections = [...state.projections].flatMap((projection) => {
    const cancel = projection.captureCancellation();
    return cancel === undefined ? [] : [cancel];
  });
  const streamControllers = new Set(streams.map(({ session }) => session.controller));
  const failures: unknown[] = [];
  const attempt = (action: () => void): void => {
    try {
      action();
    } catch (error) {
      failures.push(error);
    }
  };
  if (operation !== undefined) {
    if (state.resetOperation === operation) state.resetOperation = undefined;
    attempt(() => operation.reject(cancellation));
    // Preserve the cancellation reason for captured queries before aborting their reset signal.
    for (const request of resetRequests) attempt(() => request.removeCallerAbort());
    attempt(() => operation.abort(cancellation));
  }
  for (const controller of controllers)
    if (!streamControllers.has(controller)) attempt(() => controller.abort(cancellation));
  for (const { runtime, session } of streams)
    attempt(() => {
      endStream(runtime, session, "closed", undefined, { abortReason: cancellation });
    });
  for (const cancel of projections) attempt(() => cancel(cancellation));
  if (failures.length > 0)
    throw new AggregateError(failures, "Several graph cancellation operations failed");
}

function* disposalSteps<C extends Contract>(
  state: GraphRuntimeState<C>,
): Generator<() => void, void, void> {
  if (state.resetOperation !== undefined) {
    const operation = state.resetOperation;
    yield () => operation.reject(new Fault("disposed", []));
    yield () => operation.abort(new Fault("disposed", []));
  }
  state.resetOperation = undefined;
  if (state.stopAbort !== undefined) yield () => state.stopAbort?.();
  state.stopAbort = undefined;
  const collectionTimer = state.collectionTimer;
  if (collectionTimer != null) yield () => clearTimeout(collectionTimer);
  state.collectionTimer = undefined;
  state.collectionHeap.length = 0;
  for (const projection of Array.from(state.projections)) {
    yield () => {
      try {
        projection.dispose();
      } catch (error) {
        reportContinuationError(state, error);
      }
    };
  }
  state.projections.clear();
  for (const runtime of state.stores.values()) {
    const timer = runtime.timer;
    if (timer != null) yield () => clearTimeout(timer);
    runtime.timer = undefined;
    for (const stop of runtime.livenessStops) yield stop;
    for (const stop of runtime.readStops) yield stop;
    runtime.livenessStops.length = 0;
    runtime.readStops.length = 0;
    const stream = runtime.stream;
    if (stream != null && !stream.ended) {
      yield () => {
        try {
          endStream(runtime, stream, "closed");
        } catch (error) {
          reportContinuationError(state, error);
        }
      };
    }
    yield () => runtime.presence.set(false);
    runtime.disposed = true;
    runtime.collectionIndex = -1;
    yield () => {
      try {
        physicalCleanup(state.retirement, () => runtime.lifecycle.update((value) => value + 1));
      } catch (error) {
        reportContinuationError(state, error);
      }
    };
  }
  for (const controller of Array.from(state.activeControllers)) yield () => controller.abort();
  state.activeControllers.clear();
  yield () => __unregisterFaultChannel(state.graph);
  state.faultObservers.clear();
}

function disposeGraph<C extends Contract>(
  state: GraphRuntimeState<C>,
  physical?: PhysicalWork,
): void {
  if (physical === undefined && state.disposed) return;
  let progress = state.disposal;
  if (progress?.complete) return;
  if (progress?.active) {
    if (physical !== undefined && progress.resumeWork === undefined)
      progress.resumeWork = beginPhysicalWork(state.retirement, "cleanup");
    return;
  }
  const work = physical ?? beginPhysicalWork(state.retirement, "cleanup");
  try {
    if (progress === undefined) {
      progress = { steps: disposalSteps(state), active: false, complete: false };
      state.disposal = progress;
      state.disposed = true;
    }
    progress.active = true;
    runDisposal(state, progress, physical !== undefined);
  } catch (cause) {
    recordCleanupFailure(work, cause);
    throw cause;
  } finally {
    if (progress !== undefined) {
      progress.active = false;
      const resume = progress.resumeWork;
      if (resume !== undefined) {
        progress.resumeWork = undefined;
        if (!progress.complete) {
          progress.active = true;
          try {
            runDisposal(state, progress, true);
          } catch (cause) {
            recordCleanupFailure(resume, cause);
            reportContinuationError(state, cause);
          } finally {
            progress.active = false;
          }
        }
        finishPhysicalWork(resume);
      }
    }
    finishPhysicalWork(work);
  }
}

function runDisposal<C extends Contract>(
  state: GraphRuntimeState<C>,
  progress: NonNullable<GraphRuntimeState<C>["disposal"]>,
  retiring: boolean,
): void {
  while (!progress.complete) {
    const step = progress.steps.next();
    if (step.done) {
      progress.complete = true;
      break;
    }
    if (!retiring) {
      step.value();
      continue;
    }
    try {
      physicalCleanup(state.retirement, step.value);
    } catch (cause) {
      reportContinuationError(state, cause);
    }
  }
}

/**
 * Creates an isolated state graph for a contract.
 *
 * @param options - Contract, lifetime signal, and resource bounds for the graph.
 * @returns A graph whose typed API is derived from `options.contract`.
 * @throws A {@link Fault} If a numeric resource bound is invalid.
 * @throws TypeError if the contract, cancellation signal, or diagnostic callback is malformed.
 */
export function createGraph<C extends Contract>(options: GraphOptions<C>): Graph<C> {
  if (!isCancellationOptions(options)) throw new TypeError("Invalid graph cancellation options.");
  if (options.onUnobservedFault !== undefined && typeof options.onUnobservedFault !== "function")
    throw new TypeError("Graph onUnobservedFault must be a function.");
  validateContract(options.contract);
  const idleMs = graphIdleMs(options.idleMs === undefined ? DEFAULT_IDLE_MS : options.idleMs);
  const maxPredictions = finiteBound(
    options.maxPredictions === undefined ? DEFAULT_MAX_PREDICTIONS : options.maxPredictions,
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
    retirement: createRetirementOwner(),
    resetCleanupScopes: new Set(),
    projections: new Set(),
    collectionHeap: [],
    sweepScheduled: false,
    nextPredictionId: 0,
    resetEpoch: 0,
    resetVersion: signal(0),
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
    resetVersion: computed(() => state.resetVersion.get(), { label: `graph:${id}:reset-version` }),
    reset: () => resetGraph(state),
    cancelPending: (reason?: unknown) => cancelPending(state, reason),
    store: graphStore,
    at: graphAt,
    dispose: () => disposeGraph(state),
    retire: () => {
      if (state.retirement.promise !== undefined) return state.retirement.promise;
      if (!state.disposed) reactiveInternal.assertWritesAllowed(state.resetVersion);
      return retireOwner(state.retirement, (work) => {
        try {
          disposeGraph(state, work);
        } finally {
          if (state.disposed) drainResetCleanups(state);
        }
      });
    },
  });
  graphRuntimeByGraph.set(graph, state);
  __registerFaultChannel(graph, onFault);
  const acquisition = beginPhysicalWork(state.retirement, "acquisition");
  try {
    const abortSignal = options.abortSignal;
    if (abortSignal != null) {
      if (abortSignal.aborted || state.disposed) disposeGraph(state);
      else {
        const abort = (): void => disposeGraph(state);
        const stop = (): void => abortSignal.removeEventListener("abort", abort);
        state.stopAbort = stop;
        try {
          abortSignal.addEventListener("abort", abort, { once: true });
        } catch (cause) {
          state.stopAbort = undefined;
          try {
            physicalCleanup(state.retirement, stop);
          } catch (cleanupCause) {
            reportContinuationError(state, cleanupCause);
          }
          throw cause;
        }
        if (state.disposed) {
          state.stopAbort = undefined;
          physicalCleanup(state.retirement, stop);
        }
      }
    }
    return graph;
  } finally {
    finishPhysicalWork(acquisition);
  }
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
