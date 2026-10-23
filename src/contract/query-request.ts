import type {
  QueryCaller,
  QueryOutcome,
  QueryRequest,
  QuerySource,
  StoreRuntime,
} from "./runtime.ts";
import type { QueryFetchOptions } from "./types.ts";
import { __internal as reactiveInternal } from "../reactive.ts";
import { Fault } from "../fault.ts";
import { AbortError, assertStoreOpen, reportContinuationError } from "./faults.ts";
import {
  isCurrentRequest,
  createCaller,
  asWindow,
  writeAllStatuses,
  writeElapsedStatuses,
} from "./status.ts";
import { interact, scheduleSweep, touch, updateCollectionCandidate } from "./collection.ts";
import { issue, nextGeneration, recordLanding, retirePredictions } from "./store-state.ts";
import {
  beginQueryProgress,
  completeQueryProgress,
  failQueryProgress,
  interruptQueryProgress,
  reportQueryProgress,
} from "./query-progress.ts";

import { beginBindingAddress, changeBoundAddress, endBindingAddress } from "./registry.ts";
import {
  beginPhysicalWork,
  canReplacePhysicalCarrier,
  createPhysicalCompletion,
  finishPhysicalWork,
  normalizePhysicalPromise,
  physicalCleanup,
} from "./retirement.ts";

const MAX_TIMER_DELAY_MS = 2_147_483_647;
const nativeApply = Reflect.apply;

function linkCallerAbort<T>(
  runtime: StoreRuntime<T>,
  signal: AbortSignal | undefined,
  controller: AbortController,
): () => void {
  if (signal === undefined) return () => undefined;
  const abort = (): void => controller.abort(signal.reason);
  const remove = (): void => signal.removeEventListener("abort", abort);
  try {
    if (signal.aborted) abort();
    else signal.addEventListener("abort", abort, { once: true });
    return remove;
  } catch (error) {
    try {
      physicalCleanup(
        runtime.binding?.context.retirement ?? runtime.graph.__runtime.retirement,
        remove,
      );
    } catch (cleanupError) {
      reportContinuationError(runtime.graph.__runtime, cleanupError);
    }
    throw error;
  }
}

export function finishRequest<T>(runtime: StoreRuntime<T>, request: QueryRequest): boolean {
  if (request.settled) return false;
  request.settled = true;
  changeBoundAddress(runtime);
  const owner = runtime.binding?.context.retirement ?? runtime.graph.__runtime.retirement;
  let cleanupFailed = false;
  let cleanupError: unknown;
  for (const remove of [request.removeCallerAbort, request.removeRequestAbort]) {
    if (remove === undefined) continue;
    try {
      physicalCleanup(owner, remove);
    } catch (error) {
      if (!cleanupFailed) cleanupError = error;
      cleanupFailed = true;
    }
  }
  request.removeRequestAbort = undefined;
  runtime.graph.__runtime.activeControllers.delete(request.controller);
  runtime.requests.delete(request);
  updateCollectionCandidate(runtime);
  const current =
    !request.superseded &&
    request.generation === runtime.generation &&
    request.resetEpoch === runtime.graph.__runtime.resetEpoch;
  if (current) runtime.lastSettledAt = Date.now();
  if (cleanupFailed) throw cleanupError;
  return current;
}

function reportRequestOutcome(
  request: QueryRequest,
  outcome: { readonly ok: true } | { readonly ok: false; readonly error: unknown },
): void {
  if (request.outcomeReported) return;
  request.outcomeReported = true;
  request.resolveOutcome(outcome);
}

export function settleQuerySuccess<T>(
  runtime: StoreRuntime<T>,
  request: QueryRequest,
  value: T,
): void {
  reportRequestOutcome(request, { ok: true });
  const state = runtime.graph.__runtime;
  const current = finishRequest(runtime, request);
  const wasActive = runtime.activeRequest === request;
  if (wasActive) runtime.activeRequest = undefined;
  if (!current || runtime.disposed || !wasActive) {
    scheduleSweep(runtime.graph.__runtime);
    return;
  }
  runtime.recoveryDisarmed = false;
  try {
    reactiveInternal.batch(() => {
      retirePredictions(runtime, request.predictionIds);
      recordLanding(runtime, value, request.generation);
    });
  } catch (error) {
    if (!runtime.disposed && request.resetEpoch === state.resetEpoch)
      reportContinuationError(state, error);
  }
  completeQueryProgress(runtime, request);
  if (!runtime.disposed && request.resetEpoch === state.resetEpoch) {
    try {
      armStoreTimer(runtime);
    } catch (error) {
      if (!runtime.disposed && request.resetEpoch === state.resetEpoch)
        reportContinuationError(state, error);
    }
  }
}

export function settleQueryFailure<T>(
  runtime: StoreRuntime<T>,
  request: QueryRequest,
  reason: unknown,
): void {
  reportRequestOutcome(request, { ok: false, error: reason });
  const state = runtime.graph.__runtime;
  const current = finishRequest(runtime, request);
  const wasActive = runtime.activeRequest === request;
  if (wasActive) runtime.activeRequest = undefined;
  if (!current || runtime.disposed || !wasActive) {
    scheduleSweep(runtime.graph.__runtime);
    return;
  }
  runtime.recoveryDisarmed = request.automaticRecovery;
  try {
    reactiveInternal.batch(() => {
      runtime.failing = true;
      runtime.invalidated = true;
      runtime.errorCell.set(reason);
      if (request.controller.signal.aborted) interruptQueryProgress(runtime, request, reason);
      else failQueryProgress(runtime, request, reason);
      touch(runtime);
      writeAllStatuses(runtime);
    });
  } catch (error) {
    if (!runtime.disposed && request.resetEpoch === state.resetEpoch)
      reportContinuationError(state, error);
  }
  if (runtime.disposed || request.resetEpoch !== state.resetEpoch) return;
  if (request.automaticRecovery)
    reportContinuationError(
      state,
      new Fault("contract", [runtime.key as string, "automatic query recovery exhausted"]),
    );
  if (!runtime.disposed && request.resetEpoch === state.resetEpoch) {
    try {
      armStoreTimer(runtime);
    } catch (error) {
      if (!runtime.disposed && request.resetEpoch === state.resetEpoch)
        reportContinuationError(state, error);
    }
  }
}

export function startQuery<T>(
  runtime: StoreRuntime<T>,
  caller: QueryCaller<T>,
  force: boolean,
  abortSignal?: AbortSignal,
  automaticRecovery = false,
  expectedIncarnation?: object,
): Promise<void> {
  return (
    startQueryRequest(runtime, caller, force, abortSignal, automaticRecovery, expectedIncarnation)
      ?.promise ?? Promise.resolve()
  );
}

export function startQueryRequest<T>(
  runtime: StoreRuntime<T>,
  caller: QueryCaller<T>,
  force: boolean,
  abortSignal?: AbortSignal,
  automaticRecovery = false,
  expectedIncarnation?: object,
): QueryRequest | undefined {
  assertStoreOpen(runtime);
  reactiveInternal.assertWritesAllowed(runtime.committed);
  const state = runtime.graph.__runtime;
  const owner = runtime.binding?.context.retirement ?? state.retirement;
  const acquisition = beginPhysicalWork(owner, "acquisition");
  let admission: object | undefined;
  try {
    const resetEpoch = state.resetEpoch;
    const entryIncarnation =
      runtime.binding === undefined ? undefined : (expectedIncarnation ?? runtime.incarnation);
    if (entryIncarnation !== undefined && runtime.incarnation !== entryIncarnation)
      return undefined;
    if ((force || isCurrentRequest(runtime) === undefined) && (force || !runtime.recoveryDisarmed))
      nextGeneration(runtime.generation);
    admission = beginBindingAddress(runtime.binding, String(runtime.key));
    // Effects supply their ticket before interaction can collect or replace the entry.
    interact(state);
    if (runtime.binding !== undefined) {
      assertStoreOpen(runtime);
      if (
        state.resetEpoch !== resetEpoch ||
        (entryIncarnation !== undefined && runtime.incarnation !== entryIncarnation)
      )
        return undefined;
    }
    if (!force) {
      const existing = isCurrentRequest(runtime);
      if (existing != null) {
        runtime.refreshOnActivation = false;
        return existing;
      }
    }
    if (!force && runtime.recoveryDisarmed) return undefined;
    runtime.refreshOnActivation = false;
    if (force) {
      runtime.recoveryDisarmed = false;
    }
    if (runtime.activeRequest != null && !runtime.activeRequest.settled) {
      const predecessor = runtime.activeRequest;
      predecessor.superseded = true;
      predecessor.controller.abort();
      interruptQueryProgress(runtime, predecessor, predecessor.controller.signal.reason);
      if (state.resetEpoch !== resetEpoch || state.disposed) return undefined;
      if (runtime.binding !== undefined) {
        assertStoreOpen(runtime);
        if (entryIncarnation !== undefined && runtime.incarnation !== entryIncarnation)
          return undefined;
      }
    }
    nextGeneration(runtime.generation);
    if (runtime.timer != null) {
      clearTimeout(runtime.timer);
      runtime.timer = undefined;
    }
    if (runtime.binding !== undefined) {
      assertStoreOpen(runtime);
      if (state.resetEpoch !== resetEpoch) return undefined;
    }
    const incarnation = runtime.incarnation ?? (runtime.incarnation = {});
    const controller = new AbortController();
    const removeCallerAbort = linkCallerAbort(runtime, abortSignal, controller);
    if (
      runtime.binding !== undefined &&
      (runtime.disposed ||
        state.disposed ||
        state.resetEpoch !== resetEpoch ||
        runtime.incarnation !== incarnation)
    ) {
      physicalCleanup(owner, removeCallerAbort);
      assertStoreOpen(runtime);
      return undefined;
    }
    let generation: number;
    try {
      generation = issue(runtime);
    } catch (error) {
      try {
        physicalCleanup(owner, removeCallerAbort);
      } catch (cleanupError) {
        reportContinuationError(state, cleanupError);
      }
      throw error;
    }
    const source: QuerySource<T> = { query: caller.query, input: caller.input };
    let resolveOutcome!: QueryRequest["resolveOutcome"];
    const outcome = new Promise<QueryOutcome>((resolve) => {
      resolveOutcome = resolve;
    });
    const request: QueryRequest = {
      controller,
      generation,
      resetEpoch,
      incarnation,
      progressIdentity: Object.freeze({}),
      automaticRecovery,
      predictionIds: reactiveInternal
        .signalValue(runtime.predictionStack)
        .filter((prediction) => prediction.succeeded && !prediction.dead)
        .map((prediction) => prediction.id),
      promise: Promise.resolve(),
      outcome,
      resolveOutcome,
      outcomeReported: false,
      removeCallerAbort,
      settled: false,
      superseded: false,
    };
    runtime.source = source;
    runtime.requests.add(request);
    updateCollectionCandidate(runtime);
    const predecessor = runtime.activeRequest;
    if (
      state.resetOperation?.epoch === resetEpoch &&
      predecessor !== undefined &&
      !predecessor.settled &&
      !predecessor.outcomeReported &&
      predecessor.resetEpoch === resetEpoch
    ) {
      predecessor.outcomeReported = true;
      void outcome.then((result) => predecessor.resolveOutcome(result));
    }
    runtime.activeRequest = request;
    state.activeControllers.add(controller);
    let setupError: unknown;
    let setupFailed = false;
    try {
      reactiveInternal.batch(() => {
        beginQueryProgress(runtime, request);
        runtime.failing = false;
        runtime.errorCell.set(undefined);
        touch(runtime);
        writeAllStatuses(runtime);
      });
    } catch (error) {
      setupError = error;
      setupFailed = true;
    }
    if (state.resetEpoch !== resetEpoch || state.disposed || runtime.disposed) {
      settleQueryFailure(
        runtime,
        request,
        state.disposed || runtime.disposed
          ? new Fault("disposed", [String(runtime.key)])
          : new AbortError("Query request was superseded before dispatch."),
      );
      if (setupFailed) reportContinuationError(state, setupError);
      return request;
    }
    const onRequestAbort = (): void => {
      if (
        !request.superseded &&
        !request.settled &&
        runtime.activeRequest === request &&
        request.generation === runtime.generation &&
        request.resetEpoch === state.resetEpoch &&
        !runtime.disposed
      )
        settleQueryFailure(runtime, request, controller.signal.reason);
    };
    request.removeRequestAbort = () =>
      controller.signal.removeEventListener("abort", onRequestAbort);
    try {
      controller.signal.addEventListener("abort", onRequestAbort, { once: true });
    } catch (error) {
      try {
        finishRequest(runtime, request);
      } catch (cleanupError) {
        reportContinuationError(state, cleanupError);
      }
      throw error;
    }
    if (controller.signal.aborted) onRequestAbort();
    if (request.resetEpoch !== state.resetEpoch || runtime.disposed) {
      settleQueryFailure(
        runtime,
        request,
        runtime.disposed
          ? new Fault("disposed", [String(runtime.key)])
          : new AbortError("Query request was superseded before dispatch."),
      );
      return request;
    }
    const original = beginPhysicalWork(owner, "query");
    let fetched: unknown;
    let fetchedReturned = false;
    let acknowledged = true;
    let result: Promise<T>;
    try {
      const options: QueryFetchOptions<unknown> = {
        abortSignal: controller.signal,
        graph: state.graph.id,
        key: runtime.key,
        reportProgress: (value) => reportQueryProgress(runtime, request, value),
      };
      fetched = caller.query.fetch(caller.input, options);
      fetchedReturned = true;
      original.original = fetched;
      result = normalizePhysicalPromise(fetched as T | PromiseLike<T>);
    } catch (error) {
      if (
        fetchedReturned &&
        fetched !== null &&
        (typeof fetched === "object" || typeof fetched === "function")
      )
        acknowledged = false;
      const rejected = createPhysicalCompletion<T>();
      rejected.reject(error);
      result = rejected.promise;
    }
    try {
      const observer = result.then;
      if (!canReplacePhysicalCarrier(result, observer)) acknowledged = false;
      request.promise = nativeApply(observer, result, [
        (value: T) => {
          try {
            settleQuerySuccess(runtime, request, value);
          } finally {
            if (acknowledged) finishPhysicalWork(original);
          }
        },
        (error: unknown) => {
          try {
            settleQueryFailure(runtime, request, error);
          } finally {
            if (acknowledged) finishPhysicalWork(original);
          }
        },
      ]) as Promise<void>;
    } catch (error) {
      original.original = fetched;
      throw error;
    }
    if (original.active) original.continuation = request.promise;
    if (setupFailed) throw setupError;
    return request;
  } finally {
    try {
      endBindingAddress(runtime.binding, String(runtime.key), admission);
    } finally {
      finishPhysicalWork(acquisition);
    }
  }
}

export function callerForSource<T>(
  runtime: StoreRuntime<T>,
  source: QuerySource<T>,
): QueryCaller<T> {
  const existing = runtime.callers.find(
    (entry) => entry.query === source.query && Object.is(entry.input, source.input),
  );
  if (existing != null) return existing;
  const caller = createCaller(
    runtime,
    source.query,
    source.input,
    asWindow(undefined, source.query.revalidateAfterMs),
  );
  runtime.callers.push(caller);
  return caller;
}

export function armStoreTimer<T>(runtime: StoreRuntime<T>): void {
  assertStoreOpen(runtime);
  if (runtime.timer != null) {
    clearTimeout(runtime.timer);
    runtime.timer = undefined;
  }
  if (runtime.recoveryDisarmed) return;
  if (runtime.dropped || isCurrentRequest(runtime) != null) return;
  const base = Math.max(runtime.lastLandingAt ?? 0, runtime.lastSettledAt ?? 0);
  if (base === 0 && runtime.lastLandingAt === undefined && runtime.lastSettledAt === undefined)
    return;
  const at = Date.now();
  let deadline: number | undefined =
    runtime.invalidated && !runtime.failing && runtime.valueLive ? at : undefined;
  for (const caller of runtime.callers) {
    if (!Number.isFinite(caller.windowMs)) continue;
    const candidate = base + caller.windowMs;
    if (candidate <= at) {
      if (runtime.valueLive || caller.statusCell.peek() === "ready") deadline = at;
      continue;
    }
    if (deadline === undefined || candidate < deadline) deadline = candidate;
  }
  if (deadline === undefined) return;
  const delay = Math.min(MAX_TIMER_DELAY_MS, Math.max(0, deadline - at));
  runtime.timer = setTimeout(() => {
    runtime.timer = undefined;
    if (runtime.disposed || runtime.dropped) return;
    const firedAt = Date.now();
    if (firedAt < deadline) {
      armStoreTimer(runtime);
      return;
    }
    try {
      writeElapsedStatuses(runtime, base, firedAt);
    } catch (error) {
      reportContinuationError(runtime.graph.__runtime, error);
    }
    if (runtime.valueLive && runtime.source != null) {
      const source = runtime.source;
      const caller = callerForSource(runtime, source);
      const recovering = runtime.failing;
      try {
        void startQuery(runtime, caller, false, undefined, recovering);
      } catch (error) {
        reportContinuationError(runtime.graph.__runtime, error);
      }
      return;
    }
    armStoreTimer(runtime);
  }, delay);
}

export function invalidateStore<T>(runtime: StoreRuntime<T>): void {
  if (runtime.disposed) return;
  interact(runtime.graph.__runtime);
  if (runtime.invalidated) {
    // Repeated invalidation is still an interaction for collection's idle
    // period, even though it does not create another state transition.
    touch(runtime);
    return;
  }
  runtime.invalidated = true;
  touch(runtime);
  writeAllStatuses(runtime);
  armStoreTimer(runtime);
}
