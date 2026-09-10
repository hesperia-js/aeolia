import type { QueryCaller, QueryRequest, QuerySource, StoreRuntime } from "./runtime.ts";
import { __internal as reactiveInternal } from "../reactive.ts";
import { addSignalAbort } from "../utils.ts";
import { assertStoreOpen, reportContinuationError } from "./faults.ts";
import {
  currentRequest,
  createCaller,
  asWindow,
  writeAllStatuses,
  writeElapsedStatuses,
} from "./status.ts";
import { interact, scheduleSweep, touch, updateCollectionCandidate } from "./collection.ts";
import { issue, recordLanding, retirePredictions } from "./store-state.ts";

const MAX_TIMER_DELAY_MS = 2_147_483_647;

export function finishRequest<T>(runtime: StoreRuntime<T>, request: QueryRequest): boolean {
  if (request.settled) return false;
  request.settled = true;
  request.removeCallerAbort();
  request.removeRequestAbort?.();
  request.removeRequestAbort = undefined;
  runtime.graph.__runtime.activeControllers.delete(request.controller);
  runtime.requests.delete(request);
  updateCollectionCandidate(runtime);
  const current = !request.superseded && request.generation === runtime.generation;
  if (current) runtime.lastSettledAt = Date.now();
  return current;
}

export function settleQuerySuccess<T>(
  runtime: StoreRuntime<T>,
  request: QueryRequest,
  value: T,
): void {
  const current = finishRequest(runtime, request);
  const wasActive = runtime.activeRequest === request;
  if (wasActive) runtime.activeRequest = undefined;
  if (!current || runtime.disposed || !wasActive) {
    scheduleSweep(runtime.graph.__runtime);
    return;
  }
  try {
    reactiveInternal.batch(() => {
      retirePredictions(runtime, request.predictionIds);
      recordLanding(runtime, value, request.generation);
    });
  } catch (error) {
    reportContinuationError(runtime.graph.__runtime, error);
  }
  armStoreTimer(runtime);
}

export function settleQueryFailure<T>(
  runtime: StoreRuntime<T>,
  request: QueryRequest,
  reason: unknown,
): void {
  const current = finishRequest(runtime, request);
  const wasActive = runtime.activeRequest === request;
  if (wasActive) runtime.activeRequest = undefined;
  if (!current || runtime.disposed || !wasActive) {
    scheduleSweep(runtime.graph.__runtime);
    return;
  }
  try {
    runtime.failing = true;
    runtime.invalidated = true;
    runtime.errorCell.set(reason);
    touch(runtime);
    writeAllStatuses(runtime);
  } catch (error) {
    reportContinuationError(runtime.graph.__runtime, error);
  }
  armStoreTimer(runtime);
}

export function startQuery<T>(
  runtime: StoreRuntime<T>,
  caller: QueryCaller<T>,
  force: boolean,
  abortSignal?: AbortSignal,
): Promise<void> {
  assertStoreOpen(runtime);
  const state = runtime.graph.__runtime;
  interact(state);
  if (!force) {
    const existing = currentRequest(runtime);
    if (existing != null) return existing.promise;
  }
  if (runtime.activeRequest != null && !runtime.activeRequest.settled) {
    runtime.activeRequest.superseded = true;
    runtime.activeRequest.controller.abort();
  }
  if (runtime.timer != null) {
    clearTimeout(runtime.timer);
    runtime.timer = undefined;
  }
  const controller = new AbortController();
  const removeCallerAbort = addSignalAbort(abortSignal, controller);
  const generation = issue(runtime);
  const source: QuerySource<T> = { query: caller.query, input: caller.input };
  const request: QueryRequest = {
    controller,
    generation,
    predictionIds: runtime.predictionStack
      .peek()
      .filter((prediction) => prediction.succeeded && !prediction.dead)
      .map((prediction) => prediction.id),
    promise: Promise.resolve(),
    removeCallerAbort,
    settled: false,
    superseded: false,
  };
  runtime.source = source;
  runtime.requests.add(request);
  updateCollectionCandidate(runtime);
  runtime.activeRequest = request;
  state.activeControllers.add(controller);
  let setupError: unknown;
  let setupFailed = false;
  try {
    reactiveInternal.batch(() => {
      runtime.failing = false;
      runtime.errorCell.set(undefined);
      touch(runtime);
      writeAllStatuses(runtime);
    });
  } catch (error) {
    setupError = error;
    setupFailed = true;
  }
  const onRequestAbort = (): void => {
    if (
      !request.superseded &&
      !request.settled &&
      runtime.activeRequest === request &&
      request.generation === runtime.generation &&
      !runtime.disposed
    )
      settleQueryFailure(runtime, request, controller.signal.reason);
  };
  controller.signal.addEventListener("abort", onRequestAbort, { once: true });
  request.removeRequestAbort = () => controller.signal.removeEventListener("abort", onRequestAbort);
  if (controller.signal.aborted) onRequestAbort();
  let result: Promise<T>;
  try {
    result = Promise.resolve(
      caller.query.fetch(caller.input, {
        abortSignal: controller.signal,
        graph: state.graph.id,
        key: runtime.key,
      }),
    );
  } catch (error) {
    result = Promise.reject(error);
  }
  request.promise = result.then(
    (value) => {
      settleQuerySuccess(runtime, request, value);
    },
    (error) => {
      settleQueryFailure(runtime, request, error);
    },
  );
  if (setupFailed) throw setupError;
  return request.promise;
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
  if (runtime.dropped || currentRequest(runtime) != null) return;
  const base = Math.max(runtime.lastLandingAt ?? 0, runtime.lastSettledAt ?? 0);
  if (base === 0 && runtime.lastLandingAt === undefined && runtime.lastSettledAt === undefined)
    return;
  const at = Date.now();
  let deadline: number | undefined;
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
      try {
        void startQuery(runtime, caller, true);
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
