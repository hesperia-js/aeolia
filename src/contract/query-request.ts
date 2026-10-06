import type {
  QueryCaller,
  QueryOutcome,
  QueryRequest,
  QuerySource,
  StoreRuntime,
} from "./runtime.ts";
import { __internal as reactiveInternal } from "../reactive.ts";
import { addSignalAbort } from "../utils.ts";
import { Fault } from "../fault.ts";
import { assertStoreOpen, reportContinuationError } from "./faults.ts";
import {
  isCurrentRequest,
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
  const current =
    !request.superseded &&
    request.generation === runtime.generation &&
    request.resetEpoch === runtime.graph.__runtime.resetEpoch;
  if (current) runtime.lastSettledAt = Date.now();
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
): Promise<void> {
  return (
    startQueryRequest(runtime, caller, force, abortSignal, automaticRecovery)?.promise ??
    Promise.resolve()
  );
}

export function startQueryRequest<T>(
  runtime: StoreRuntime<T>,
  caller: QueryCaller<T>,
  force: boolean,
  abortSignal?: AbortSignal,
  automaticRecovery = false,
): QueryRequest | undefined {
  assertStoreOpen(runtime);
  reactiveInternal.assertWritesAllowed(runtime.committed);
  const state = runtime.graph.__runtime;
  const resetEpoch = state.resetEpoch;
  interact(state);
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
    runtime.activeRequest.superseded = true;
    runtime.activeRequest.controller.abort();
    if (state.resetEpoch !== resetEpoch || state.disposed) return undefined;
  }
  if (runtime.timer != null) {
    clearTimeout(runtime.timer);
    runtime.timer = undefined;
  }
  const controller = new AbortController();
  const removeCallerAbort = addSignalAbort(abortSignal, controller);
  const generation = issue(runtime);
  const source: QuerySource<T> = { query: caller.query, input: caller.input };
  let resolveOutcome!: QueryRequest["resolveOutcome"];
  const outcome = new Promise<QueryOutcome>((resolve) => {
    resolveOutcome = resolve;
  });
  const request: QueryRequest = {
    controller,
    generation,
    resetEpoch,
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
      runtime.failing = false;
      runtime.errorCell.set(undefined);
      touch(runtime);
      writeAllStatuses(runtime);
    });
  } catch (error) {
    setupError = error;
    setupFailed = true;
  }
  if (state.resetEpoch !== resetEpoch || state.disposed) {
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
  controller.signal.addEventListener("abort", onRequestAbort, { once: true });
  request.removeRequestAbort = () => controller.signal.removeEventListener("abort", onRequestAbort);
  if (controller.signal.aborted) onRequestAbort();
  if (request.resetEpoch !== state.resetEpoch || runtime.disposed) return request;
  let result: Promise<T>;
  try {
    const options = {
      abortSignal: controller.signal,
      graph: state.graph.id,
      key: runtime.key,
    };
    result = Promise.resolve(caller.query.fetch(caller.input, options));
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
  return request;
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
