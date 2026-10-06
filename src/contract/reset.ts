import { __internal as reactiveInternal } from "../reactive.ts";
import { AbortError, assertGraphOpen, reportContinuationError } from "./faults.ts";
import { isLive, touch, updateCollectionCandidate } from "./collection.ts";
import { issue } from "./store-state.ts";
import { callerForSource, startQueryRequest } from "./query-request.ts";
import { openStream } from "./stream.ts";
import type { Contract } from "./types.ts";
import type {
  GraphResetOperation,
  GraphRuntimeState,
  QueryCaller,
  QueryRequest,
  StreamSession,
  StreamSource,
  StoreRuntime,
} from "./runtime.ts";

interface ActiveQuery<T> {
  readonly runtime: StoreRuntime<T>;
  readonly caller: QueryCaller<T>;
}

interface ActiveStream<T> {
  readonly runtime: StoreRuntime<T>;
  readonly source: StreamSource<T>;
  readonly key: StoreRuntime<T>["key"];
}

interface ResetWork {
  readonly requests: QueryRequest[];
  readonly failures: unknown[];
}

function detachRequest<C extends Contract>(
  state: GraphRuntimeState<C>,
  runtime: StoreRuntime<unknown>,
  request: QueryRequest,
  reason: unknown,
): AbortController | undefined {
  if (request.settled) return undefined;
  request.superseded = true;
  request.settled = true;
  request.removeCallerAbort();
  request.removeRequestAbort?.();
  request.removeRequestAbort = undefined;
  request.outcomeReported = true;
  request.resolveOutcome({ ok: false, error: reason });
  runtime.requests.delete(request);
  if (runtime.activeRequest === request) runtime.activeRequest = undefined;
  state.activeControllers.delete(request.controller);
  return request.controller;
}

function clearRuntime(runtime: StoreRuntime<unknown>): void {
  if (runtime.timer !== undefined) clearTimeout(runtime.timer);
  runtime.timer = undefined;
  runtime.generation = issue(runtime);
  runtime.hasCommitted = runtime.declared;
  runtime.initial = runtime.declared ? runtime.initial : undefined;
  runtime.initialSpecified = runtime.declared;
  runtime.refreshOnActivation = !runtime.declared && runtime.source !== undefined;
  runtime.failing = false;
  runtime.recoveryDisarmed = false;
  runtime.invalidated = false;
  runtime.lastLandingAt = runtime.declared ? Date.now() : undefined;
  runtime.lastSettledAt = undefined;
  runtime.errorCell.set(undefined);
  for (const prediction of reactiveInternal.signalValue(runtime.predictionStack))
    prediction.dead = true;
  runtime.predictionStack.set([]);
  runtime.committed.set(runtime.declared ? runtime.initial : undefined);
  runtime.presence.set(runtime.declared);
  runtime.streamStatusCell.set(runtime.streamSource?.abortSignal?.aborted ? "closed" : "empty");
  for (const caller of runtime.callers) caller.statusCell.set("empty");
  touch(runtime);
  updateCollectionCandidate(runtime);
}

function currentOperation<C extends Contract>(
  state: GraphRuntimeState<C>,
  operation: GraphResetOperation,
): boolean {
  return (
    !state.disposed && state.resetOperation === operation && state.resetEpoch === operation.epoch
  );
}

function dispatchStreams<C extends Contract>(
  state: GraphRuntimeState<C>,
  operation: GraphResetOperation,
  streams: readonly ActiveStream<unknown>[],
): void {
  for (const active of streams) {
    if (!currentOperation(state, operation)) return;
    const runtime = active.runtime;
    if (runtime.disposed) continue;
    if (active.source.abortSignal?.aborted) continue;
    if (runtime.stream !== undefined && !runtime.stream.ended) continue;
    try {
      openStream(
        runtime,
        active.source.stream,
        active.source.input,
        active.key,
        active.source.abortSignal,
      );
    } catch (error) {
      reportContinuationError(state, error);
    }
    if (!currentOperation(state, operation)) return;
  }
}

function dispatchQueries<C extends Contract>(
  state: GraphRuntimeState<C>,
  operation: GraphResetOperation,
  queries: readonly ActiveQuery<unknown>[],
  work: ResetWork,
): void {
  const seen = new Set<QueryRequest>();
  for (const active of queries) {
    if (!currentOperation(state, operation)) return;
    const runtime = active.runtime;
    if (runtime.disposed) continue;
    const existing = runtime.activeRequest;
    if (existing !== undefined && !existing.settled && existing.resetEpoch === operation.epoch) {
      if (!seen.has(existing)) {
        seen.add(existing);
        work.requests.push(existing);
      }
      continue;
    }
    let started: QueryRequest | undefined;
    try {
      started = startQueryRequest(
        runtime,
        active.caller,
        true,
        state.resetOperation === operation ? operation.signal : undefined,
      );
    } catch (error) {
      const current = runtime.activeRequest;
      if (current !== undefined && !current.settled && current.resetEpoch === operation.epoch)
        reportContinuationError(state, error);
      else work.failures.push(error);
    }
    if (!currentOperation(state, operation)) return;
    const current = runtime.activeRequest;
    const required =
      current !== undefined && !current.settled && current.resetEpoch === operation.epoch
        ? current
        : started?.resetEpoch === operation.epoch
          ? started
          : undefined;
    if (required !== undefined && !seen.has(required)) {
      seen.add(required);
      work.requests.push(required);
    }
  }
}

function awaitOutcomes<C extends Contract>(
  state: GraphRuntimeState<C>,
  operation: GraphResetOperation,
  work: ResetWork,
  promise: Promise<void>,
  resolve: () => void,
  reject: (error: unknown) => void,
): void {
  const cancellation = new Promise<never>((_resolve, rejectCancellation) => {
    const signal = operation.signal;
    if (signal.aborted) rejectCancellation(signal.reason);
    else signal.addEventListener("abort", () => rejectCancellation(signal.reason), { once: true });
  });
  void Promise.race([promise, cancellation]).then(
    () => {
      if (!currentOperation(state, operation)) return;
      state.resetOperation = undefined;
      if (work.failures.length === 1) reject(work.failures[0]);
      else if (work.failures.length > 1)
        reject(new AggregateError(work.failures, "Several graph reset operations failed"));
      else resolve();
    },
    (error) => {
      if (!currentOperation(state, operation)) return;
      state.resetOperation = undefined;
      reject(error);
    },
  );
}

export function resetGraph<C extends Contract>(state: GraphRuntimeState<C>): Promise<void> {
  assertGraphOpen(state);
  reactiveInternal.assertWritesAllowed(state.resetVersion);

  const queries: ActiveQuery<unknown>[] = [];
  const streams: ActiveStream<unknown>[] = [];
  for (const runtime of state.stores.values()) {
    if (!isLive(runtime)) continue;
    if (runtime.source !== undefined)
      queries.push({
        runtime,
        caller: callerForSource(runtime, runtime.source),
      });
    if (runtime.streamSource !== undefined)
      streams.push({ runtime, source: runtime.streamSource, key: runtime.key });
  }
  const projections = [...state.projections];

  const previous = state.resetOperation;
  const epoch = state.resetEpoch + 1;
  state.resetEpoch = epoch;
  let resolvePromise!: () => void;
  let rejectPromise!: (error: unknown) => void;
  let settled = false;
  const promise = new Promise<void>((resolve, reject) => {
    resolvePromise = resolve;
    rejectPromise = reject;
  });
  const controller = new AbortController();
  const operation: GraphResetOperation = {
    epoch,
    signal: controller.signal,
    reject(error) {
      if (settled) return;
      settled = true;
      rejectPromise(error);
    },
    abort(reason) {
      if (!controller.signal.aborted) controller.abort(reason);
    },
  };
  state.resetOperation = operation;
  previous?.reject(new AbortError("A newer graph reset superseded this reset."));

  const oldControllers: AbortController[] = [];
  const oldSessions: StreamSession<unknown>[] = [];
  for (const runtime of state.stores.values()) {
    for (const request of runtime.requests) {
      const detached = detachRequest(
        state,
        runtime,
        request,
        new AbortError("A graph reset canceled this query request."),
      );
      if (detached !== undefined) oldControllers.push(detached);
    }
    runtime.requests.clear();
    runtime.activeRequest = undefined;
    const session = runtime.stream;
    if (session !== undefined && !session.ended) {
      session.ended = true;
      runtime.stream = undefined;
      session.listeners.clear();
      state.activeControllers.delete(session.controller);
      oldSessions.push(session);
    }
  }
  oldControllers.push(...state.activeControllers);
  state.activeControllers.clear();

  const projectionCleanup: Array<() => void> = [];
  try {
    reactiveInternal.batch(() => {
      for (const projection of projections) {
        if (!currentOperation(state, operation)) return;
        try {
          const cleanup = projection.reset();
          if (cleanup !== undefined) projectionCleanup.push(cleanup);
        } catch (error) {
          reportContinuationError(state, error);
        }
      }
      for (const runtime of state.stores.values()) {
        if (!currentOperation(state, operation)) return;
        try {
          clearRuntime(runtime);
        } catch (error) {
          reportContinuationError(state, error);
        }
      }
      if (currentOperation(state, operation)) state.resetVersion.set(epoch);
    });
  } catch (error) {
    reportContinuationError(state, error);
  }

  previous?.abort(new AbortError("A newer graph reset superseded this reset."));
  for (const controller of oldControllers)
    controller.abort(new AbortError("A graph reset canceled work from the previous state."));
  for (const session of oldSessions) {
    session.controller.abort(
      new AbortError("A graph reset canceled work from the previous state."),
    );
    const iterator = session.iterator;
    if (iterator != null && typeof iterator.return === "function") {
      try {
        void Promise.resolve(iterator.return()).catch(() => undefined);
      } catch {
        // The session has already been retired from the graph.
      }
    }
  }
  for (const cleanup of projectionCleanup) cleanup();

  if (!currentOperation(state, operation)) return promise;

  dispatchStreams(state, operation, streams);
  if (!currentOperation(state, operation)) return promise;
  for (const projection of projections) {
    try {
      projection.restart();
    } catch (error) {
      reportContinuationError(state, error);
    }
    if (!currentOperation(state, operation)) return promise;
  }

  const work: ResetWork = { requests: [], failures: [] };
  dispatchQueries(state, operation, queries, work);
  const outcomes = Promise.all(work.requests.map((request) => request.outcome)).then(
    (settlements) => {
      work.failures.push(
        ...settlements.flatMap((settlement) => (settlement.ok ? [] : [settlement.error])),
      );
    },
  );
  awaitOutcomes(
    state,
    operation,
    work,
    outcomes,
    () => {
      if (settled) return;
      settled = true;
      resolvePromise();
    },
    (error) => {
      operation.reject(error);
    },
  );
  return promise;
}
