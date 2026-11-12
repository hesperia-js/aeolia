import { __internal as reactiveInternal } from "../reactive.ts";
import { AbortError, assertGraphOpen, reportContinuationError } from "./faults.ts";
import { endStream, touch, updateCollectionCandidate } from "./collection.ts";
import { nextGeneration } from "./store-state.ts";
import { retireObservation } from "./observation-state.ts";
import { nextValueRevision } from "./identity.ts";
import { callerForSource, startQueryRequest } from "./query-request.ts";
import { interruptQueryProgress } from "./query-progress.ts";
import { openStream } from "./stream.ts";
import {
  beginPhysicalWork,
  finishPhysicalWork,
  observePhysicalWork,
  observeUnreturnedPromise,
  retainResetCleanup,
} from "./retirement.ts";
import type { ResetCleanupScope } from "./retirement.ts";
import { noteTransferChange, registerTransferWork } from "./query-policy.ts";
import type { Contract } from "./types.ts";
import type {
  GraphResetOperation,
  GraphRuntimeState,
  QueryCaller,
  QueryRequest,
  QuerySource,
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
  noteTransferChange(runtime);
  noteTransferChange(request);
  request.superseded = true;
  request.settled = true;
  interruptQueryProgress(runtime, request, reason, { authorityChanged: true });
  const failures: unknown[] = [];
  for (const remove of [request.removeCallerAbort, request.removeRequestAbort]) {
    if (remove === undefined) continue;
    try {
      remove();
    } catch (cause) {
      failures.push(cause);
    }
  }
  if (failures.length !== 0) throw failures[0];
  request.removeRequestAbort = undefined;
  request.outcomeReported = true;
  request.resolveOutcome({ ok: false, error: reason });
  runtime.requests.delete(request);
  if (runtime.activeRequest === request) runtime.activeRequest = undefined;
  state.activeControllers.delete(request.controller);
  return request.controller;
}

function clearRuntime(
  runtime: StoreRuntime<unknown>,
  current: () => boolean,
  generation: number,
): void {
  const streamToken = runtime.streamToken;
  const incarnation = runtime.incarnation;
  const revision = runtime.valueRevision;
  const owns = (): boolean =>
    current() &&
    !runtime.disposed &&
    runtime.streamToken === streamToken &&
    runtime.valueRevision === revision &&
    runtime.incarnation === incarnation &&
    runtime.generation === generation;
  if (!owns()) return;
  noteTransferChange(runtime);
  if (runtime.timer !== undefined) clearTimeout(runtime.timer);
  runtime.timer = undefined;
  const readerInitial = runtime.readerScope !== undefined && runtime.initialSpecified;
  const resetInitial = runtime.declared || readerInitial;
  runtime.baseAvailable = resetInitial;
  runtime.hasAcceptedLanding = resetInitial;
  runtime.transferEligible = true;
  runtime.initial = resetInitial ? runtime.initial : undefined;
  runtime.initialSpecified = resetInitial ? runtime.initialSpecified : runtime.declared;
  runtime.refreshOnActivation = !runtime.declared && runtime.source !== undefined;
  runtime.failing = false;
  runtime.recoveryDisarmed = false;
  runtime.invalidated = false;
  runtime.lastLandingAt = runtime.declared ? Date.now() : undefined;
  runtime.lastSettledAt = undefined;
  runtime.errorCell.set(undefined);
  if (!owns()) return;
  for (const prediction of reactiveInternal.signalValue(runtime.predictionStack))
    prediction.dead = true;
  runtime.predictionStack.set([]);
  if (!owns()) return;
  reactiveInternal.guardedWrite(
    runtime.committed,
    resetInitial ? runtime.initial : undefined,
    owns,
    () => undefined,
  );
  if (!owns()) return;
  runtime.presence.set(resetInitial);
  if (!owns()) return;
  const abortSignal = runtime.streamSource?.abortSignal;
  if (!owns()) return;
  const aborted = abortSignal?.aborted;
  if (!owns()) return;
  runtime.streamStatusCell.set(runtime.readerScope !== undefined || aborted ? "closed" : "empty");
  for (const caller of runtime.callers) {
    if (!owns()) return;
    caller.statusCell.set("empty");
  }
  if (!owns()) return;
  touch(runtime);
  updateCollectionCandidate(runtime);
  if (owns()) {
    noteTransferChange(runtime);
    runtime.transitioning = false;
  }
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
  const physical = beginPhysicalWork(state.retirement, "acquisition");
  registerTransferWork(state, physical);
  const completion = Promise.race([promise, cancellation]).then(
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
  observePhysicalWork(physical, completion);
}

export function drainResetCleanups<C extends Contract>(state: GraphRuntimeState<C>): void {
  for (const scope of state.resetCleanupScopes) {
    if (scope.active) continue;
    for (const cleanup of [...scope.pending]) {
      try {
        cleanup();
      } catch (error) {
        reportContinuationError(state, error);
      }
    }
    if (scope.pending.size === 0) state.resetCleanupScopes.delete(scope);
  }
}
export function resetGraph<C extends Contract>(state: GraphRuntimeState<C>): Promise<void> {
  assertGraphOpen(state);
  reactiveInternal.assertWritesAllowed(state.resetVersion);
  const queryCandidates: Array<{
    readonly runtime: StoreRuntime<unknown>;
    readonly source: QuerySource<unknown>;
  }> = [];
  const streams: ActiveStream<unknown>[] = [];
  for (const runtime of state.stores.values()) {
    if (runtime.disposed) continue;
    const positioned = runtime.readerScope?.session.positioned;
    const positionedReaderLive = positioned !== undefined && positioned.phase !== "closed";
    const hasResetLiveness =
      runtime.liveReadableCount > runtime.metadataReadableCount ||
      runtime.progressReadableCount > 0 ||
      runtime.readyWaiterCount > 0 ||
      (runtime.stream?.listeners.size ?? 0) > 0;
    if (
      (hasResetLiveness || positionedReaderLive || runtime.metadataReadableCount > 0) &&
      runtime.source !== undefined
    )
      queryCandidates.push({ runtime, source: runtime.source });
    if (
      (hasResetLiveness || positionedReaderLive) &&
      runtime.readerScope === undefined &&
      runtime.binding?.kind !== "stream" &&
      runtime.streamSource !== undefined
    )
      streams.push({ runtime, source: runtime.streamSource, key: runtime.key });
  }
  const refreshes = new Set(queryCandidates.map(({ runtime }) => runtime));
  for (const runtime of state.stores.values()) {
    if (!runtime.disposed) {
      const generation = nextGeneration(runtime.generation);
      if (refreshes.has(runtime)) nextGeneration(generation);
    }
    nextValueRevision(runtime.valueRevision);
  }
  const physical = beginPhysicalWork(state.retirement, "acquisition");
  registerTransferWork(state, physical);
  const scope: ResetCleanupScope = { pending: new Set(), active: true };
  const previousScope = state.resetCleanupScope;
  state.resetCleanupScope = scope;
  state.resetCleanupScopes.add(scope);
  let unreturnedPromise: Promise<void> | undefined;
  const observeUnreturned = (): void => {
    if (unreturnedPromise === undefined) return;
    try {
      observeUnreturnedPromise(unreturnedPromise);
    } catch {
      // The observer must not replace the synchronous reset failure.
    }
  };
  try {
    const queries = queryCandidates.map(({ runtime, source }) => ({
      runtime,
      caller: callerForSource(runtime, source),
    }));
    const generations = new Map(
      [...state.stores.values()]
        .filter((runtime) => !runtime.disposed)
        .map((runtime) => {
          const generation = nextGeneration(runtime.generation);
          if (refreshes.has(runtime)) nextGeneration(generation);
          return [runtime, generation] as const;
        }),
    );
    const revisions = new Map(
      [...state.stores.values()].map(
        (runtime) => [runtime, nextValueRevision(runtime.valueRevision)] as const,
      ),
    );
    const projections = [...state.projections];
    noteTransferChange(state);
    noteTransferChange(state.graph);
    for (const context of state.bindingContexts) {
      noteTransferChange(context);
      noteTransferChange(context.handle);
      for (const profiles of context.bindings.values())
        for (const binding of profiles.values()) {
          noteTransferChange(binding);
          noteTransferChange(binding.handle);
          for (const address of binding.addresses.values()) noteTransferChange(address);
          for (const runtime of binding.stores.values()) noteTransferChange(runtime);
        }
      for (const members of context.streamBindings.values())
        for (const binding of members.values()) {
          noteTransferChange(binding);
          noteTransferChange(binding.handle);
          for (const address of binding.addresses.values()) noteTransferChange(address);
          for (const runtime of binding.stores.values()) noteTransferChange(runtime);
        }
    }
    for (const runtime of state.stores.values()) {
      noteTransferChange(runtime);
      if (runtime.stream !== undefined) noteTransferChange(runtime.stream);
      if (runtime.boundRetirement !== undefined) noteTransferChange(runtime.boundRetirement);
    }
    for (const projection of projections) noteTransferChange(projection);

    const previous = state.resetOperation;
    const epoch = state.resetEpoch + 1;
    noteTransferChange(state);
    noteTransferChange(state.graph);
    state.resetEpoch = epoch;
    for (const context of state.bindingContexts) {
      for (const profiles of context.bindings.values())
        for (const binding of profiles.values()) binding.addresses.clear();
      for (const members of context.streamBindings.values())
        for (const binding of members.values()) binding.addresses.clear();
      for (const runtime of state.stores.values()) {
        const address = runtime.readerScope?.address;
        if (address !== undefined) {
          address.operations.clear();
          address.guard = {};
        }
      }
    }
    for (const runtime of state.stores.values())
      if (
        !runtime.disposed &&
        runtime.binding?.kind !== "stream" &&
        (runtime.binding === undefined ? !runtime.dropped : runtime.incarnation !== undefined)
      )
        runtime.incarnation = {};
    for (const [runtime, revision] of revisions) runtime.valueRevision = revision;
    for (const [runtime, generation] of generations) runtime.generation = generation;
    for (const runtime of state.stores.values()) {
      noteTransferChange(runtime);
      runtime.transitioning = true;
    }

    let resolvePromise!: () => void;
    let rejectPromise!: (error: unknown) => void;
    let settled = false;
    const promise = new Promise<void>((resolve, reject) => {
      resolvePromise = resolve;
      rejectPromise = reject;
    });
    unreturnedPromise = promise;
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
    noteTransferChange(state);
    noteTransferChange(operation);
    state.resetOperation = operation;
    if (previous !== undefined) noteTransferChange(previous);
    previous?.reject(new AbortError("A newer graph reset superseded this reset."));

    const oldControllers: AbortController[] = [];
    const streamCleanup: Array<() => void> = [];
    const projectionCleanup: Array<() => void> = [];
    let synchronousFailure: unknown;
    let synchronousFailureOccurred = false;
    try {
      reactiveInternal.batch(() => {
        for (const runtime of state.stores.values()) {
          if (!currentOperation(state, operation)) return;
          try {
            retireObservation(runtime);
          } catch (error) {
            synchronousFailure = error;
            synchronousFailureOccurred = true;
            throw error;
          }
        }
        for (const runtime of state.stores.values()) {
          if (!currentOperation(state, operation)) return;
          for (const request of runtime.requests) {
            let detached: AbortController | undefined;
            try {
              detached = detachRequest(
                state,
                runtime,
                request,
                new AbortError("A graph reset canceled this query request."),
              );
            } catch (error) {
              synchronousFailure = error;
              synchronousFailureOccurred = true;
              throw error;
            }
            if (detached !== undefined) oldControllers.push(detached);
            if (!currentOperation(state, operation)) return;
          }
          if (!currentOperation(state, operation)) return;
          runtime.requests.clear();
          runtime.activeRequest = undefined;
          const session = runtime.stream;
          if (session !== undefined && !session.ended) {
            let cleanup: (() => void) | undefined;
            try {
              cleanup = endStream(runtime, session, "closed", undefined, {
                abortReason: new AbortError("A graph reset canceled work from the previous state."),
                deferCleanup: true,
              });
            } catch (error) {
              synchronousFailure = error;
              synchronousFailureOccurred = true;
              throw error;
            }
            if (cleanup !== undefined) streamCleanup.push(retainResetCleanup(scope, cleanup));
          }
          if (!currentOperation(state, operation)) return;
          if (runtime.binding?.kind === "stream" && runtime.incarnation !== undefined) {
            noteTransferChange(runtime);
            runtime.incarnation = {};
            runtime.streamSource = undefined;
          }
        }
        if (!currentOperation(state, operation)) return;
        oldControllers.push(...state.activeControllers);
        state.activeControllers.clear();
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
          const generation = generations.get(runtime);
          if (generation === undefined) continue;
          try {
            clearRuntime(runtime, () => currentOperation(state, operation), generation);
          } catch (error) {
            reportContinuationError(state, error);
          }
        }
        if (currentOperation(state, operation))
          for (const runtime of state.stores.values()) {
            noteTransferChange(runtime);
            runtime.transitioning = false;
          }
        if (currentOperation(state, operation)) state.resetVersion.set(epoch);
      });
    } catch (error) {
      if (!synchronousFailureOccurred) reportContinuationError(state, error);
    }
    if (synchronousFailureOccurred) throw synchronousFailure;

    if (previous !== undefined) noteTransferChange(previous);
    previous?.abort(new AbortError("A newer graph reset superseded this reset."));
    for (const controller of oldControllers) {
      noteTransferChange(controller);
      controller.abort(new AbortError("A graph reset canceled work from the previous state."));
    }
    for (const cleanup of [...streamCleanup, ...projectionCleanup]) {
      try {
        cleanup();
      } catch (error) {
        reportContinuationError(state, error);
      }
    }

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
  } catch (error) {
    observeUnreturned();
    throw error;
  } finally {
    try {
      scope.active = false;
      state.resetCleanupScope = previousScope;
      if (state.disposed && state.retirement.promise !== undefined) drainResetCleanups(state);
      if (scope.pending.size === 0) state.resetCleanupScopes.delete(scope);
      finishPhysicalWork(physical);
    } catch (error) {
      observeUnreturned();
      throw error;
    }
  }
}
