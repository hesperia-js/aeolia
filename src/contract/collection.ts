import type { CollectionCandidate, GraphRuntimeState } from "./runtime.ts";
import type { Contract, StreamStatus } from "./types.ts";
import { __internal as reactiveInternal } from "../reactive.ts";
import { assertGraphOpen, reportContinuationError } from "./faults.ts";
import { writeAllStatuses } from "./status.ts";
import type { StoreRuntime, StreamSession } from "./runtime.ts";
import { publishStreamEvent } from "./stream-events.ts";
import { retireObservation } from "./observation-state.ts";
import { nextValueRevision } from "./identity.ts";
import { Fault } from "../fault.ts";
import type { StreamDetachment, StreamTerminalCapture } from "./stream-binding.ts";
import {
  beginStoreAddress,
  captureStoreRuntime,
  changeBoundAddress,
  endStoreAddress,
} from "./registry.ts";
import {
  beginPhysicalWork,
  finishPhysicalWork,
  normalizePhysicalPromise,
  observePhysicalWork,
  physicalCleanup,
  recordCleanupFailure,
} from "./retirement.ts";
import { noteTransferChange, registerTransferWork } from "./query-policy.ts";

const MAX_TIMER_DELAY_MS = 2_147_483_647;

export function collectionCandidateBefore(
  left: CollectionCandidate,
  right: CollectionCandidate,
): boolean {
  return left.deadline < right.deadline;
}

export function swapCollectionCandidates(
  heap: CollectionCandidate[],
  leftIndex: number,
  rightIndex: number,
): void {
  const left = heap[leftIndex]!;
  const right = heap[rightIndex]!;
  heap[leftIndex] = right;
  heap[rightIndex] = left;
  right.runtime.collectionIndex = leftIndex;
  left.runtime.collectionIndex = rightIndex;
}

export function collectionSiftUp(state: GraphRuntimeState<any>, index: number): number {
  const heap = state.collectionHeap;
  let current = index;
  while (current > 0) {
    const parent = Math.floor((current - 1) / 2);
    if (!collectionCandidateBefore(heap[current]!, heap[parent]!)) break;
    swapCollectionCandidates(heap, current, parent);
    current = parent;
  }
  return current;
}

export function collectionSiftDown(state: GraphRuntimeState<any>, index: number): number {
  const heap = state.collectionHeap;
  let current = index;
  while (true) {
    const left = current * 2 + 1;
    if (left >= heap.length) break;
    const right = left + 1;
    let smallest = left;
    if (right < heap.length && collectionCandidateBefore(heap[right]!, heap[left]!))
      smallest = right;
    if (!collectionCandidateBefore(heap[smallest]!, heap[current]!)) break;
    swapCollectionCandidates(heap, current, smallest);
    current = smallest;
  }
  return current;
}

export function removeCollectionCandidateAt(state: GraphRuntimeState<any>, index: number): void {
  const heap = state.collectionHeap;
  const removed = heap[index];
  if (removed === undefined) return;
  noteTransferChange(removed.runtime);
  const last = heap.pop()!;
  removed.runtime.collectionIndex = -1;
  if (last === removed) return;
  heap[index] = last;
  last.runtime.collectionIndex = index;
  const movedIndex = collectionSiftUp(state, index);
  collectionSiftDown(state, movedIndex);
}

export function popCollectionCandidate(
  state: GraphRuntimeState<any>,
): CollectionCandidate | undefined {
  const candidate = state.collectionHeap[0];
  if (candidate === undefined) return undefined;
  removeCollectionCandidateAt(state, 0);
  return candidate;
}

export function isLive<T>(runtime: StoreRuntime<T>): boolean {
  const positionedStore = runtime.readerScope?.session.positioned;
  if (positionedStore !== undefined && positionedStore.phase !== "closed") return true;
  if (runtime.binding?.kind === "stream") {
    const session = runtime.stream;
    return (
      session !== undefined &&
      !session.ended &&
      session.bound?.phase === "active" &&
      !session.bound.binding.context.disposed &&
      session.resetEpoch === runtime.graph.__runtime.resetEpoch &&
      runtime.incarnation === session.bound.incarnation &&
      (session.bound.liveReadables > 0 || session.listeners.size > 0)
    );
  }
  return (
    runtime.liveReadableCount > 0 ||
    runtime.progressReadableCount > 0 ||
    runtime.readyWaiterCount > 0 ||
    (runtime.stream?.listeners.size ?? 0) > 0
  );
}

function collectionCandidateEligible<T>(runtime: StoreRuntime<T>): boolean {
  return (
    !runtime.disposed &&
    !runtime.declared &&
    !runtime.dropped &&
    runtime.boundRetirement === undefined &&
    !isLive(runtime) &&
    runtime.requests.size === 0 &&
    reactiveInternal.signalValue(runtime.predictionStack).length === 0
  );
}

export function updateCollectionCandidate<T>(runtime: StoreRuntime<T>): void {
  const state = runtime.graph.__runtime;
  const index = runtime.collectionIndex;
  if (!collectionCandidateEligible(runtime)) {
    if (index >= 0) removeCollectionCandidateAt(state, index);
    return;
  }
  const deadline = (runtime.stream?.unobservedSince ?? runtime.lastInteraction) + state.idleMs;
  if (index < 0) {
    noteTransferChange(runtime);
    const candidate: CollectionCandidate = {
      runtime: runtime as StoreRuntime<unknown>,
      deadline,
    };
    runtime.collectionIndex = state.collectionHeap.length;
    state.collectionHeap.push(candidate);
    collectionSiftUp(state, runtime.collectionIndex);
    return;
  }
  const candidate = state.collectionHeap[index];
  if (candidate === undefined || candidate.runtime !== runtime) {
    noteTransferChange(runtime);
    runtime.collectionIndex = -1;
    updateCollectionCandidate(runtime);
    return;
  }
  if (candidate.deadline === deadline) return;
  candidate.deadline = deadline;
  const movedIndex = collectionSiftUp(state, index);
  collectionSiftDown(state, movedIndex);
}

export function scheduleSweep<C extends Contract>(state: GraphRuntimeState<C>): void {
  if (state.disposed || state.sweepScheduled) return;
  state.sweepScheduled = true;
  queueMicrotask(() => {
    state.sweepScheduled = false;
    if (state.disposed) return;
    collectExpired(state);
    armCollectionTimer(state);
  });
}

export function touch<T>(runtime: StoreRuntime<T>): void {
  if (runtime.disposed) return;
  runtime.lastInteraction = Date.now();
  updateCollectionCandidate(runtime);
  scheduleSweep(runtime.graph.__runtime);
}

export function interact<C extends Contract>(state: GraphRuntimeState<C>): void {
  assertGraphOpen(state);
  scheduleSweep(state);
}

export function setStreamStatus<T>(runtime: StoreRuntime<T>, status: StreamStatus): void {
  const bound = runtime.stream?.bound;
  const boundLifecycleChanges =
    bound?.phase === "active" &&
    !bound.session.ended &&
    runtime.stream?.token === bound.session.token;
  if (reactiveInternal.signalValue(runtime.streamStatusCell) !== status || boundLifecycleChanges) {
    noteTransferChange(runtime);
    if (boundLifecycleChanges) noteTransferChange(bound);
  }
  runtime.streamStatusCell.set(status);
  if (
    bound?.phase === "active" &&
    !bound.session.ended &&
    runtime.stream?.token === bound.session.token
  )
    bound.lifecycle.update((version) => version + 1);
}

export function returnStreamIterator<T>(
  session: StreamSession<T>,
  iterator: AsyncIterator<T>,
  swallowCallFailure: boolean,
): void {
  const work = beginPhysicalWork(session.physical.owner, "stream-return");
  registerTransferWork(session, work);
  noteTransferChange(session);
  let hasOriginal = false;
  try {
    let close: AsyncIterator<T>["return"];
    try {
      close = iterator.return;
    } catch (cause) {
      recordCleanupFailure(work, cause);
      throw cause;
    }
    if (typeof close !== "function") return;
    let original: unknown;
    try {
      original = close.call(iterator);
    } catch (cause) {
      recordCleanupFailure(work, cause);
      if (!swallowCallFailure) throw cause;
      return;
    }
    hasOriginal = true;
    noteTransferChange(work);
    work.original = original;
    try {
      observePhysicalWork(work, normalizePhysicalPromise(original), true, original);
    } catch (cause) {
      // The returned original remains unacknowledged if normalization or attachment fails.
      if (!swallowCallFailure) throw cause;
    }
  } finally {
    if (!hasOriginal) finishPhysicalWork(work);
  }
}

function closePositionedStoreSession<T>(runtime: StoreRuntime<T>, session: StreamSession<T>): void {
  const positioned = session.positioned;
  if (positioned === undefined || positioned.phase === "closed") return;
  const dormant = positioned.phase === "dormant";
  noteTransferChange(runtime);
  if (runtime.binding !== undefined) noteTransferChange(runtime.binding.handle);
  noteTransferChange(session);
  noteTransferChange(positioned);
  positioned.phase = "closed";
  positioned.dispatch = undefined;
  if (runtime.binding?.context.disposed !== true) changeBoundAddress(runtime);
  const reader = positioned.reader;
  try {
    if (reader !== undefined && reader.owned && !reader.released) {
      reader.released = true;
      physicalCleanup(session.physical.owner, () =>
        positioned.provider.releaseReader(positioned.input, reader.reader),
      );
    }
  } catch (error) {
    reportContinuationError(runtime.graph.__runtime, error);
  } finally {
    if (dormant) finishPhysicalWork(session.physical);
  }
}

export function discardPositionedStoreSession<T>(
  runtime: StoreRuntime<T>,
  session: StreamSession<T>,
): void {
  if (runtime.readerScope?.session !== session || session.positioned === undefined)
    throw new TypeError("The value is not a positioned reader Store session");
  if (!session.ended) endStream(runtime, session, "closed");
  noteTransferChange(runtime);
  if (runtime.binding !== undefined) noteTransferChange(runtime.binding.handle);
  noteTransferChange(session);
  runtime.disposed = true;
  runtime.incarnation = undefined;
  runtime.graph.__runtime.stores.delete(runtime.identity);
  const binding = runtime.binding;
  if (binding?.kind === "stream") binding.readerStores?.delete(runtime as StoreRuntime<unknown>);
  if (runtime.binding?.context.disposed !== true) changeBoundAddress(runtime);
  updateCollectionCandidate(runtime);
}

export function endStream<T>(
  runtime: StoreRuntime<T>,
  session: StreamSession<T>,
  status: "empty" | "closed" | "failed",
  reason?: unknown,
  options?: { readonly abortReason?: unknown; readonly deferCleanup?: boolean },
): (() => void) | undefined {
  if (session.bound !== undefined) return endBoundStream(runtime, session, status, reason, options);
  if (session.ended) return;
  const cleanupWork = beginPhysicalWork(session.physical.owner, "cleanup");
  registerTransferWork(runtime, cleanupWork);
  registerTransferWork(session, cleanupWork);
  const state = runtime.graph.__runtime;
  const current = (): boolean =>
    !state.disposed &&
    runtime.streamToken === session.token &&
    session.resetEpoch === state.resetEpoch;
  noteTransferChange(runtime);
  noteTransferChange(session);
  session.ended = true;
  closePositionedStoreSession(runtime, session);
  const iterator = session.iterator;
  const removeCallerAbort = session.removeCallerAbort;
  const removeControllerAbort = session.removeControllerAbort;
  session.iterator = undefined;
  session.removeCallerAbort = undefined;
  session.removeControllerAbort = undefined;
  state.activeControllers.delete(session.controller);

  let cleaned = false;
  const cleanup = (): void => {
    if (cleaned) return;
    cleaned = true;
    const failures: unknown[] = [];
    const attempt = (action: () => void, tracked = true): void => {
      try {
        if (tracked) physicalCleanup(session.physical.owner, action);
        else action();
      } catch (error) {
        failures.push(error);
      }
    };
    if (current() && runtime.stream === session)
      attempt(() =>
        publishStreamEvent(runtime, session, {
          kind: "close",
          failed: status === "failed",
          reason,
        }),
      );
    session.listeners.clear();
    if (runtime.stream === session) runtime.stream = undefined;
    if (removeCallerAbort !== undefined) attempt(removeCallerAbort);
    if (removeControllerAbort !== undefined) attempt(removeControllerAbort);
    attempt(() => session.controller.abort(options?.abortReason));
    if (iterator != null) attempt(() => returnStreamIterator(session, iterator, true), false);
    if (current()) {
      try {
        physicalCleanup(session.physical.owner, () =>
          reactiveInternal.batch(() => {
            if (!current()) return;
            if (status === "failed") {
              runtime.failing = true;
              runtime.invalidated = true;
              runtime.errorCell.set(reason);
              if (!current()) return;
              writeAllStatuses(runtime);
            } else if (status === "empty") {
              runtime.failing = false;
              runtime.invalidated = false;
              runtime.errorCell.set(undefined);
            }
            if (current()) setStreamStatus(runtime, status);
          }),
        );
      } catch (error) {
        if (current()) reportContinuationError(state, error);
      }
      if (current()) scheduleSweep(state);
    }
    finishPhysicalWork(cleanupWork);
    if (failures.length === 1) throw failures[0];
    if (failures.length > 1)
      throw new AggregateError(failures, "Several stream cleanup operations failed");
  };
  if (options?.deferCleanup) {
    session.listeners.clear();
    if (runtime.stream === session) runtime.stream = undefined;
    return cleanup;
  }
  cleanup();
}

function endBoundStream<T>(
  runtime: StoreRuntime<T>,
  session: StreamSession<T>,
  status: "empty" | "closed" | "failed",
  reason: unknown,
  options?: { readonly abortReason?: unknown; readonly deferCleanup?: boolean },
): (() => void) | undefined {
  const bound = session.bound!;
  if (bound.phase !== "active") return;
  const cleanupWork = beginPhysicalWork(session.physical.owner, "cleanup");
  registerTransferWork(runtime, cleanupWork);
  registerTransferWork(session, cleanupWork);
  registerTransferWork(bound, cleanupWork);
  const state = runtime.graph.__runtime;
  const retirement = { token: {}, epoch: state.resetEpoch };
  noteTransferChange(runtime);
  noteTransferChange(session);
  noteTransferChange(bound);
  noteTransferChange(bound.binding);
  noteTransferChange(bound.binding.handle);
  bound.retirement = retirement;
  bound.phase = "retiring";
  bound.liveReadables = 0;
  session.ended = true;
  closePositionedStoreSession(runtime, session);
  runtime.boundRetirement = bound;
  runtime.transitioning = true;
  const addressOperation = beginStoreAddress(runtime);
  let terminalCapture: StreamTerminalCapture<T>;
  try {
    terminalCapture = Object.freeze({
      kind: "captured",
      capture: captureStoreRuntime(runtime, bound),
    });
  } catch (cause) {
    terminalCapture = Object.freeze({ kind: "fault", cause });
  }
  const iterator = session.iterator;
  const removeCallerAbort = session.removeCallerAbort;
  const removeControllerAbort = session.removeControllerAbort;
  session.iterator = undefined;
  session.removeCallerAbort = undefined;
  session.removeControllerAbort = undefined;
  state.activeControllers.delete(session.controller);
  const owns = (): boolean =>
    runtime.boundRetirement === bound &&
    bound.retirement === retirement &&
    runtime.streamToken === session.token &&
    state.resetEpoch === retirement.epoch;
  let cleaned = false;
  const cleanup = (): void => {
    if (cleaned) return;
    cleaned = true;
    const failures: unknown[] = [];
    const attempt = (action: () => void, tracked = true): void => {
      try {
        if (tracked) physicalCleanup(session.physical.owner, action);
        else action();
      } catch (cause) {
        failures.push(cause);
      }
    };
    if (owns() && runtime.stream === session)
      attempt(() =>
        publishStreamEvent(runtime, session, {
          kind: "close",
          failed: status === "failed",
          reason,
        }),
      );
    session.listeners.clear();
    if (runtime.stream === session) runtime.stream = undefined;
    if (removeCallerAbort !== undefined) attempt(removeCallerAbort);
    if (removeControllerAbort !== undefined) attempt(removeControllerAbort);
    attempt(() => session.controller.abort(options?.abortReason));
    if (iterator !== undefined)
      attempt(() => returnStreamIterator(session, iterator, false), false);
    if (owns() && !runtime.disposed && !state.disposed && !bound.binding.context.disposed) {
      attempt(() =>
        reactiveInternal.batch(() => {
          if (!owns()) return;
          if (status === "failed") {
            runtime.failing = true;
            runtime.invalidated = true;
            runtime.errorCell.set(reason);
            if (!owns()) return;
            writeAllStatuses(runtime);
          } else if (status === "empty") {
            runtime.failing = false;
            runtime.invalidated = false;
            runtime.errorCell.set(undefined);
          }
          if (owns()) runtime.streamStatusCell.set(status);
        }),
      );
    }
    // All destructive native tails run before this deferred seal/release; physical cleanup is separate.
    queueMicrotask(() => {
      if (bound.phase !== "retiring" || bound.retirement !== retirement) {
        finishPhysicalWork(cleanupWork);
        return;
      }
      if (terminalCapture.kind === "captured") {
        if (state.disposed || bound.binding.context.disposed)
          terminalCapture = Object.freeze({
            kind: "fault",
            cause: new Fault("disposed", [String(runtime.key)]),
          });
        else if (!owns())
          terminalCapture = Object.freeze({
            kind: "captured",
            capture: Object.freeze({ kind: "changed" }),
          });
      }
      const receipt: StreamDetachment<T> = Object.freeze({
        session: bound.handle.identity,
        status,
        reason,
        terminalCapture,
      });
      noteTransferChange(runtime);
      noteTransferChange(session);
      noteTransferChange(bound);
      noteTransferChange(bound.binding);
      bound.receipt = receipt;
      bound.phase = "detached";
      try {
        physicalCleanup(session.physical.owner, () =>
          bound.lifecycle.update((version) => version + 1),
        );
      } catch (cause) {
        reportContinuationError(state, cause);
      }
      queueMicrotask(() => {
        try {
          physicalCleanup(session.physical.owner, () => {
            if (runtime.boundRetirement === bound) {
              noteTransferChange(runtime);
              noteTransferChange(bound);
              const owned = owns();
              runtime.boundRetirement = undefined;
              if (owned) runtime.transitioning = false;
            }
            endStoreAddress(runtime, addressOperation);
            updateCollectionCandidate(runtime);
            scheduleSweep(state);
          });
        } catch (cause) {
          reportContinuationError(state, cause);
        } finally {
          bound.resolveDetached(receipt);
          finishPhysicalWork(cleanupWork);
        }
      });
    });
    for (const failure of failures) reportContinuationError(state, failure);
  };
  if (options?.deferCleanup) return cleanup;
  cleanup();
}

export function dropStore<T>(runtime: StoreRuntime<T>): void {
  if (
    runtime.readerScope !== undefined ||
    runtime.disposed ||
    runtime.declared ||
    runtime.dropped ||
    isLive(runtime) ||
    runtime.requests.size > 0 ||
    runtime.predictionStack.peek().length > 0
  )
    return;
  const addressDrop = beginStoreAddress(runtime);
  noteTransferChange(runtime);
  if (runtime.binding !== undefined) noteTransferChange(runtime.binding.handle);
  if (runtime.binding !== undefined) noteTransferChange(runtime.binding.handle);
  try {
    const streamToken = runtime.streamToken;
    const revision = nextValueRevision(runtime.valueRevision);
    let valueRevision = runtime.valueRevision;
    const resetEpoch = runtime.graph.__runtime.resetEpoch;
    let incarnation = runtime.incarnation;
    const current = (): boolean =>
      runtime.streamToken === streamToken &&
      runtime.graph.__runtime.resetEpoch === resetEpoch &&
      !runtime.graph.__runtime.disposed &&
      !runtime.disposed &&
      runtime.incarnation === incarnation &&
      runtime.valueRevision === valueRevision;
    if (runtime.stream != null) endStream(runtime, runtime.stream, "empty");
    if (!current() || isLive(runtime) || runtime.requests.size > 0) return;
    if (runtime.timer != null) {
      clearTimeout(runtime.timer);
      runtime.timer = undefined;
    }
    reactiveInternal.batch(() => {
      runtime.incarnation = undefined;
      incarnation = undefined;
      runtime.valueRevision = revision;
      valueRevision = revision;
      retireObservation(runtime);
      if (!current()) return;
      runtime.source = undefined;
      runtime.streamSource = undefined;
      runtime.onReadableActivation = undefined;
      runtime.refreshOnActivation = false;
      runtime.baseAvailable = false;
      runtime.hasAcceptedLanding = false;
      runtime.queryProgressClear?.();
      if (!current()) return;
      runtime.transferEligible = true;
      runtime.failing = false;
      runtime.recoveryDisarmed = false;
      runtime.invalidated = false;
      runtime.lastLandingAt = undefined;
      runtime.lastSettledAt = undefined;
      runtime.errorCell.set(undefined);
      if (!current()) return;
      runtime.presence.set(false);
      if (!current()) return;
      runtime.committed.set(undefined);
      if (!current()) return;
      runtime.predictionStack.set([]);
      if (!current()) return;
      runtime.streamStatusCell.set("empty");
      if (!current()) return;
      writeAllStatuses(runtime);
      if (!current()) return;
      runtime.callers.length = 0;
      runtime.dropped = true;
      runtime.lastInteraction = Date.now();
      updateCollectionCandidate(runtime);
    });
  } finally {
    endStoreAddress(runtime, addressDrop);
  }
}

export function collectExpired<C extends Contract>(state: GraphRuntimeState<C>): void {
  if (state.disposed) return;
  const at = Date.now();
  while (state.collectionHeap[0] != null && state.collectionHeap[0].deadline <= at) {
    const candidate = popCollectionCandidate(state)!;
    dropStore(candidate.runtime);
  }
}

export function armCollectionTimer<C extends Contract>(state: GraphRuntimeState<C>): void {
  if (state.disposed) return;
  const due = state.collectionHeap[0]?.deadline;
  if (state.collectionTimer != null) clearTimeout(state.collectionTimer);
  if (due === undefined) {
    state.collectionTimer = undefined;
    return;
  }
  const at = Date.now();
  const delay = Math.max(0, due - at);
  state.collectionTimer = setTimeout(
    () => {
      state.collectionTimer = undefined;
      if (state.disposed) return;
      collectExpired(state);
      armCollectionTimer(state);
    },
    Math.min(MAX_TIMER_DELAY_MS, delay),
  );
}
