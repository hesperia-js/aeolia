import type { CollectionCandidate, GraphRuntimeState } from "./runtime.ts";
import type { Contract, StreamStatus } from "./types.ts";
import { __internal as reactiveInternal } from "../reactive.ts";
import { assertGraphOpen, reportContinuationError } from "./faults.ts";
import { writeAllStatuses } from "./status.ts";
import type { StoreRuntime, StreamSession } from "./runtime.ts";
import { publishStreamEvent } from "./stream-events.ts";
import {
  beginPhysicalWork,
  finishPhysicalWork,
  physicalCleanup,
  returnPhysicalIterator,
} from "./retirement.ts";

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
  return (
    runtime.liveReadableCount > 0 ||
    runtime.readyWaiterCount > 0 ||
    (runtime.stream?.listeners.size ?? 0) > 0
  );
}

function collectionCandidateEligible<T>(runtime: StoreRuntime<T>): boolean {
  return (
    !runtime.disposed &&
    !runtime.declared &&
    !runtime.dropped &&
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
  runtime.streamStatusCell.set(status);
}

export function endStream<T>(
  runtime: StoreRuntime<T>,
  session: StreamSession<T>,
  status: "empty" | "closed" | "failed",
  reason?: unknown,
  options?: { readonly abortReason?: unknown; readonly deferCleanup?: boolean },
): (() => void) | undefined {
  if (session.ended) return;
  const state = runtime.graph.__runtime;
  const cleanupWork = beginPhysicalWork(session.physical.owner, "cleanup");
  const current = (): boolean =>
    !state.disposed &&
    runtime.streamToken === session.token &&
    session.resetEpoch === state.resetEpoch;
  session.ended = true;
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
    try {
      const failures: unknown[] = [];
      const attempt = (action: () => void, alreadyOwned = false): void => {
        try {
          if (alreadyOwned) action();
          else physicalCleanup(session.physical.owner, action);
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
      if (removeCallerAbort !== undefined) attempt(removeCallerAbort, true);
      if (removeControllerAbort !== undefined) attempt(removeControllerAbort, true);
      attempt(() => session.controller.abort(options?.abortReason));
      attempt(() => {
        if (iterator == null) return;
        const close = iterator.return;
        if (typeof close !== "function") return;
        try {
          returnPhysicalIterator(session.physical.owner, iterator, "stream-return", close);
        } catch {
          /* closing is best effort; the source is already terminal */
        }
      });
      if (current()) {
        try {
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
          });
        } catch (error) {
          if (current()) reportContinuationError(state, error);
        }
        if (current()) scheduleSweep(state);
      }
      if (failures.length === 1) throw failures[0];
      if (failures.length > 1)
        throw new AggregateError(failures, "Several stream cleanup operations failed");
    } finally {
      finishPhysicalWork(cleanupWork);
    }
  };
  if (options?.deferCleanup) {
    session.listeners.clear();
    if (runtime.stream === session) runtime.stream = undefined;
    return cleanup;
  }
  cleanup();
}

export function dropStore<T>(runtime: StoreRuntime<T>): void {
  if (
    runtime.disposed ||
    runtime.declared ||
    runtime.dropped ||
    isLive(runtime) ||
    runtime.requests.size > 0 ||
    runtime.predictionStack.peek().length > 0
  )
    return;
  const streamToken = runtime.streamToken;
  const resetEpoch = runtime.graph.__runtime.resetEpoch;
  const current = (): boolean =>
    runtime.streamToken === streamToken &&
    runtime.graph.__runtime.resetEpoch === resetEpoch &&
    !runtime.graph.__runtime.disposed &&
    !runtime.disposed;
  if (runtime.stream != null) endStream(runtime, runtime.stream, "empty");
  if (!current() || isLive(runtime) || runtime.requests.size > 0) return;
  if (runtime.timer != null) {
    clearTimeout(runtime.timer);
    runtime.timer = undefined;
  }
  runtime.source = undefined;
  runtime.streamSource = undefined;
  runtime.onReadableActivation = undefined;
  runtime.refreshOnActivation = false;
  runtime.hasCommitted = false;
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
