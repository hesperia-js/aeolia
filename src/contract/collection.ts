import type { CollectionCandidate, GraphRuntimeState } from "./runtime.ts";
import type { Contract, StreamStatus } from "./types.ts";
import { __internal as reactiveInternal } from "../reactive.ts";
import { assertGraphOpen } from "./faults.ts";
import { writeAllStatuses } from "./status.ts";
import type { StoreRuntime, StreamSession } from "./runtime.ts";

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
  return runtime.liveReadableCount > 0;
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
  const deadline = runtime.lastInteraction + state.idleMs;
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
): void {
  if (session.ended) return;
  session.ended = true;
  if (runtime.stream === session) runtime.stream = undefined;
  runtime.graph.__runtime.activeControllers.delete(session.controller);
  session.controller.abort();
  const iterator = session.iterator;
  if (iterator != null && typeof iterator.return === "function") {
    try {
      void Promise.resolve(iterator.return()).catch(() => undefined);
    } catch {
      /* closing is best effort; the source is already terminal */
    }
  }
  if (status === "failed") {
    runtime.failing = true;
    runtime.invalidated = true;
    runtime.errorCell.set(reason);
    writeAllStatuses(runtime);
  } else if (status === "empty") {
    runtime.failing = false;
    runtime.invalidated = false;
    runtime.errorCell.set(undefined);
  }
  setStreamStatus(runtime, status);
  scheduleSweep(runtime.graph.__runtime);
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
  if (runtime.stream != null) endStream(runtime, runtime.stream, "empty");
  if (runtime.timer != null) {
    clearTimeout(runtime.timer);
    runtime.timer = undefined;
  }
  runtime.source = undefined;
  runtime.hasCommitted = false;
  runtime.failing = false;
  runtime.invalidated = false;
  runtime.lastLandingAt = undefined;
  runtime.lastSettledAt = undefined;
  runtime.errorCell.set(undefined);
  runtime.presence.set(false);
  runtime.committed.set(undefined);
  runtime.predictionStack.set([]);
  runtime.streamStatusCell.set("empty");
  writeAllStatuses(runtime);
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
