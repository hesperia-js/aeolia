import { computed, signal, __internal as reactiveInternal } from "../reactive.ts";
import type { ObservationState, StoreRuntime } from "./runtime.ts";
import { reportContinuationError } from "./faults.ts";

/** Acquire metadata only after the native query incarnation was validated. */
export function observationState<T>(runtime: StoreRuntime<T>): ObservationState {
  const existing = runtime.observation;
  if (existing !== undefined && !existing.retired && existing.incarnation === runtime.incarnation)
    return existing;
  if (runtime.incarnation === undefined) throw new TypeError("The query incarnation is inactive");
  const state: ObservationState = {
    incarnation: runtime.incarnation,
    issue: signal<Readonly<object> | undefined>(undefined),
    live: signal(true),
    publisherActivity: undefined,
    stops: [],
    publisher: undefined,
    retired: false,
    liveReadables: 0,
  };
  for (const cell of [state.issue, state.live])
    reactiveInternal.bindReadable(cell, { graphId: runtime.graph.id });
  runtime.observation = state;
  return state;
}

/** Revoke old metadata before any issue/activity subscriber can reenter. */
export function retireObservation<T>(runtime: StoreRuntime<T>): void {
  const state = runtime.observation;
  if (state === undefined || state.retired) return;
  state.retired = true;
  state.publisher = undefined;
  const activity = state.publisherActivity;
  state.publisherActivity = undefined;
  runtime.observation = undefined;
  runtime.liveReadableCount = Math.max(0, runtime.liveReadableCount - state.liveReadables);
  state.liveReadables = 0;
  const attempt = (action: () => void): void => {
    try {
      action();
    } catch (error) {
      reportContinuationError(runtime.graph.__runtime, error);
    }
  };
  for (const stop of state.stops.splice(0)) attempt(stop);
  attempt(() =>
    reactiveInternal.batch(() => {
      attempt(() => state.live.set(false));
      if (activity !== undefined) attempt(() => activity.set(false));
      attempt(() => state.issue.set(undefined));
    }),
  );
}

/** Nonretaining lifecycle metadata never follows a successor incarnation. */
export function publisherActive(state: ObservationState) {
  const active = signal(true);
  state.publisherActivity = active;
  return { cell: active, readable: computed(() => state.live.get() && active.get()) };
}
