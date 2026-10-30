import { computed, signal, __internal as reactiveInternal } from "../reactive.ts";
import type { Readable, WritableSignal } from "../reactive.ts";
import type { ObservationState, StoreRuntime } from "./runtime.ts";
import { reportContinuationError } from "./faults.ts";
import { physicalCleanup } from "./retirement.ts";
import { noteTransferChange, registerTransferReadable, unregisterTransferReadable } from "./query-policy.ts";

interface PublisherActivityEntry {
  readable: Readable<boolean> | undefined;
}
const publisherActivityReadableByState = new WeakMap<ObservationState, PublisherActivityEntry>();
export interface PublisherActivity {
  readonly cell: WritableSignal<boolean>;
  readonly readable: Readable<boolean>;
}

/** @internal Allocates genuine observation metadata without attaching it to a StoreRuntime. */
export function createObservationState<T>(
  runtime: StoreRuntime<T>,
  incarnation: object,
  initiallyLive: boolean,
): ObservationState {
  const state: ObservationState = {
    incarnation,
    issue: signal<Readonly<object> | undefined>(undefined),
    live: signal(initiallyLive),
    publisherActivity: undefined,
    stops: [],
    publisher: undefined,
    retired: false,
    liveReadables: 0,
  };
  for (const cell of [state.issue, state.live])
    reactiveInternal.bindReadable(cell, { graphId: runtime.graph.id });
  return state;
}

/** @internal Installs a genuine allocated ObservationState as the runtime actual state. */
export function installObservationState<T>(runtime: StoreRuntime<T>, state: ObservationState): void {
  if (
    runtime.incarnation !== state.incarnation ||
    (runtime.observation !== undefined &&
      !runtime.observation.retired &&
      runtime.observation.incarnation === runtime.incarnation)
  )
    throw new TypeError('Observation state no longer matches its Store incarnation');
  registerTransferReadable(state.issue, runtime, 'observation-issue-cell');
  registerTransferReadable(state.live, runtime, 'observation-live');
  noteTransferChange(runtime);
  runtime.observation = state;
}

/** @internal Releases only facets allocated for an unpublished ObservationState. */
export function discardObservationState<T>(runtime: StoreRuntime<T>, state: ObservationState): void {
  if (runtime.observation === state) return;
  unregisterTransferReadable(state.issue, runtime);
  unregisterTransferReadable(state.live, runtime);
}

export function preparePublisherActivity(state: ObservationState, initiallyActive: boolean): PublisherActivity {
  const cell = signal(initiallyActive);
  const readable = computed(() => state.live.get() && cell.get());
  return { cell, readable };
}

/** @internal Publishes the already-allocated publisher activity facets. */
export function installPublisherActivity(state: ObservationState, activity: PublisherActivity): void {
  state.publisherActivity = activity.cell;
  let entry = publisherActivityReadableByState.get(state);
  if (entry === undefined) {
    entry = { readable: activity.readable };
    publisherActivityReadableByState.set(state, entry);
  } else {
    entry.readable = activity.readable;
  }
}

/** @internal Reserves the exact WeakMap entry so publication only replaces its value. */
export function reservePublisherActivityReadable(
  state: ObservationState,
  readable: Readable<boolean>,
): { publish(): void; discard(): void } {
  let entry = publisherActivityReadableByState.get(state);
  const reserveEmpty = entry === undefined;
  if (entry === undefined) {
    entry = { readable: undefined };
    publisherActivityReadableByState.set(state, entry);
  }
  let published = false;
  let discarded = false;
  return {
    publish: () => {
      if (published || discarded) return;
      entry!.readable = readable;
      published = true;
    },
    discard: () => {
      if (published || discarded) return;
      discarded = true;
      if (reserveEmpty && entry!.readable === undefined && publisherActivityReadableByState.get(state) === entry)
        publisherActivityReadableByState.delete(state);
    },
  };
}

/** @internal Returns only an exact current observation facet; no publisher is created. */
export function getObservationTransferFacet<T>(
  runtime: StoreRuntime<T>,
  role: string,
): Readable<unknown> | undefined {
  const state = runtime.observation;
  if (
    state === undefined ||
    state.retired ||
    runtime.observation !== state ||
    runtime.incarnation !== state.incarnation
  )
    return undefined;
  switch (role) {
    case "observation-issue":
      return state.issueReadable;
    case "observation-issue-cell":
      return state.issue;
    case "observation-live":
      return state.live;
    case "observation-activity-cell":
      return state.publisher === undefined ? undefined : state.publisherActivity;
    case "observation-activity":
      return state.publisher === undefined ? undefined : publisherActivityReadableByState.get(state)?.readable;
    default:
      return undefined;
  }
}

/** Acquire metadata only after the native query incarnation was validated. */
export function observationState<T>(runtime: StoreRuntime<T>): ObservationState {
  const existing = runtime.observation;
  if (existing !== undefined && !existing.retired && existing.incarnation === runtime.incarnation)
    return existing;
  if (runtime.incarnation === undefined) throw new TypeError("The query incarnation is inactive");
  const state = createObservationState(runtime, runtime.incarnation, true);
  installObservationState(runtime, state);
  return state;
}

/** Revoke old metadata before any issue/activity subscriber can reenter. */
export function retireObservation<T>(runtime: StoreRuntime<T>): void {
  const state = runtime.observation;
  if (state === undefined || state.retired) return;
  noteTransferChange(runtime);
  if (runtime.binding !== undefined) noteTransferChange(runtime.binding.handle);
  noteTransferChange(state);
  state.retired = true;
  state.publisher = undefined;
  const activity = state.publisherActivity;
  state.publisherActivity = undefined;
  runtime.observation = undefined;
  runtime.liveReadableCount = Math.max(0, runtime.liveReadableCount - state.liveReadables);
  state.liveReadables = 0;
  const attempt = (action: () => void): void => {
    try {
      physicalCleanup(
        runtime.binding?.context.retirement ?? runtime.graph.__runtime.retirement,
        action,
      );
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
export function publisherActive(state: ObservationState): PublisherActivity {
  const activity = preparePublisherActivity(state, true);
  noteTransferChange(state);
  installPublisherActivity(state, activity);
  return activity;
}
