import type { Graph, QueryStore, Readable } from "./types.ts";
import type { WritableSignal } from "../reactive.ts";
import type { GraphRuntimeState, ObservationState, StoreRuntime } from "./runtime.ts";
import { computed, __internal as reactiveInternal } from "../reactive.ts";
import { assertStoreHandlePublished, graphRuntimeByGraph, queryRuntimeByStore } from "./registry.ts";
import { assertGraphOpen, assertStoreOpen, reportContinuationError } from "./faults.ts";
import {
  createObservationState,
  discardObservationState,
  observationState,
  preparePublisherActivity,
  publisherActive,
  reservePublisherActivityReadable,
} from "./observation-state.ts";
import { publishObservation } from "./store-state.ts";
import { scheduleSweep, touch, updateCollectionCandidate } from "./collection.ts";
import {
  inheritTransferCoverage,
  noteTransferChange,
  prepareTransferChange,
  registerTransferReadable,
  transferReadableRole,
  transferReadableStore,
  unregisterTransferReadable,
} from "./query-policy.ts";

/** Transfer eligibility of the exact newly observed committed base. */
export interface ObservationPublishOptions {
  /** Defaults to eligible; exclude is for an explicitly qualified native fallback. */
  readonly transfer?: "eligible" | "exclude";
}

/** @internal Native participant in a fixed required-adoption publication. */
export interface PreparedObservationTransfer<Value> {
  readonly lease: ObservationLease<Value>;
  readonly state: ObservationState;
  readonly writes: readonly { readonly target: WritableSignal<unknown>; readonly value: unknown }[];
  current(): boolean;
  publish(): void;
  deliver(): void;
  discard(): void;
  getFacet(role: string): Readable<unknown> | undefined;
}
interface ObservationLeaseRecord<Value> {
  readonly runtime: StoreRuntime<Value>;
  readonly state: ObservationState;
  readonly token: object;
  readonly incarnation: object;
  readonly activity: { readonly cell: WritableSignal<boolean>; readonly readable: Readable<boolean> };
  readonly current: () => boolean;
}
const observationLeaseRecords = new WeakMap<object, ObservationLeaseRecord<unknown>>();
const issueHooksInstalled = new WeakSet<ObservationState>();

function observationIssueReadable<Value>(
  runtime: StoreRuntime<Value>,
  state: ObservationState,
  graph: Graph,
  graphState: GraphRuntimeState<any>,
): Readable<Readonly<object> | undefined> {
  const issue = computed(() => {
    if (!state.live.get()) return undefined;
    assertStoreOpen(runtime);
    return state.issue.get();
  });
  reactiveInternal.bindReadable(issue, {
    graphId: graph.id,
    reportFault: (fault) => reportContinuationError(graphState, fault),
  });
  return issue;
}

function attachObservationIssueHooks<Value>(
  runtime: StoreRuntime<Value>,
  state: ObservationState,
  issue: Readable<Readonly<object> | undefined>,
  graphState: GraphRuntimeState<any>,
): void {
  if (issueHooksInstalled.has(state)) return;
  const stops: (() => void)[] = [];
  try {
    stops.push(reactiveInternal.onActualRead(issue, () => {
      if (state.retired) return;
      assertStoreOpen(runtime);
      touch(runtime);
    }));
    stops.push(reactiveInternal.onLivenessChange(issue, (live) => {
      if (state.retired) return;
      state.liveReadables += live ? 1 : -1;
      runtime.liveReadableCount += live ? 1 : -1;
      if (live) touch(runtime);
      updateCollectionCandidate(runtime);
      scheduleSweep(graphState);
    }));
    for (const stop of stops) state.stops.push(stop);
    issueHooksInstalled.add(state);
  } catch (cause) {
    const errors: unknown[] = [cause];
    for (const stop of stops) {
      try { stop(); } catch (cleanupCause) { errors.push(cleanupCause); }
    }
    if (errors.length === 1) throw cause;
    throw new AggregateError(errors, "Observation issue hook preparation failed");
  }
}

/** One native query incarnation's exclusive, nonretaining publication lease. */
export interface ObservationLease<Value> {
  /** Nonretaining lifecycle metadata; false permanently after lease retirement. */
  readonly active: Readable<boolean>;
  /** Shared issue; metadata reads/observers retain only while its incarnation lives. */
  readonly issue: Readable<Readonly<object> | undefined>;
  /**
   * Publish beneath predictions without completing or superseding a request.
   * @param value - Actual observed base, including a present undefined.
   * @param options - Eligibility attached to this base revision.
   * @returns False without effects when this lease is stale.
   * @throws TypeError for malformed options; native write/propagation faults retain their route.
   */
  publish(value: Value, options?: ObservationPublishOptions): boolean;
  /**
   * Retain the supplied opaque issue without changing value/request state.
   * @param issue - Immutable caller-owned issue retained by identity.
   * @param diagnostic - Optional original cause, including undefined.
   * @returns False without effects or diagnostics when stale.
   * @throws TypeError for malformed arguments, or native write/propagation faults.
   */
  reject(issue: Readonly<object>, diagnostic?: { readonly cause: unknown }): boolean;
  /**
   * Report through the existing continuation diagnostic route without changing issue.
   * @param cause - Original processing fault.
   * @returns Whether this lease was current when reporting began.
   */
  report(cause: unknown): boolean;
  /** Idempotently revoke this publisher, preserving its incarnation's shared issue. */
  dispose(): void;
}


function observationTransferFacet(
  state: ObservationState,
  activity: { readonly cell: WritableSignal<boolean>; readonly readable: Readable<boolean> },
  issue: Readable<Readonly<object> | undefined>,
  role: string,
): Readable<unknown> | undefined {
  switch (role) {
    case "observation-issue": return issue;
    case "observation-issue-cell": return state.issue;
    case "observation-live": return state.live;
    case "observation-activity-cell": return activity.cell;
    case "observation-activity": return activity.readable;
    default: return undefined;
  }
}

/** @internal Prepares genuine observation metadata for one exact passive query claim. */
export function prepareObservationTransfer<Value>(
  runtime: StoreRuntime<Value>,
  incarnation: object,
  claimCurrent: () => boolean,
  existingLease?: ObservationLease<Value>,
): PreparedObservationTransfer<Value> {
  if (incarnation === null || typeof incarnation !== "object" || typeof claimCurrent !== "function")
    throw new TypeError("Observation transfer requires an exact native claim and incarnation");
  if (!claimCurrent()) throw new TypeError("Observation transfer claim is no longer current");
  assertStoreOpen(runtime);
  if (runtime.transitioning || runtime.graph.__runtime.disposed || runtime.binding?.context.disposed === true)
    throw new TypeError("Observation transfer runtime is not current");

  if (existingLease !== undefined) {
    const record = observationLeaseRecords.get(existingLease as object) as ObservationLeaseRecord<Value> | undefined;
    if (record === undefined || record.runtime !== runtime || record.incarnation !== incarnation ||
        record.state.incarnation !== incarnation || runtime.observation !== record.state ||
        runtime.incarnation !== incarnation || record.state.publisher !== record.token ||
        record.state.retired || !record.current())
      throw new TypeError("Existing observation lease is not the exact current lease for this claim");
    let adopted = false;
    return Object.freeze({
      lease: existingLease,
      state: record.state,
      writes: Object.freeze([]),
      current: () => adopted ? record.current() : claimCurrent() && record.current(),
      publish: () => { adopted = true; },
      deliver: () => undefined,
      discard: () => undefined,
      getFacet: (role: string) => observationTransferFacet(record.state, record.activity, existingLease.issue, role),
    });
  }


  const observationBefore = runtime.observation;
  if (observationBefore !== undefined &&
      (observationBefore.retired || observationBefore.incarnation !== incarnation || observationBefore.publisher !== undefined))
    throw new TypeError("The query already has an observation publisher or another incarnation");
  if (observationBefore?.publisherActivity !== undefined)
    throw new TypeError("The query has an unowned observation activity cell");
  const ownsState = observationBefore === undefined;
  const state = observationBefore ?? createObservationState(runtime, incarnation, false);
  if (!ownsState && !reactiveInternal.signalValue(state.live))
    throw new TypeError("An inactive observation state cannot be reused");
  const issueBefore = state.issueReadable;
  const ownsIssue = issueBefore === undefined;
  const issue = issueBefore ?? observationIssueReadable(runtime, state, runtime.graph, runtime.graph.__runtime);
  const activity = preparePublisherActivity(state, false);
  const token = {};
  const runtimeIncarnationBefore = runtime.incarnation;
  const stateTransition = prepareTransferChange(state);
  const activityReadableReservation = reservePublisherActivityReadable(state, activity.readable);
  const registeredFacets: Readable<unknown>[] = [];
  const registerFacet = (readable: Readable<unknown>, role: string): void => {
    const previousRuntime = transferReadableStore(readable);
    const previousRole = transferReadableRole(readable);
    if (previousRuntime === runtime && previousRole === role) return;
    if (previousRuntime !== undefined || previousRole !== undefined)
      throw new TypeError("Observation facet already belongs to another native owner");
    registerTransferReadable(readable, runtime, role);
    registeredFacets.push(readable);
  };
  let published = false;
  let delivered = false;
  let discarded = false;
  const leaseCurrent = (): boolean => published && !discarded &&
    runtime.observation === state && runtime.incarnation === incarnation &&
    !state.retired && state.publisher === token && !runtime.disposed &&
    !runtime.transitioning && !runtime.graph.__runtime.disposed && runtime.binding?.context.disposed !== true;
  const current = (): boolean => {
    if (discarded) return false;
    if (published) return leaseCurrent();
    return claimCurrent() && runtime.observation === observationBefore &&
      runtime.incarnation === runtimeIncarnationBefore && !runtime.disposed &&
      !runtime.transitioning && !runtime.graph.__runtime.disposed && runtime.binding?.context.disposed !== true &&
      !state.retired && state.incarnation === incarnation && state.publisher === undefined &&
      stateTransition.current();
  };
  const discard = (): void => {
    if (published || discarded) return;
    discarded = true;
    const failures: unknown[] = [];
    try { activityReadableReservation.discard(); } catch (cause) { failures.push(cause); }
    if (failures.length === 1) throw failures[0];
    if (failures.length > 1) throw new AggregateError(failures, "Observation participant cleanup failed");
  };
  const lease: ObservationLease<Value> = Object.freeze({
    active: activity.readable,
    issue,
    publish(value: Value, options?: ObservationPublishOptions): boolean {
      if (!leaseCurrent()) return false;
      const data: Record<string, unknown> = options === undefined ? {} : ownData(options, ["transfer"]);
      if (data.transfer !== undefined && data.transfer !== "eligible" && data.transfer !== "exclude")
        throw new TypeError("Invalid observation transfer policy");
      if (!leaseCurrent()) return false;
      reactiveInternal.assertWritesAllowed(runtime.committed);
      return publishObservation(runtime, value, data.transfer !== "exclude", leaseCurrent);
    },
    reject(value: Readonly<object>, diagnostic?: { readonly cause: unknown }): boolean {
      if (!leaseCurrent()) return false;
      if (value === null || typeof value !== "object") throw new TypeError("Expected an observation issue object");
      const data = diagnostic === undefined ? undefined : ownData(diagnostic, ["cause"]);
      if (data !== undefined && !Object.hasOwn(data, "cause")) throw new TypeError("Missing diagnostic cause");
      if (!leaseCurrent()) return false;
      reactiveInternal.assertWritesAllowed(state.issue);
      if (state.issue.peek() !== value) {
        noteTransferChange(state);
        noteTransferChange(runtime);
        if (runtime.binding !== undefined) noteTransferChange(runtime.binding.handle);
      }
      state.issue.set(value);
      if (data !== undefined) reportContinuationError(runtime.graph.__runtime, data.cause);
      return true;
    },
    report(cause: unknown): boolean {
      if (!leaseCurrent()) return false;
      reportContinuationError(runtime.graph.__runtime, cause);
      return true;
    },
    dispose(): void {
      if (!published) {
        if (!discarded) discard();
        return;
      }
      if (state.publisher !== token) return;
      reactiveInternal.assertWritesAllowed(activity.cell);
      noteTransferChange(state);
      noteTransferChange(runtime);
      if (runtime.binding !== undefined) noteTransferChange(runtime.binding.handle);
      state.publisher = undefined;
      state.publisherActivity = undefined;
      activity.cell.set(false);
    },
  });
  observationLeaseRecords.set(lease, { runtime, state, token, incarnation, activity, current: leaseCurrent } as unknown as ObservationLeaseRecord<unknown>);

  try {
    registerFacet(state.issue, "observation-issue-cell");
    registerFacet(state.live, "observation-live");
    registerFacet(issue, "observation-issue");
    reactiveInternal.bindReadable(activity.readable, { graphId: runtime.graph.id });
    registerFacet(activity.cell, "observation-activity-cell");
    registerFacet(activity.readable, "observation-activity");
  } catch (cause) {
    const failures: unknown[] = [cause];
    try { activityReadableReservation.discard(); } catch (cleanupCause) { failures.push(cleanupCause); }
    for (const readable of registeredFacets) {
      try { unregisterTransferReadable(readable, runtime); } catch (cleanupCause) { failures.push(cleanupCause); }
    }
    if (ownsState) {
      try { discardObservationState(runtime, state); } catch (cleanupCause) { failures.push(cleanupCause); }
    }
    if (failures.length === 1) throw cause;
    throw new AggregateError(failures, "Observation participant preparation and cleanup failed");
  }
  const writes: { readonly target: WritableSignal<unknown>; readonly value: unknown }[] = [];
  if (ownsState) writes.push({ target: state.live as WritableSignal<unknown>, value: true });
  writes.push({ target: activity.cell as WritableSignal<unknown>, value: true });
  const participant: PreparedObservationTransfer<Value> = Object.freeze({
    lease,
    state,
    writes: Object.freeze(writes),
    current,
    publish: () => {
      if (published || discarded) return;
      if (ownsState) runtime.observation = state;
      if (ownsIssue) state.issueReadable = issue;
      state.publisherActivity = activity.cell;
      state.publisher = token;
      activityReadableReservation.publish();
      stateTransition.publish();
      published = true;
    },
    deliver: () => {
      if (!published || delivered || discarded) return;
      delivered = true;
      if (ownsIssue) attachObservationIssueHooks(runtime, state, issue, runtime.graph.__runtime);
    },
    discard,
    getFacet: (role: string) => observationTransferFacet(state, activity, issue, role),
  });
  return participant;
}

function ownData(value: unknown, allowed: readonly string[]): Record<string, unknown> {
  if (value === null || typeof value !== "object")
    throw new TypeError("Expected observation options");
  const result: Record<string, unknown> = Object.create(null);
  for (const key of Reflect.ownKeys(value)) {
    if (typeof key !== "string" || !allowed.includes(key))
      throw new TypeError("Unknown observation option");
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (descriptor === undefined || !("value" in descriptor))
      throw new TypeError("Observation options require own data fields");
    result[key] = descriptor.value;
  }
  return result;
}

/**
 * Acquire the sole publisher for an already-existing native query incarnation.
 * @param graph - Genuine graph owning the exact query wrapper.
 * @param existingProfileQuery - Already acquired query; this operation never revives it.
 * @returns A lease with shared issue metadata and independent publication authority.
 * @throws TypeError for foreign/fabricated/inactive queries or a concurrent publisher;
 * disposed Fault for an already closed graph/store/context. Acquisition never fetches,
 * creates a store or touches retention. Schema/provider issue classification belongs
 * to the caller. Publication does not independently renew idle activity.
 */
export function observeStore<Value>(
  graph: Graph,
  existingProfileQuery: QueryStore<Value, any>,
): ObservationLease<Value> {
  const graphState = graphRuntimeByGraph.get(graph);
  if (graphState === undefined) throw new TypeError("The value is not an Aeolia graph");
  assertGraphOpen(graphState);
  assertStoreHandlePublished(existingProfileQuery as object);
  const runtime = queryRuntimeByStore.get(existingProfileQuery) as StoreRuntime<Value> | undefined;
  if (runtime === undefined || runtime.graph !== graph)
    throw new TypeError("Query does not belong to this graph");
  assertStoreOpen(runtime);
  if (runtime.incarnation === undefined) throw new TypeError("The query incarnation is inactive");
  inheritTransferCoverage(runtime);
  const state = observationState(runtime);
  if (state.publisher !== undefined)
    throw new TypeError("The query already has an observation publisher");
  const token = {};
  const activity = publisherActive(state);
  registerTransferReadable(activity.cell, runtime, "observation-activity-cell");
  noteTransferChange(runtime);
  if (runtime.binding !== undefined) noteTransferChange(runtime.binding.handle);
  noteTransferChange(state);
  const current = (): boolean =>
    !state.retired &&
    state.publisher === token &&
    runtime.observation === state &&
    runtime.incarnation === state.incarnation &&
    !runtime.disposed &&
    !runtime.transitioning &&
    !graphState.disposed &&
    runtime.binding?.context.disposed !== true;
  state.publisher = token;
  let issue = state.issueReadable;
  if (issue === undefined) {
    issue = observationIssueReadable(runtime, state, graph, graphState);
    state.issueReadable = issue;
    registerTransferReadable(issue, runtime, "observation-issue");
    attachObservationIssueHooks(runtime, state, issue, graphState);
  }
  reactiveInternal.bindReadable(activity.readable, { graphId: graph.id });
  registerTransferReadable(activity.readable, runtime, "observation-activity");
  const lease: ObservationLease<Value> = Object.freeze({
    active: activity.readable,
    issue,
    publish(value: Value, options?: ObservationPublishOptions): boolean {
      if (!current()) return false;
      const data: Record<string, unknown> =
        options === undefined ? {} : ownData(options, ["transfer"]);
      if (
        data.transfer !== undefined &&
        data.transfer !== "eligible" &&
        data.transfer !== "exclude"
      )
        throw new TypeError("Invalid observation transfer policy");
      if (!current()) return false;
      reactiveInternal.assertWritesAllowed(runtime.committed);
      return publishObservation(runtime, value, data.transfer !== "exclude", current);
    },
    reject(value: Readonly<object>, diagnostic?: { readonly cause: unknown }): boolean {
      if (!current()) return false;
      if (value === null || typeof value !== "object")
        throw new TypeError("Expected an observation issue object");
      const data = diagnostic === undefined ? undefined : ownData(diagnostic, ["cause"]);
      if (data !== undefined && !Object.hasOwn(data, "cause"))
        throw new TypeError("Missing diagnostic cause");
      if (!current()) return false;
      reactiveInternal.assertWritesAllowed(state.issue);
      if (state.issue.peek() !== value) {
        noteTransferChange(state);
        noteTransferChange(runtime);
        if (runtime.binding !== undefined) noteTransferChange(runtime.binding.handle);
      }
      state.issue.set(value);
      if (data !== undefined) reportContinuationError(graphState, data.cause);
      return true;
    },
    report(cause: unknown): boolean {
      if (!current()) return false;
      reportContinuationError(graphState, cause);
      return true;
    },
    dispose(): void {
      if (state.publisher !== token) return;
      reactiveInternal.assertWritesAllowed(activity.cell);
      noteTransferChange(state);
      noteTransferChange(runtime);
      if (runtime.binding !== undefined) noteTransferChange(runtime.binding.handle);
      state.publisher = undefined;
      state.publisherActivity = undefined;
      activity.cell.set(false);
    },
  });
  observationLeaseRecords.set(lease, {
    runtime,
    state,
    token,
    incarnation: state.incarnation,
    activity,
    current,
  } as unknown as ObservationLeaseRecord<unknown>);
  return lease;
}
