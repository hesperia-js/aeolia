import type { Graph, QueryStore, Readable } from "./types.ts";
import type { StoreRuntime } from "./runtime.ts";
import { computed, __internal as reactiveInternal } from "../reactive.ts";
import { graphRuntimeByGraph, queryRuntimeByStore } from "./registry.ts";
import { assertGraphOpen, assertStoreOpen, reportContinuationError } from "./faults.ts";
import { observationState, publisherActive } from "./observation-state.ts";
import { publishObservation } from "./store-state.ts";
import { scheduleSweep, touch, updateCollectionCandidate } from "./collection.ts";

/** Transfer eligibility of the exact newly observed committed base. */
export interface ObservationPublishOptions {
  /** Defaults to eligible; exclude is for an explicitly qualified native fallback. */
  readonly transfer?: "eligible" | "exclude";
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
  existingProfileQuery: QueryStore<Value>,
): ObservationLease<Value> {
  const graphState = graphRuntimeByGraph.get(graph);
  if (graphState === undefined) throw new TypeError("The value is not an Aeolia graph");
  assertGraphOpen(graphState);
  const runtime = queryRuntimeByStore.get(existingProfileQuery) as StoreRuntime<Value> | undefined;
  if (runtime === undefined || runtime.graph !== graph)
    throw new TypeError("Query does not belong to this graph");
  assertStoreOpen(runtime);
  if (runtime.incarnation === undefined) throw new TypeError("The query incarnation is inactive");
  const state = observationState(runtime);
  if (state.publisher !== undefined)
    throw new TypeError("The query already has an observation publisher");
  const token = {};
  const activity = publisherActive(state);
  const current = (): boolean =>
    !state.retired &&
    state.publisher === token &&
    runtime.incarnation === state.incarnation &&
    !runtime.disposed &&
    !runtime.transitioning &&
    !graphState.disposed &&
    runtime.binding?.context.disposed !== true;
  state.publisher = token;
  let issue = state.issueReadable;
  if (issue === undefined) {
    issue = computed(() => {
      if (!state.live.get()) return undefined;
      assertStoreOpen(runtime);
      return state.issue.get();
    });
    state.issueReadable = issue;
    reactiveInternal.bindReadable(issue, {
      graphId: graph.id,
      reportFault: (fault) => reportContinuationError(graphState, fault),
    });
    state.stops.push(
      reactiveInternal.onActualRead(issue, () => {
        if (state.retired) return;
        assertStoreOpen(runtime);
        touch(runtime);
      }),
    );
    state.stops.push(
      reactiveInternal.onLivenessChange(issue, (live) => {
        if (state.retired) return;
        state.liveReadables += live ? 1 : -1;
        runtime.liveReadableCount += live ? 1 : -1;
        if (live) touch(runtime);
        updateCollectionCandidate(runtime);
        scheduleSweep(graphState);
      }),
    );
  }
  reactiveInternal.bindReadable(activity.readable, { graphId: graph.id });
  return Object.freeze({
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
      state.publisher = undefined;
      state.publisherActivity = undefined;
      activity.cell.set(false);
    },
  });
}
