import { storeKey } from "./identity.ts";
import type { Graph, OpenOptions, StreamDefinition, StreamStore } from "./types.ts";
import {
  graphRuntimeByGraph,
  registerProjection,
  reportGraphContinuationError,
  runtimeForStore,
} from "./registry.ts";
import { Fault } from "../fault.ts";
import { isObject } from "../utils.ts";
import { signal } from "../reactive.ts";
import type { Readable } from "../reactive.ts";
import { assertStoreOpen } from "./faults.ts";
import { isLive, scheduleSweep, updateCollectionCandidate } from "./collection.ts";
import type { GraphProjection, GraphRuntimeState, StreamEvent, StoreRuntime } from "./runtime.ts";

const projectionInitialByValueCell = new WeakMap<object, unknown>();

interface TerminalProjectionCleanup {
  remainingReadables: number;
  readonly state: WeakRef<GraphRuntimeState<any>>;
  readonly projection: WeakRef<GraphProjection>;
}

const terminalProjectionReadableFinalizer = new FinalizationRegistry<TerminalProjectionCleanup>(
  (cleanup) => {
    cleanup.remainingReadables -= 1;
    if (cleanup.remainingReadables === 0) {
      const state = cleanup.state.deref();
      const projection = cleanup.projection.deref();
      if (state !== undefined && projection !== undefined) state.projections.delete(projection);
    }
  },
);

/**
 * Lifecycle state of a stream projection.
 *
 * A projection starts `open`, becomes `closed` on explicit close, normal
 * iterator completion, or graph disposal, and becomes `failed` when its
 * source reports a gap, the iterator fails, accumulation overflows with the
 * default policy, or a reducer throws.
 */
export type ProjectionStatus = "open" | "closed" | "failed";

/**
 * Accumulates stream emissions in a bounded array.
 *
 * `max` must be a finite positive integer. When the bound is reached,
 * `onOverflow` defaults to `"fail"`; `"drop-oldest"` keeps the newest `max`
 * items instead.
 */
export type AccumulatePolicy = {
  /** Selects bounded array accumulation. */
  readonly kind: "accumulate";

  /** Maximum number of emissions retained in the projected value. */
  readonly max: number;

  /** Overflow behavior; defaults to `"fail"`. */
  readonly onOverflow?: "drop-oldest" | "fail";
};

/**
 * Folds each stream emission into one projected value.
 *
 * The initial value is exposed before the first emission. `step` is called
 * once per emission with the previous projected value and the new item.
 */
export type ReducePolicy<T, V> = {
  /** Selects reducer-based projection. */
  readonly kind: "reduce";

  /** Projected value before the first stream item is received. */
  readonly initial: V;

  /** Computes the next projected value from the previous value and an item. */
  readonly step: (accumulator: V, item: T) => V;
};

/** The accumulation or reduction policy used by {@link project}. */
export type ProjectionPolicy<T, V> = AccumulatePolicy | ReducePolicy<T, V>;

/** Validates policy fields without executing the reducer or inspecting its data. */
function isProjectionPolicy(value: unknown): value is ProjectionPolicy<unknown, unknown> {
  if (!isObject(value) || !("kind" in value)) return false;
  if (value.kind === "reduce")
    return "initial" in value && "step" in value && typeof value.step === "function";
  if (value.kind !== "accumulate") return false;
  return (
    "max" in value &&
    typeof value.max === "number" &&
    (!("onOverflow" in value) ||
      value.onOverflow === undefined ||
      value.onOverflow === "fail" ||
      value.onOverflow === "drop-oldest")
  );
}

/**
 * A value maintained from stream emissions.
 *
 * The graph/definition overload of {@link project} opens an independent source.
 * The StreamStore overload consumes future emissions from its existing shared
 * session. Closing a shared projection releases only that projection's listener;
 * the source remains subject to the graph's normal listener/collection lifetime.
 */
export interface Projection<V> {
  /** The accumulated array or reduced value produced so far. */
  readonly value: Readable<V>;

  /** The projection's current lifecycle state. */
  readonly status: Readable<ProjectionStatus>;

  /** The failure reason, or `undefined` until a failure occurs. */
  readonly error: Readable<unknown>;

  /**
   * Terminates the projection and releases its source listener.
   * An independently opened source is also aborted.
   *
   * Closing is idempotent. It sets `status` to `"closed"`, does not create an
   * error, and ignores later source emissions. A projection that has already
   * failed remains `"failed"` when closed again.
   */
  close(): void;
}

interface ProjectionRuntime<T, V> {
  readonly graph: Graph;
  readonly valueCell: ReturnType<typeof signal<V>>;
  readonly statusCell: ReturnType<typeof signal<ProjectionStatus>>;
  readonly errorCell: ReturnType<typeof signal<unknown>>;
  readonly policy: ProjectionPolicy<T, V>;
  controller: AbortController;
  iterator?: AsyncIterator<T>;
  unregister?: () => void;
  detach?: () => void;
  restartSource?: () => void;
  generation: number;
  closed: boolean;
}

function validateMax(max: number): void {
  // short-circuit to avoid the more expensive Number.isInteger check when max is not finite
  if (!(Number.isFinite(max) && max >= 1 && Number.isInteger(max))) {
    throw new Fault("contract", ["max"]);
  }
}

function reportProjectionError<T, V>(runtime: ProjectionRuntime<T, V>, error: unknown): void {
  reportGraphContinuationError(runtime.graph, error);
}

function safeSet<T, V>(runtime: ProjectionRuntime<T, V>, set: () => void): void {
  try {
    set();
  } catch (error) {
    reportProjectionError(runtime, error);
  }
}

function stopSource<T, V>(runtime: ProjectionRuntime<T, V>): void {
  runtime.detach?.();
  runtime.detach = undefined;
  runtime.controller.abort();
  if (runtime.iterator != null && typeof runtime.iterator.return === "function") {
    try {
      void Promise.resolve(runtime.iterator.return()).catch(() => undefined);
    } catch {
      // A source's close error does not replace its current lifecycle result.
    }
  }
  runtime.iterator = undefined;
}

function registerTerminalReset<V>(
  graph: Graph,
  terminalStatus: ProjectionStatus,
  valueCell: WeakRef<ReturnType<typeof signal<V>>>,
  statusCell: WeakRef<ReturnType<typeof signal<ProjectionStatus>>>,
  errorCell: WeakRef<ReturnType<typeof signal<unknown>>>,
): void {
  const state = graphRuntimeByGraph.get(graph as object);
  if (state === undefined || state.disposed) return;

  let unregister: (() => void) | undefined;
  const finalizerToken = {};
  let active = true;
  const reset = (): void => {
    if (!active) return;
    active = false;
    terminalProjectionReadableFinalizer.unregister(finalizerToken);
    unregister?.();
    unregister = undefined;

    const value = valueCell.deref();
    const status = statusCell.deref();
    const error = errorCell.deref();
    const initial =
      value !== undefined && projectionInitialByValueCell.has(value)
        ? projectionInitialByValueCell.get(value)
        : ([] as unknown as V);
    if (value !== undefined) projectionInitialByValueCell.delete(value);

    if (status !== undefined) {
      try {
        status.set(terminalStatus);
      } catch (reason) {
        reportGraphContinuationError(graph, reason);
      }
    }
    if (value !== undefined) {
      try {
        value.set(initial as V);
      } catch (reason) {
        reportGraphContinuationError(graph, reason);
      }
    }
    if (error !== undefined) {
      try {
        error.set(undefined);
      } catch (reason) {
        reportGraphContinuationError(graph, reason);
      }
    }
  };
  const projection: GraphProjection = {
    reset: () => {
      reset();
      return undefined;
    },
    restart: () => undefined,
    dispose: () => undefined,
  };
  unregister = registerProjection(graph, projection);
  const cleanup: TerminalProjectionCleanup = {
    remainingReadables: 0,
    state: new WeakRef(state),
    projection: new WeakRef(projection),
  };
  for (const readable of [valueCell, statusCell, errorCell]) {
    const cell = readable.deref();
    if (cell === undefined) continue;
    cleanup.remainingReadables += 1;
    terminalProjectionReadableFinalizer.register(cell, cleanup, finalizerToken);
  }
}

function finish<T, V>(runtime: ProjectionRuntime<T, V>, failed: boolean, reason?: unknown): void {
  if (runtime.closed) return;
  runtime.closed = true;
  const resetVersion = graphRuntimeByGraph.get(runtime.graph as object)?.resetVersion.peek() ?? 0;
  runtime.unregister?.();
  runtime.unregister = undefined;
  const terminalStatus = failed ? "failed" : "closed";
  registerTerminalReset(
    runtime.graph,
    terminalStatus,
    new WeakRef(runtime.valueCell),
    new WeakRef(runtime.statusCell),
    new WeakRef(runtime.errorCell),
  );
  stopSource(runtime);
  if ((graphRuntimeByGraph.get(runtime.graph as object)?.resetVersion.peek() ?? 0) !== resetVersion)
    return;
  if (failed) {
    if (reason != null) safeSet(runtime, () => runtime.errorCell.set(reason));
    if (
      (graphRuntimeByGraph.get(runtime.graph as object)?.resetVersion.peek() ?? 0) !== resetVersion
    )
      return;
  }
  safeSet(runtime, () => runtime.statusCell.set(terminalStatus));
}

function prepareReset<T, V>(runtime: ProjectionRuntime<T, V>): (() => void) | undefined {
  if (runtime.closed) return undefined;
  const controller = runtime.controller;
  const iterator = runtime.iterator;
  runtime.detach?.();
  runtime.detach = undefined;
  runtime.iterator = undefined;
  runtime.generation += 1;
  runtime.controller = new AbortController();
  const initial = runtime.policy.kind === "reduce" ? runtime.policy.initial : ([] as unknown as V);
  safeSet(runtime, () => {
    runtime.valueCell.set(initial);
    runtime.errorCell.set(undefined);
    runtime.statusCell.set("open");
  });
  return () => {
    controller.abort();
    if (iterator != null && typeof iterator.return === "function") {
      try {
        void Promise.resolve(iterator.return()).catch(() => undefined);
      } catch {
        // Reset already retired this source session.
      }
    }
  };
}

function fail<T, V>(runtime: ProjectionRuntime<T, V>, reason: unknown): void {
  finish(runtime, true, reason);
}

function isCurrentGeneration<T, V>(
  runtime: ProjectionRuntime<T, V>,
  controller: AbortController,
  generation: number,
): boolean {
  return !runtime.closed && runtime.controller === controller && runtime.generation === generation;
}

function accept<T, V>(
  runtime: ProjectionRuntime<T, V>,
  item: T,
  controller: AbortController,
  generation: number,
): void {
  if (!isCurrentGeneration(runtime, controller, generation)) return;
  const policy = runtime.policy;
  if (policy.kind === "reduce") {
    let next: V;
    try {
      next = policy.step(runtime.valueCell.peek(), item);
    } catch (error) {
      if (isCurrentGeneration(runtime, controller, generation)) fail(runtime, error);
      return;
    }
    if (!isCurrentGeneration(runtime, controller, generation)) return;
    safeSet(runtime, () => runtime.valueCell.set(next));
    return;
  }

  const current = runtime.valueCell.peek() as unknown as readonly T[];
  if (current.length >= policy.max) {
    if ((policy.onOverflow ?? "fail") === "fail") {
      fail(runtime, new Error("Aeolia projection accumulation exceeded max"));
      return;
    }
    safeSet(runtime, () => runtime.valueCell.set([...current.slice(1), item] as unknown as V));
    return;
  }
  safeSet(runtime, () => runtime.valueCell.set([...current, item] as unknown as V));
}

async function consume<T, V>(
  runtime: ProjectionRuntime<T, V>,
  controller: AbortController,
  generation: number,
): Promise<void> {
  const iterator = runtime.iterator;
  if (iterator === undefined) return;
  try {
    while (
      !runtime.closed &&
      runtime.controller === controller &&
      runtime.generation === generation
    ) {
      const result = await iterator.next();
      if (runtime.closed || runtime.controller !== controller || runtime.generation !== generation)
        return;
      if (result.done) {
        finish(runtime, false);
        return;
      }
      accept(runtime, result.value, controller, generation);
    }
  } catch (error) {
    if (!runtime.closed && runtime.controller === controller && runtime.generation === generation)
      fail(runtime, error);
  }
}

function initializeProjection<T, V>(
  graph: Graph,
  policy: ProjectionPolicy<T, V>,
): { runtime: ProjectionRuntime<T, V>; projection: Projection<V> } {
  const valueCell = signal<V>(policy.kind === "reduce" ? policy.initial : ([] as unknown as V));
  const statusCell = signal<ProjectionStatus>("open");
  const errorCell = signal<unknown>(undefined);
  const controller = new AbortController();
  const runtime: ProjectionRuntime<T, V> = {
    graph,
    valueCell,
    statusCell,
    errorCell,
    policy,
    controller,
    generation: 0,
    closed: false,
  };
  if (policy.kind === "reduce") projectionInitialByValueCell.set(valueCell, policy.initial);

  const close = (): void => finish(runtime, false);
  const projection: Projection<V> = Object.freeze({
    value: valueCell,
    status: statusCell,
    error: errorCell,
    close,
  });

  runtime.unregister = registerProjection(graph, {
    reset: () => prepareReset(runtime),
    restart: () => {
      if (!runtime.closed) runtime.restartSource?.();
    },
    dispose: close,
  });
  return { runtime, projection };
}

function makeProjection<T, V>(
  graph: Graph,
  stream: StreamDefinition<any, T, any>,
  input: unknown,
  policy: ProjectionPolicy<T, V>,
): Projection<V> {
  const { runtime, projection } = initializeProjection(graph, policy);
  runtime.restartSource = () => openIndependent(runtime, graph, stream, input);
  openIndependent(runtime, graph, stream, input, true);
  return projection;
}

function openIndependent<T, V>(
  runtime: ProjectionRuntime<T, V>,
  graph: Graph,
  stream: StreamDefinition<any, T, any>,
  input: unknown,
  propagateKeyError = false,
): void {
  const controller = runtime.controller;
  const generation = runtime.generation;
  let key: ReturnType<typeof storeKey>;
  try {
    key = storeKey(stream.key(input));
  } catch (error) {
    if (runtime.controller !== controller || runtime.generation !== generation) return;
    if (propagateKeyError) {
      runtime.unregister?.();
      runtime.unregister = undefined;
      runtime.closed = true;
      controller.abort();
      throw error;
    }
    fail(runtime, error);
    return;
  }
  if (!isCurrentGeneration(runtime, controller, generation)) return;
  const options: OpenOptions = {
    abortSignal: controller.signal,
    graph: graph.id,
    key,
    reportGap: () => {
      if (!runtime.closed && runtime.controller === controller && runtime.generation === generation)
        fail(runtime, new Error("Aeolia projection stream reported a gap"));
    },
  };
  let iterable: AsyncIterable<T>;
  try {
    iterable = stream.open(input, options);
    const iterator = iterable[Symbol.asyncIterator]();
    if (runtime.closed || runtime.controller !== controller || runtime.generation !== generation) {
      if (typeof iterator.return === "function")
        void Promise.resolve(iterator.return()).catch(() => undefined);
      return;
    }
    runtime.iterator = iterator;
  } catch (error) {
    if (runtime.controller !== controller || runtime.generation !== generation) return;
    fail(runtime, error);
    return;
  }
  void consume(runtime, controller, generation);
}

function makeSharedProjection<T, V>(
  source: StreamStore<T>,
  policy: ProjectionPolicy<T, V>,
): Projection<V> {
  const store = runtimeForStore(source) as StoreRuntime<T> | undefined;
  if (store === undefined || source.status !== store.streamStatus)
    throw new TypeError("A shared projection requires an Aeolia StreamStore");
  assertStoreOpen(store);
  const { runtime, projection } = initializeProjection(store.graph, policy);
  runtime.restartSource = () => attachSharedProjection(runtime, store, source);
  attachSharedProjection(runtime, store, source);
  return projection;
}

function attachSharedProjection<T, V>(
  runtime: ProjectionRuntime<T, V>,
  store: StoreRuntime<T>,
  source: StreamStore<T>,
): void {
  if (runtime.closed || runtime.detach !== undefined) return;
  const session = store.stream;
  if (session === undefined || session.ended) {
    const failed = source.status.peek() === "failed";
    finish(runtime, failed, failed ? source.error.peek() : undefined);
    return;
  }
  const controller = runtime.controller;
  const generation = runtime.generation;
  const listener = (event: StreamEvent<unknown>): void => {
    if (
      runtime.closed ||
      runtime.controller !== controller ||
      runtime.generation !== generation ||
      store.stream !== session ||
      session.resetEpoch !== store.graph.__runtime.resetEpoch
    )
      return;
    if (event.kind === "value") accept(runtime, event.value as T, controller, generation);
    else if (event.kind === "gap")
      fail(runtime, new Error("Aeolia projection stream reported a gap"));
    else finish(runtime, event.failed, event.reason);
  };
  session.listeners.add(listener);
  session.unobservedSince = undefined;
  updateCollectionCandidate(store);
  scheduleSweep(store.graph.__runtime);
  runtime.detach = () => {
    session.listeners.delete(listener);
    if (store.stream === session && !isLive(store)) session.unobservedSince = Date.now();
    updateCollectionCandidate(store);
    scheduleSweep(store.graph.__runtime);
  };
}

/**
 * Opens an independent stream source and projects its emissions.
 *
 * Accumulation starts with an empty array and reduction starts with the
 * policy's `initial` value. Every source gap is terminal for the projection.
 * A normal iterator completion closes it; iterator failures, source gaps,
 * reducer failures, and fatal accumulation overflow set `error` and move the
 * status to `"failed"`. The value remains at its last successful state when a
 * projection fails.
 *
 * `project` does not reuse or write the keyed stream store. Its source is
 * opened separately and its abort signal belongs to this projection alone.
 * The graph closes registered projections during graph disposal.
 *
 * @param graph - The open graph that owns the projection lifecycle.
 * @param stream - The stream definition whose source is opened independently.
 * @param input - Input passed to the stream key and open callbacks.
 * @param policy - Bounded accumulation or reducer policy.
 * @returns A projection whose reactive value and lifecycle readables are
 * updated as source items arrive.
 * @throws {@link Fault} With kind `contract` when an accumulation maximum is
 * not a finite positive integer, or kind `disposed` when the graph is closed.
 * The stream key's synchronous error is rethrown. Errors from opening or
 * consuming the source are represented by a failed projection instead.
 * @throws TypeError if the policy fields or reducer type are malformed.
 *
 * @example
 * ```ts
 * import { project, subscribe, type Graph, type StreamDefinition } from "aeolia";
 *
 * declare const graph: Graph;
 * declare const events: StreamDefinition<void, number>;
 *
 * const projection = project(graph, events, undefined, {
 *   kind: "accumulate",
 *   max: 50,
 *   onOverflow: "drop-oldest",
 * });
 * const stop = subscribe(projection.value, (value) => console.log(value));
 * // Later:
 * stop();
 * projection.close();
 *
 * const total = project(graph, events, undefined, {
 *   kind: "reduce",
 *   initial: 0,
 *   step: (sum, item) => sum + item,
 * });
 * ```
 */
export function project<I, T, V, K extends "reduce" | "accumulate">(
  graph: Graph,
  stream: StreamDefinition<I, T>,
  input: I,
  policy: ProjectionPolicy<T, V> & { readonly kind: K },
): Projection<K extends "accumulate" ? readonly T[] : V>;
/**
 * Projects future emissions from an existing keyed stream session.
 *
 * Does not open a source or replay the store's retained value. Every subsequent
 * source emission is consumed, including equal values suppressed by the store's
 * equality rule. Direct store writes are not source emissions. Each projection
 * has separate accumulation/reduction state and counts as a source listener until
 * it closes or fails. Source gaps fail this projection without closing the shared
 * source. Normal source completion closes it; source failure preserves the reason.
 * A later source reopen does not reopen an already-terminal projection.
 *
 * @param source - StreamStore whose current session supplies emissions.
 * @param policy - Bounded accumulation or reduction, with the same rules as the
 * independent overload.
 * @returns An eager projection with its own value and lifecycle readables.
 * @throws TypeError if source is not an Aeolia StreamStore or the policy is malformed.
 * @throws Fault if its graph is disposed or the accumulation maximum is invalid.
 */
export function project<T, V, K extends "reduce" | "accumulate">(
  source: StreamStore<T>,
  policy: ProjectionPolicy<T, V> & { readonly kind: K },
): Projection<K extends "accumulate" ? readonly T[] : V>;
export function project<I, T, V>(
  graphOrSource: Graph | StreamStore<T>,
  streamOrPolicy: StreamDefinition<I, T> | ProjectionPolicy<T, V>,
  input?: I,
  suppliedPolicy?: ProjectionPolicy<T, V>,
): Projection<V> {
  const policy = suppliedPolicy ?? (streamOrPolicy as ProjectionPolicy<T, V>);
  if (!isProjectionPolicy(policy)) throw new TypeError("Invalid projection policy.");
  if (policy.kind === "accumulate") validateMax(policy.max);
  if (suppliedPolicy === undefined)
    return makeSharedProjection(graphOrSource as StreamStore<T>, policy);
  return makeProjection(
    graphOrSource as Graph,
    streamOrPolicy as StreamDefinition<I, T>,
    input,
    policy,
  );
}
