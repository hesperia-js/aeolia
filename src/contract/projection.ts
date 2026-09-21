import { storeKey } from "./identity.ts";
import type { Graph, OpenOptions, StreamDefinition, StreamStore } from "./types.ts";
import { registerProjection, reportGraphContinuationError, runtimeForStore } from "./registry.ts";
import { Fault } from "../fault.ts";
import { signal } from "../reactive.ts";
import type { Readable } from "../reactive.ts";
import { assertStoreOpen } from "./faults.ts";
import { isLive, scheduleSweep, updateCollectionCandidate } from "./collection.ts";
import type { StreamEvent } from "./runtime.ts";

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
  readonly controller: AbortController;
  iterator?: AsyncIterator<T>;
  unregister?: () => void;
  detach?: () => void;
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

function finish<T, V>(runtime: ProjectionRuntime<T, V>, failed: boolean, reason?: unknown): void {
  if (runtime.closed) return;
  runtime.closed = true;
  runtime.detach?.();
  runtime.detach = undefined;
  runtime.controller.abort();
  if (runtime.iterator != null && typeof runtime.iterator.return === "function") {
    try {
      void Promise.resolve(runtime.iterator.return()).catch(() => undefined);
    } catch {
      // Closing is idempotent and the stream's own close error is not a new
      // projection failure once the projection has already terminated.
    }
  }
  if (failed) {
    if (reason != null) safeSet(runtime, () => runtime.errorCell.set(reason));
    safeSet(runtime, () => runtime.statusCell.set("failed"));
  } else {
    safeSet(runtime, () => runtime.statusCell.set("closed"));
  }
  runtime.unregister?.();
  runtime.unregister = undefined;
}

function fail<T, V>(runtime: ProjectionRuntime<T, V>, reason: unknown): void {
  finish(runtime, true, reason);
}

function accept<T, V>(runtime: ProjectionRuntime<T, V>, item: T): void {
  if (runtime.closed) return;
  const policy = runtime.policy;
  if (policy.kind === "reduce") {
    let next: V;
    try {
      next = policy.step(runtime.valueCell.peek(), item);
    } catch (error) {
      fail(runtime, error);
      return;
    }
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

async function consume<T, V>(runtime: ProjectionRuntime<T, V>): Promise<void> {
  const iterator = runtime.iterator;
  if (iterator === undefined) return;
  try {
    while (!runtime.closed) {
      const result = await iterator.next();
      if (runtime.closed) return;
      if (result.done) {
        finish(runtime, false);
        return;
      }
      accept(runtime, result.value);
    }
  } catch (error) {
    if (!runtime.closed) fail(runtime, error);
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
    closed: false,
  };

  const close = (): void => finish(runtime, false);
  const projection: Projection<V> = Object.freeze({
    value: valueCell,
    status: statusCell,
    error: errorCell,
    close,
  });

  runtime.unregister = registerProjection(graph, close);
  return { runtime, projection };
}

function makeProjection<T, V>(
  graph: Graph,
  stream: StreamDefinition<any, T, any>,
  input: unknown,
  policy: ProjectionPolicy<T, V>,
): Projection<V> {
  const { runtime, projection } = initializeProjection(graph, policy);
  const controller = runtime.controller;

  let key: ReturnType<typeof storeKey>;
  try {
    key = storeKey(stream.key(input));
  } catch (error) {
    // The key is a caller-side synchronous failure. Do not leave a graph
    // registration or an abort controller behind when it escapes.
    runtime.unregister?.();
    runtime.unregister = undefined;
    runtime.closed = true;
    controller.abort();
    throw error;
  }
  const options: OpenOptions = {
    abortSignal: controller.signal,
    graph: graph.id,
    key,
    reportGap: () => {
      if (!runtime.closed) fail(runtime, new Error("Aeolia projection stream reported a gap"));
    },
  };
  let iterable: AsyncIterable<T>;
  try {
    iterable = stream.open(input, options);
    runtime.iterator = iterable[Symbol.asyncIterator]();
  } catch (error) {
    fail(runtime, error);
    return projection;
  }
  void consume(runtime);
  return projection;
}

function makeSharedProjection<T, V>(
  source: StreamStore<T>,
  policy: ProjectionPolicy<T, V>,
): Projection<V> {
  const store = runtimeForStore(source);
  if (store === undefined || source.status !== store.streamStatus)
    throw new TypeError("A shared projection requires an Aeolia StreamStore");
  assertStoreOpen(store);
  const { runtime, projection } = initializeProjection(store.graph, policy);
  const session = store.stream;
  if (session === undefined || session.ended) {
    const failed = source.status.peek() === "failed";
    finish(runtime, failed, failed ? source.error.peek() : undefined);
    return projection;
  }
  const listener = (event: StreamEvent<unknown>): void => {
    if (event.kind === "value") accept(runtime, event.value as T);
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
  return projection;
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
 * @throws TypeError if source is not an Aeolia StreamStore; Fault if its graph is
 * disposed or the accumulation maximum is invalid.
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
