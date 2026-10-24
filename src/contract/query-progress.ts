import type {
  QueryProgressEvent,
  QueryProgressObservation,
  QueryProgressSnapshot,
  Readable,
} from "./types.ts";
import type {
  QueryProgressAuthority,
  QueryProgressDelivery,
  QueryProgressListener,
  QueryRequest,
  StoreRuntime,
} from "./runtime.ts";
import type { Unsubscribe } from "../fault.ts";
import { signal, __internal as reactiveInternal } from "../reactive.ts";
import { assertStoreOpen, graphFault, reportContinuationError } from "./faults.ts";
import { scheduleSweep, updateCollectionCandidate } from "./collection.ts";

function idleSnapshot(): QueryProgressSnapshot<unknown> {
  return Object.freeze({
    hasValue: false,
    value: undefined,
    status: "idle",
    reportRevision: 0,
    error: undefined,
  });
}

function snapshot<P>(
  execution: object,
  reportRevision: number,
  status: QueryProgressSnapshot<P>["status"],
  hasValue: boolean,
  value: P | undefined,
  error: unknown,
): QueryProgressSnapshot<P> {
  return Object.freeze({ execution, hasValue, value, status, reportRevision, error });
}

function progressCell<T>(runtime: StoreRuntime<T>) {
  const existing = runtime.queryProgressCell;
  if (existing !== undefined) return existing;

  const state = runtime.graph.__runtime;
  const initial = idleSnapshot();
  const created = signal(initial, {
    label: `query:${String(runtime.key)}:progress`,
  });
  runtime.queryProgressCell = created;
  runtime.queryProgressAuthority = {
    current: initial,
    sequence: 0,
    listeners: new Set(),
    queue: [],
    continuationErrors: [],
    dispatching: false,
  };
  runtime.queryProgressClear = () => clearQueryProgress(runtime);
  runtime.queryProgressAuthorityChange = (request) => {
    interruptQueryProgress(runtime, request, undefined, { authorityChanged: true });
  };
  reactiveInternal.bindReadable(created, {
    graphId: state.graph.id,
    reportFault: (fault) => graphFault(state, fault),
  });
  observeQueryProgressReadable(runtime, created);
  return created;
}

function authority<T>(runtime: StoreRuntime<T>): QueryProgressAuthority {
  progressCell(runtime);
  return runtime.queryProgressAuthority!;
}

function observeQueryProgressReadable<T>(
  runtime: StoreRuntime<T>,
  readable: Readable<unknown>,
): void {
  runtime.readStops.push(
    reactiveInternal.onActualRead(readable, () => {
      assertStoreOpen(runtime);
    }),
  );
  runtime.livenessStops.push(
    reactiveInternal.onLivenessChange(readable, (live) => {
      retainProgress(runtime, live ? 1 : -1);
    }),
  );
}

function retainProgress<T>(runtime: StoreRuntime<T>, amount: number): void {
  runtime.progressReadableCount = Math.max(0, runtime.progressReadableCount + amount);
  updateCollectionCandidate(runtime);
  scheduleSweep(runtime.graph.__runtime);
}

function currentRequest<T>(runtime: StoreRuntime<T>, request: QueryRequest): boolean {
  const state = runtime.graph.__runtime;
  const context = runtime.binding?.context;
  return (
    runtime.activeRequest === request &&
    !request.settled &&
    !request.superseded &&
    !request.controller.signal.aborted &&
    request.generation === runtime.generation &&
    request.resetEpoch === state.resetEpoch &&
    request.incarnation === runtime.incarnation &&
    !runtime.disposed &&
    !state.disposed &&
    context?.disposed !== true
  );
}

function ownsSnapshot<T>(runtime: StoreRuntime<T>, request: QueryRequest): boolean {
  const state = runtime.graph.__runtime;
  const current = runtime.queryProgressAuthority?.current;
  return (
    current?.execution === request.progressIdentity &&
    request.generation === runtime.generation &&
    request.resetEpoch === state.resetEpoch &&
    request.incarnation === runtime.incarnation &&
    !runtime.disposed &&
    !state.disposed &&
    runtime.binding?.context.disposed !== true
  );
}

function nextSequence(current: number): number {
  if (!Number.isSafeInteger(current) || current < 0 || current >= Number.MAX_SAFE_INTEGER)
    throw new RangeError("Query progress event sequence exhausted");
  return current + 1;
}

function makeEvent<P>(
  kind: QueryProgressEvent<P>["kind"],
  value: QueryProgressSnapshot<P>,
): QueryProgressEvent<P> {
  return Object.freeze({ kind, snapshot: value });
}

function sameExecution(
  authorityState: QueryProgressAuthority,
  request: QueryRequest | undefined,
): boolean {
  return (
    request !== undefined && authorityState.current.execution === request.progressIdentity
  );
}

function seedIsCurrent<T>(
  runtime: StoreRuntime<T>,
  owner: QueryProgressAuthority,
  delivery: QueryProgressDelivery,
): boolean {
  const { event, request } = delivery;
  if (runtime.disposed || runtime.graph.__runtime.disposed) return false;
  if (runtime.binding?.context.disposed === true) return false;
  if (event.snapshot.execution === undefined)
    return (
      request === undefined &&
      owner.current.execution === undefined &&
      owner.current.status === event.snapshot.status
    );
  if (
    owner.current.execution !== event.snapshot.execution
  )
    return false;
  if (request === undefined)
    return (
      owner.request === undefined &&
      event.snapshot.status !== "active" &&
      owner.current.status === event.snapshot.status
    );
  if (owner.request !== request) return false;
  if (event.snapshot.status === "interrupted")
    return owner.current.status === "interrupted";
  if (
    request.generation !== runtime.generation ||
    request.resetEpoch !== runtime.graph.__runtime.resetEpoch ||
    request.incarnation !== runtime.incarnation
  )
    return false;
  if (event.snapshot.status === "active")
    return (
      owner.current.status === "active" ||
      owner.current.status === "complete" ||
      owner.current.status === "failed"
    );
  return owner.current.status === event.snapshot.status;
}

function deferContinuationError(owner: QueryProgressAuthority, error: unknown): void {
  owner.continuationErrors.push(error);
}

function deliveryIsCurrent<T>(
  runtime: StoreRuntime<T>,
  delivery: QueryProgressDelivery,
): boolean {
  const owner = authority(runtime);
  const { event, request } = delivery;
  if (runtime.disposed || runtime.graph.__runtime.disposed) return false;
  if (runtime.binding?.context.disposed === true) return false;

  if (event.snapshot.execution === undefined)
    return (
      request === undefined &&
      owner.current.execution === undefined &&
      owner.current.status === event.snapshot.status
    );
  if (!sameExecution(owner, request)) return false;

  if (event.snapshot.status === "interrupted")
    return owner.current.status === "interrupted";

  if (
    request!.generation !== runtime.generation ||
    request!.resetEpoch !== runtime.graph.__runtime.resetEpoch ||
    request!.incarnation !== runtime.incarnation
  )
    return false;

  const currentStatus = owner.current.status;
  if (event.kind === "report")
    return currentStatus === "active" || currentStatus === "complete" || currentStatus === "failed";
  if (event.snapshot.status === "active")
    return currentStatus === "active" || currentStatus === "complete" || currentStatus === "failed";
  return currentStatus === event.snapshot.status;
}

function deliverTo(
  owner: QueryProgressAuthority,
  listener: QueryProgressListener,
  event: QueryProgressEvent<unknown>,
): void {
  if (!listener.active) return;
  try {
    listener.observer(event);
  } catch (error) {
    deferContinuationError(owner, error);
  }
}

function drain<T>(runtime: StoreRuntime<T>, owner: QueryProgressAuthority): void {
  while (owner.queue.length !== 0) {
    const delivery = owner.queue.shift()!;
    if (delivery.target !== undefined) {
      const target = delivery.target;
      if (!target.active) continue;
      if (runtime.disposed || runtime.graph.__runtime.disposed || runtime.binding?.context.disposed)
        continue;
      const current = seedIsCurrent(runtime, owner, delivery);
      const event = current ? delivery.event : makeEvent("current", owner.current);
      target.frontier = current ? Math.max(target.frontier, delivery.sequence) : owner.sequence;
      deliverTo(owner, target, event);
      continue;
    }
    if (!deliveryIsCurrent(runtime, delivery)) continue;
    for (const listener of [...owner.listeners]) {
      if (!listener.active || delivery.sequence <= listener.frontier) continue;
      if (!deliveryIsCurrent(runtime, delivery)) break;
      listener.frontier = delivery.sequence;
      deliverTo(owner, listener, delivery.event);
      if (!deliveryIsCurrent(runtime, delivery)) break;
    }
  }
}

function drainAndReport<T>(runtime: StoreRuntime<T>, owner: QueryProgressAuthority): void {
  try {
    while (owner.queue.length !== 0 || owner.continuationErrors.length !== 0) {
      drain(runtime, owner);
      const errors = owner.continuationErrors.splice(0);
      for (const error of errors) reportContinuationError(runtime.graph.__runtime, error);
    }
    if (owner.current.status !== "active") owner.request = undefined;
  } finally {
    owner.dispatching = false;
  }
}

function publish<T, P>(
  runtime: StoreRuntime<T>,
  value: QueryProgressSnapshot<P>,
  kind: "report" | "lifecycle",
  request?: QueryRequest,
): void {
  const cell = progressCell(runtime);
  const owner = authority(runtime);
  reactiveInternal.assertWritesAllowed(cell);
  const sequence = nextSequence(owner.sequence);
  const event = makeEvent(kind, value) as QueryProgressEvent<unknown>;
  const delivery: QueryProgressDelivery = {
    sequence,
    event,
    ...(request === undefined ? {} : { request }),
  };

  owner.current = value as QueryProgressSnapshot<unknown>;
  if (request !== undefined) owner.request = request;
  else if (value.execution === undefined) owner.request = undefined;
  owner.sequence = sequence;
  owner.queue.push(delivery);
  const ownsDispatch = !owner.dispatching;
  if (ownsDispatch) owner.dispatching = true;
  try {
    cell.set(value as QueryProgressSnapshot<unknown>);
  } catch (error) {
    deferContinuationError(owner, error);
  }
  if (ownsDispatch) drainAndReport(runtime, owner);
}

function subscribeProgress<T, P>(
  runtime: StoreRuntime<T>,
  observer: (event: QueryProgressEvent<P>) => void,
): Unsubscribe {
  assertStoreOpen(runtime);
  if (typeof observer !== "function") throw new TypeError("Progress observer must be a function");
  progressCell(runtime);
  const owner = authority(runtime);
  const listener: QueryProgressListener = {
    observer: observer as (event: QueryProgressEvent<unknown>) => void,
    active: true,
    frontier: owner.sequence,
  };
  owner.listeners.add(listener);
  retainProgress(runtime, 1);
  let stopped = false;
  const stop = (): void => {
    if (stopped) return;
    stopped = true;
    listener.active = false;
    owner.listeners.delete(listener);
    retainProgress(runtime, -1);
  };
  const seed = makeEvent("current", owner.current);
  const seedDelivery: QueryProgressDelivery = {
    sequence: owner.sequence,
    event: seed as QueryProgressEvent<unknown>,
    ...(owner.request === undefined ? {} : { request: owner.request }),
    target: listener,
  };
  if (owner.dispatching) {
    owner.queue.push(seedDelivery);
    return stop;
  }

  owner.dispatching = true;
  let seedFailed = false;
  let seedError: unknown;
  try {
    listener.observer(seed as QueryProgressEvent<P>);
  } catch (error) {
    seedFailed = true;
    seedError = error;
    stop();
  }
  drainAndReport(runtime, owner);
  if (seedFailed) throw seedError;
  return stop;
}

function clearQueryProgress<T>(runtime: StoreRuntime<T>): void {
  if (runtime.disposed || runtime.graph.__runtime.disposed) return;
  publish(runtime, idleSnapshot(), "lifecycle");
}

/** Releases observer callbacks and request references when their store is disposed. @internal */
export function retireQueryProgress<T>(runtime: StoreRuntime<T>): void {
  const owner = runtime.queryProgressAuthority;
  if (owner === undefined) return;
  owner.request = undefined;
  owner.queue.length = 0;
  for (const listener of owner.listeners) listener.active = false;
  owner.listeners.clear();
  runtime.progressReadableCount = 0;
}

/** Returns the stable, read-only progress readable for one query runtime. @internal */
export function queryProgressReadable<T, P>(
  runtime: StoreRuntime<T>,
): Readable<QueryProgressSnapshot<P>> {
  assertStoreOpen(runtime);
  return progressCell(runtime) as Readable<QueryProgressSnapshot<P>>;
}

/** Acquires the non-coalescing query-owned progress observation. @internal */
export function queryProgressObservation<T, P>(
  runtime: StoreRuntime<T>,
): QueryProgressObservation<P> {
  assertStoreOpen(runtime);
  progressCell(runtime);
  return Object.freeze({
    capture: () => {
      assertStoreOpen(runtime);
      return authority(runtime).current as QueryProgressSnapshot<P>;
    },
    subscribe: (observer: (event: QueryProgressEvent<P>) => void) =>
      subscribeProgress(runtime, observer),
  });
}

/** Begins a fresh progress epoch when a real request becomes current. @internal */
export function beginQueryProgress<T>(
  runtime: StoreRuntime<T>,
  request: QueryRequest,
): void {
  if (!currentRequest(runtime, request)) return;
  publish(
    runtime,
    snapshot(request.progressIdentity, 0, "active", false, undefined, undefined),
    "lifecycle",
    request,
  );
}

/** Publishes one value only while its actual request remains current and active. @internal */
export function reportQueryProgress<T, P>(
  runtime: StoreRuntime<T>,
  request: QueryRequest,
  value: P,
): void {
  if (!currentRequest(runtime, request)) return;
  const owner = authority(runtime);
  const current = owner.current;
  if (current.execution !== request.progressIdentity || current.status !== "active") return;
  if (current.reportRevision >= Number.MAX_SAFE_INTEGER)
    throw new RangeError("Query progress report revision exhausted");
  publish(
    runtime,
    snapshot(
      request.progressIdentity,
      current.reportRevision + 1,
      "active",
      true,
      value,
      undefined,
    ),
    "report",
    request,
  );
}

/** Marks the current request complete after its full fetch promise succeeds. @internal */
export function completeQueryProgress<T>(
  runtime: StoreRuntime<T>,
  request: QueryRequest,
): void {
  if (!ownsSnapshot(runtime, request)) return;
  const current = authority(runtime).current;
  if (current.status !== "active") return;
  publish(
    runtime,
    snapshot(
      request.progressIdentity,
      current.reportRevision,
      "complete",
      current.hasValue,
      current.value,
      undefined,
    ),
    "lifecycle",
    request,
  );
}

/** Preserves the latest progress value while recording the request's failure. @internal */
export function failQueryProgress<T>(
  runtime: StoreRuntime<T>,
  request: QueryRequest,
  error: unknown,
): void {
  if (!ownsSnapshot(runtime, request)) return;
  const current = authority(runtime).current;
  if (current.status !== "active") return;
  publish(
    runtime,
    snapshot(
      request.progressIdentity,
      current.reportRevision,
      "failed",
      current.hasValue,
      current.value,
      error,
    ),
    "lifecycle",
    request,
  );
}

/**
 * Records logical interruption while the old request still owns the snapshot.
 * This transition never acknowledges or settles the underlying fetch promise.
 * @internal
 */
export function interruptQueryProgress<T>(
  runtime: StoreRuntime<T>,
  request: QueryRequest,
  reason: unknown,
  options: { readonly authorityChanged: true } | undefined = undefined,
): void {
  if (runtime.disposed || runtime.graph.__runtime.disposed) return;
  const current = runtime.queryProgressAuthority?.current;
  if (current?.execution !== request.progressIdentity || current.status !== "active") return;
  if (options?.authorityChanged === true) {
    if (
      !runtime.requests.has(request) &&
      runtime.queryProgressAuthority?.request !== request
    )
      return;
  } else if (runtime.activeRequest !== request && !ownsSnapshot(runtime, request)) return;
  publish(
    runtime,
    snapshot(
      request.progressIdentity,
      current.reportRevision,
      "interrupted",
      current.hasValue,
      current.value,
      reason,
    ),
    "lifecycle",
    request,
  );
}
