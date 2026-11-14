import type {
  QueryProgressCohortEntry,
  QueryProgressEvent,
  QueryProgressObservation,
  QueryProgressSnapshot,
  QueryStore,
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
import type { WritableSignal } from "../reactive.ts";
import { isObject } from "../utils.ts";
import { assertStoreOpen, graphFault, reportContinuationError } from "./faults.ts";
import { scheduleSweep, updateCollectionCandidate } from "./collection.ts";
import { physicalCleanup } from "./retirement.ts";
import {
  noteTransferChange,
  prepareTransferChange,
  registerTransferReadable,
  unregisterTransferReadable,
} from "./query-policy.ts";

interface CapturedQueryProgressCohortEntry<T, P> {
  readonly receiver: QueryProgressCohortEntry<T, P>;
  readonly signal: AbortSignal;
  readonly onEvent: QueryProgressCohortEntry<T, P>["onEvent"];
  readonly addAbortListener: AbortSignal["addEventListener"];
  readonly removeAbortListener: AbortSignal["removeEventListener"];
}

/** Internal captured input; never exposed from the public package surface. */
export interface CapturedQueryProgressCohort<T, P> {
  readonly entries: readonly CapturedQueryProgressCohortEntry<T, P>[];
}

/** Captures every authored cohort field before query key or store acquisition. @internal */
export function captureQueryProgressCohort<T, P>(
  value: unknown,
  assertCurrent: () => void,
): CapturedQueryProgressCohort<T, P> {
  assertCurrent();
  if (!Array.isArray(value)) throw new TypeError("Query progress entries must be an array");

  const length: unknown = Reflect.get(value, "length", value);
  assertCurrent();
  if (
    typeof length !== "number" ||
    !Number.isSafeInteger(length) ||
    length < 0 ||
    length > 0xffff_ffff
  )
    throw new TypeError("Query progress entries must be an array");

  const values: unknown[] = [];
  for (let index = 0; index < length; index += 1) {
    const entry: unknown = Reflect.get(value, String(index), value);
    assertCurrent();
    values.push(entry);
  }

  const captured: CapturedQueryProgressCohortEntry<T, P>[] = [];
  for (const entryValue of values) {
    if (!isObject(entryValue) || typeof entryValue === "function")
      throw new TypeError("Each progress cohort entry must be an object");
    const receiver = entryValue as QueryProgressCohortEntry<T, P>;
    const signalValue: unknown = Reflect.get(entryValue, "signal", entryValue);
    assertCurrent();
    const onEventValue: unknown = Reflect.get(entryValue, "onEvent", entryValue);
    assertCurrent();
    if (typeof onEventValue !== "function")
      throw new TypeError("Each progress cohort entry requires an onEvent function");
    if (!isObject(signalValue)) throw new TypeError("Each progress cohort entry requires a signal");

    const signal = signalValue as AbortSignal;
    const aborted: unknown = Reflect.get(signal, "aborted", signal);
    assertCurrent();
    const onabort: unknown = Reflect.get(signal, "onabort", signal);
    assertCurrent();
    const addAbortListener: unknown = Reflect.get(signal, "addEventListener", signal);
    assertCurrent();
    const removeAbortListener: unknown = Reflect.get(signal, "removeEventListener", signal);
    assertCurrent();
    const dispatchEvent: unknown = Reflect.get(signal, "dispatchEvent", signal);
    assertCurrent();
    const throwIfAborted: unknown = Reflect.get(signal, "throwIfAborted", signal);
    assertCurrent();
    if (
      typeof aborted !== "boolean" ||
      (onabort !== null && typeof onabort !== "function") ||
      typeof addAbortListener !== "function" ||
      typeof removeAbortListener !== "function" ||
      typeof dispatchEvent !== "function" ||
      typeof throwIfAborted !== "function"
    )
      throw new TypeError("Each progress cohort entry requires an AbortSignal");
    captured.push({
      receiver,
      signal,
      onEvent: onEventValue as QueryProgressCohortEntry<T, P>["onEvent"],
      addAbortListener: addAbortListener as AbortSignal["addEventListener"],
      removeAbortListener: removeAbortListener as AbortSignal["removeEventListener"],
    });
  }
  return Object.freeze({ entries: Object.freeze(captured) });
}

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
  noteTransferChange(runtime);
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
  registerTransferReadable(created, runtime, "query-progress");
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
  return request !== undefined && authorityState.current.execution === request.progressIdentity;
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
  if (owner.current.execution !== event.snapshot.execution) return false;
  if (request === undefined)
    return (
      owner.request === undefined &&
      event.snapshot.status !== "active" &&
      owner.current.status === event.snapshot.status
    );
  if (owner.request !== request) return false;
  if (event.snapshot.status === "interrupted") return owner.current.status === "interrupted";
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

function deliveryIsCurrent<T>(runtime: StoreRuntime<T>, delivery: QueryProgressDelivery): boolean {
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

  if (event.snapshot.status === "interrupted") return owner.current.status === "interrupted";

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

  noteTransferChange(runtime);
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
  noteTransferChange(runtime);
  owner.request = undefined;
  owner.queue.length = 0;
  const cleanupFailures: unknown[] = [];
  const listeners = Array.from(owner.listeners);
  for (const listener of listeners) {
    if (listener.cohortStop !== undefined) {
      try {
        listener.cohortStop(true);
      } catch (error) {
        cleanupFailures.push(error);
      }
    } else {
      listener.active = false;
    }
  }
  owner.listeners.clear();
  runtime.progressReadableCount = 0;
  for (const error of cleanupFailures) reportContinuationError(runtime.graph.__runtime, error);
}

/** Returns the stable, read-only progress readable for one query runtime. @internal */
export interface PreparedQueryProgressCell {
  readonly readable: Readable<QueryProgressSnapshot<unknown>>;
  readonly cell: WritableSignal<QueryProgressSnapshot<unknown>>;
  current(): boolean;
  publish(): void;
  deliver(): void;
  discard(): void;
}

export function prepareQueryProgressCell<T>(runtime: StoreRuntime<T>): PreparedQueryProgressCell {
  const existing = runtime.queryProgressCell;
  if (existing !== undefined) {
    return {
      readable: existing as Readable<QueryProgressSnapshot<unknown>>,
      cell: existing as WritableSignal<QueryProgressSnapshot<unknown>>,
      current: () => runtime.queryProgressCell === existing,
      publish: () => undefined,
      deliver: () => undefined,
      discard: () => undefined,
    };
  }
  const state = runtime.graph.__runtime;
  const initial = idleSnapshot();
  const created = signal(initial, { label: "query:" + String(runtime.key) + ":progress" });
  const nextAuthority: QueryProgressAuthority = {
    current: initial,
    sequence: 0,
    listeners: new Set(),
    queue: [],
    continuationErrors: [],
    dispatching: false,
  };
  registerTransferReadable(created, runtime, "query-progress");
  reactiveInternal.bindReadable(created, {
    graphId: state.graph.id,
    reportFault: (fault) => graphFault(state, fault),
  });
  const transition = prepareTransferChange(runtime);
  const clear = (): void => clearQueryProgress(runtime);
  const authorityChange = (request: QueryRequest): void => {
    interruptQueryProgress(runtime, request, undefined, { authorityChanged: true });
  };
  let published = false;
  let delivered = false;
  let discarded = false;
  return {
    readable: created as Readable<QueryProgressSnapshot<unknown>>,
    cell: created,
    current: () => !published && transition.current() && runtime.queryProgressCell === undefined,
    publish: () => {
      if (published) return;
      transition.publish();
      runtime.queryProgressCell = created;
      runtime.queryProgressAuthority = nextAuthority;
      runtime.queryProgressClear = clear;
      runtime.queryProgressAuthorityChange = authorityChange;
      published = true;
    },
    deliver: () => {
      if (!published || delivered) return;
      delivered = true;
      observeQueryProgressReadable(runtime, created);
    },
    discard: () => {
      if (published || discarded) return;
      discarded = true;
      unregisterTransferReadable(created, runtime);
    },
  };
}
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

/** Enrolls one query-call cohort at a single native progress frontier. @internal */
export function enrollQueryProgressCohort<T, P>(
  runtime: StoreRuntime<T>,
  store: QueryStore<T, P>,
  captured: CapturedQueryProgressCohort<T, P>,
  assertCurrent: () => void,
): Unsubscribe {
  assertCurrent();
  if (captured.entries.length === 0) return () => undefined;
  const observation = queryProgressObservation<T, P>(runtime);
  assertCurrent();
  const owner = authority(runtime);
  const cleanupOwner = runtime.binding?.context.retirement ?? runtime.graph.__runtime.retirement;
  const registrations: {
    readonly listener: QueryProgressListener;
    stop(retiring?: boolean): void;
    attach(): void;
    eligible(): boolean;
    commit(): void;
  }[] = [];
  let enrollmentOpen = true;
  let enrollmentFailed = false;
  let enrollmentError: unknown;
  const invalidationCleanupErrors: unknown[] = [];
  let revokeCohort: () => void = () => undefined;
  const checkCurrent = (): void => {
    if (enrollmentFailed) throw enrollmentError;
    try {
      assertCurrent();
    } catch (error) {
      enrollmentFailed = true;
      enrollmentError = error;
      throw error;
    }
  };

  const makeRegistration = (
    entry: CapturedQueryProgressCohortEntry<T, P>,
  ): (typeof registrations)[number] => {
    let stopped = false;
    let retained = false;
    let addAttempted = false;
    let adding = false;
    let removePending = false;
    let removeAttempted = false;
    let stop!: (retiring?: boolean) => void;
    let abortHandler!: EventListener;
    const listener: QueryProgressListener = {
      observer: (event) => {
        let callbackFailed = false;
        let callbackError: unknown;
        try {
          Reflect.apply(entry.onEvent, entry.receiver, [event as QueryProgressEvent<P>, context]);
        } catch (error) {
          callbackFailed = true;
          callbackError = error;
        }
        let currentFailed = false;
        let currentError: unknown;
        if (enrollmentOpen) {
          try {
            checkCurrent();
          } catch (error) {
            currentFailed = true;
            currentError = error;
            try {
              revokeCohort();
            } catch (cleanupError) {
              invalidationCleanupErrors.push(cleanupError);
            }
          }
        }
        if (callbackFailed) throw callbackError;
        if (currentFailed) throw currentError;
      },
      active: false,
      frontier: 0,
    };
    const context = Object.freeze({
      store,
      observation,
      stop: (): void => stop(false),
    });
    const unlink = (): void => {
      if (!addAttempted || removeAttempted) return;
      if (adding) {
        removePending = true;
        return;
      }
      removeAttempted = true;
      physicalCleanup(cleanupOwner, () =>
        Reflect.apply(entry.removeAbortListener, entry.signal, ["abort", abortHandler]),
      );
      if (enrollmentOpen && !enrollmentFailed) checkCurrent();
    };
    stop = (retiring = false): void => {
      if (stopped) return;
      stopped = true;
      const failures: unknown[] = [];
      if (listener.active) {
        listener.active = false;
        owner.listeners.delete(listener);
        if (retained && !retiring) {
          retained = false;
          try {
            retainProgress(runtime, -1);
          } catch (error) {
            failures.push(error);
          }
        } else {
          retained = false;
        }
      }
      try {
        unlink();
      } catch (error) {
        failures.push(error);
      }
      if (failures.length === 1) throw failures[0];
      if (failures.length > 1)
        throw new AggregateError(failures, "Query progress listener cleanup failed");
    };
    listener.cohortStop = stop;
    abortHandler = (): void => {
      try {
        stop(false);
      } catch (error) {
        reportContinuationError(runtime.graph.__runtime, error);
      }
    };
    const registration = {
      listener,
      stop,
      eligible: (): boolean => !stopped,
      commit: (): void => {
        if (stopped || listener.active) return;
        listener.active = true;
        owner.listeners.add(listener);
        retained = true;
        retainProgress(runtime, 1);
      },
      attach: (): void => {
        const before: unknown = Reflect.get(entry.signal, "aborted", entry.signal);
        checkCurrent();
        if (typeof before !== "boolean") throw new TypeError("Invalid progress cohort signal");
        if (before) {
          stop(false);
          return;
        }
        addAttempted = true;
        adding = true;
        try {
          Reflect.apply(entry.addAbortListener, entry.signal, [
            "abort",
            abortHandler,
            { once: true },
          ]);
        } catch (error) {
          adding = false;
          const failures: unknown[] = [error];
          try {
            stop(false);
          } catch (cleanupError) {
            failures.push(cleanupError);
          }
          try {
            unlink();
          } catch (cleanupError) {
            failures.push(cleanupError);
          }
          if (failures.length === 1) throw error;
          throw new AggregateError(
            failures,
            "Progress cohort signal attachment and cleanup failed",
          );
        }
        adding = false;
        if (removePending || stopped) unlink();
        checkCurrent();
        const after: unknown = Reflect.get(entry.signal, "aborted", entry.signal);
        checkCurrent();
        if (typeof after !== "boolean") throw new TypeError("Invalid progress cohort signal");
        if (after) stop(false);
      },
    };
    return registration;
  };

  const ownsDispatcher = !owner.dispatching;
  if (ownsDispatcher) owner.dispatching = true;
  const rollback = (): void => {
    const failures: unknown[] = [];
    for (const registration of [...registrations].reverse()) {
      try {
        registration.stop(false);
      } catch (error) {
        failures.push(error);
      }
    }
    if (failures.length === 1) throw failures[0];
    if (failures.length > 1)
      throw new AggregateError(failures, "Query progress cohort rollback failed");
  };
  revokeCohort = rollback;

  try {
    for (const entry of captured.entries) {
      checkCurrent();
      const registration = makeRegistration(entry);
      registrations.push(registration);
      registration.attach();
    }

    for (const registration of registrations) {
      if (registration.eligible()) registration.commit();
      checkCurrent();
    }

    checkCurrent();
    const seedSnapshot = owner.current;
    const seedSequence = owner.sequence;
    const seedRequest = owner.request;
    const seedEvent = makeEvent("current", seedSnapshot);
    for (const registration of registrations) {
      const listener = registration.listener;
      if (!listener.active) continue;
      listener.frontier = seedSequence;
      owner.queue.push({
        sequence: seedSequence,
        event: seedEvent,
        ...(seedRequest === undefined ? {} : { request: seedRequest }),
        target: listener,
      });
    }

    if (ownsDispatcher) drainAndReport(runtime, owner);
    checkCurrent();
    enrollmentOpen = false;
    return rollback;
  } catch (cause) {
    enrollmentOpen = false;
    const failures: unknown[] = [cause, ...invalidationCleanupErrors];
    try {
      rollback();
    } catch (cleanupError) {
      failures.push(cleanupError);
    }
    if (ownsDispatcher && owner.dispatching) {
      try {
        drainAndReport(runtime, owner);
      } catch (drainError) {
        failures.push(drainError);
      }
    }
    if (failures.length === 1) throw cause;
    throw new AggregateError(failures, "Query progress cohort enrollment and cleanup failed");
  }
}

/** Begins a fresh progress epoch when a real request becomes current. @internal */
export function beginQueryProgress<T>(runtime: StoreRuntime<T>, request: QueryRequest): void {
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
export function completeQueryProgress<T>(runtime: StoreRuntime<T>, request: QueryRequest): void {
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
    if (!runtime.requests.has(request) && runtime.queryProgressAuthority?.request !== request)
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
