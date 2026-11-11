import type { Readable, Store, StoreCapture, StoreDefinition, StreamStatus } from "./types.ts";
import { nextValueRevision, storeKey, type StoreIdentity, type ValueRevision } from "./identity.ts";
import { Fault } from "../fault.ts";
import type { Unsubscribe } from "../fault.ts";
import { storeSubscribe } from "../subscribe.ts";
import { __internal as reactiveInternal, computed, signal, subscribe } from "../reactive.ts";
import { assertStoreOpen, graphFault } from "./faults.ts";
import type {
  BoundStreamSessionRuntime,
  GraphRuntimeState,
  Prediction,
  StoreBindingRuntime,
  StoreRuntime,
} from "./runtime.ts";
import { configureDeclaredStore, issue, nextGeneration, recordLanding } from "./store-state.ts";
import { interact, isLive, scheduleSweep, touch, updateCollectionCandidate } from "./collection.ts";
import { armStoreTimer } from "./query-request.ts";
import {
  assertBindingContextOpen,
  beginBindingAddress,
  boundStreamByStore,
  captureStoreRuntime,
  changeBoundAddress,
  endBindingAddress,
  runtimeByStore,
} from "./registry.ts";

export const STORE_ABSENT = Symbol("aeolia.store-absent");

export function storeEquals<T>(
  runtime: StoreRuntime<T>,
  a: T | undefined,
  b: T | undefined,
): boolean {
  if (a === undefined && b === undefined) return true;
  if (a === undefined || b === undefined) return false;
  const incarnation = runtime.incarnation;
  const generation = runtime.generation;
  let equal: boolean;
  try {
    equal = runtime.equals(a, b);
  } catch {
    graphFault(runtime.graph.__runtime, new Fault("equals", [runtime.key as string]));
    equal = false;
  }
  // Equality runs before signal storage; a revoked bound write must not follow its callout.
  if (
    runtime.binding !== undefined &&
    (runtime.disposed || runtime.incarnation !== incarnation || runtime.generation !== generation)
  )
    return true;
  return equal;
}

interface StoreReadableObservation {
  readonly isValue?: boolean;
  readonly armsFreshness?: boolean;
  readonly metadata?: boolean;
}

export function observeStoreReadable<T>(
  runtime: StoreRuntime<T>,
  readable: Readable<unknown>,
  observation: StoreReadableObservation = {},
): void {
  const state = runtime.graph.__runtime;
  runtime.readStops.push(
    reactiveInternal.onActualRead(readable, () => {
      assertStoreOpen(runtime);
      touch(runtime);
    }),
  );
  runtime.livenessStops.push(
    reactiveInternal.onLivenessChange(readable, (live) => {
      runtime.liveReadableCount += live ? 1 : -1;
      if (runtime.liveReadableCount < 0) runtime.liveReadableCount = 0;
      if (observation.metadata === true) {
        runtime.metadataReadableCount += live ? 1 : -1;
        if (runtime.metadataReadableCount < 0) runtime.metadataReadableCount = 0;
      }
      if (observation.isValue === true) runtime.valueLive = live;
      if (runtime.stream !== undefined) {
        if (live) runtime.stream.unobservedSince = undefined;
        else if (!isLive(runtime)) runtime.stream.unobservedSince = Date.now();
      }
      if (live) {
        touch(runtime);
        if (observation.metadata !== true) runtime.onReadableActivation?.();
        if (observation.armsFreshness === true) armStoreTimer(runtime);
      }
      updateCollectionCandidate(runtime);
      scheduleSweep(state);
    }),
  );
}

export function subscribeStore<T>(
  runtime: StoreRuntime<T>,
  observer: (value: T) => void,
): Unsubscribe {
  assertStoreOpen(runtime);
  const presentValue = computed<symbol | T | undefined>(
    () => {
      const value = runtime.value.get();
      if (!runtime.store.available.get()) return STORE_ABSENT;
      return value;
    },
    {
      equals: () => false,
      label: `store:${String(runtime.key)}:subscription-value`,
    },
  );
  return subscribe(presentValue, (value) => {
    if (value !== STORE_ABSENT) observer(value as T);
  });
}

export function directWrite<T>(
  runtime: StoreRuntime<T>,
  value: T,
  expectedSession?: BoundStreamSessionRuntime<T>,
): void {
  assertStoreOpen(runtime);
  reactiveInternal.assertWritesAllowed(runtime.committed);
  nextValueRevision(runtime.valueRevision);
  if (runtime.binding === undefined) {
    interact(runtime.graph.__runtime);
    issue(runtime);
  } else {
    issue(runtime);
    interact(runtime.graph.__runtime);
  }
  // Direct writes issue a generation but intentionally do not abort a request.
  try {
    recordLanding(runtime, value, undefined, expectedSession);
  } finally {
    armStoreTimer(runtime);
  }
}

export function makeStore<T>(
  state: GraphRuntimeState<any>,
  key: ReturnType<typeof storeKey>,
  definition?: StoreDefinition<T>,
  binding?: StoreBindingRuntime,
): StoreRuntime<T> {
  let runtime!: StoreRuntime<T>;
  const identity = Object.freeze({}) as StoreIdentity;
  const committed = signal<T | undefined>(definition?.initial, {
    label: `store:${String(key)}:committed`,
    equals: (a, b) => storeEquals(runtime, a, b),
  });
  const predictionStack = signal<readonly Prediction<T>[]>([], {
    label: `store:${String(key)}:predictions`,
  });
  const presence = signal(definition != null, {
    label: `store:${String(key)}:presence`,
  });
  const lifecycle = signal(0, { label: `store:${String(key)}:lifecycle` });
  const errorCell = signal<unknown>(undefined, { label: `store:${String(key)}:error` });
  const streamStatusCell = signal<StreamStatus>("empty", {
    label: `store:${String(key)}:stream-status`,
  });
  const error = computed(
    () => {
      runtime.lifecycle.get();
      assertStoreOpen(runtime);
      return errorCell.get();
    },
    { label: `store:${String(key)}:error-read` },
  );
  const streamStatus = computed(
    () => {
      runtime.lifecycle.get();
      assertStoreOpen(runtime);
      return streamStatusCell.get();
    },
    { label: `store:${String(key)}:stream-status-read` },
  );
  const folded = computed(
    () => {
      runtime.lifecycle.get();
      assertStoreOpen(runtime);
      const committedValue = committed.get();
      const predictions = predictionStack.get();
      let result = committedValue;
      for (const prediction of predictions) result = prediction.apply(result);
      return result;
    },
    { equals: (a, b) => storeEquals(runtime, a, b), label: `store:${String(key)}` },
  );

  const available = computed(
    () => {
      lifecycle.get();
      assertStoreOpen(runtime);
      presence.get();
      committed.get();
      predictionStack.get();
      const revision = runtime.valueRevision;
      const incarnation = runtime.incarnation;
      const epoch = state.resetEpoch;
      reactiveInternal.nativePeek(
        folded,
        () =>
          runtime.valueRevision === revision &&
          runtime.incarnation === incarnation &&
          state.resetEpoch === epoch &&
          !runtime.disposed &&
          !state.disposed,
      );
      assertStoreOpen(runtime);
      return (
        runtime.baseAvailable ||
        reactiveInternal.signalValue(predictionStack).some((prediction) => !prediction.dead)
      );
    },
    { label: `store:${String(key)}:available` },
  );

  const store: Store<T> = {
    identity,
    key,
    graph: state.graph.id,
    value: folded,
    available,
    set(value: T): void {
      directWrite(runtime, value);
    },
    update(next: (current: T | undefined) => T): void {
      assertStoreOpen(runtime);
      reactiveInternal.assertWritesAllowed(runtime.committed);
      nextValueRevision(runtime.valueRevision);
      if (runtime.binding !== undefined) nextGeneration(runtime.generation);
      interact(state);
      const addressWrite = beginBindingAddress(runtime.binding, String(runtime.key));
      try {
        directWrite(runtime, next(reactiveInternal.signalValue(runtime.committed)));
      } finally {
        endBindingAddress(runtime.binding, String(runtime.key), addressWrite);
      }
    },
  };
  Object.defineProperty(store, storeSubscribe, {
    configurable: false,
    enumerable: false,
    value: (observer: (value: T) => void): Unsubscribe => subscribeStore(runtime, observer),
  });
  Object.defineProperty(store, "identity", { writable: false, configurable: false });
  runtime = {
    graph: state.graph,
    identity,
    key,
    store,
    committed,
    predictionStack,
    presence,
    value: folded,
    lifecycle,
    error,
    errorCell,
    streamStatusCell,
    streamStatus,
    callers: [],
    requests: new Set(),
    livenessStops: [],
    readStops: [],
    ...(binding === undefined ? {} : { binding }),
    incarnation: {},
    streamToken: undefined,
    ...(definition?.initial === undefined ? {} : { initial: definition.initial }),
    equals: definition?.equals ?? Object.is,
    snapshot: definition?.snapshot ?? false,
    configured: definition != null,
    declared: definition != null,
    initialSpecified: definition != null,
    equalsSpecified: definition?.equals != null,
    snapshotSpecified: definition?.snapshot != null,
    baseAvailable: definition != null,
    hasAcceptedLanding: definition != null,
    valueRevision: (definition == null ? 0 : 1) as ValueRevision,
    transferEligible: true,
    transitioning: false,
    failing: false,
    recoveryDisarmed: false,
    invalidated: false,
    ...(definition === undefined ? {} : { lastLandingAt: Date.now() }),
    refreshOnActivation: false,
    generation: 0,
    lastInteraction: Date.now(),
    collectionIndex: -1,
    liveReadableCount: 0,
    metadataReadableCount: 0,
    readyWaiterCount: 0,
    valueLive: false,
    dropped: false,
    disposed: false,
  };
  const report = (fault: Fault): void => graphFault(state, fault);
  for (const readable of [
    committed,
    predictionStack,
    presence,
    folded,
    available,
    lifecycle,
    errorCell,
    error,
    streamStatusCell,
    streamStatus,
  ])
    reactiveInternal.bindReadable(readable, { graphId: state.graph.id, reportFault: report });

  observeStoreReadable(runtime, folded, { isValue: true, armsFreshness: true });
  observeStoreReadable(runtime, available, { metadata: true });
  observeStoreReadable(runtime, error, { armsFreshness: true });
  runtimeByStore.set(store, runtime as StoreRuntime<unknown>);
  if (definition != null) touch(runtime);
  return runtime;
}

/**
 * Captures one coherent visible value without tracking or retaining the store.
 * The frozen result borrows its authored value; it does not clone or freeze it.
 * @param store - A genuine native store shell or returned query/stream wrapper.
 * @returns Stable identity/revision/presence, or changed after a reentrant transition.
 * @throws TypeError for a fabricated store; disposed Fault for a closed lifetime.
 * Native read, predictor and propagation failures retain their original channel.
 */
export function peekStore<T>(store: Store<T>): StoreCapture<T> {
  const runtime = runtimeByStore.get(store) as StoreRuntime<T> | undefined;
  if (runtime === undefined) throw new TypeError("The value is not an Aeolia store");
  const bound = boundStreamByStore.get(store);
  if (bound !== undefined) {
    assertBindingContextOpen(bound.binding.context);
    if (
      bound.phase !== "active" ||
      bound.session.ended ||
      runtime.stream?.token !== bound.session.token
    )
      throw new TypeError("The bound stream wrapper no longer has active session authority");
  }
  return captureStoreRuntime(runtime);
}

/** Deferred callers validate configuration before admitting this physical shell. */
export function getOrCreateStore<T>(
  state: GraphRuntimeState<any>,
  key: ReturnType<typeof storeKey>,
  definition?: StoreDefinition<T>,
  binding?: StoreBindingRuntime,
  acquire = true,
): StoreRuntime<T> {
  interact(state);
  if (binding?.context.disposed) throw new Fault("disposed", [String(state.graph.id)]);
  const existing =
    binding === undefined ? state.stores.get(String(key)) : binding.stores.get(String(key));
  if (existing != null) {
    if (definition != null) configureDeclaredStore(existing as StoreRuntime<T>, definition);
    if (acquire) admitStore(existing);
    return existing as StoreRuntime<T>;
  }
  const runtime = makeStore(state, key, definition, binding);
  if (!acquire && definition === undefined) {
    runtime.incarnation = undefined;
    runtime.dropped = true;
  }
  changeBoundAddress(runtime);
  if (binding !== undefined) binding.stores.set(String(key), runtime as StoreRuntime<unknown>);
  state.stores.set(
    binding === undefined ? String(key) : runtime.identity,
    runtime as StoreRuntime<unknown>,
  );
  if (acquire) admitStore(runtime);
  return runtime;
}

/** Establish current authority only after the native owner accepts acquisition. */
export function admitStore<T>(runtime: StoreRuntime<T>): void {
  assertStoreOpen(runtime);
  changeBoundAddress(runtime);
  if (runtime.incarnation === undefined) runtime.incarnation = {};
  runtime.dropped = false;
  touch(runtime);
}
