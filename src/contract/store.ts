import type { Readable, Store, StoreDefinition, StreamStatus } from "./types.ts";
import { storeKey } from "./identity.ts";
import { Fault } from "../fault.ts";
import type { Unsubscribe } from "../fault.ts";
import { storeSubscribe } from "../subscribe.ts";
import { __internal as reactiveInternal, computed, signal, subscribe } from "../reactive.ts";
import { assertStoreOpen, graphFault } from "./faults.ts";
import type { GraphRuntimeState, Prediction, StoreRuntime } from "./runtime.ts";
import { configureDeclaredStore, issue, recordLanding } from "./store-state.ts";
import { interact, scheduleSweep, touch, updateCollectionCandidate } from "./collection.ts";
import { armStoreTimer } from "./query-request.ts";
import { runtimeByStore } from "./registry.ts";

export const STORE_ABSENT = Symbol("aeolia.store-absent");

export function storeEquals<T>(
  runtime: StoreRuntime<T>,
  a: T | undefined,
  b: T | undefined,
): boolean {
  if (a === undefined && b === undefined) return true;
  if (a === undefined || b === undefined) return false;
  try {
    return runtime.equals(a, b);
  } catch {
    graphFault(runtime.graph.__runtime, new Fault("equals", [runtime.key as string]));
    return false;
  }
}

interface StoreReadableObservation {
  readonly isValue?: boolean;
  readonly armsFreshness?: boolean;
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
      if (observation.isValue === true) runtime.valueLive = live;
      if (live) {
        touch(runtime);
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
      if (!runtime.presence.get()) return STORE_ABSENT;
      return runtime.value.get();
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

export function directWrite<T>(runtime: StoreRuntime<T>, value: T): void {
  assertStoreOpen(runtime);
  interact(runtime.graph.__runtime);
  issue(runtime);
  // Direct writes issue a generation but intentionally do not abort a request.
  try {
    recordLanding(runtime, value);
  } finally {
    armStoreTimer(runtime);
  }
}

export function makeStore<T>(
  state: GraphRuntimeState<any>,
  key: ReturnType<typeof storeKey>,
  definition?: StoreDefinition<T>,
): StoreRuntime<T> {
  let runtime!: StoreRuntime<T>;
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

  const store: Store<T> = {
    key,
    graph: state.graph.id,
    value: folded,
    set(value: T): void {
      directWrite(runtime, value);
    },
    update(next: (current: T | undefined) => T): void {
      assertStoreOpen(runtime);
      interact(state);
      directWrite(runtime, next(reactiveInternal.signalValue(runtime.committed)));
    },
  };
  Object.defineProperty(store, storeSubscribe, {
    configurable: false,
    enumerable: false,
    value: (observer: (value: T) => void): Unsubscribe => subscribeStore(runtime, observer),
  });
  runtime = {
    graph: state.graph,
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
    ...(definition?.initial === undefined ? {} : { initial: definition.initial }),
    equals: definition?.equals ?? Object.is,
    snapshot: definition?.snapshot ?? false,
    configured: definition != null,
    declared: definition != null,
    initialSpecified: definition != null,
    equalsSpecified: definition?.equals != null,
    snapshotSpecified: definition?.snapshot != null,
    hasCommitted: definition != null,
    failing: false,
    invalidated: false,
    ...(definition === undefined ? {} : { lastLandingAt: Date.now() }),
    generation: 0,
    lastInteraction: Date.now(),
    collectionIndex: -1,
    liveReadableCount: 0,
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
    lifecycle,
    errorCell,
    error,
    streamStatusCell,
    streamStatus,
  ])
    reactiveInternal.bindReadable(readable, { graphId: state.graph.id, reportFault: report });

  observeStoreReadable(runtime, folded, { isValue: true, armsFreshness: true });
  observeStoreReadable(runtime, presence);
  observeStoreReadable(runtime, error, { armsFreshness: true });
  runtimeByStore.set(store, runtime as StoreRuntime<unknown>);
  if (definition != null) touch(runtime);
  return runtime;
}

export function getOrCreateStore<T>(
  state: GraphRuntimeState<any>,
  key: ReturnType<typeof storeKey>,
  definition?: StoreDefinition<T>,
): StoreRuntime<T> {
  interact(state);
  const existing = state.stores.get(String(key));
  if (existing != null) {
    if (definition != null) configureDeclaredStore(existing as StoreRuntime<T>, definition);
    touch(existing);
    return existing as StoreRuntime<T>;
  }
  const runtime = makeStore(state, key, definition);
  state.stores.set(String(key), runtime as StoreRuntime<unknown>);
  return runtime;
}
