import type { QueryDefinition, QueryStore, StoreOptions, StoreStatus } from "./types.ts";
import { Fault, type Unsubscribe } from "../fault.ts";
import { __internal as reactiveInternal, computed, subscribe } from "../reactive.ts";
import type { Computed } from "../reactive.ts";
import type { GraphRuntimeState, QueryCaller, StoreRuntime } from "./runtime.ts";
import { assertGraphOpen, assertStoreOpen, graphFault } from "./faults.ts";
import { getOrCreateStore } from "./store.ts";
import { observeStoreReadable } from "./store.ts";
import { configureKeyedValue } from "./store-state.ts";
import { startQuery, armStoreTimer } from "./query-request.ts";
import { asWindow, createCaller, queryIsStale, writeCallerStatus } from "./status.ts";
import { storeKey } from "./identity.ts";
import { touch } from "./collection.ts";

export function addCaller<T>(runtime: StoreRuntime<T>, caller: QueryCaller<T>): void {
  if (!runtime.callers.includes(caller)) runtime.callers.push(caller);
  runtime.dropped = false;
  touch(runtime);
}

export function readinessState<T>(
  caller: QueryCaller<T>,
): Computed<StoreStatus | "pending" | "disposed"> {
  return computed(
    () => {
      caller.runtime.lifecycle.get();
      if (caller.runtime.disposed || caller.runtime.graph.__runtime.disposed) return "disposed";
      const pending = caller.pending.get();
      const status = caller.status.get();
      return pending ? "pending" : status;
    },
    { label: `query:${String(caller.runtime.key)}:ready-state` },
  );
}

export function ready<T>(runtime: StoreRuntime<T>, caller: QueryCaller<T>): Promise<T> {
  assertStoreOpen(runtime);
  if (!runtime.callers.includes(caller)) {
    addCaller(runtime, caller);
    writeCallerStatus(caller);
  }
  const state = readinessState(caller);
  let settled = false;
  let stop: Unsubscribe | undefined;
  const promise = new Promise<T>((resolve, reject) => {
    const finish = (complete: () => void): void => {
      if (settled) return;
      settled = true;
      if (stop !== undefined) {
        const cleanup = stop;
        stop = undefined;
        try {
          cleanup();
        } finally {
          complete();
        }
        return;
      }
      complete();
    };
    const observe = (current: StoreStatus | "pending" | "disposed"): void => {
      if (current === "ready")
        finish(() => resolve(reactiveInternal.signalValue(runtime.committed) as T));
      else if (current === "failed") finish(() => reject(runtime.error.peek()));
      else if (current === "disposed")
        finish(() => reject(new Fault("disposed", [runtime.key as string])));
    };
    const subscription = subscribe(state, observe);
    if (settled) subscription();
    else stop = subscription;
  });
  return promise;
}

export function makeQueryStore<T>(
  state: GraphRuntimeState<any>,
  query: QueryDefinition<any, T, any>,
  input: unknown,
  options: StoreOptions<T> | undefined,
): QueryStore<T> {
  assertGraphOpen(state);
  const key = storeKey(query.key(input));
  const runtime = getOrCreateStore(state, key) as StoreRuntime<T>;
  configureKeyedValue(runtime, query, options);
  const caller = createCaller(
    runtime,
    query,
    input,
    asWindow(options?.revalidateAfterMs, query.revalidateAfterMs),
    options?.abortSignal,
  );
  addCaller(runtime, caller);
  runtime.source = { query, input };
  writeCallerStatus(caller);
  const status = caller.status;
  const pending = caller.pending;
  const queryStore = Object.create(runtime.store) as QueryStore<T>;
  const revalidate = (revalidateOptions?: {
    readonly abortSignal?: AbortSignal;
  }): Promise<void> => {
    assertStoreOpen(runtime);
    addCaller(runtime, caller);
    return startQuery(runtime, caller, true, revalidateOptions?.abortSignal ?? caller.abortSignal);
  };
  Object.assign(queryStore, { pending, error: runtime.error, status, revalidate });
  Object.defineProperty(queryStore, "ready", {
    configurable: false,
    enumerable: true,
    get: () => ready(runtime, caller),
  });
  Object.freeze(queryStore);
  observeStoreReadable(runtime, status, { armsFreshness: true });
  observeStoreReadable(runtime, pending, { armsFreshness: true });
  reactiveInternal.bindReadable(pending, {
    graphId: state.graph.id,
    reportFault: (fault) => graphFault(state, fault),
  });
  const stale = queryIsStale(caller);
  if (stale) void startQuery(runtime, caller, false, caller.abortSignal);
  else armStoreTimer(runtime);
  touch(runtime);
  return queryStore;
}
