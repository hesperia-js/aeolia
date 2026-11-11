import type {
  QueryDefinition,
  QueryStore,
  StoreOptions,
  StoreStatus,
  ValueOptions,
} from "./types.ts";
import { isCancellationOptions } from "./types.ts";
import { beginPhysicalWork, finishPhysicalWork } from "./retirement.ts";
import { Fault, type Unsubscribe } from "../fault.ts";
import { __internal as reactiveInternal, computed, subscribe } from "../reactive.ts";
import type { Computed } from "../reactive.ts";
import type {
  GraphRuntimeState,
  QueryBindingRuntime,
  QueryCaller,
  StoreRuntime,
} from "./runtime.ts";
import {
  AbortError,
  assertGraphOpen,
  assertStoreOpen,
  graphFault,
  reportContinuationError,
} from "./faults.ts";
import {
  beginBindingAddress,
  endBindingAddress,
  queryRuntimeByStore,
  runtimeByStore,
} from "./registry.ts";
import { admitStore, getOrCreateStore } from "./store.ts";
import { observeStoreReadable } from "./store.ts";
import { configureKeyedValue, nextGeneration } from "./store-state.ts";
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
      const status = caller.statusCell.get();
      const pending = status === "fetching" || status === "revalidating";
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
  runtime.readyWaiterCount += 1;
  touch(runtime);
  let waiterHeld = true;
  const releaseWaiter = (): void => {
    if (!waiterHeld) return;
    waiterHeld = false;
    runtime.readyWaiterCount = Math.max(0, runtime.readyWaiterCount - 1);
    touch(runtime);
  };
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
          releaseWaiter();
          complete();
        }
        return;
      }
      releaseWaiter();
      complete();
    };
    const observe = (current: StoreStatus | "pending" | "disposed"): void => {
      if (current === "ready")
        finish(() => resolve(reactiveInternal.signalValue(runtime.committed) as T));
      else if (current === "failed") finish(() => reject(runtime.error.peek()));
      else if (current === "disposed")
        finish(() => reject(new Fault("disposed", [runtime.key as string])));
    };
    try {
      const subscription = subscribe(state, observe);
      if (settled) subscription();
      else stop = subscription;
    } catch (error) {
      finish(() => reject(error));
    }
  });
  return promise;
}

export function makeQueryStore<T>(
  state: GraphRuntimeState<any>,
  query: QueryDefinition<any, T, any>,
  input: unknown,
  options: StoreOptions<T> | undefined,
  binding?: QueryBindingRuntime,
  bindingDefaults?: Pick<ValueOptions<NoInfer<T>>, "initial">,
): QueryStore<T> {
  assertGraphOpen(state);
  const physical = beginPhysicalWork(
    binding?.context.retirement ?? state.retirement,
    "acquisition",
  );
  try {
    const epoch = state.resetEpoch;
    const assertCurrent = (): void => {
      assertGraphOpen(state);
      if (binding !== undefined) {
        if (binding.context.disposed) throw new Fault("disposed", [String(state.graph.id)]);
        if (state.resetEpoch !== epoch)
          throw new AbortError("Query acquisition crossed a graph reset.");
      }
    };
    const requestedWindow = options?.revalidateAfterMs;
    assertCurrent();
    const defaultWindow = query.revalidateAfterMs;
    assertCurrent();
    const window = asWindow(requestedWindow, defaultWindow);
    const keyForInput = query.key;
    assertCurrent();
    const keyValue = Reflect.apply(keyForInput, query, [input]);
    assertCurrent();
    const key = storeKey(keyValue);
    const admission = beginBindingAddress(binding, String(key));
    try {
      const runtime = getOrCreateStore<T>(state, key, undefined, binding, false);
      assertCurrent();
      let configuredOptions = options;
      if (!runtime.configured && bindingDefaults !== undefined) {
        const hasInitial = Object.hasOwn(bindingDefaults, "initial");
        assertCurrent();
        if (hasInitial) {
          const initial = bindingDefaults.initial;
          assertCurrent();
          const merged: StoreOptions<T> = { initial };
          if (options !== undefined) {
            const keys = Reflect.ownKeys(options);
            assertCurrent();
            for (const key of keys) {
              const descriptor = Reflect.getOwnPropertyDescriptor(options, key);
              assertCurrent();
              if (!descriptor?.enumerable) continue;
              const value: unknown = Reflect.get(options, key);
              assertCurrent();
              Object.defineProperty(merged, key, {
                value,
                enumerable: true,
                writable: true,
                configurable: true,
              });
            }
          }
          configuredOptions = merged;
        }
      }
      configureKeyedValue(runtime, query, configuredOptions, undefined, false, true, assertCurrent);
      assertCurrent();
      if (binding !== undefined) {
        assertStoreOpen(runtime);
        if (state.resetEpoch !== epoch)
          throw new AbortError("Query acquisition crossed a graph reset.");
      }
      const abortSignal = options?.abortSignal;
      assertCurrent();
      const caller = createCaller(runtime, query, input, window, abortSignal);
      admitStore(runtime);
      addCaller(runtime, caller);
      runtime.source = { query, input };
      runtime.onReadableActivation = () => {
        if (!runtime.refreshOnActivation || runtime.source === undefined) return;
        const activeSource = runtime.source;
        const activeCaller = runtime.callers.find(
          (candidate) =>
            candidate.query === activeSource.query &&
            Object.is(candidate.input, activeSource.input),
        );
        if (activeCaller === undefined) return;
        try {
          void startQuery(runtime, activeCaller, false, activeCaller.abortSignal, runtime.failing);
        } catch (error) {
          reportContinuationError(state, error);
        }
      };
      writeCallerStatus(caller);
      const status = caller.status;
      const pending = caller.pending;
      const queryStore = Object.create(runtime.store) as QueryStore<T>;
      const revalidate = (revalidateOptions?: {
        readonly abortSignal?: AbortSignal;
      }): Promise<void> => {
        assertStoreOpen(runtime);
        if (
          revalidateOptions !== undefined &&
          !isCancellationOptions(revalidateOptions, () => assertStoreOpen(runtime))
        )
          throw new TypeError("Invalid query revalidation options.");
        assertStoreOpen(runtime);
        if (binding !== undefined) nextGeneration(runtime.generation);
        addCaller(runtime, caller);
        const abortSignal = revalidateOptions?.abortSignal ?? caller.abortSignal;
        assertStoreOpen(runtime);
        return startQuery(runtime, caller, true, abortSignal);
      };
      Object.assign(queryStore, { pending, error: runtime.error, status, revalidate });
      Object.defineProperty(queryStore, "ready", {
        configurable: false,
        enumerable: true,
        get: () => ready(runtime, caller),
      });
      Object.freeze(queryStore);
      queryRuntimeByStore.set(queryStore, runtime as StoreRuntime<unknown>);
      runtimeByStore.set(queryStore, runtime as StoreRuntime<unknown>);
      observeStoreReadable(runtime, status, { armsFreshness: true });
      observeStoreReadable(runtime, pending, { armsFreshness: true });
      reactiveInternal.bindReadable(pending, {
        graphId: state.graph.id,
        reportFault: (fault) => graphFault(state, fault),
      });
      const stale = queryIsStale(caller);
      if (stale) void startQuery(runtime, caller, false, caller.abortSignal, runtime.failing);
      else armStoreTimer(runtime);
      touch(runtime);
      return queryStore;
    } finally {
      endBindingAddress(binding, String(key), admission);
    }
  } finally {
    finishPhysicalWork(physical);
  }
}
