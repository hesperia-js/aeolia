import type {
  QueryAcquisition,
  QueryDefinition,
  QueryProgressCohortEntry,
  QueryProgressObservation,
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
  assertStoreHandlePublished,
  queryRuntimeByStore,
  runtimeByStore,
} from "./registry.ts";
import { admitStore, getOrCreateStore } from "./store.ts";
import { observeStoreReadable } from "./store.ts";
import { configureKeyedValue, nextGeneration } from "./store-state.ts";
import { startQuery, armStoreTimer } from "./query-request.ts";
import {
  captureQueryProgressCohort,
  enrollQueryProgressCohort,
  queryProgressObservation,
  queryProgressReadable,
} from "./query-progress.ts";
import { asWindow, createCaller, queryIsStale, writeCallerStatus } from "./status.ts";
import { storeKey } from "./identity.ts";
import { touch } from "./collection.ts";
import {
  beginTransferAdmission,
  finishTransferAdmission,
  noteTransferChange,
  registerTransferReadable,
  registerTransferStoreHandle,
  registerTransferCaller,
  transferSource,
} from "./query-policy.ts";
import type { TransferAdmission } from "./query-policy.ts";
import type { queryOptions } from "./query-options.ts";

const nativePromiseThen = Promise.prototype.then;

/** @internal Checks caller authority without accepting asynchronous validation. */
export function assertSynchronousQueryAuthority(assertCurrent?: () => void): void {
  if (assertCurrent === undefined) return;
  if (typeof assertCurrent !== "function")
    throw new TypeError("Query acquisition authority must be a function.");
  const result: unknown = Reflect.apply(assertCurrent, undefined, []);
  if ((typeof result === "object" && result !== null) || typeof result === "function") {
    void Reflect.apply(nativePromiseThen, result, [undefined, () => undefined]);
    throw new TypeError("Query acquisition authority must be synchronous.");
  }
}

/**
 * Acquire a genuine query member with progress listeners registered before dispatch.
 *
 * The original arguments retain their arity and normal query-options precedence.
 * Eligible entries join at one current frontier and receive later events in FIFO
 * order. Entry signals and stops do not cancel the query. Callbacks can run before
 * this function returns. Callback failures use the graph's continuation error
 * route and do not cancel other listeners or the shared request.
 * If supplied, `assertCurrent` must complete synchronously. Aeolia invokes it
 * during acquisition and before admitting a new request. Primitive return values
 * are ignored; object or function returns refuse acquisition. A thrown value refuses
 * this acquisition without cancelling an already-admitted shared request.
 *
 * @param member - An actual raw graph query member or bound query member.
 * @param args - The arguments to the member, including any explicit `undefined`.
 * @param entries - Independently owned listeners for this call.
 * @param assertCurrent - Optional synchronous authority check for this acquisition.
 * @returns The genuine query-store wrapper produced by this acquisition.
 * @throws TypeError for a non-query member, malformed arguments, invalid entries,
 * or an invalid authority check. Promise observation can throw its original cause.
 * @throws Original setup failures, or an AggregateError when setup and cleanup fail.
 */
export function callQueryWithProgress<T, P>(
  member: (input: void, options?: StoreOptions<T>) => QueryStore<T, P>,
  args:
    | readonly []
    | readonly [input: undefined, options?: StoreOptions<T>]
    | readonly [options: StoreOptions<T>]
    | readonly [
        options: ReturnType<typeof queryOptions<StoreOptions<T>>>,
        overrides?: StoreOptions<T>,
      ],
  entries: readonly QueryProgressCohortEntry<T, P>[],
  assertCurrent?: () => void,
): QueryStore<T, P>;
export function callQueryWithProgress<I, T, P>(
  member: (input: I, options?: StoreOptions<T>) => QueryStore<T, P>,
  args: readonly [input: I, options?: StoreOptions<T>],
  entries: readonly QueryProgressCohortEntry<T, P>[],
  assertCurrent?: () => void,
): QueryStore<T, P>;
export function callQueryWithProgress<T, P>(
  member: (...args: any[]) => QueryStore<T, P>,
  args: readonly unknown[],
  entries: readonly QueryProgressCohortEntry<T, P>[],
  assertCurrent?: () => void,
): QueryStore<T, P> {
  if (typeof member !== "function") throw new TypeError("Expected a genuine query member.");
  const source = transferSource(member);
  if (source?.kind !== "query" || source.invokeQuery === undefined)
    throw new TypeError("Expected a genuine query member.");
  return source.invokeQuery(args, entries, assertCurrent);
}

/** @internal Builds the ordinary activation callback without starting or enrolling work. */
export function createQueryRuntimeActivation<T>(runtime: StoreRuntime<T>): () => void {
  return () => {
    if (!runtime.refreshOnActivation || runtime.source === undefined) return;
    const activeSource = runtime.source;
    const activeCaller = runtime.callers.find(
      (candidate) =>
        candidate.query === activeSource.query && Object.is(candidate.input, activeSource.input),
    );
    if (activeCaller === undefined) return;
    try {
      void startQuery(runtime, activeCaller, false, activeCaller.abortSignal, runtime.failing);
    } catch (error) {
      reportContinuationError(runtime.graph.__runtime, error);
    }
  };
}

/** @internal Retains the exact native query source for later normal refresh. */
export function setQueryRuntimeSource<T>(
  runtime: StoreRuntime<T>,
  query: QueryDefinition<any, T, any, any>,
  input: unknown,
): void {
  noteTransferChange(runtime);
  runtime.source = { query, input };
  runtime.onReadableActivation = createQueryRuntimeActivation(runtime);
}

export function addCaller<T>(runtime: StoreRuntime<T>, caller: QueryCaller<T>): void {
  if (!runtime.callers.includes(caller)) {
    noteTransferChange(runtime);
    runtime.callers.push(caller);
  }
  runtime.dropped = false;
  touch(runtime);
}

export function readinessState<T>(
  caller: QueryCaller<T>,
): Computed<StoreStatus | "pending" | "disposed"> {
  return computed(
    () => {
      caller.runtime.lifecycle.get();
      if (
        caller.runtime.disposed ||
        caller.runtime.graph.__runtime.disposed ||
        caller.runtime.binding?.context.disposed
      )
        return "disposed";
      const status = caller.statusCell.get();
      const pending = status === "fetching" || status === "revalidating";
      return pending ? "pending" : status;
    },
    { label: `query:${String(caller.runtime.key)}:ready-state` },
  );
}

/** @internal Starts the normal caller readiness wait for a genuine query view. */
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

export function makeQueryStore<T, P = never>(
  state: GraphRuntimeState<any>,
  query: QueryDefinition<any, T, any, P>,
  input: unknown,
  options: StoreOptions<T> | undefined,
  acquisition: QueryAcquisition,
  binding?: QueryBindingRuntime,
  bindingDefaults?: Pick<ValueOptions<NoInfer<T>>, "initial">,
  cohortInput?: { readonly entries: unknown; readonly assertCurrent?: () => void },
): QueryStore<T, P> {
  assertGraphOpen(state);
  const hasAuthority = cohortInput?.assertCurrent !== undefined;
  let hostAuthority = cohortInput?.assertCurrent;
  const physical = beginPhysicalWork(
    binding?.context.retirement ?? state.retirement,
    "acquisition",
  );
  let admission: TransferAdmission | undefined;
  let admittedRuntime: StoreRuntime<T> | undefined;
  try {
    admission = beginTransferAdmission(state, binding?.definition ?? query, binding, input);
    const epoch = state.resetEpoch;
    let admitted = false;
    const assertNativeCurrent = (): void => {
      assertGraphOpen(state);
      if (binding !== undefined) {
        if (binding.context.disposed) throw new Fault("disposed", [String(state.graph.id)]);
        if (state.resetEpoch !== epoch)
          throw new AbortError("Query acquisition crossed a graph reset.");
      }
      if (hasAuthority && state.resetEpoch !== epoch)
        throw new AbortError("Query acquisition crossed a graph reset.");
    };
    const assertCurrent = (): void => {
      assertNativeCurrent();
      if (!admitted && hostAuthority !== undefined) {
        assertSynchronousQueryAuthority(hostAuthority);
        assertNativeCurrent();
      }
    };
    const cohort =
      cohortInput === undefined
        ? undefined
        : captureQueryProgressCohort<T, P>(cohortInput.entries, () => {
            assertCurrent();
            if (state.resetEpoch !== epoch)
              throw new AbortError("Query acquisition crossed a graph reset.");
          });
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
    assertCurrent();
    const addressAdmission = beginBindingAddress(binding, String(key));
    try {
      const runtime = getOrCreateStore<T>(state, key, undefined, binding, false);
      admittedRuntime = runtime;
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
            for (const optionKey of keys) {
              const descriptor = Reflect.getOwnPropertyDescriptor(options, optionKey);
              assertCurrent();
              if (!descriptor?.enumerable) continue;
              const value: unknown = Reflect.get(options, optionKey);
              assertCurrent();
              Object.defineProperty(merged, optionKey, {
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
      if (hostAuthority !== undefined) assertCurrent();
      if (admission !== undefined) {
        finishTransferAdmission(admission, runtime);
        admission = undefined;
      }
      registerTransferReadable(caller.statusCell, runtime, "query-status-cell", caller);
      registerTransferReadable(caller.status, runtime, "query-status", caller);
      registerTransferReadable(caller.pending, runtime, "query-pending", caller);
      admitStore(runtime);
      if (hostAuthority !== undefined) assertCurrent();
      addCaller(runtime, caller);
      if (hostAuthority !== undefined) assertCurrent();
      setQueryRuntimeSource(runtime, query, input);
      if (hostAuthority !== undefined) assertCurrent();
      writeCallerStatus(caller);
      if (hostAuthority !== undefined) assertCurrent();
      const status = caller.status;
      const pending = caller.pending;
      const queryStore = Object.create(runtime.store) as QueryStore<T, P>;
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
        nextGeneration(runtime.generation);
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
      Object.defineProperty(queryStore, "progress", {
        configurable: false,
        enumerable: true,
        get: () => queryProgressReadable<T, P>(runtime),
      });
      Object.freeze(queryStore);
      registerTransferStoreHandle(queryStore, runtime);
      registerTransferCaller(queryStore, runtime, caller);
      queryRuntimeByStore.set(queryStore, runtime as StoreRuntime<unknown>);
      runtimeByStore.set(queryStore, runtime as StoreRuntime<unknown>);
      observeStoreReadable(runtime, status, { armsFreshness: true });
      observeStoreReadable(runtime, pending, { armsFreshness: true });
      reactiveInternal.bindReadable(pending, {
        graphId: state.graph.id,
        reportFault: (fault) => graphFault(state, fault),
      });
      let rollback: Unsubscribe | undefined;
      let assertCohortCurrent: (() => void) | undefined;
      try {
        if (cohort !== undefined) {
          const incarnation = runtime.incarnation;
          const source = runtime.source;
          const callerWasAborted = caller.abortSignal?.aborted === true;
          const assertCohortNative = (): void => {
            const callerAborted = caller.abortSignal?.aborted === true;
            assertNativeCurrent();
            assertStoreOpen(runtime);
            if (
              state.resetEpoch !== epoch ||
              runtime.incarnation !== incarnation ||
              runtime.source?.query !== source?.query ||
              !Object.is(runtime.source?.input, source?.input)
            )
              throw new AbortError("Query acquisition lost its current source.");
            if (!callerWasAborted && callerAborted)
              throw new AbortError("Query acquisition caller was aborted.");
          };
          assertCohortCurrent = (): void => {
            assertCohortNative();
            if (!admitted && hostAuthority !== undefined) {
              assertSynchronousQueryAuthority(hostAuthority);
              assertCohortNative();
            }
          };
          assertCohortCurrent();
          rollback = enrollQueryProgressCohort(runtime, queryStore, cohort, assertCohortCurrent);
          assertCohortCurrent();
        }
        const stale = queryIsStale(caller);
        if (hostAuthority !== undefined) assertCohortCurrent?.();
        if (acquisition === "fetch" || stale) {
          const admission =
            hostAuthority === undefined || assertCohortCurrent === undefined
              ? undefined
              : {
                  assertCurrent: assertCohortCurrent,
                  admitted: () => {
                    admitted = true;
                    hostAuthority = undefined;
                  },
                };
          void startQuery(
            runtime,
            caller,
            false,
            caller.abortSignal,
            runtime.failing,
            undefined,
            admission,
          );
        } else armStoreTimer(runtime);
        assertCohortCurrent?.();
        touch(runtime);
        return queryStore;
      } catch (cause) {
        if (rollback !== undefined) {
          try {
            rollback();
          } catch (cleanupCause) {
            throw new AggregateError(
              [cause, cleanupCause],
              "Query acquisition and cohort cleanup failed",
            );
          }
        }
        throw cause;
      }
    } finally {
      endBindingAddress(binding, String(key), addressAdmission);
    }
  } finally {
    hostAuthority = undefined;
    try {
      if (admission !== undefined) finishTransferAdmission(admission, admittedRuntime);
    } finally {
      finishPhysicalWork(physical);
    }
  }
}

/**
 * Observe the authoritative progress of an already acquired query store.
 *
 * `capture()` reads plain owner state without reactive tracking. `subscribe()`
 * gives one current seed and then every newer accepted report and lifecycle
 * event in FIFO order, even when `store.progress` coalesces several writes in
 * one reactive pass. Observation does not start work, touch query freshness, or
 * make the observer an owner of request cancellation.
 *
 * @param store - Genuine query-store wrapper returned by an Aeolia graph.
 * @returns A non-coalescing current capture and ordered event subscription.
 * @throws TypeError if the value is not an Aeolia query store.
 * @throws Fault if the store or graph is disposed.
 */
export function observeQueryProgress<T, P>(store: QueryStore<T, P>): QueryProgressObservation<P> {
  assertStoreHandlePublished(store);
  const runtime = queryRuntimeByStore.get(store);
  if (runtime === undefined) throw new TypeError("The value is not an Aeolia query store");
  return queryProgressObservation<unknown, P>(runtime);
}
