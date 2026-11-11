import type {
  OpenOptions,
  Readable,
  StreamDefinition,
  StreamStore,
  ValueOptions,
  ValueRevision,
} from "./types.ts";
import { isValueOptions } from "./types.ts";
import { addSignalAbort } from "../utils.ts";
import { __internal as reactiveInternal, computed, signal, subscribe } from "../reactive.ts";
import type {
  BoundStreamSessionRuntime,
  StreamBindingRuntime,
  StreamSession,
  StoreRuntime,
  GraphRuntimeState,
} from "./runtime.ts";
import { assertGraphOpen, graphFault, reportContinuationError } from "./faults.ts";
import { nextValueRevision, storeKey } from "./identity.ts";
import { configureKeyedValue, issue, nextGeneration, recordLanding } from "./store-state.ts";
import {
  STORE_ABSENT,
  admitStore,
  directWrite,
  getOrCreateStore,
  observeStoreReadable,
} from "./store.ts";
import {
  endStream,
  returnStreamIterator,
  scheduleSweep,
  setStreamStatus,
  touch,
  updateCollectionCandidate,
} from "./collection.ts";
import { writeAllStatuses } from "./status.ts";
import { publishStreamEvent } from "./stream-events.ts";
import {
  assertBindingContextOpen,
  boundStreamByStore,
  captureStoreRuntime,
  changeBoundAddress,
  beginBindingAddress,
  endBindingAddress,
  runtimeByStore,
} from "./registry.ts";
import { importedBaselineCurrent, reserveBoundStreamAddress } from "./binding-address.ts";
import type {
  BoundStreamSession,
  StreamDetachment,
  StreamSessionIdentity,
} from "./stream-binding.ts";
import { storeSubscribe } from "../subscribe.ts";
import { Fault } from "../fault.ts";
import {
  beginPhysicalWork,
  finishPhysicalWork,
  observePhysicalWork,
  physicalCleanup,
  returnPhysicalIterator,
} from "./retirement.ts";

function sameSessionEpoch<T>(runtime: StoreRuntime<T>, session: StreamSession<T>): boolean {
  return (
    session.resetEpoch === runtime.graph.__runtime.resetEpoch &&
    runtime.streamToken === session.token
  );
}

function isCurrentSession<T>(runtime: StoreRuntime<T>, session: StreamSession<T>): boolean {
  return (
    !session.ended &&
    !runtime.disposed &&
    !runtime.graph.__runtime.disposed &&
    runtime.stream === session &&
    sameSessionEpoch(runtime, session) &&
    (session.bound === undefined || boundSessionCurrent(session.bound))
  );
}

export function failStream<T>(
  runtime: StoreRuntime<T>,
  session: StreamSession<T>,
  reason: unknown,
): void {
  try {
    endStream(runtime, session, "failed", reason);
  } catch (error) {
    if (sameSessionEpoch(runtime, session)) reportContinuationError(runtime.graph.__runtime, error);
  }
}

export async function consumeStream<T>(
  runtime: StoreRuntime<T>,
  session: StreamSession<T>,
): Promise<void> {
  const iterator = session.iterator;
  if (iterator === undefined) return;
  try {
    while (!session.ended) {
      const next = iterator.next;
      if (!isCurrentSession(runtime, session)) return;
      const result = await Reflect.apply(next, iterator, []);
      if (!isCurrentSession(runtime, session)) return;
      if (result.done) {
        endStream(runtime, session, "closed");
        return;
      }
      issue(runtime);
      session.emitted = true;
      try {
        reactiveInternal.batch(() =>
          recordLanding(runtime, result.value, undefined, session.bound),
        );
      } catch (error) {
        if (isCurrentSession(runtime, session))
          reportContinuationError(runtime.graph.__runtime, error);
      }
      if (!isCurrentSession(runtime, session)) return;
      publishStreamEvent(runtime, session, { kind: "value", value: result.value });
      if (!isCurrentSession(runtime, session)) return;
      try {
        setStreamStatus(runtime, "live");
      } catch (error) {
        if (isCurrentSession(runtime, session))
          reportContinuationError(runtime.graph.__runtime, error);
      }
    }
  } catch (error) {
    if (isCurrentSession(runtime, session)) failStream(runtime, session, error);
  }
}

export function openStream<T>(
  runtime: StoreRuntime<T>,
  stream: StreamDefinition<any, T, any>,
  input: unknown,
  key: ReturnType<typeof storeKey>,
  abortSignal: AbortSignal | undefined,
): void {
  const state = runtime.graph.__runtime;
  const physical = beginPhysicalWork(state.retirement, "stream");
  let driven = false;
  try {
    const resetEpoch = state.resetEpoch;
    const controller = new AbortController();
    const removeSignalAbort = addSignalAbort(
      abortSignal,
      controller,
      (remove) => physicalCleanup(state.retirement, remove),
      () => !state.disposed && state.resetEpoch === resetEpoch,
    );
    let callerAttached = true;
    const removeAbort = (): void => {
      if (!callerAttached) return;
      callerAttached = false;
      physicalCleanup(state.retirement, removeSignalAbort);
    };
    if (state.disposed || state.resetEpoch !== resetEpoch) {
      removeAbort();
      return;
    }
    const session: StreamSession<T> = {
      physical,
      controller,
      token: {},
      resetEpoch: state.resetEpoch,
      listeners: new Set(),
      ended: false,
      emitted: false,
      removeCallerAbort: removeAbort,
      unobservedSince: runtime.liveReadableCount === 0 ? Date.now() : undefined,
    };
    runtime.stream = session;
    runtime.streamToken = session.token;
    state.activeControllers.add(controller);
    try {
      reactiveInternal.batch(() => {
        runtime.failing = false;
        runtime.invalidated = false;
        runtime.errorCell.set(undefined);
        if (isCurrentSession(runtime, session)) setStreamStatus(runtime, "opening");
      });
    } catch (error) {
      if (sameSessionEpoch(runtime, session)) reportContinuationError(state, error);
    }
    if (!isCurrentSession(runtime, session)) {
      removeAbort();
      return;
    }
    const closeOnAbort = (): void => {
      if (session.ended) return;
      try {
        endStream(runtime, session, "closed");
      } catch (error) {
        if (sameSessionEpoch(runtime, session)) reportContinuationError(state, error);
      }
    };
    if (controller.signal.aborted) {
      closeOnAbort();
      removeAbort();
      return;
    }
    let iterable: AsyncIterable<T>;
    try {
      const open = stream.open;
      if (!isCurrentSession(runtime, session)) {
        removeAbort();
        return;
      }
      iterable = Reflect.apply(open, stream, [
        input,
        {
          abortSignal: controller.signal,
          graph: state.graph.id,
          key,
          reportGap: () => {
            if (!isCurrentSession(runtime, session)) return;
            publishStreamEvent(runtime, session, { kind: "gap" });
            if (!isCurrentSession(runtime, session)) return;
            if (!session.emitted) return;
            runtime.invalidated = true;
            try {
              reactiveInternal.batch(() => {
                writeAllStatuses(runtime);
                if (isCurrentSession(runtime, session)) setStreamStatus(runtime, "stale");
              });
            } catch (error) {
              if (sameSessionEpoch(runtime, session)) reportContinuationError(state, error);
            }
          },
        },
      ]);
    } catch (error) {
      removeAbort();
      failStream(runtime, session, error);
      return;
    }
    if (!isCurrentSession(runtime, session)) {
      removeAbort();
      return;
    }
    let iterator: AsyncIterator<T>;
    try {
      iterator = iterable[Symbol.asyncIterator]();
    } catch (error) {
      removeAbort();
      failStream(runtime, session, error);
      return;
    }
    if (!isCurrentSession(runtime, session)) {
      try {
        returnPhysicalIterator(state.retirement, iterator, "stream-return");
      } catch {
        /* A reset owns the current session now. */
      }
      removeAbort();
      return;
    }
    session.iterator = iterator;
    controller.signal.addEventListener("abort", closeOnAbort, { once: true });
    let controllerAttached = true;
    const removeControllerAbort = (): void => {
      if (!controllerAttached) return;
      controllerAttached = false;
      physicalCleanup(state.retirement, () =>
        controller.signal.removeEventListener("abort", closeOnAbort),
      );
    };
    session.removeControllerAbort = removeControllerAbort;
    const consuming = consumeStream(runtime, session);
    driven = true;
    void consuming
      .finally(() => {
        removeControllerAbort();
        removeAbort();
        state.activeControllers.delete(controller);
      })
      .catch((error) => {
        if (sameSessionEpoch(runtime, session)) reportContinuationError(state, error);
      });
    observePhysicalWork(physical, consuming);
  } finally {
    if (!driven) finishPhysicalWork(physical);
  }
}

function driveStreamSource<T>(
  runtime: StoreRuntime<T>,
  session: StreamSession<T>,
  stream: StreamDefinition<any, T, any>,
  input: unknown,
  key: ReturnType<typeof storeKey>,
  abortSignal: AbortSignal | undefined,
): void {
  const state = runtime.graph.__runtime;
  let observed = false;
  try {
    const controller = session.controller;
    let callerAttached = false;
    const abort = (): void => controller.abort(abortSignal?.reason);
    const removeCallerAbort = (): void => {
      if (!callerAttached || abortSignal === undefined) return;
      callerAttached = false;
      physicalCleanup(session.physical.owner, () =>
        abortSignal.removeEventListener("abort", abort),
      );
    };
    session.removeCallerAbort = removeCallerAbort;
    const removeAbort = (): void => {
      try {
        removeCallerAbort();
      } catch (cause) {
        reportContinuationError(state, cause);
      }
    };
    try {
      reactiveInternal.batch(() => {
        runtime.failing = false;
        runtime.invalidated = false;
        runtime.errorCell.set(undefined);
        if (isCurrentSession(runtime, session)) setStreamStatus(runtime, "opening");
      });
    } catch (error) {
      if (sameSessionEpoch(runtime, session)) reportContinuationError(state, error);
    }
    if (!isCurrentSession(runtime, session)) {
      removeAbort();
      return;
    }
    const closeOnAbort = (): void => {
      if (session.ended) return;
      try {
        endStream(runtime, session, "closed");
      } catch (error) {
        if (sameSessionEpoch(runtime, session)) reportContinuationError(state, error);
      }
    };
    let controllerAttached = false;
    const removeControllerAbort = (): void => {
      if (!controllerAttached) return;
      controllerAttached = false;
      physicalCleanup(session.physical.owner, () =>
        controller.signal.removeEventListener("abort", closeOnAbort),
      );
    };
    const attachController = (): void => {
      if (controllerAttached) return;
      const add = controller.signal.addEventListener;
      if (!isCurrentSession(runtime, session)) return;
      controllerAttached = true;
      session.removeControllerAbort = removeControllerAbort;
      try {
        Reflect.apply(add, controller.signal, ["abort", closeOnAbort, { once: true }]);
      } catch (cause) {
        if (isCurrentSession(runtime, session)) {
          try {
            removeControllerAbort();
          } catch (cleanupCause) {
            reportContinuationError(state, cleanupCause);
          }
        }
        throw cause;
      } finally {
        if (!isCurrentSession(runtime, session)) {
          controllerAttached = true;
          try {
            removeControllerAbort();
          } catch (cause) {
            reportContinuationError(state, cause);
          }
        }
      }
    };
    {
      try {
        attachController();
        if (!isCurrentSession(runtime, session)) return;
        if (abortSignal !== undefined) {
          const aborted = abortSignal.aborted;
          if (!isCurrentSession(runtime, session)) {
            removeAbort();
            return;
          }
          if (aborted) abort();
          else {
            const add = abortSignal.addEventListener;
            if (!isCurrentSession(runtime, session)) return;
            callerAttached = true;
            try {
              Reflect.apply(add, abortSignal, ["abort", abort, { once: true }]);
            } catch (cause) {
              if (isCurrentSession(runtime, session)) removeAbort();
              throw cause;
            } finally {
              // A signal can install its listener after a reentrant retirement already unlinked it.
              if (!isCurrentSession(runtime, session)) {
                callerAttached = true;
                removeAbort();
              }
            }
          }
        }
      } catch (cause) {
        removeAbort();
        failStream(runtime, session, cause);
        return;
      }
      if (!isCurrentSession(runtime, session)) {
        removeAbort();
        return;
      }
    }
    if (controller.signal.aborted) {
      closeOnAbort();
      removeAbort();
      return;
    }
    let iterable: AsyncIterable<T>;
    try {
      const open = stream.open;
      if (!isCurrentSession(runtime, session)) {
        removeAbort();
        return;
      }
      const sourceOptions: OpenOptions = {
        abortSignal: controller.signal,
        graph: state.graph.id,
        key,
        reportGap: () => {
          if (!isCurrentSession(runtime, session)) return;
          publishStreamEvent(runtime, session, { kind: "gap" });
          if (!isCurrentSession(runtime, session) || !session.emitted) return;
          runtime.invalidated = true;
          try {
            reactiveInternal.batch(() => {
              writeAllStatuses(runtime);
              if (isCurrentSession(runtime, session)) setStreamStatus(runtime, "stale");
            });
          } catch (error) {
            if (sameSessionEpoch(runtime, session)) reportContinuationError(state, error);
          }
        },
      };
      iterable = Reflect.apply(open, stream, [input, sourceOptions]);
    } catch (cause) {
      removeAbort();
      failStream(runtime, session, cause);
      return;
    }
    if (!isCurrentSession(runtime, session)) {
      removeAbort();
      return;
    }
    let iterator: AsyncIterator<T>;
    try {
      iterator = iterable[Symbol.asyncIterator]();
    } catch (cause) {
      removeAbort();
      failStream(runtime, session, cause);
      return;
    }
    if (!isCurrentSession(runtime, session)) {
      try {
        returnStreamIterator(session, iterator, true);
      } finally {
        removeAbort();
      }
      return;
    }
    session.iterator = iterator;
    attachController();
    const original = consumeStream(runtime, session);
    session.physical.original = original;
    observed = true;
    const continuation = original
      .finally(() => {
        try {
          removeControllerAbort();
        } finally {
          try {
            removeAbort();
          } finally {
            state.activeControllers.delete(controller);
          }
        }
      })
      .catch((cause) => {
        if (sameSessionEpoch(runtime, session)) reportContinuationError(state, cause);
      });
    observePhysicalWork(session.physical, continuation, false, original);
  } finally {
    if (!observed) finishPhysicalWork(session.physical);
  }
}

function boundSessionCurrent<T>(bound: BoundStreamSessionRuntime<T>): boolean {
  const { runtime, session } = bound;
  return (
    bound.phase === "active" &&
    !session.ended &&
    !runtime.disposed &&
    !runtime.graph.__runtime.disposed &&
    !bound.binding.context.disposed &&
    runtime.graph.__runtime.resetEpoch === session.resetEpoch &&
    runtime.stream?.token === session.token &&
    runtime.incarnation === bound.incarnation
  );
}

function assertBoundSession<T>(bound: BoundStreamSessionRuntime<T>): void {
  assertBindingContextOpen(bound.binding.context);
  if (!boundSessionCurrent(bound))
    throw new TypeError("The bound stream session is no longer active");
}

function installBoundSession<T>(
  runtime: StoreRuntime<T>,
  binding: StreamBindingRuntime,
): StreamSession<T> {
  const physical = beginPhysicalWork(binding.context.retirement, "stream");
  try {
    const session: StreamSession<T> = {
      physical,
      controller: new AbortController(),
      token: Object.freeze({}),
      resetEpoch: runtime.graph.__runtime.resetEpoch,
      listeners: new Set(),
      ended: false,
      emitted: false,
      unobservedSince: Date.now(),
    };
    let resolveDetached!: (receipt: StreamDetachment<T>) => void;
    const detached = new Promise<StreamDetachment<T>>((resolve) => {
      resolveDetached = resolve;
    });
    const handle: BoundStreamSession<T> = Object.freeze({
      identity: session.token as StreamSessionIdentity,
      storeIdentity: runtime.identity,
      abortSignal: session.controller.signal,
      detached,
    });
    const lifecycle = signal(0);
    const bound: BoundStreamSessionRuntime<T> = {
      runtime,
      binding,
      session,
      incarnation: runtime.incarnation!,
      handle,
      lifecycle,
      resolveDetached,
      phase: "active",
      liveReadables: 0,
    };
    session.bound = bound;
    runtime.stream = session;
    runtime.streamToken = session.token;
    runtime.graph.__runtime.activeControllers.add(session.controller);
    reactiveInternal.bindReadable(lifecycle, {
      graphId: runtime.graph.id,
      reportFault: (fault) => graphFault(runtime.graph.__runtime, fault),
    });
    return session;
  } catch (cause) {
    finishPhysicalWork(physical);
    throw cause;
  }
}

function boundTerminalValue<T>(bound: BoundStreamSessionRuntime<T>): {
  readonly present: boolean;
  readonly value?: T;
} {
  assertBindingContextOpen(bound.binding.context);
  const receipt = bound.receipt;
  if (bound.phase !== "detached" || receipt === undefined)
    throw new TypeError("The bound stream is completing its native retirement");
  const terminal = receipt.terminalCapture;
  if (terminal.kind === "fault") throw terminal.cause;
  if (terminal.capture.kind === "changed")
    throw new TypeError("The terminal stream capture changed");
  return terminal.capture.present
    ? { present: true, value: terminal.capture.value }
    : { present: false };
}

function boundFacets<T>(
  bound: BoundStreamSessionRuntime<T>,
): NonNullable<BoundStreamSessionRuntime<T>["facets"]> {
  if (bound.facets !== undefined) return bound.facets;
  const { runtime } = bound;
  const readValue = (): { readonly present: boolean; readonly value?: T } => {
    assertBindingContextOpen(bound.binding.context);
    if (bound.phase !== "active") return boundTerminalValue(bound);
    assertBoundSession(bound);
    const capture = captureStoreRuntime(runtime);
    assertBoundSession(bound);
    if (capture.kind === "changed")
      throw new TypeError("The bound stream changed during its native read");
    return capture.present ? { present: true, value: capture.value } : { present: false };
  };
  const facet = <V>(read: () => V): Readable<V> => {
    const readable = computed(() => {
      bound.lifecycle.get();
      return read();
    });
    reactiveInternal.bindReadable(readable, {
      graphId: runtime.graph.id,
      reportFault: (fault) => graphFault(runtime.graph.__runtime, fault),
    });
    reactiveInternal.onActualRead(readable, () => {
      assertBindingContextOpen(bound.binding.context);
      if (boundSessionCurrent(bound)) touch(runtime);
    });
    reactiveInternal.onLivenessChange(readable, (live) => {
      if (!boundSessionCurrent(bound)) return;
      bound.liveReadables = Math.max(0, bound.liveReadables + (live ? 1 : -1));
      if (bound.liveReadables > 0 || bound.session.listeners.size > 0)
        bound.session.unobservedSince = undefined;
      else bound.session.unobservedSince = Date.now();
      if (live) touch(runtime);
      updateCollectionCandidate(runtime);
      scheduleSweep(runtime.graph.__runtime);
    });
    return readable;
  };
  const status = (): "empty" | "opening" | "live" | "stale" | "closed" | "failed" => {
    assertBindingContextOpen(bound.binding.context);
    if (bound.phase !== "active") {
      if (bound.receipt === undefined)
        throw new TypeError("The bound stream is completing retirement");
      return bound.receipt.status;
    }
    assertBoundSession(bound);
    return reactiveInternal.signalValue(runtime.streamStatusCell);
  };
  bound.facets = Object.freeze({
    value: facet(() => {
      const value = readValue();
      return value.present ? value.value : undefined;
    }),
    available: facet(() => readValue().present),
    status: facet(status),
    pending: facet(() => status() === "opening"),
    error: facet(() => {
      assertBindingContextOpen(bound.binding.context);
      if (bound.phase !== "active") {
        if (bound.receipt === undefined)
          throw new TypeError("The bound stream is completing retirement");
        return bound.receipt.status === "failed" ? bound.receipt.reason : undefined;
      }
      assertBoundSession(bound);
      return reactiveInternal.signalValue(runtime.errorCell);
    }),
  });
  return bound.facets;
}

function boundWrapper<T>(runtime: StoreRuntime<T>, session: StreamSession<T>): StreamStore<T> {
  const bound = session.bound!;
  const facets = boundFacets(bound);
  const wrapper: StreamStore<T> = {
    graph: runtime.graph.id,
    identity: runtime.identity,
    key: runtime.key,
    ...facets,
    set(value: T): void {
      assertBoundSession(bound);
      directWrite(runtime, value, bound);
    },
    update(next: (current: T | undefined) => T): void {
      assertBoundSession(bound);
      reactiveInternal.assertWritesAllowed(runtime.committed);
      nextValueRevision(runtime.valueRevision);
      nextGeneration(runtime.generation);
      const write = beginBindingAddress(bound.binding, String(runtime.key));
      try {
        const value = next(reactiveInternal.signalValue(runtime.committed));
        assertBoundSession(bound);
        directWrite(runtime, value, bound);
      } finally {
        endBindingAddress(bound.binding, String(runtime.key), write);
      }
    },
  };
  Object.defineProperty(wrapper, storeSubscribe, {
    value: (observer: (value: T) => void) => {
      const present = computed(
        () => {
          const value = facets.value.get();
          return facets.available.get() ? value : STORE_ABSENT;
        },
        { equals: () => false },
      );
      return subscribe(present, (value) => {
        if (value !== STORE_ABSENT) observer(value as T);
      });
    },
    enumerable: false,
    configurable: false,
  });
  Object.freeze(wrapper);
  boundStreamByStore.set(wrapper, bound as unknown as BoundStreamSessionRuntime<unknown>);
  runtimeByStore.set(wrapper, runtime as StoreRuntime<unknown>);
  return wrapper;
}

function boundMember<T>(
  state: GraphRuntimeState<any>,
  stream: StreamDefinition<any, T, any>,
  input: unknown,
  supplied: ValueOptions<T> | undefined,
  binding: StreamBindingRuntime,
): StreamStore<T> {
  assertBindingContextOpen(binding.context);
  const epoch = state.resetEpoch;
  const current = (): void => {
    assertBindingContextOpen(binding.context);
    if (state.resetEpoch !== epoch)
      throw new TypeError("Bound stream acquisition crossed a graph reset");
  };
  if (
    supplied !== undefined &&
    (supplied === null || typeof supplied !== "object" || Array.isArray(supplied))
  )
    throw new TypeError("Invalid bound stream options");
  const read = (key: string): unknown => {
    const value = supplied === undefined ? undefined : Reflect.get(supplied, key);
    current();
    return value;
  };
  const abortSignal = read("abortSignal");
  const initial = read("initial");
  const equals = read("equals");
  const snapshot = read("snapshot");
  const expectEmpty = read("expectEmpty");
  const initialStated = supplied !== undefined && Object.hasOwn(supplied, "initial");
  current();
  const options: Record<string, unknown> = {
    ...(abortSignal === undefined ? {} : { abortSignal }),
    ...(initialStated ? { initial } : {}),
    ...(equals === undefined ? {} : { equals }),
    ...(snapshot === undefined ? {} : { snapshot }),
  };
  if (!isValueOptions(options, current)) throw new TypeError("Invalid bound stream value options");
  current();
  const values = options as ValueOptions<T>;
  const keyForInput = stream.key;
  current();
  const keyValue = Reflect.apply(keyForInput, stream, [input]);
  current();
  const key = storeKey(keyValue);
  const existing = binding.stores.get(String(key)) as StoreRuntime<T> | undefined;
  if (existing?.boundRetirement !== undefined || existing?.transitioning === true)
    throw new TypeError("The bound stream is still completing native retirement");
  if (existing !== undefined)
    configureKeyedValue(existing, stream, values, undefined, true, true, current);
  else {
    if (values.equals !== undefined) {
      const declaredEquals = stream.equals;
      current();
      if (declaredEquals !== undefined) {
        const comparedEquals = stream.equals;
        current();
        if (values.equals !== comparedEquals) throw new Fault("contract", [String(key), "equals"]);
      }
    }
    if (values.snapshot !== undefined) {
      const declaredSnapshot = stream.snapshot;
      current();
      if (declaredSnapshot !== undefined) {
        const comparedSnapshot = stream.snapshot;
        current();
        if (values.snapshot !== comparedSnapshot)
          throw new Fault("contract", [String(key), "snapshot"]);
      }
    }
  }
  current();
  const joined = existing?.stream;
  if (
    expectEmpty === undefined &&
    joined?.bound !== undefined &&
    boundSessionCurrent(joined.bound)
  ) {
    const admission = beginBindingAddress(binding, String(key));
    try {
      configureKeyedValue(existing!, stream, values, joined.bound, false, true, () =>
        assertBoundSession(joined.bound!),
      );
      assertBoundSession(joined.bound);
      return boundWrapper(existing!, joined);
    } finally {
      endBindingAddress(binding, String(key), admission);
    }
  }
  const claim =
    existing !== undefined && importedBaselineCurrent(existing as StoreRuntime<unknown>);
  nextGeneration(existing?.generation ?? 0);
  const clearRevision = claim
    ? existing!.valueRevision
    : nextValueRevision(existing?.valueRevision ?? (0 as ValueRevision));
  const admission = reserveBoundStreamAddress(binding, key, expectEmpty);
  let session: StreamSession<T> | undefined;
  let runtime: StoreRuntime<T> | undefined;
  let driven = false;
  try {
    runtime = getOrCreateStore<T>(state, key, undefined, binding, false);
    current();
    if (claim && !importedBaselineCurrent(runtime as StoreRuntime<unknown>))
      throw new TypeError("The imported stream baseline is no longer current");
    issue(runtime);
    runtime.importedBaseline = undefined;
    runtime.incarnation = {};
    runtime.dropped = false;
    session = installBoundSession(runtime, binding);
    const bound = session.bound!;
    runtime.transitioning = true;
    changeBoundAddress(runtime);
    reactiveInternal.batch(() => {
      if (!claim) runtime!.valueRevision = clearRevision;
      if (!claim) runtime!.baseAvailable = false;
      if (!claim) runtime!.hasAcceptedLanding = false;
      runtime!.transferEligible = true;
      runtime!.failing = false;
      runtime!.invalidated = false;
      if (!claim) runtime!.lastLandingAt = undefined;
      if (!claim) runtime!.lastSettledAt = undefined;
      runtime!.refreshOnActivation = false;
      for (const prediction of reactiveInternal.signalValue(runtime!.predictionStack))
        prediction.dead = true;
      runtime!.predictionStack.set([]);
      if (!boundSessionCurrent(bound)) return;
      if (!claim)
        reactiveInternal.guardedWrite(
          runtime!.committed,
          undefined,
          () => boundSessionCurrent(bound),
          () => undefined,
        );
      if (!boundSessionCurrent(bound)) return;
      if (!claim) runtime!.presence.set(false);
      if (!boundSessionCurrent(bound)) return;
      runtime!.errorCell.set(undefined);
    });
    assertBoundSession(bound);
    configureKeyedValue(runtime, stream, values, bound, false, true, () =>
      assertBoundSession(bound),
    );
    assertBoundSession(bound);
    if (initialStated && !runtime.baseAvailable)
      recordLanding(runtime, initial as T, undefined, bound);
    assertBoundSession(bound);
    runtime.transitioning = false;
    runtime.streamSource = {
      stream,
      input,
      ...(values.abortSignal === undefined ? {} : { abortSignal: values.abortSignal }),
    };
    admitStore(runtime);
    driven = true;
    driveStreamSource(runtime, session, stream, input, key, values.abortSignal);
    return boundWrapper(runtime, session);
  } catch (cause) {
    if (runtime !== undefined && session !== undefined && !session.ended)
      failStream(runtime, session, cause);
    throw cause;
  } finally {
    if (session !== undefined && !driven) finishPhysicalWork(session.physical);
    endBindingAddress(binding, String(key), admission);
  }
}

export function streamMember<T>(
  state: GraphRuntimeState<any>,
  stream: StreamDefinition<any, T, any>,
  input: unknown,
  options: ValueOptions<T> | undefined,
  binding?: StreamBindingRuntime,
): StreamStore<T> {
  if (binding !== undefined) assertBindingContextOpen(binding.context);
  else assertGraphOpen(state);
  const acquisition = beginPhysicalWork(
    binding?.context.retirement ?? state.retirement,
    "acquisition",
  );
  try {
    if (binding !== undefined) return boundMember(state, stream, input, options, binding);
    assertGraphOpen(state);
    if (options !== undefined && !isValueOptions(options, () => assertGraphOpen(state)))
      throw new TypeError("Invalid stream options.");
    assertGraphOpen(state);
    const keyForInput = stream.key;
    assertGraphOpen(state);
    const keyValue = Reflect.apply(keyForInput, stream, [input]);
    assertGraphOpen(state);
    const key = storeKey(keyValue);
    const runtime = getOrCreateStore<T>(state, key, undefined, undefined, false);
    assertGraphOpen(state);
    configureKeyedValue(runtime, stream, options, undefined, false, true, () =>
      assertGraphOpen(state),
    );
    assertGraphOpen(state);
    admitStore(runtime);

    const active = runtime.stream;
    if (active === undefined) {
      const abortSignal = options?.abortSignal;
      assertGraphOpen(state);
      runtime.streamSource = {
        stream,
        input,
        ...(abortSignal === undefined ? {} : { abortSignal }),
      };
      openStream(runtime, stream, input, key, abortSignal);
      assertGraphOpen(state);
    }
    const status = runtime.streamStatus;
    const name = stream.name;
    assertGraphOpen(state);
    const pending = computed(() => status.get() === "opening", {
      label: `stream:${name}:${String(key)}:pending`,
    });
    const streamStore = Object.create(runtime.store) as StreamStore<T>;
    Object.assign(streamStore, { pending, error: runtime.error, status });
    Object.freeze(streamStore);
    runtimeByStore.set(streamStore, runtime as StoreRuntime<unknown>);

    observeStoreReadable(runtime, status);
    observeStoreReadable(runtime, pending);
    reactiveInternal.bindReadable(pending, {
      graphId: state.graph.id,
      reportFault: (fault) => graphFault(state, fault),
    });
    touch(runtime);
    return streamStore;
  } finally {
    finishPhysicalWork(acquisition);
  }
}
