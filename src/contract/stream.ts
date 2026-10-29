import type {
  OpenOptions,
  Readable,
  NativeStreamDefinition,
  PositionedStreamDefinition,
  StreamStore,
  ValueOptions,
  ValueRevision,
} from "./types.ts";
import { isValueOptions } from "./types.ts";
import { __internal as reactiveInternal, computed, signal, subscribe } from "../reactive.ts";
import type {
  BoundStreamSessionRuntime,
  StreamBindingRuntime,
  StreamSession,
  StoreRuntime,
  GraphRuntimeState,
  PositionedStoreSessionState,
  PositionedStoreCompletedEvent,
} from "./runtime.ts";
import { AbortError, assertGraphOpen, graphFault, reportContinuationError } from "./faults.ts";
import { nextValueRevision, storeKey } from "./identity.ts";
import { configureKeyedValue, issue, nextGeneration, recordLanding } from "./store-state.ts";
import {
  STORE_ABSENT,
  admitStore,
  directWrite,
  getOrCreateStore,
  makeStore,
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
  positionedOpenRequestByOpenOptions,
  positionedPositionWriterByOpenOptions,
  positionedStreamSourceByDefinition,
} from "./registry.ts";
import {
  assertBindingContextOpen,
  boundStreamByStore,
  captureStoreRuntime,
  changeBoundAddress,
  beginBindingAddress,
  endBindingAddress,
  beginStoreAddress,
  endStoreAddress,
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
      const positioned = session.positioned;
      if (positioned !== undefined) {
        positioned.pendingPosition = undefined;
        positioned.hasPendingPosition = false;
      }
      const result = await iterator.next();
      if (!isCurrentSession(runtime, session)) return;
      if (result.done) {
        if (positioned !== undefined) {
          positioned.pendingPosition = undefined;
          positioned.hasPendingPosition = false;
        }
        endStream(runtime, session, "closed");
        return;
      }
      let event = positioned?.dispatch;
      if (positioned !== undefined) {
        const hasPosition = positioned.hasPendingPosition;
        const position = positioned.pendingPosition;
        positioned.pendingPosition = undefined;
        positioned.hasPendingPosition = false;
        session.hasPosition = hasPosition;
        if (hasPosition) session.position = position;
        event = {
          identity: Object.freeze({}),
          ordinal: positioned.nextOrdinal++,
          position: hasPosition ? position : undefined,
          latest: result.value,
        };
        positioned.dispatch = event;
        positioned.diverged = !hasPosition;
      }
      issue(runtime);
      session.emitted = true;
      try {
        reactiveInternal.batch(() =>
          recordLanding(runtime, result.value, undefined, session.bound, session),
        );
      } catch (error) {
        if (isCurrentSession(runtime, session))
          reportContinuationError(runtime.graph.__runtime, error);
      }
      if (!isCurrentSession(runtime, session)) return;
      publishStreamEvent(
        runtime,
        session,
        session.hasPosition
          ? { kind: "value", value: result.value, hasPosition: true, position: session.position }
          : { kind: "value", value: result.value },
      );
      if (!isCurrentSession(runtime, session)) return;
      try {
        setStreamStatus(runtime, "live");
      } catch (error) {
        if (isCurrentSession(runtime, session))
          reportContinuationError(runtime.graph.__runtime, error);
      }
      if (positioned !== undefined && positioned.dispatch === event) {
        const landingRevision = event?.landingRevision;
        const coherent =
          event !== undefined &&
          session.hasPosition &&
          landingRevision !== undefined &&
          positioned.baseRevision === landingRevision &&
          !positioned.diverged;
        if (coherent) {
          positioned.completed = Object.freeze(event) as PositionedStoreCompletedEvent;
        } else positioned.diverged = true;
        positioned.dispatch = undefined;
      }
    }
  } catch (error) {
    if (isCurrentSession(runtime, session)) failStream(runtime, session, error);
  }
}
export function openStream<T>(
  runtime: StoreRuntime<T>,
  stream: NativeStreamDefinition<any, T, any, any, any>,
  input: unknown,
  key: ReturnType<typeof storeKey>,
  abortSignal: AbortSignal | undefined,
): void {
  const state = runtime.graph.__runtime;
  assertGraphOpen(state);
  const physical = beginPhysicalWork(state.retirement, "stream");
  let driven = false;
  try {
    const controller = new AbortController();
    const abort = (): void => controller.abort(abortSignal?.reason);
    let callerAttached = false;
    const removeAbort = (): void => {
      if (!callerAttached || abortSignal === undefined) return;
      callerAttached = false;
      abortSignal.removeEventListener("abort", abort);
    };
    try {
      if (abortSignal !== undefined) {
        const aborted = abortSignal.aborted;
        assertGraphOpen(state);
        callerAttached = true;
        if (aborted) abort();
        else abortSignal.addEventListener("abort", abort, { once: true });
      }
    } catch (cause) {
      try {
        physicalCleanup(physical.owner, removeAbort);
      } catch (cleanupCause) {
        reportContinuationError(state, cleanupCause);
      }
      throw cause;
    }
    if (state.disposed) {
      physicalCleanup(physical.owner, removeAbort);
      assertGraphOpen(state);
    }
    const session: StreamSession<T> = {
      physical,
      controller,
      token: {},
      resetEpoch: state.resetEpoch,
      listeners: new Set(),
      hasPosition: false,
      ended: false,
      emitted: false,
      removeCallerAbort: removeAbort,
      unobservedSince: runtime.liveReadableCount === 0 ? Date.now() : undefined,
    };
    runtime.stream = session;
    runtime.streamToken = session.token;
    state.activeControllers.add(controller);
    driven = true;
    driveStreamSource(runtime, session, stream, input, key, abortSignal, removeAbort);
  } finally {
    if (!driven) finishPhysicalWork(physical);
  }
}

function ensurePositionedStoreState<T>(
  runtime: StoreRuntime<T>,
  session: StreamSession<T>,
  stream: NativeStreamDefinition<any, T, any, any, any>,
  input: unknown,
): PositionedStoreSessionState<T> | undefined {
  if (session.positioned !== undefined) return session.positioned;
  if (stream.kind !== "positioned-stream") return undefined;
  const definition = stream as PositionedStreamDefinition<any, T, any, any, any>;
  const provider = positionedStreamSourceByDefinition.get(definition);
  const member = session.bound?.binding.handle ?? runtime.streamSource?.member;
  if (provider === undefined || member === undefined) return undefined;
  const positioned: PositionedStoreSessionState<T> = {
    runtime,
    definition,
    provider,
    input,
    member,
    ...(session.bound === undefined ? {} : { binding: session.bound.binding }),
    phase: "active",
    nextOrdinal: 0,
    hasPendingPosition: false,
    diverged: false,
    participants: new Set(),
  };
  session.positioned = positioned;
  return positioned;
}

function driveStreamSource<T>(
  runtime: StoreRuntime<T>,
  session: StreamSession<T>,
  stream: NativeStreamDefinition<any, T, any, any, any>,
  input: unknown,
  key: ReturnType<typeof storeKey>,
  abortSignal: AbortSignal | undefined,
  rawRemoveAbort?: () => void,
): void {
  const state = runtime.graph.__runtime;
  ensurePositionedStoreState(runtime, session, stream, input);
  let observed = false;
  try {
    const controller = session.controller;
    const removeAbort = (): void => {
      try {
        physicalCleanup(session.physical.owner, () => {
          if (rawRemoveAbort !== undefined) rawRemoveAbort();
          else session.removeCallerAbort?.();
        });
      } catch (cause) {
        if (rawRemoveAbort !== undefined) throw cause;
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
      controller.signal.removeEventListener("abort", closeOnAbort);
    };
    const attachController = (): void => {
      if (controllerAttached) return;
      controllerAttached = true;
      session.removeControllerAbort = removeControllerAbort;
      controller.signal.addEventListener("abort", closeOnAbort, { once: true });
    };
    if (session.bound !== undefined) {
      const callerAlreadyAttached = session.removeCallerAbort !== undefined;
      let linked = false;
      const abort = (): void => controller.abort(abortSignal?.reason);
      const unlinkCaller = (): void => {
        if (!linked || abortSignal === undefined) return;
        linked = false;
        abortSignal.removeEventListener("abort", abort);
      };
      if (!callerAlreadyAttached) session.removeCallerAbort = unlinkCaller;
      try {
        attachController();
        if (abortSignal !== undefined && !callerAlreadyAttached) {
          const aborted = abortSignal.aborted;
          if (!isCurrentSession(runtime, session)) {
            removeAbort();
            return;
          }
          if (aborted) abort();
          else {
            linked = true;
            try {
              abortSignal.addEventListener("abort", abort, { once: true });
            } finally {
              // A signal can install its listener after a reentrant retirement already unlinked it.
              if (!isCurrentSession(runtime, session)) {
                linked = true;
                try {
                  physicalCleanup(session.physical.owner, unlinkCaller);
                } catch (cause) {
                  reportContinuationError(state, cause);
                }
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
          const positioned = session.positioned;
          if (positioned !== undefined) positioned.diverged = true;
          if (positioned?.reader?.owned) {
            failStream(runtime, session, new Error("Positioned Store reader reported a history gap"));
            return;
          }
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
      positionedPositionWriterByOpenOptions.set(sourceOptions, (position) => {
        if (session.positioned?.phase === "closed" || session.ended) return;
        const positioned = session.positioned;
        if (positioned === undefined) {
          session.position = position;
          session.hasPosition = true;
          return;
        }
        positioned.pendingPosition = position;
        positioned.hasPendingPosition = true;
      });
      const positionedReader = session.positioned?.reader;
      if (positionedReader !== undefined)
        positionedOpenRequestByOpenOptions.set(sourceOptions, {
          after: positionedReader.after,
          reader: positionedReader.reader,
        });
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
    const original = consumeStream(runtime, session)
      .finally(() => {
        try {
          physicalCleanup(session.physical.owner, removeControllerAbort);
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
    observePhysicalWork(session.physical, original);
    observed = true;
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
      hasPosition: false,
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
      const write = beginStoreAddress(runtime);
      try {
        const value = next(reactiveInternal.signalValue(runtime.committed));
        assertBoundSession(bound);
        directWrite(runtime, value, bound);
      } finally {
        endStoreAddress(runtime, write);
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
  stream: NativeStreamDefinition<any, T, any, any, any>,
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
  if (!isValueOptions(options)) throw new TypeError("Invalid bound stream value options");
  current();
  const values = options as ValueOptions<T>;
  const keyForInput = stream.key;
  current();
  const key = storeKey(Reflect.apply(keyForInput, stream, [input]));
  current();
  const existing = binding.stores.get(String(key)) as StoreRuntime<T> | undefined;
  if (existing?.boundRetirement !== undefined || existing?.transitioning === true)
    throw new TypeError("The bound stream is still completing native retirement");
  if (existing !== undefined) configureKeyedValue(existing, stream, values, undefined, true);
  else {
    if (
      values.equals !== undefined &&
      stream.equals !== undefined &&
      values.equals !== stream.equals
    )
      throw new Fault("contract", [String(key), "equals"]);
    if (
      values.snapshot !== undefined &&
      stream.snapshot !== undefined &&
      values.snapshot !== stream.snapshot
    )
      throw new Fault("contract", [String(key), "snapshot"]);
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
      configureKeyedValue(existing!, stream, values, joined.bound);
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
    configureKeyedValue(runtime, stream, values, bound);
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

function rawStreamWrapper<T>(
  runtime: StoreRuntime<T>,
  stream: NativeStreamDefinition<any, T, any, any, any>,
): StreamStore<T> {
  const state = runtime.graph.__runtime;
  const status = runtime.streamStatus;
  const pending = computed(() => status.get() === "opening", {
    label: `stream:${stream.name}:${String(runtime.key)}:pending`,
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
}

export function streamMember<T>(
  state: GraphRuntimeState<any>,
  stream: NativeStreamDefinition<any, T, any, any, any>,
  input: unknown,
  options: ValueOptions<T> | undefined,
  binding?: StreamBindingRuntime,
  originalMember?: object,
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
    if (options !== undefined && !isValueOptions(options))
      throw new TypeError("Invalid stream options.");
    assertGraphOpen(state);
    const keyForInput = stream.key;
    assertGraphOpen(state);
    const key = storeKey(Reflect.apply(keyForInput, stream, [input]));
    assertGraphOpen(state);
    const runtime = getOrCreateStore<T>(state, key, undefined, undefined, false);
    configureKeyedValue(runtime, stream, options);
    assertGraphOpen(state);
    const abortSignal = options?.abortSignal;
    assertGraphOpen(state);
    admitStore(runtime);

    const active = runtime.stream;
    if (active === undefined) {
      runtime.streamSource = {
        stream,
        input,
        ...(originalMember === undefined ? {} : { member: originalMember }),
        ...(abortSignal === undefined ? {} : { abortSignal }),
      };
      openStream(runtime, stream, input, key, abortSignal);
    }
    return rawStreamWrapper(runtime, stream);
  } finally {
    finishPhysicalWork(acquisition);
  }
}


/** @internal Issues a private Store/session pair for one admitted positioned reader. */
export function createPositionedStoreSession<T>(
  state: GraphRuntimeState<any>,
  definition: PositionedStreamDefinition<any, T, any, any, any>,
  input: unknown,
  key: ReturnType<typeof storeKey>,
  member: object,
  binding: StreamBindingRuntime | undefined,
  after: unknown,
  latest: T,
  reader: unknown,
  seed: unknown,
  options?: ValueOptions<T>,
): StreamStore<T> {
  if (binding !== undefined) assertBindingContextOpen(binding.context);
  else assertGraphOpen(state);
  if (options !== undefined && !isValueOptions(options))
    throw new TypeError("Invalid positioned Store value options");
  const runtime = makeStore<T>(state, key, undefined, binding);
  let session: StreamSession<T> | undefined;
  const ownAddress =
    binding === undefined
      ? undefined
      : {
          binding,
          key: String(key),
          operations: new Set<object>(),
          guard: {},
        };
  try {
    runtime.incarnation = {};
    runtime.dropped = false;
    runtime.streamSource = {
      stream: definition,
      input,
      member,
      ...(options?.abortSignal === undefined ? {} : { abortSignal: options.abortSignal }),
    };
    state.stores.set(runtime.identity, runtime as StoreRuntime<unknown>);
    if (binding !== undefined) {
      const readerStores = binding.readerStores ?? new Set<StoreRuntime<unknown>>();
      binding.readerStores = readerStores;
      readerStores.add(runtime as StoreRuntime<unknown>);
      session = installBoundSession(runtime, binding);
    } else {
      const physical = beginPhysicalWork(state.retirement, "stream");
      session = {
        physical,
        controller: new AbortController(),
        token: {},
        resetEpoch: state.resetEpoch,
        listeners: new Set(),
        hasPosition: false,
        ended: false,
        emitted: false,
        unobservedSince: Date.now(),
      };
      runtime.stream = session;
      runtime.streamToken = session.token;
      state.activeControllers.add(session.controller);
    }
    runtime.readerScope = {
      session,
      ...(ownAddress === undefined ? {} : { address: ownAddress }),
    };
    const provider = positionedStreamSourceByDefinition.get(definition);
    if (provider === undefined) throw new TypeError("Positioned source provider is not registered");
    const lease = { reader, after, seed, owned: false, released: false };
    session.positioned = {
      runtime,
      definition,
      provider,
      input,
      member,
      ...(binding === undefined ? {} : { binding }),
      phase: "dormant",
      nextOrdinal: 1,
      hasPendingPosition: false,
      completed: undefined,
      diverged: false,
      participants: new Set(),
      reader: lease,
    };
    changeBoundAddress(runtime);
    configureKeyedValue(runtime, definition, options, session.bound, false, false);
    admitStore(runtime);
    recordLanding(runtime, latest, undefined, session.bound, session);
    session.position = after;
    session.hasPosition = true;
    session.emitted = true;
    const completed: PositionedStoreCompletedEvent = Object.freeze({
      identity: Object.freeze({}),
      ordinal: 0,
      position: after,
      latest,
      landingRevision: runtime.valueRevision,
    });
    session.positioned.completed = completed;
    session.positioned.baseRevision = runtime.valueRevision;
    session.positioned.diverged = false;
    return binding === undefined
      ? rawStreamWrapper(runtime, definition)
      : boundWrapper(runtime, session);
  } catch (error) {
    if (session !== undefined && !session.ended) {
      try {
        endStream(runtime, session, "closed");
      } catch (cleanupError) {
        reportContinuationError(state, cleanupError);
      }
    }
    if (binding?.readerStores !== undefined)
      binding.readerStores.delete(runtime as StoreRuntime<unknown>);
    runtime.disposed = true;
    state.stores.delete(runtime.identity);
    changeBoundAddress(runtime);
    throw error;
  }
}

/** @internal Installs caller cancellation on the staged native reader session. */
export function attachPositionedStoreAbort<T>(
  runtime: StoreRuntime<T>,
  session: StreamSession<T>,
  signal: AbortSignal | undefined,
): void {
  if (signal === undefined) return;
  let attached = false;
  const abort = (): void => {
    try {
      session.controller.abort(signal.reason);
    } catch (error) {
      reportContinuationError(runtime.graph.__runtime, error);
    }
    if (session.ended) return;
    try {
      endStream(runtime, session, "closed", undefined, { abortReason: signal.reason });
    } catch (error) {
      reportContinuationError(runtime.graph.__runtime, error);
    }
  };
  const remove = (): void => {
    if (!attached) return;
    attached = false;
    signal.removeEventListener("abort", abort);
  };
  const aborted = signal.aborted;
  if (aborted) {
    abort();
    throw new AbortError("Positioned Store caller signal aborted before reader ownership");
  }
  attached = true;
  try {
    signal.addEventListener("abort", abort, { once: true });
  } catch (error) {
    try {
      remove();
    } catch (cleanupError) {
      reportContinuationError(runtime.graph.__runtime, cleanupError);
    }
    throw error;
  }
  if (
    signal.aborted ||
    session.ended ||
    runtime.stream !== session ||
    session.resetEpoch !== runtime.graph.__runtime.resetEpoch ||
    runtime.graph.__runtime.disposed ||
    runtime.binding?.context.disposed === true
  ) {
    const isAborted = signal.aborted;
    try {
      remove();
    } catch (error) {
      reportContinuationError(runtime.graph.__runtime, error);
    }
    if (isAborted && !session.ended) abort();
    throw new AbortError("Positioned Store caller cancellation interrupted reader staging");
  }
  session.removeCallerAbort = remove;
}

/** @internal Activates a prepared reader on its already-issued Store session. */
export function activatePositionedStoreSession<T>(
  runtime: StoreRuntime<T>,
  session: StreamSession<T>,
): void {
  const positioned = session.positioned;
  const reader = positioned?.reader;
  if (
    positioned === undefined ||
    reader === undefined ||
    !reader.owned ||
    reader.released ||
    positioned.phase !== "dormant" ||
    session.ended ||
    runtime.stream !== session ||
    !sameSessionEpoch(runtime, session)
  )
    throw new TypeError("Positioned Store reader session is no longer current");
  const admittedEvent = positioned.completed;
  if (positioned.provider.isReaderCurrent(
    positioned.input,
    reader.reader,
    reader.after,
    reader.seed,
  ) !== true)
    throw new TypeError("Positioned Store reader reservation is no longer current");
  if (
    positioned.phase !== "dormant" ||
    session.ended ||
    runtime.stream !== session ||
    session.resetEpoch !== runtime.graph.__runtime.resetEpoch ||
    runtime.graph.__runtime.disposed ||
    runtime.binding?.context.disposed === true ||
    positioned.reader !== reader ||
    !reader.owned ||
    reader.released ||
    positioned.diverged ||
    positioned.completed !== admittedEvent ||
    admittedEvent === undefined ||
    positioned.baseRevision !== admittedEvent.landingRevision ||
    reactiveInternal.signalValue(runtime.predictionStack).some((prediction) => !prediction.dead) ||
    [...positioned.participants].some(
      (participant) => participant.failed || participant.event !== admittedEvent,
    )
  )
    throw new TypeError("Positioned Store reader changed during activation validation");
  const abortSignal = runtime.streamSource?.abortSignal;
  if (
    session.ended ||
    runtime.stream !== session ||
    positioned.phase !== "dormant" ||
    positioned.reader !== reader ||
    reader.released
  )
    throw new TypeError("Positioned Store session changed before activation");
  positioned.phase = "active";
  driveStreamSource(
    runtime,
    session,
    positioned.definition,
    positioned.input,
    runtime.key as ReturnType<typeof storeKey>,
    abortSignal,
  );
}
