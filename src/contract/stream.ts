import type {
  OpenOptions,
  Readable,
  NativeStreamDefinition,
  PositionedStreamDefinition,
  PositionedStreamSource,
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
  StreamSource,
  StoreRuntime,
  GraphRuntimeState,
  PositionedStoreSessionState,
  PositionedStoreCompletedEvent,
  PositionedStorePreparation,
  PositionedContinuationAnchor,
} from "./runtime.ts";
import { AbortError, assertGraphOpen, assertStoreOpen, graphFault, reportContinuationError } from "./faults.ts";
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
  positionedInitialBoundaryWriterByOpenOptions,
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
  registerDormantStoreHandle,
  assertStoreHandlePublished,
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
  readerLifetimeNow,
  retireOwner,
} from "./retirement.ts";
import type { RetirementOwner } from "./retirement.ts";
import {
  beginTransferAdmission,
  finishTransferAdmission,
  inheritTransferCoverage,
  noteTransferChange,
  registerTransferReadable,
  registerTransferSource,
  registerTransferStoreHandle,
  registerTransferWork,
} from "./query-policy.ts";

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
        noteTransferChange(positioned);
        positioned.pendingPosition = undefined;
        positioned.hasPendingPosition = false;
      }
      const result = await iterator.next();
      if (!isCurrentSession(runtime, session)) return;
      const lifetime = positioned?.preparation?.lifetime;
      if (lifetime !== undefined && !lifetime.currentAt(readerLifetimeNow())) {
        const reason = new AbortError("Positioned Store reader lifetime expired");
        endStream(runtime, session, "closed", reason, { abortReason: reason });
        return;
      }
      noteTransferChange(runtime);
      noteTransferChange(session);
      if (positioned !== undefined) noteTransferChange(positioned);
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
        if (coherent && event !== undefined) {
          positioned.completed = Object.freeze(event) as PositionedStoreCompletedEvent;
          positioned.continuation = {
            kind: "event",
            after: event.position,
            token: event.identity,
          };
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
  let admission: ReturnType<typeof beginTransferAdmission> | undefined;
  let driven = false;
  try {
    admission = beginTransferAdmission(
      state,
      stream,
      runtime.binding?.kind === "stream" ? runtime.binding.handle : undefined,
      input,
    );
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
    registerTransferWork(runtime, physical);
    registerTransferWork(session, physical);
    noteTransferChange(runtime);
    noteTransferChange(session);
    runtime.stream = session;
    runtime.streamToken = session.token;
    state.activeControllers.add(controller);
    driven = true;
    driveStreamSource(runtime, session, stream, input, key, abortSignal, removeAbort);
  } finally {
    try {
      if (admission !== undefined) finishTransferAdmission(admission, runtime);
    } finally {
      if (!driven) finishPhysicalWork(physical);
    }
  }
}

/** @internal Builds a dormant raw-stream activation without opening its source. */
export function createStreamRuntimeActivation<T>(
  runtime: StoreRuntime<T>,
  stream: NativeStreamDefinition<any, T, any, any, any>,
  input: unknown,
  member: object,
  abortSignal?: AbortSignal,
  preparedSource?: StreamSource<T>,
): () => void {
  if (stream.kind === "positioned-stream")
    throw new TypeError("Positioned streams require an admitted reader session");
  const matches = (source: StreamSource<T> | undefined): source is StreamSource<T> =>
    source !== undefined &&
    source.stream === stream &&
    Object.is(source.input, input) &&
    source.member === member &&
    (abortSignal === undefined || source.abortSignal === abortSignal);
  if (preparedSource !== undefined && !matches(preparedSource))
    throw new TypeError("Stream activation requires its exact prepared raw source");
  const graphState = runtime.graph.__runtime;
  const resetEpoch = graphState.resetEpoch;
  let activated = false;
  return (): void => {
    const source = preparedSource ?? runtime.streamSource;
    if (
      activated ||
      !matches(source) ||
      runtime.streamSource !== source ||
      runtime.stream !== undefined ||
      runtime.incarnation === undefined ||
      runtime.disposed ||
      graphState.disposed ||
      graphState.resetEpoch !== resetEpoch ||
      runtime.binding !== undefined
    )
      return;
    activated = true;
    try {
      assertGraphOpen(graphState);
      if (runtime.stream !== undefined || runtime.incarnation === undefined) return;
      openStream(runtime, stream, input, runtime.key, abortSignal ?? source.abortSignal);
    } catch (error) {
      reportContinuationError(graphState, error);
    }
  };
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
  noteTransferChange(runtime);
  noteTransferChange(session);
  session.positioned = positioned;
  return positioned;
}

function acceptPositionedInitialBoundary<T>(
  runtime: StoreRuntime<T>,
  session: StreamSession<T>,
  result: unknown,
): void {
  const positioned = session.positioned;
  if (positioned === undefined) return;
  if (!isCurrentSession(runtime, session) || positioned.phase === "closed")
    throw new TypeError("The positioned opening changed before its initial boundary was recorded");
  const preparation = positioned.preparation;
  if (preparation !== undefined && (preparation.discarded || !preparation.published))
    throw new TypeError("The positioned reader was not published before opening");
  if (preparation?.lifetime !== undefined && !preparation.lifetime.currentAt(readerLifetimeNow()))
    throw new TypeError("Positioned Store reader lifetime expired before opening");
  if (result === null || typeof result !== "object")
    throw new TypeError("Positioned initial boundary must be an own-data result");
  const fields = Reflect.ownKeys(result);
  const availableField = Object.getOwnPropertyDescriptor(result, "available");
  if (
    availableField === undefined ||
    !("value" in availableField) ||
    (availableField.value !== true && availableField.value !== false)
  )
    throw new TypeError("Positioned initial boundary requires an own boolean available field");
  if (availableField.value === false) {
    if (fields.length !== 1 || fields[0] !== "available")
      throw new TypeError("An unavailable positioned boundary has an invalid shape");
    if (positioned.continuation?.kind === "initial")
      throw new TypeError("The admitted initial boundary is no longer available");
    return;
  }
  const afterField = Object.getOwnPropertyDescriptor(result, "after");
  if (afterField === undefined || !("value" in afterField))
    throw new TypeError("An available positioned initial boundary requires an own after field");
  if (fields.length !== 2 || !fields.includes("available") || !fields.includes("after"))
    throw new TypeError("An available positioned boundary has an invalid shape");
  const after = afterField.value;
  const existing =
    positioned.continuation !== undefined
      ? positioned.continuation.after
      : positioned.completed !== undefined
        ? positioned.completed.position
        : positioned.hasPendingPosition
          ? positioned.pendingPosition
          : session.hasPosition
            ? session.position
            : undefined;
  const hasExisting =
    positioned.continuation !== undefined ||
    positioned.completed !== undefined ||
    positioned.hasPendingPosition ||
    session.hasPosition;
  if (hasExisting) {
    if (!Object.is(after, existing))
      throw new TypeError("The positioned opening boundary changed after reader admission");
    return;
  }
  noteTransferChange(session);
  noteTransferChange(positioned);
  positioned.continuation = { kind: "initial", after, token: Object.freeze({}) };
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
      noteTransferChange(runtime);
      noteTransferChange(session);
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
          noteTransferChange(runtime);
          noteTransferChange(session);
          if (positioned !== undefined) {
            noteTransferChange(positioned);
            positioned.diverged = true;
          }
          if (positioned?.reader?.owned) {
            failStream(
              runtime,
              session,
              new Error("Positioned Store reader reported a history gap"),
            );
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
        noteTransferChange(session);
        if (positioned === undefined) {
          session.position = position;
          session.hasPosition = true;
          return;
        }
        noteTransferChange(positioned);
        positioned.pendingPosition = position;
        positioned.hasPendingPosition = true;
      });
      if (session.positioned?.provider.initialBoundary !== undefined)
        positionedInitialBoundaryWriterByOpenOptions.set(sourceOptions, (result) => {
          positionedInitialBoundaryWriterByOpenOptions.delete(sourceOptions);
          acceptPositionedInitialBoundary(runtime, session, result);
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
    const lifetime = session.positioned?.preparation?.lifetime;
    if (lifetime !== undefined && !lifetime.currentAt(readerLifetimeNow())) {
      const reason = new AbortError("Positioned Store reader lifetime expired");
      endStream(runtime, session, "closed", reason, { abortReason: reason });
      removeAbort();
      return;
    }
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
  retirementOwner?: RetirementOwner,
  deferredPublication = false,
): StreamSession<T> {
  const physical = beginPhysicalWork(retirementOwner ?? binding.context.retirement, "stream");
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
    noteTransferChange(runtime);
    noteTransferChange(session);
    if (!deferredPublication) {
      noteTransferChange(binding);
      noteTransferChange(binding.handle);
    }
    registerTransferWork(runtime, physical);
    registerTransferWork(session, physical);
    registerTransferWork(bound, physical);
    session.bound = bound;
    runtime.stream = session;
    runtime.streamToken = session.token;
    if (!deferredPublication) runtime.graph.__runtime.activeControllers.add(session.controller);
    reactiveInternal.bindReadable(lifecycle, {
      graphId: runtime.graph.id,
      reportFault: (fault) => graphFault(runtime.graph.__runtime, fault),
    });
    registerTransferReadable(lifecycle, runtime, "stream-lifecycle");
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
  const preparation = bound.session.positioned?.preparation;
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
  const facet = <V>(role: string, read: () => V): Readable<V> => {
    const readable = computed(() => {
      bound.lifecycle.get();
      return read();
    });
    registerTransferReadable(readable, runtime, role);
    reactiveInternal.bindReadable(readable, {
      graphId: runtime.graph.id,
      reportFault: (fault) => graphFault(runtime.graph.__runtime, fault),
    });
    reactiveInternal.onActualRead(readable, () => {
      if (preparation !== undefined && !preparation.published)
        throw new TypeError("The positioned Store is still dormant");
      assertBindingContextOpen(bound.binding.context);
      if (boundSessionCurrent(bound)) touch(runtime);
    });
    reactiveInternal.onLivenessChange(readable, (live) => {
      if ((preparation !== undefined && !preparation.published) || !boundSessionCurrent(bound))
        return;
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
    value: facet("stream-value", () => {
      const value = readValue();
      return value.present ? value.value : undefined;
    }),
    available: facet("stream-available", () => readValue().present),
    status: facet("stream-status", status),
    pending: facet("stream-pending", () => status() === "opening"),
    error: facet("stream-error", () => {
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
  const preparation = session.positioned?.preparation;
  const wrapper: StreamStore<T> = {
    graph: runtime.graph.id,
    identity: runtime.identity,
    key: runtime.key,
    ...facets,
    set(value: T): void {
      if (preparation !== undefined && !preparation.published) assertStoreHandlePublished(wrapper);
      assertBoundSession(bound);
      directWrite(runtime, value, bound);
    },
    update(next: (current: T | undefined) => T): void {
      if (preparation !== undefined && !preparation.published) assertStoreHandlePublished(wrapper);
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
      if (preparation !== undefined && !preparation.published) assertStoreHandlePublished(wrapper);
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
  registerTransferStoreHandle(wrapper, runtime);
  if (preparation !== undefined && !preparation.published)
    registerDormantStoreHandle(wrapper, runtime, () => preparation.published, false);
  else runtimeByStore.set(wrapper, runtime as StoreRuntime<unknown>);
  registerTransferSource(bound.binding.handle, {
    graph: runtime.graph,
    kind: "stream",
    definition: bound.binding.definition,
    binding: bound.binding,
  });

  return wrapper;
}

function boundMember<T>(
  state: GraphRuntimeState<any>,
  stream: NativeStreamDefinition<any, T, any, any, any>,
  input: unknown,
  supplied: ValueOptions<T> | undefined,
  binding: StreamBindingRuntime,
  onRuntime?: (runtime: StoreRuntime<T>) => void,
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
    onRuntime?.(runtime);
    inheritTransferCoverage(runtime);
    current();
    if (claim && !importedBaselineCurrent(runtime as StoreRuntime<unknown>))
      throw new TypeError("The imported stream baseline is no longer current");
    noteTransferChange(runtime);
    issue(runtime);
    runtime.importedBaseline = undefined;
    noteTransferChange(runtime);
    runtime.incarnation = {};
    runtime.dropped = false;
    session = installBoundSession(runtime, binding);
    const bound = session.bound!;
    noteTransferChange(runtime);
    noteTransferChange(binding.handle);
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
    noteTransferChange(runtime);
    runtime.transitioning = false;
    noteTransferChange(runtime);
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

function suspendDetachedStoreReadTouches<T>(runtime: StoreRuntime<T>): void {
  for (const stop of runtime.readStops) stop();
  runtime.readStops.length = 0;
  for (const stop of runtime.livenessStops) stop();
  runtime.livenessStops.length = 0;
}

function guardPositionedStoreReads<T>(
  runtime: StoreRuntime<T>,
  handle: StreamStore<T>,
  readables: readonly Readable<unknown>[],
): void {
  for (const readable of new Set(readables))
    reactiveInternal.onActualRead(readable, () => {
      assertStoreHandlePublished(handle);
      assertStoreOpen(runtime);
    });
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
  const preparation = runtime.readerScope?.session.positioned?.preparation;
  const dormant = preparation !== undefined && !preparation.published;
  const streamStore = Object.create(dormant ? Object.prototype : runtime.store) as StreamStore<T>;
  Object.assign(streamStore, {
    ...(dormant
      ? {
          graph: runtime.store.graph,
          identity: runtime.store.identity,
          key: runtime.store.key,
          value: runtime.value,
          available: runtime.store.available,
        }
      : {}),
    pending,
    error: runtime.error,
    status,
  });
  if (dormant) {
    Object.assign(streamStore, {
      set(value: T): void {
        assertStoreHandlePublished(streamStore);
        runtime.store.set(value);
      },
      update(next: (current: T | undefined) => T): void {
        assertStoreHandlePublished(streamStore);
        runtime.store.update(next);
      },
    });
    const subscribeDescriptor = Object.getOwnPropertyDescriptor(runtime.store, storeSubscribe);
    if (
      subscribeDescriptor === undefined ||
      !("value" in subscribeDescriptor) ||
      typeof subscribeDescriptor.value !== "function"
    )
      throw new TypeError("The native Store has no subscription implementation");
    Object.defineProperty(streamStore, storeSubscribe, {
      value: (observer: (value: T) => void) => {
        assertStoreHandlePublished(streamStore);
        return Reflect.apply(subscribeDescriptor.value, runtime.store, [observer]);
      },
      enumerable: false,
      configurable: false,
    });
  }
  Object.freeze(streamStore);
  registerTransferStoreHandle(streamStore, runtime);
  if (dormant)
    registerDormantStoreHandle(streamStore, runtime, () => preparation!.published, false);
  else runtimeByStore.set(streamStore, runtime as StoreRuntime<unknown>);
  const source = runtime.streamSource?.member ?? streamStore;
  registerTransferSource(source, { graph: runtime.graph, kind: "stream", definition: stream });
  registerTransferReadable(runtime.value, runtime, "stream-value");
  registerTransferReadable(runtime.store.available, runtime, "stream-available");
  registerTransferReadable(pending, runtime, "stream-pending");
  registerTransferReadable(status, runtime, "stream-status");
  registerTransferReadable(runtime.error, runtime, "stream-error");
  if (dormant)
    guardPositionedStoreReads(runtime, streamStore, [
      runtime.value,
      runtime.store.available,
      runtime.error,
      status,
      pending,
    ]);
  if (!dormant) {
    observeStoreReadable(runtime, status);
    observeStoreReadable(runtime, pending);
  }
  reactiveInternal.bindReadable(pending, {
    graphId: state.graph.id,
    reportFault: (fault) => graphFault(state, fault),
  });
  if (!dormant) touch(runtime);
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
  let admission: ReturnType<typeof beginTransferAdmission> | undefined;
  let admissionRuntime: StoreRuntime<T> | undefined;
  try {
    admission = beginTransferAdmission(state, stream, binding?.handle, input);
    const source = originalMember ?? binding?.handle;
    if (source !== undefined)
      registerTransferSource(source, {
        graph: state.graph,
        kind: "stream",
        definition: stream,
        ...(binding === undefined ? {} : { binding }),
      });
    if (binding !== undefined)
      return boundMember(state, stream, input, options, binding, (runtime) => {
        admissionRuntime = runtime;
      });
    assertGraphOpen(state);
    if (options !== undefined && !isValueOptions(options))
      throw new TypeError("Invalid stream options.");
    assertGraphOpen(state);
    const keyForInput = stream.key;
    assertGraphOpen(state);
    const key = storeKey(Reflect.apply(keyForInput, stream, [input]));
    assertGraphOpen(state);
    const runtime = getOrCreateStore<T>(state, key, undefined, undefined, false);
    admissionRuntime = runtime;
    inheritTransferCoverage(runtime);
    configureKeyedValue(runtime, stream, options);
    assertGraphOpen(state);
    const abortSignal = options?.abortSignal;
    assertGraphOpen(state);
    admitStore(runtime);

    const active = runtime.stream;
    if (active === undefined) {
      noteTransferChange(runtime);
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
    try {
      if (admission !== undefined) finishTransferAdmission(admission, admissionRuntime);
    } finally {
      finishPhysicalWork(acquisition);
    }
  }
}

interface PositionedStoreSessionConstruction<T> {
  readonly boundary: { readonly kind: "event" | "initial"; readonly after: unknown };
  readonly base: { readonly present: false } | { readonly present: true; readonly value: T };
  readonly legacy: boolean;
  readonly detached: boolean;
  readonly retirement: RetirementOwner & { readonly promise: Promise<void> };
}

type PositionedStoreSessionSetup<T> =
  | { readonly kind: "ordinary"; readonly latest: T }
  | {
      readonly kind: "legacy";
      readonly latest: T;
      readonly construction: PositionedStoreSessionConstruction<T> & { readonly legacy: true };
    }
  | {
      readonly kind: "boundary";
      readonly construction: PositionedStoreSessionConstruction<T> & { readonly legacy: false };
    };

function addPendingPositionedStore<T>(
  state: GraphRuntimeState<any>,
  binding: StreamBindingRuntime | undefined,
  runtime: StoreRuntime<T>,
): void {
  let graphPending = state.pendingPositionedStores;
  if (graphPending === undefined) {
    graphPending = new Set<StoreRuntime<unknown>>();
    state.pendingPositionedStores = graphPending;
  }
  graphPending.add(runtime as StoreRuntime<unknown>);
  if (binding !== undefined) {
    const context = binding.context;
    let contextPending = context.pendingPositionedStores;
    if (contextPending === undefined) {
      contextPending = new Set<StoreRuntime<unknown>>();
      context.pendingPositionedStores = contextPending;
    }
    contextPending.add(runtime as StoreRuntime<unknown>);
  }
}

function removePendingPositionedStore<T>(
  state: GraphRuntimeState<any>,
  binding: StreamBindingRuntime | undefined,
  runtime: StoreRuntime<T>,
): void {
  state.pendingPositionedStores?.delete(runtime as StoreRuntime<unknown>);
  binding?.context.pendingPositionedStores?.delete(runtime as StoreRuntime<unknown>);
}

function materializePositionedStoreInitial<T>(runtime: StoreRuntime<T>): void {
  if (!runtime.initialSpecified || runtime.baseAvailable) return;
  const revision = nextValueRevision(runtime.valueRevision);
  noteTransferChange(runtime);
  runtime.valueRevision = revision;
  runtime.baseAvailable = true;
  runtime.hasAcceptedLanding = false;
  runtime.lastLandingAt = undefined;
  runtime.committed.set(runtime.initial as T);
  runtime.presence.set(true);
}

function seedPreparedPositionedStoreBase<T>(
  runtime: StoreRuntime<T>,
  positioned: PositionedStoreSessionState<T>,
  construction: PositionedStoreSessionConstruction<T>,
): void {
  const base = construction.base;
  positioned.diverged = false;
  if (!base.present || runtime.baseAvailable) {
    positioned.baseRevision = undefined;
    return;
  }
  const revision = nextValueRevision(runtime.valueRevision);
  noteTransferChange(runtime);
  runtime.valueRevision = revision;
  runtime.importedBaseline = undefined;
  runtime.baseAvailable = true;
  runtime.hasAcceptedLanding = construction.boundary.kind === "event";
  runtime.transferEligible = true;
  runtime.transitioning = false;
  runtime.dropped = false;
  runtime.failing = false;
  runtime.invalidated = false;
  runtime.refreshOnActivation = false;
  runtime.lastLandingAt = construction.boundary.kind === "event" ? Date.now() : undefined;
  positioned.baseRevision = construction.boundary.kind === "event" ? revision : undefined;
  runtime.committed.set(base.value);
  runtime.presence.set(true);
  runtime.errorCell.set(undefined);
}

function seedLegacyPositionedStoreBase<T>(
  runtime: StoreRuntime<T>,
  session: StreamSession<T>,
  positioned: PositionedStoreSessionState<T>,
  after: unknown,
  latest: T,
): void {
  const revision = nextValueRevision(runtime.valueRevision);
  noteTransferChange(runtime);
  runtime.valueRevision = revision;
  runtime.importedBaseline = undefined;
  runtime.baseAvailable = true;
  runtime.hasAcceptedLanding = true;
  runtime.transferEligible = true;
  runtime.transitioning = false;
  runtime.dropped = false;
  runtime.failing = false;
  runtime.invalidated = false;
  runtime.refreshOnActivation = false;
  runtime.lastLandingAt = Date.now();
  runtime.committed.set(latest);
  runtime.presence.set(true);
  runtime.errorCell.set(undefined);
  session.position = after;
  session.hasPosition = true;
  session.emitted = true;
  const completed: PositionedStoreCompletedEvent = Object.freeze({
    identity: Object.freeze({}),
    ordinal: 0,
    position: after,
    latest,
    landingRevision: revision,
  });
  positioned.completed = completed;
  positioned.continuation = { kind: "event", after, token: completed.identity };
  positioned.baseRevision = revision;
  positioned.diverged = false;
}

/** @internal Issues one native Store/session pair for an admitted positioned reader. */
export function createPositionedStoreSession<T>(
  state: GraphRuntimeState<any>,
  definition: PositionedStreamDefinition<any, T, any, any, any>,
  input: unknown,
  key: ReturnType<typeof storeKey>,
  member: object,
  binding: StreamBindingRuntime | undefined,
  after: unknown,
  reader: unknown,
  seed: unknown,
  options: ValueOptions<T> | undefined,
  setup: PositionedStoreSessionSetup<T>,
): StreamStore<T> {
  const construction = setup.kind === "ordinary" ? undefined : setup.construction;
  const detached = construction?.detached === true;
  let provider: PositionedStreamSource<any, T, any, any> | undefined;
  try {
    if (binding !== undefined) assertBindingContextOpen(binding.context);
    else assertGraphOpen(state);
    if (options !== undefined && !isValueOptions(options))
      throw new TypeError("Invalid positioned Store value options");
    if (
      construction !== undefined &&
      !construction.legacy &&
      !Object.is(after, construction.boundary.after)
    )
      throw new TypeError("Positioned Store reader boundary changed before issuance");
    provider = positionedStreamSourceByDefinition.get(definition);
    if (provider === undefined) throw new TypeError("Positioned source provider is not registered");
  } catch (error) {
    if (construction !== undefined) {
      try {
        retireOwner(construction.retirement, () => {});
      } catch {}
    }
    throw error;
  }
  let runtime: StoreRuntime<T>;
  try {
    runtime = makeStore<T>(state, key, undefined, binding, detached);
    if (detached) suspendDetachedStoreReadTouches(runtime);
    inheritTransferCoverage(runtime);
  } catch (error) {
    if (construction !== undefined) {
      try {
        retireOwner(construction.retirement, () => {});
      } catch {}
    }
    throw error;
  }
  let session: StreamSession<T> | undefined;
  const ownAddress =
    binding === undefined
      ? undefined
      : { binding, key: String(key), operations: new Set<object>(), guard: {} };
  try {
    noteTransferChange(runtime);
    if (runtime.incarnation === undefined || runtime.dropped) {
      runtime.incarnation = {};
      runtime.dropped = false;
    }
    runtime.streamSource = {
      stream: definition,
      input,
      member,
      ...(options?.abortSignal === undefined ? {} : { abortSignal: options.abortSignal }),
    };
    noteTransferChange(runtime);
    registerTransferSource(member, {
      graph: state.graph,
      kind: "stream",
      definition,
      ...(binding === undefined ? {} : { binding: binding.handle }),
    });
    if (detached) addPendingPositionedStore(state, binding, runtime);
    else {
      state.stores.set(runtime.identity, runtime as StoreRuntime<unknown>);
      if (binding !== undefined) {
        const readerStores = binding.readerStores ?? new Set<StoreRuntime<unknown>>();
        binding.readerStores = readerStores;
        readerStores.add(runtime as StoreRuntime<unknown>);
      }
    }
    if (binding !== undefined) {
      session = installBoundSession(runtime, binding, construction?.retirement, detached);
    } else {
      const physical = beginPhysicalWork(construction?.retirement ?? state.retirement, "stream");
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
      registerTransferWork(runtime, physical);
      registerTransferWork(session, physical);
      noteTransferChange(runtime);
      noteTransferChange(session);
      runtime.stream = session;
      runtime.streamToken = session.token;
      if (!detached) state.activeControllers.add(session.controller);
    }
    noteTransferChange(runtime);
    runtime.readerScope = {
      session,
      ...(ownAddress === undefined ? {} : { address: ownAddress }),
    };
    const prepared: PositionedStorePreparation | undefined =
      construction === undefined
        ? undefined
        : {
            published: !detached,
            discarded: false,
            observed: !detached,
            retirement: construction.retirement,
            retired: construction.retirement.promise,
            registrations: [],
            registerStops: [],
          };
    const continuation: PositionedContinuationAnchor | undefined =
      construction === undefined || construction.legacy
        ? undefined
        : {
            kind: construction.boundary.kind,
            after: construction.boundary.after,
            token: Object.freeze({}),
          };
    session.positioned = {
      runtime,
      definition,
      provider: provider!,
      input,
      member,
      ...(binding === undefined ? {} : { binding }),
      phase: "dormant",
      nextOrdinal: construction === undefined || construction.legacy ? 1 : 0,
      hasPendingPosition: false,
      ...(continuation === undefined ? {} : { continuation }),
      ...(prepared === undefined ? {} : { preparation: prepared }),
      diverged: false,
      participants: new Set(),
      reader: {
        reader,
        after:
          construction !== undefined && !construction.legacy ? construction.boundary.after : after,
        seed,
        owned: false,
        released: false,
      },
    };
    if (!detached) changeBoundAddress(runtime);
    configureKeyedValue(runtime, definition, options, session.bound, false, false, !detached);
    if (construction !== undefined && !construction.legacy)
      materializePositionedStoreInitial(runtime);
    if (setup.kind === "ordinary") {
      admitStore(runtime);
      recordLanding(runtime, setup.latest, undefined, session.bound, session);
      session.position = after;
      session.hasPosition = true;
      session.emitted = true;
      const revision = runtime.valueRevision;
      const completed: PositionedStoreCompletedEvent = Object.freeze({
        identity: Object.freeze({}),
        ordinal: 0,
        position: after,
        latest: setup.latest,
        landingRevision: revision,
      });
      session.positioned.completed = completed;
      session.positioned.continuation = { kind: "event", after, token: completed.identity };
      session.positioned.baseRevision = revision;
      session.positioned.diverged = false;
    } else if (setup.kind === "legacy") {
      if (!detached) admitStore(runtime);
      if (detached)
        seedLegacyPositionedStoreBase(runtime, session, session.positioned, after, setup.latest);
      else {
        recordLanding(runtime, setup.latest, undefined, session.bound, session);
        session.position = after;
        session.hasPosition = true;
        session.emitted = true;
        const revision = runtime.valueRevision;
        const completed: PositionedStoreCompletedEvent = Object.freeze({
          identity: Object.freeze({}),
          ordinal: 0,
          position: after,
          latest: setup.latest,
          landingRevision: revision,
        });
        session.positioned.completed = completed;
        session.positioned.continuation = { kind: "event", after, token: completed.identity };
        session.positioned.baseRevision = revision;
        session.positioned.diverged = false;
      }
    } else {
      if (!detached) admitStore(runtime);
      seedPreparedPositionedStoreBase(runtime, session.positioned, setup.construction);
    }
    return binding === undefined
      ? rawStreamWrapper(runtime, definition)
      : boundWrapper(runtime, session);
  } catch (error) {
    const preparation = session?.positioned?.preparation;
    if (session !== undefined && !session.ended) {
      try {
        endStream(runtime, session, "closed");
      } catch (cleanupError) {
        reportContinuationError(state, cleanupError);
      }
    }
    if (detached) {
      removePendingPositionedStore(state, binding, runtime);
      noteTransferChange(runtime);
      runtime.disposed = true;
      runtime.incarnation = undefined;
      if (preparation !== undefined) preparation.discarded = true;
    } else {
      if (binding?.readerStores !== undefined)
        binding.readerStores.delete(runtime as StoreRuntime<unknown>);
      if (state.stores.get(runtime.identity) === runtime) state.stores.delete(runtime.identity);
      if (runtime.binding?.context.disposed !== true) changeBoundAddress(runtime);
    }
    if (construction !== undefined) {
      try {
        retireOwner(construction.retirement, () => {});
      } catch (cleanupError) {
        reportContinuationError(state, cleanupError);
      }
    }
    throw error;
  }
}

/** @internal Publishes only the already-prepared positioned session marker. */
export function publishPositionedStoreSession<T>(
  runtime: StoreRuntime<T>,
  session: StreamSession<T>,
): void {
  void runtime;
  session.positioned!.preparation!.published = true;
}

/** @internal Delivers dormant Store observers after the positioned publication cut. */
export function deliverPositionedStoreSession<T>(
  runtime: StoreRuntime<T>,
  session: StreamSession<T>,
  source: StreamStore<T>,
): void {
  const preparation = session.positioned?.preparation;
  if (
    preparation === undefined ||
    !preparation.published ||
    preparation.discarded ||
    preparation.observed
  )
    return;
  preparation.observed = true;
  observeStoreReadable(runtime, runtime.value, { isValue: true, armsFreshness: true });
  observeStoreReadable(runtime, runtime.store.available, { metadata: true });
  observeStoreReadable(runtime, runtime.error, { armsFreshness: true });
  if (runtime.binding === undefined) {
    observeStoreReadable(runtime, source.status);
    observeStoreReadable(runtime, source.pending);
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
  const preparation = positioned?.preparation;
  const continuation = positioned?.continuation;
  if (
    positioned === undefined ||
    reader === undefined ||
    !reader.owned ||
    reader.released ||
    positioned.phase !== "dormant" ||
    session.ended ||
    runtime.stream !== session ||
    !sameSessionEpoch(runtime, session) ||
    (preparation !== undefined && (!preparation.published || preparation.discarded))
  )
    throw new TypeError("Positioned Store reader session is no longer current");
  const admittedEvent = positioned.completed;
  if (
    positioned.provider.isReaderCurrent(
      positioned.input,
      reader.reader,
      reader.after,
      reader.seed,
    ) !== true
  )
    throw new TypeError("Positioned Store reader reservation is no longer current");
  if (preparation?.lifetime !== undefined && !preparation.lifetime.currentAt(readerLifetimeNow()))
    throw new TypeError("Positioned Store reader lifetime is no longer current");
  const stagedContinuation =
    admittedEvent === undefined && preparation !== undefined && continuation !== undefined;
  const legacyEventCurrent =
    admittedEvent !== undefined &&
    positioned.completed === admittedEvent &&
    positioned.baseRevision === admittedEvent.landingRevision &&
    [...positioned.participants].every(
      (participant) => !participant.failed && participant.event === admittedEvent,
    );
  const continuationCurrent =
    stagedContinuation &&
    Object.is(continuation.after, reader.after) &&
    (positioned.baseRevision === undefined || runtime.valueRevision === positioned.baseRevision) &&
    [...positioned.participants].every(
      (participant) =>
        !participant.failed &&
        participant.anchor === continuation.token &&
        participant.event === undefined,
    );
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
    (!legacyEventCurrent && !continuationCurrent) ||
    reactiveInternal.signalValue(runtime.predictionStack).some((prediction) => !prediction.dead)
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
  noteTransferChange(runtime);
  noteTransferChange(session);
  noteTransferChange(positioned);
  const admission = beginTransferAdmission(
    runtime.graph.__runtime,
    positioned.definition,
    positioned.binding?.handle,
    positioned.input,
  );
  positioned.phase = "active";
  try {
    driveStreamSource(
      runtime,
      session,
      positioned.definition,
      positioned.input,
      runtime.key as ReturnType<typeof storeKey>,
      abortSignal,
    );
  } finally {
    finishTransferAdmission(admission, runtime);
  }
}
