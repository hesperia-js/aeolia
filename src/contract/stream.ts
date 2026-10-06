import type { StreamDefinition, StreamStore, ValueOptions } from "./types.ts";
import { isValueOptions } from "./types.ts";
import { __internal as reactiveInternal, computed } from "../reactive.ts";
import { addSignalAbort } from "../utils.ts";
import type { StreamSession, StoreRuntime, GraphRuntimeState } from "./runtime.ts";
import { assertGraphOpen, graphFault, reportContinuationError } from "./faults.ts";
import { storeKey } from "./identity.ts";
import { configureKeyedValue, issue, recordLanding } from "./store-state.ts";
import { getOrCreateStore, observeStoreReadable } from "./store.ts";
import { endStream, setStreamStatus, touch } from "./collection.ts";
import { writeAllStatuses } from "./status.ts";
import { publishStreamEvent } from "./stream-events.ts";

function sameSessionEpoch<T>(runtime: StoreRuntime<T>, session: StreamSession<T>): boolean {
  return session.resetEpoch === runtime.graph.__runtime.resetEpoch;
}

function isCurrentSession<T>(runtime: StoreRuntime<T>, session: StreamSession<T>): boolean {
  return !session.ended && runtime.stream === session && sameSessionEpoch(runtime, session);
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
      const result = await iterator.next();
      if (!isCurrentSession(runtime, session)) return;
      if (result.done) {
        endStream(runtime, session, "closed");
        return;
      }
      issue(runtime);
      session.emitted = true;
      try {
        reactiveInternal.batch(() => recordLanding(runtime, result.value));
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
  const controller = new AbortController();
  const removeAbort = addSignalAbort(abortSignal, controller);
  const session: StreamSession<T> = {
    controller,
    resetEpoch: state.resetEpoch,
    listeners: new Set(),
    ended: false,
    emitted: false,
    unobservedSince: runtime.liveReadableCount === 0 ? Date.now() : undefined,
  };
  runtime.stream = session;
  state.activeControllers.add(controller);
  try {
    reactiveInternal.batch(() => {
      runtime.failing = false;
      runtime.invalidated = false;
      runtime.errorCell.set(undefined);
      setStreamStatus(runtime, "opening");
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
    iterable = stream.open(input, {
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
            setStreamStatus(runtime, "stale");
          });
        } catch (error) {
          if (sameSessionEpoch(runtime, session)) reportContinuationError(state, error);
        }
      },
    });
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
    if (typeof iterator.return === "function") {
      try {
        void Promise.resolve(iterator.return()).catch(() => undefined);
      } catch {
        /* A reset owns the current session now. */
      }
    }
    removeAbort();
    return;
  }
  session.iterator = iterator;
  controller.signal.addEventListener("abort", closeOnAbort, { once: true });
  void consumeStream(runtime, session)
    .finally(() => {
      controller.signal.removeEventListener("abort", closeOnAbort);
      removeAbort();
      state.activeControllers.delete(controller);
    })
    .catch((error) => {
      if (sameSessionEpoch(runtime, session)) reportContinuationError(state, error);
    });
}

export function streamMember<T>(
  state: GraphRuntimeState<any>,
  stream: StreamDefinition<any, T, any>,
  input: unknown,
  options: ValueOptions<T> | undefined,
): StreamStore<T> {
  assertGraphOpen(state);
  if (options !== undefined && !isValueOptions(options))
    throw new TypeError("Invalid stream options.");
  const key = storeKey(stream.key(input));
  const runtime = getOrCreateStore(state, key) as StoreRuntime<T>;
  configureKeyedValue(runtime, stream, options);

  const active = runtime.stream;
  if (active === undefined) {
    runtime.streamSource = {
      stream,
      input,
      ...(options?.abortSignal === undefined ? {} : { abortSignal: options.abortSignal }),
    };
    openStream(runtime, stream, input, key, options?.abortSignal);
  }
  const status = runtime.streamStatus;
  const pending = computed(() => status.get() === "opening", {
    label: `stream:${stream.name}:${String(key)}:pending`,
  });
  const streamStore = Object.create(runtime.store) as StreamStore<T>;
  Object.assign(streamStore, { pending, error: runtime.error, status });
  Object.freeze(streamStore);

  observeStoreReadable(runtime, status);
  observeStoreReadable(runtime, pending);
  reactiveInternal.bindReadable(pending, {
    graphId: state.graph.id,
    reportFault: (fault) => graphFault(state, fault),
  });
  touch(runtime);
  return streamStore;
}
