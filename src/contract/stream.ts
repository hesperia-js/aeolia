import type { StreamDefinition, StreamStore, ValueOptions } from "./types.ts";
import { __internal as reactiveInternal, computed } from "../reactive.ts";
import { addSignalAbort } from "../utils.ts";
import type { StreamSession, StoreRuntime, GraphRuntimeState } from "./runtime.ts";
import { assertGraphOpen, graphFault, reportContinuationError } from "./faults.ts";
import { storeKey } from "./identity.ts";
import { configureKeyedValue, issue, recordLanding } from "./store-state.ts";
import { getOrCreateStore, observeStoreReadable } from "./store.ts";
import { endStream, setStreamStatus, touch } from "./collection.ts";
import { writeAllStatuses } from "./status.ts";

export function failStream<T>(
  runtime: StoreRuntime<T>,
  session: StreamSession<T>,
  reason: unknown,
): void {
  try {
    endStream(runtime, session, "failed", reason);
  } catch (error) {
    reportContinuationError(runtime.graph.__runtime, error);
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
      if (session.ended) return;
      if (result.done) {
        endStream(runtime, session, "closed");
        return;
      }
      issue(runtime);
      session.emitted = true;
      try {
        recordLanding(runtime, result.value);
      } catch (error) {
        reportContinuationError(runtime.graph.__runtime, error);
      }
      try {
        setStreamStatus(runtime, "live");
      } catch (error) {
        reportContinuationError(runtime.graph.__runtime, error);
      }
    }
  } catch (error) {
    if (!session.ended) failStream(runtime, session, error);
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
  const session: StreamSession<T> = { controller, ended: false, emitted: false };
  runtime.stream = session;
  state.activeControllers.add(controller);
  runtime.failing = false;
  runtime.invalidated = false;
  runtime.errorCell.set(undefined);
  setStreamStatus(runtime, "opening");
  const closeOnAbort = (): void => {
    if (session.ended) return;
    try {
      endStream(runtime, session, "closed");
    } catch (error) {
      reportContinuationError(state, error);
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
        if (session.ended || !session.emitted) return;
        runtime.invalidated = true;
        try {
          writeAllStatuses(runtime);
          setStreamStatus(runtime, "stale");
        } catch (error) {
          reportContinuationError(state, error);
        }
      },
    });
    session.iterator = iterable[Symbol.asyncIterator]();
  } catch (error) {
    removeAbort();
    failStream(runtime, session, error);
    return;
  }
  controller.signal.addEventListener("abort", closeOnAbort, { once: true });
  void consumeStream(runtime, session)
    .finally(() => {
      controller.signal.removeEventListener("abort", closeOnAbort);
      removeAbort();
      state.activeControllers.delete(controller);
    })
    .catch((error) => reportContinuationError(state, error));
}

export function streamMember<T>(
  state: GraphRuntimeState<any>,
  stream: StreamDefinition<any, T, any>,
  input: unknown,
  options: ValueOptions<T> | undefined,
): StreamStore<T> {
  assertGraphOpen(state);
  const key = storeKey(stream.key(input));
  const runtime = getOrCreateStore(state, key) as StoreRuntime<T>;
  configureKeyedValue(runtime, stream, options);

  const active = runtime.stream;
  if (active === undefined) {
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
