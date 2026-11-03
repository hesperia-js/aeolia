import { afterEach, beforeEach, describe, expect, it, jest } from "bun:test";
import {
  capturePositionedStore,
  createGraph,
  createPositionedStream,
  defineContract,
  createStream,
  snapshot,
  observePositionedStoreCaptureState,
  project,
  subscribe,
  watch,
  type Graph,
  type StreamDefinition,
  type StreamStore,
  type ValueOptions,
  type PositionedStoreCaptureNotice,
  type Readable,
} from "../../src/index.ts";
import { testBackend, type TestBackend } from "../../src/testing.ts";
import { __internal as graphInternal } from "../../src/contract/engine.ts";

beforeEach(() => {
  jest.useFakeTimers();
  jest.setSystemTime(0);
});

afterEach(() => {
  jest.clearAllTimers();
  jest.useRealTimers();
});

async function flush(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
}

function waitForCaptureNotice(
  readable: Readable<PositionedStoreCaptureNotice>,
  matches: (notice: PositionedStoreCaptureNotice) => boolean,
): Promise<PositionedStoreCaptureNotice> {
  let stop: (() => void) | undefined;
  const pending = new Promise<PositionedStoreCaptureNotice>((resolve) => {
    stop = subscribe(readable, (notice) => {
      if (matches(notice)) resolve(notice);
    });
  });
  return pending.finally(() => stop?.());
}

function streamGraph<T>(stream: StreamDefinition<void, T>): Graph {
  return createGraph({
    contract: defineContract({ namespace: "stream-tests", operations: { stream } }),
    idleMs: 10,
  });
}

function numberStream(backend: TestBackend, name = "numbers"): StreamDefinition<void, number> {
  return createStream({
    name,
    key: () => `${name}/one`,
    open: backend.stream<void, number>(name),
  });
}

function streamMember<T>(graph: Graph): StreamStore<T> {
  const api = graph.api as unknown as {
    readonly stream: (input: void, options?: ValueOptions<T>) => StreamStore<T>;
  };
  return api.stream(undefined);
}

describe("stream stores", () => {
  for (const phase of ["open", "iterator"] as const) {
    it(`retires a stream cancelled during ${phase} and admits a later session`, async () => {
      const backend = testBackend();
      const openSuccessor = backend.stream<void, number>("cancel-setup.successor");
      const reason = new Error(`cancel during ${phase}`);
      let opens = 0;
      let acquisitions = 0;
      let nextCalls = 0;
      let returns = 0;
      let firstSignal: AbortSignal | undefined;
      const stream = createStream({
        name: "cancel-setup",
        key: () => "cancel-setup/one",
        open: (input: void, options): AsyncIterable<number> => {
          opens += 1;
          if (opens > 1) return openSuccessor(input, options);
          firstSignal = options.abortSignal;
          if (phase === "open") graph.cancelPending(reason);
          return {
            [Symbol.asyncIterator]() {
              acquisitions += 1;
              if (phase === "iterator") graph.cancelPending(reason);
              return {
                next: () => {
                  nextCalls += 1;
                  return Promise.resolve({ done: false as const, value: 99 });
                },
                return: () => {
                  returns += 1;
                  return Promise.resolve({ done: true as const, value: undefined });
                },
              };
            },
          };
        },
      });
      const graph = createGraph({
        contract: defineContract({ namespace: "cancel-setup", operations: { stream } }),
      });
      try {
        const cancelled = graph.api.stream(undefined, { initial: 7 });
        expect(firstSignal!.reason).toBe(reason);
        expect(cancelled.status.get()).toBe("closed");
        expect(cancelled.pending.get()).toBe(false);
        expect(cancelled.value.get()).toBe(7);
        expect(acquisitions).toBe(phase === "open" ? 0 : 1);
        expect(returns).toBe(phase === "open" ? 0 : 1);
        expect(nextCalls).toBe(0);
        const successor = graph.api.stream(undefined);
        const received = Promise.withResolvers<void>();
        const stop = subscribe(successor.value, (value) => {
          if (value === 8) received.resolve();
        });
        try {
          backend.emit("cancel-setup.successor", 8);
          await received.promise;
        } finally {
          stop();
        }
        expect(successor.value.get()).toBe(8);
        expect(successor.status.get()).toBe("live");
        expect(backend.calls[0]!.aborted).toBe(false);
        expect(nextCalls).toBe(0);
        expect(opens).toBe(2);
        expect(graph.resetVersion.get()).toBe(0);
      } finally {
        graph.dispose();
      }
    });
  }

  it("keeps a same-epoch successor failure when old iterator cleanup reopens and closes the source", () => {
    const failure = new Error("successor cannot open");
    const signals: AbortSignal[] = [];
    let opens = 0;
    let returns = 0;
    const stream = createStream({
      name: "cancel-successor",
      key: () => "cancel-successor/one",
      open: (_input: void, options): AsyncIterable<number> => {
        opens += 1;
        signals.push(options.abortSignal);
        if (opens === 2) throw failure;
        return {
          [Symbol.asyncIterator]() {
            return {
              next: () => new Promise<IteratorResult<number>>(() => undefined),
              return: () => {
                returns += 1;
                graph.api.stream(undefined);
                return Promise.resolve({ done: true as const, value: undefined });
              },
            };
          },
        };
      },
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "cancel-successor", operations: { stream } }),
    });
    try {
      const store = graph.api.stream(undefined, { initial: 4 });
      const reason = new Error("cancel old source");
      graph.cancelPending(reason);
      expect(opens).toBe(2);
      expect(returns).toBe(1);
      expect(signals[0]!.reason).toBe(reason);
      expect(signals[1]!.aborted).toBe(true);
      expect(store.status.get()).toBe("failed");
      expect(store.error.get()).toBe(failure);
      expect(store.value.get()).toBe(4);
      expect(graph.resetVersion.get()).toBe(0);
      graph.cancelPending();
      expect(returns).toBe(1);
      expect(store.status.get()).toBe("failed");
      expect(store.error.get()).toBe(failure);
    } finally {
      graph.dispose();
    }
  });

  it("accepts a later explicit snapshot policy after an opener omitted it", () => {
    const backend = testBackend();
    const stream = numberStream(backend, "snapshot-policy");
    const graph = streamGraph(stream);
    const api = graph.api as unknown as {
      readonly stream: (input: void, options?: ValueOptions<number>) => StreamStore<number>;
    };

    api.stream(undefined, { initial: 1 });
    expect(() => api.stream(undefined, { snapshot: false })).not.toThrow();
    expect(snapshot(graph).entries).not.toHaveProperty("snapshot-policy/one");
    graph.dispose();
  });

  it("opens on the member call, shares one source, and writes every emission", async () => {
    const backend = testBackend();
    const stream = numberStream(backend);
    const graph = streamGraph(stream);

    const first = streamMember<number>(graph);
    expect(backend.calls).toHaveLength(1);
    expect(first.status.get()).toBe("opening");
    expect(first.pending.get()).toBe(true);
    expect(first.value.get()).toBeUndefined();

    // Reads do not open a second source. A second member call gets a new
    // StreamStore view over the same keyed Store and stream lifecycle.
    first.value.get();
    const second = streamMember<number>(graph);
    expect(backend.calls).toHaveLength(1);
    expect(second.value).toBe(first.value);
    expect(second.status.get()).toBe("opening");

    backend.emit("numbers", 1);
    await flush();
    expect(first.value.get()).toBe(1);
    expect(second.value.get()).toBe(1);
    expect(first.pending.get()).toBe(false);
    expect(first.status.get()).toBe("live");

    backend.emit("numbers", 2);
    await flush();
    expect(first.value.get()).toBe(2);
    expect(second.value.get()).toBe(2);
    graph.dispose();
  });

  it("marks a committed stream stale on a gap, ignores an initial gap, and recovers on emission", async () => {
    const backend = testBackend();
    const stream = numberStream(backend, "gaps");
    const graph = streamGraph(stream);
    const store = streamMember<number>(graph);

    backend.gap("gaps");
    await flush();
    expect(store.status.get()).toBe("opening");
    expect(store.pending.get()).toBe(true);

    backend.emit("gaps", 4);
    await flush();
    backend.gap("gaps");
    await flush();
    expect(store.status.get()).toBe("stale");
    expect(store.pending.get()).toBe(false);

    backend.gap("gaps");
    await flush();
    expect(store.status.get()).toBe("stale");
    backend.emit("gaps", 5);
    await flush();
    expect(store.status.get()).toBe("live");
    expect(store.value.get()).toBe(5);
    graph.dispose();
  });

  it("closes normally, fails on iterator errors, and reopens either ended stream", async () => {
    const backend = testBackend();
    const stream = numberStream(backend, "lifecycle");
    const graph = streamGraph(stream);
    const first = streamMember<number>(graph);

    backend.emit("lifecycle", 1);
    await flush();
    backend.end("lifecycle");
    await flush();
    expect(first.status.get()).toBe("closed");
    expect(first.pending.get()).toBe(false);
    expect(first.value.get()).toBe(1);

    const reopened = streamMember<number>(graph);
    expect(backend.calls).toHaveLength(2);
    expect(reopened.status.get()).toBe("opening");
    expect(reopened.pending.get()).toBe(true);
    expect(reopened.value.get()).toBe(1);
    // Every StreamStore is a view over the shared stream lifecycle, including
    // a caller that retained the store from before the reopen.
    expect(first.status.get()).toBe("opening");

    const streamFailure = new Error("source failed");
    backend.end("lifecycle", streamFailure);
    await flush();
    expect(reopened.status.get()).toBe("failed");
    expect(reopened.pending.get()).toBe(false);
    expect(reopened.error.get()).toBe(streamFailure);
    const third = streamMember<number>(graph);
    expect(backend.calls).toHaveLength(3);
    expect(third.status.get()).toBe("opening");
    graph.dispose();
  });

  it("turns a synchronous open throw into a failed StreamStore", () => {
    const reason = new Error("cannot open");
    const stream = createStream({
      name: "open-throw",
      key: () => "open-throw/one",
      open: () => {
        throw reason;
      },
    });
    const graph = streamGraph(stream);
    const store = streamMember<number>(graph);
    expect(store.status.get()).toBe("failed");
    expect(store.pending.get()).toBe(false);
    expect(store.error.get()).toBe(reason);
    graph.dispose();
  });

  it("aborts an open stream on graph disposal and ignores late source activity", async () => {
    const backend = testBackend();
    const stream = numberStream(backend, "dispose");
    const graph = streamGraph(stream);
    const store = streamMember<number>(graph);
    const call = backend.calls[0]!;

    graph.dispose();
    expect(call.aborted).toBe(true);
    expect(() => store.value.get()).toThrow("disposed");
    expect(() => store.status.get()).toThrow("disposed");
    backend.emit("dispose", 99);
    await flush();
    expect(() => store.value.peek()).toThrow("disposed");
  });

  it("aborts an open stream when its unwatched store reaches collection", async () => {
    const backend = testBackend();
    const stream = numberStream(backend, "collect");
    const graph = streamGraph(stream);
    const store = streamMember<number>(graph);
    const call = backend.calls[0]!;
    const stop = watch(store.value, () => undefined);

    backend.emit("collect", 1);
    await flush();
    stop();
    jest.advanceTimersByTime(11);
    await flush();

    expect(call.aborted).toBe(true);
    expect(store.status.get()).toBe("empty");
    expect(store.value.get()).toBeUndefined();
    graph.dispose();
  });

  it("uses the opener's abort signal for a shared stream and ignores a joiner's signal", async () => {
    const backend = testBackend();
    const stream = numberStream(backend, "shared-abort");
    const graph = streamGraph(stream);
    const opener = new AbortController();
    const joiner = new AbortController();
    const api = graph.api as unknown as {
      readonly stream: (input: void, options?: ValueOptions<number>) => StreamStore<number>;
    };

    const first = api.stream(undefined, { abortSignal: opener.signal });
    const call = backend.calls[0]!;
    api.stream(undefined, { abortSignal: joiner.signal });
    joiner.abort();
    expect(call.aborted).toBe(false);
    opener.abort();
    expect(call.aborted).toBe(true);
    await flush();
    expect(first.status.get()).toBe("closed");

    const reopened = api.stream(undefined);
    expect(backend.calls).toHaveLength(2);
    expect(reopened.status.get()).toBe("opening");
    graph.dispose();
  });

  it("does not open a source for an already-aborted opener", () => {
    const backend = testBackend();
    const stream = numberStream(backend, "pre-aborted");
    const graph = streamGraph(stream);
    const opener = new AbortController();
    opener.abort();
    const api = graph.api as unknown as {
      readonly stream: (input: void, options?: ValueOptions<number>) => StreamStore<number>;
    };

    const store = api.stream(undefined, { abortSignal: opener.signal });
    expect(backend.calls).toHaveLength(0);
    expect(store.status.get()).toBe("closed");
    graph.dispose();
  });

  it("keeps a normal owner abort quiet when the pending iterator read rejects", async () => {
    const opener = new AbortController();
    const faults: unknown[] = [];
    let rejectNext!: (reason: unknown) => void;
    const stream = createStream({
      name: "owner-abort-pending-next",
      key: () => "owner-abort-pending-next/one",
      open: () => ({
        [Symbol.asyncIterator]: () => ({
          next: () =>
            new Promise<IteratorResult<number>>((_resolve, reject) => {
              rejectNext = reject;
            }),
          return: async () => ({ done: true, value: undefined }),
        }),
      }),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "owner-abort-pending-next", operations: { stream } }),
      onUnobservedFault: (error) => faults.push(error),
    });
    const api = graph.api as unknown as {
      readonly stream: (input: void, options?: ValueOptions<number>) => StreamStore<number>;
    };
    const store = api.stream(undefined, { abortSignal: opener.signal });
    const stop = watch(store.status, () => undefined);

    opener.abort();
    rejectNext(new DOMException("The stream owner aborted.", "AbortError"));
    await flush();

    expect(store.status.get()).toBe("closed");
    expect(faults).toEqual([]);
    stop();
    graph.dispose();
  });

  it("keeps the opener signal across reset and never revives an aborted owner", async () => {
    const backend = testBackend();
    const source = backend.stream<void, number>("reset-owner");
    const signals: AbortSignal[] = [];
    const stream = createStream({
      name: "reset-owner",
      key: () => "reset-owner/one",
      open: (input: void, options) => {
        signals.push(options.abortSignal);
        return source(input, options);
      },
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "reset-owner", operations: { stream } }),
    });
    const api = graph.api as unknown as {
      readonly stream: (input: void, options?: ValueOptions<number>) => StreamStore<number>;
    };
    const owner = new AbortController();
    const store = api.stream(undefined, { abortSignal: owner.signal });
    const stop = watch(store.status, () => undefined);
    const reset = graph.reset();
    expect(backend.calls).toHaveLength(2);
    expect(signals).toHaveLength(2);
    expect(backend.calls[0]!.aborted).toBe(true);
    expect(backend.calls[1]!.aborted).toBe(false);
    owner.abort();
    expect(backend.calls[1]!.aborted).toBe(true);
    await expect(reset).resolves.toBeUndefined();
    expect(store.status.get()).toBe("closed");
    stop();
    graph.dispose();

    const preAbortedBackend = testBackend();
    const preAbortedStream = numberStream(preAbortedBackend, "reset-pre-aborted-owner");
    const secondGraph = createGraph({
      contract: defineContract({
        namespace: "reset-pre-aborted-owner",
        operations: { stream: preAbortedStream },
      }),
    });
    const secondApi = secondGraph.api as unknown as {
      readonly stream: (input: void, options?: ValueOptions<number>) => StreamStore<number>;
    };
    const preAbortedOwner = new AbortController();
    preAbortedOwner.abort();
    const closed = secondApi.stream(undefined, { abortSignal: preAbortedOwner.signal });
    const stopClosed = watch(closed.status, () => undefined);
    await expect(secondGraph.reset()).resolves.toBeUndefined();
    expect(preAbortedBackend.calls).toHaveLength(0);
    expect(closed.status.get()).toBe("closed");
    stopClosed();
    secondGraph.dispose();
  });

  it("increments the keyed store generation for every stream emission", async () => {
    const backend = testBackend();
    const stream = numberStream(backend, "zero-max");
    const graph = streamGraph(stream);
    const stored = streamMember<number>(graph);
    const runtime = graphInternal.runtimeOf(stored);
    const before = runtime.generation;
    backend.emit("zero-max", 2);
    await flush();
    expect(runtime.generation).toBe(before + 1);
    stored.set(3);
    const afterWrite = runtime.generation;
    backend.emit("zero-max", 4);
    await flush();
    expect(runtime.generation).toBe(afterWrite + 1);
    graph.dispose();
  });

  it("preserves close and falsy positioned source failures without another pull", async () => {
    type Event = { readonly position: number; readonly value: number };
    type Ending =
      | { readonly kind: "closed" }
      | { readonly kind: "failed"; readonly cause: unknown };
    const endings: readonly Ending[] = [
      { kind: "closed" },
      { kind: "failed", cause: undefined },
      { kind: "failed", cause: false },
      { kind: "failed", cause: 0 },
      { kind: "failed", cause: "" },
    ];

    for (const ending of endings) {
      let resolveNext: ((result: IteratorResult<Event>) => void) | undefined;
      let rejectNext: ((cause: unknown) => void) | undefined;
      let nextCalls = 0;
      const name = "capture-terminal";
      const stream = createPositionedStream<void, number, string, number, object>({
        name,
        key: () => "capture-terminal/one",
        source: {
          open: (): AsyncIterable<Event> => ({
            [Symbol.asyncIterator]() {
              return {
                next: () => {
                  nextCalls += 1;
                  return new Promise<IteratorResult<Event>>((resolve, reject) => {
                    resolveNext = resolve;
                    rejectNext = reject;
                  });
                },
                return: () => Promise.resolve({ done: true as const, value: undefined }),
              };
            },
          }),
          initialBoundary: () => ({ available: true, after: 0 }),
          validateReader: () => false,
          isReaderCurrent: () => false,
          acknowledge: () => undefined,
          releaseReader: () => undefined,
        },
      });
      const graph = createGraph({
        contract: defineContract({ namespace: name, operations: { events: stream } }),
      });
      const source = graph.api.events();
      const projection = project(source, {
        kind: "reduce",
        initial: 0,
        step: (sum: number, value: number) => sum + value,
      });
      const checkpoint = capturePositionedStore(source, [projection] as const);
      const captureState = observePositionedStoreCaptureState(checkpoint);

      try {
        expect(checkpoint.present).toBe(false);
        const terminal = waitForCaptureNotice(
          captureState,
          (notice) => notice.kind === "closed" || notice.kind === "failed",
        );
        if (ending.kind === "closed") {
          if (resolveNext === undefined) throw new Error("The source has no pending first next");
          resolveNext({ done: true, value: undefined });
        } else {
          if (rejectNext === undefined) throw new Error("The source has no pending first next");
          rejectNext(ending.cause);
        }
        const notice = await terminal;
        expect(notice).toStrictEqual(ending);
        expect(nextCalls).toBe(1);
        expect(source.status.get()).toBe(ending.kind);
        expect(source.error.get()).toBe(ending.kind === "failed" ? ending.cause : undefined);
        expect(source.available.get()).toBe(false);
        expect(projection.value.get()).toBe(0);
      } finally {
        graph.dispose();
      }
    }
  });

  it.each(["reset", "abort"] as const)(
    "keeps a positioned capture closed through %s and explicit reentry",
    async (mode) => {
      type Event = { readonly position: number; readonly value: number };
      type Session = {
        nextCalls: number;
        returnCalls: number;
        resolveNext?: (result: IteratorResult<Event>) => void;
      };
      const sessions: Session[] = [];
      const stream = createPositionedStream<void, number, string, number, object>({
        name: "capture-reset-reentry",
        key: () => "capture-reset-reentry/one",
        source: {
          open: (): AsyncIterable<Event> => {
            const session: Session = { nextCalls: 0, returnCalls: 0 };
            sessions.push(session);
            return {
              [Symbol.asyncIterator]() {
                return {
                  next: () => {
                    session.nextCalls += 1;
                    return new Promise<IteratorResult<Event>>((resolve) => {
                      session.resolveNext = resolve;
                    });
                  },
                  return: () => {
                    session.returnCalls += 1;
                    session.resolveNext?.({ done: true, value: undefined });
                    return Promise.resolve({ done: true as const, value: undefined });
                  },
                };
              },
            };
          },
          initialBoundary: () => ({ available: true, after: 0 }),
          validateReader: () => false,
          isReaderCurrent: () => false,
          acknowledge: () => undefined,
          releaseReader: () => undefined,
        },
      });
      const graph = createGraph({
        contract: defineContract({
          namespace: "capture-reset-reentry",
          operations: { events: stream },
        }),
      });
      const owner = new AbortController();
      let stopFirst: (() => void) | undefined;
      let stopLate: (() => void) | undefined;
      let stopReopened: (() => void) | undefined;

      try {
        const first = graph.api.events(
          undefined,
          mode === "abort" ? { abortSignal: owner.signal } : undefined,
        );
        const firstProjection = project(first, {
          kind: "reduce",
          initial: 0,
          step: (sum: number, value: number) => sum + value,
        });
        const firstCheckpoint = capturePositionedStore(first, [firstProjection] as const);
        const firstState = observePositionedStoreCaptureState(firstCheckpoint);
        const firstNotices: PositionedStoreCaptureNotice[] = [];
        stopFirst = subscribe(firstState, (notice) => firstNotices.push(notice));
        expect(firstNotices[0]?.kind).toBe("changed");
        const terminal = waitForCaptureNotice(
          firstState,
          (notice) => notice.kind === "closed" || notice.kind === "invalidated",
        );

        if (mode === "reset") await graph.reset();
        else owner.abort(new Error("owner stopped"));
        const closed = await terminal;
        expect(closed).toStrictEqual({ kind: "closed" });
        expect(firstNotices.map((notice) => notice.kind)).toStrictEqual(["changed", "closed"]);

        const lateNotices: PositionedStoreCaptureNotice[] = [];
        stopLate = subscribe(firstState, (notice) => lateNotices.push(notice));
        expect(lateNotices).toStrictEqual([closed]);

        const reopened = graph.api.events();
        const reopenedProjection = project(reopened, {
          kind: "reduce",
          initial: 0,
          step: (sum: number, value: number) => sum + value,
        });
        const reopenedCheckpoint = capturePositionedStore(reopened, [reopenedProjection] as const);
        const reopenedState = observePositionedStoreCaptureState(reopenedCheckpoint);
        const reopenedNotices: PositionedStoreCaptureNotice[] = [];
        stopReopened = subscribe(reopenedState, (notice) => reopenedNotices.push(notice));
        expect(reopenedCheckpoint.sessionIdentity).not.toBe(firstCheckpoint.sessionIdentity);
        expect(reopenedNotices[0]?.kind).toBe("changed");
        expect(firstNotices.map((notice) => notice.kind)).toStrictEqual(["changed", "closed"]);
        expect(sessions).toHaveLength(2);
        expect(sessions[0]!.nextCalls).toBe(1);
        expect(sessions[0]!.returnCalls).toBe(1);
        expect(sessions[1]!.nextCalls).toBe(1);
      } finally {
        stopFirst?.();
        stopLate?.();
        stopReopened?.();
        graph.dispose();
      }
    },
  );
});
