import { afterEach, beforeEach, describe, expect, it, jest } from "bun:test";
import {
  createGraph,
  defineContract,
  createStream,
  snapshot,
  watch,
  type Graph,
  type StreamDefinition,
  type StreamStore,
  type ValueOptions,
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
});
