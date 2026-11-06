import { afterEach, beforeEach, describe, expect, it, jest } from "bun:test";
import {
  Fault,
  createGraph,
  defineContract,
  createStream,
  createQuery,
  project,
  subscribe,
  type Graph,
  type StreamDefinition,
} from "../../src/index.ts";
import { testBackend, type TestBackend } from "../../src/testing.ts";

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

function waitForProjectionStatus(
  projection: ReturnType<typeof project>,
  expected: "closed" | "failed",
): Promise<void> {
  return new Promise((resolve) => {
    let stop: (() => void) | undefined;
    let done = false;
    stop = subscribe(projection.status, (status) => {
      if (status === expected) {
        done = true;
        stop?.();
        resolve();
      }
    });
    if (done) stop();
  });
}

describe("stream projections", () => {
  for (const restartFrom of ["query abort", "old iterator cleanup"] as const) {
    it(`preserves a replacement projection generation created during ${restartFrom}`, async () => {
      const sources: Array<{
        signal: AbortSignal;
        read: ReturnType<typeof Promise.withResolvers<IteratorResult<number>>>;
        returns: number;
        nextCalls: number;
      }> = [];
      let nestedReset: Promise<void> | undefined;
      let reenter = false;
      const stream = createStream({
        name: "projection-cancel-generation",
        key: () => "projection-cancel-generation/one",
        open: (_input: void, options): AsyncIterable<number> => {
          const source = {
            signal: options.abortSignal,
            read: Promise.withResolvers<IteratorResult<number>>(),
            returns: 0,
            nextCalls: 0,
          };
          sources.push(source);
          return {
            [Symbol.asyncIterator]() {
              return {
                next: () => {
                  source.nextCalls += 1;
                  if (source.nextCalls > 1)
                    source.read = Promise.withResolvers<IteratorResult<number>>();
                  return source.read.promise;
                },
                return: () => {
                  source.returns += 1;
                  if (reenter && restartFrom === "old iterator cleanup") {
                    reenter = false;
                    nestedReset = graph.reset();
                  }
                  return Promise.resolve({ done: true as const, value: undefined });
                },
              };
            },
          };
        },
      });
      const query = createQuery({
        name: "projection-cancel-query",
        key: () => "projection-cancel-query/one",
        fetch: (_input: void, options) => {
          options.abortSignal.addEventListener(
            "abort",
            () => {
              if (!reenter || restartFrom !== "query abort") return;
              reenter = false;
              nestedReset = graph.reset();
            },
            { once: true },
          );
          return new Promise<number>(() => undefined);
        },
      });
      const graph = createGraph({
        contract: defineContract({
          namespace: "projection-cancel-generation",
          operations: { stream, query },
        }),
      });
      if (restartFrom === "query abort") graph.api.query();
      const projection = project(graph, stream, undefined, { kind: "accumulate", max: 4 });
      try {
        reenter = true;
        if (restartFrom === "query abort") {
          graph.cancelPending(new Error("cancel entry cohort"));
        } else {
          const superseded = graph.reset();
          await expect(superseded).rejects.toMatchObject({ name: "AbortError" });
        }
        expect(nestedReset).toBeDefined();
        await nestedReset;
        expect(sources).toHaveLength(2);
        expect(sources[0]!.signal.aborted).toBe(true);
        expect(sources[0]!.returns).toBe(1);
        expect(sources[1]!.signal.aborted).toBe(false);
        expect(sources[1]!.returns).toBe(0);
        expect(projection.status.get()).toBe("open");

        const oldRead = sources[0]!.read;
        oldRead.resolve({ done: false, value: 99 });
        await oldRead.promise;
        expect(projection.value.get()).toEqual([]);
        const received = Promise.withResolvers<void>();
        const stop = subscribe(projection.value, (items) => {
          if (items.length === 1 && items[0] === 7) received.resolve();
        });
        try {
          sources[1]!.read.resolve({ done: false, value: 7 });
          await received.promise;
        } finally {
          stop();
        }
        expect(projection.value.get()).toEqual([7]);
        expect(sources[1]!.nextCalls).toBe(2);
        const reason = new Error("cancel replacement through its retained registration");
        graph.cancelPending(reason);
        expect(sources[1]!.signal.reason).toBe(reason);
        expect(sources[1]!.returns).toBe(1);
        expect(projection.status.get()).toBe("closed");
        expect(projection.value.get()).toEqual([7]);
        expect(projection.error.get()).toBeUndefined();
      } finally {
        graph.dispose();
      }
      expect(sources.map((source) => source.returns)).toEqual([1, 1]);
    });
  }

  it("opens independently, passes the required key, accumulates, and drops oldest", async () => {
    const backend = testBackend();
    const stream = numberStream(backend, "projection");
    const graph = streamGraph(stream);
    const first = project(graph, stream, undefined, {
      kind: "accumulate",
      max: 2,
      onOverflow: "drop-oldest",
    });
    const second = project(graph, stream, undefined, {
      kind: "accumulate",
      max: 2,
      onOverflow: "drop-oldest",
    });

    expect(backend.calls).toHaveLength(2);
    expect(String(backend.calls[0]!.key)).toBe("projection/one");
    expect(String(backend.calls[1]!.key)).toBe("projection/one");
    backend.emit("projection", 1);
    backend.emit("projection", 2);
    await flush();
    expect(first.value.get()).toEqual([1, 2]);
    expect(second.value.get()).toEqual([1, 2]);
    backend.emit("projection", 3);
    await flush();
    expect(first.value.get()).toEqual([2, 3]);
    expect(second.value.get()).toEqual([2, 3]);
    expect(first.status.get()).toBe("open");
    first.close();
    expect(first.status.get()).toBe("closed");
    expect(backend.calls[0]!.aborted).toBe(true);
    second.close();
    graph.dispose();
  });

  it("fails and closes on a gap or overflow while preserving accumulated value", async () => {
    const backend = testBackend();
    const stream = numberStream(backend, "projection-failure");
    const graph = streamGraph(stream);
    const gapped = project(graph, stream, undefined, { kind: "accumulate", max: 3 });
    backend.emit("projection-failure", 7);
    await flush();
    backend.gap("projection-failure");
    await flush();
    expect(gapped.status.get()).toBe("failed");
    expect(gapped.value.get()).toEqual([7]);
    expect(backend.calls[0]!.aborted).toBe(true);

    const overflow = project(graph, stream, undefined, { kind: "accumulate", max: 1 });
    backend.emit("projection-failure", 8);
    await flush();
    backend.emit("projection-failure", 9);
    await flush();
    expect(overflow.status.get()).toBe("failed");
    expect(overflow.value.get()).toEqual([8]);
    expect(backend.calls[1]!.aborted).toBe(true);
    graph.dispose();
  });

  it("closes a projection after its source iterator completes normally", async () => {
    const stream = createStream({
      name: "projection-complete",
      key: () => "projection-complete/one",
      open: () =>
        (async function* () {
          yield 2;
          yield 3;
        })(),
    });
    const graph = streamGraph(stream);
    const projection = project(graph, stream, undefined, {
      kind: "accumulate",
      max: 3,
    });

    await waitForProjectionStatus(projection, "closed");
    expect(projection.value.get()).toEqual([2, 3]);
    expect(projection.status.get()).toBe("closed");
    expect(projection.error.get()).toBeUndefined();
    graph.dispose();
  });

  it("fails a projection when its source iterator rejects", async () => {
    const failure = new Error("projection iterator failed");
    const stream = createStream({
      name: "projection-iterator-failure",
      key: () => "projection-iterator-failure/one",
      open: () =>
        (async function* () {
          yield 4;
          throw failure;
        })(),
    });
    const graph = streamGraph(stream);
    const projection = project(graph, stream, undefined, {
      kind: "accumulate",
      max: 3,
    });

    await waitForProjectionStatus(projection, "failed");
    expect(projection.value.get()).toEqual([4]);
    expect(projection.status.get()).toBe("failed");
    expect(projection.error.get()).toBe(failure);
    graph.dispose();
  });

  it("represents a synchronous projection open throw as a failed projection", () => {
    const reason = new Error("projection cannot open");
    const stream = createStream({
      name: "projection-open-throw",
      key: () => "projection-open-throw/one",
      open: () => {
        throw reason;
      },
    });
    const graph = streamGraph(stream);
    const projection = project(graph, stream, undefined, {
      kind: "accumulate",
      max: 3,
    });

    expect(projection.value.get()).toEqual([]);
    expect(projection.status.get()).toBe("failed");
    expect(projection.error.get()).toBe(reason);
    graph.dispose();
  });

  it("rethrows a synchronous projection key throw without opening its source", async () => {
    const reason = new Error("projection key failed");
    let keyCalls = 0;
    let openCalls = 0;
    const stream = createStream({
      name: "projection-key-throw",
      key: () => {
        keyCalls += 1;
        if (keyCalls === 1) throw reason;
        return "projection-key-throw/one";
      },
      open: () => {
        openCalls += 1;
        return (async function* () {})();
      },
    });
    const graph = streamGraph(stream);

    expect(() =>
      project(graph, stream, undefined, {
        kind: "accumulate",
        max: 3,
      }),
    ).toThrow(reason);
    expect(keyCalls).toBe(1);
    expect(openCalls).toBe(0);

    const recovered = project(graph, stream, undefined, {
      kind: "accumulate",
      max: 3,
    });
    await flush();
    expect(recovered.status.get()).toBe("closed");
    expect(openCalls).toBe(1);
    graph.dispose();
  });

  it("reduces emissions and fails when the reduce step throws", async () => {
    const backend = testBackend();
    const stream = numberStream(backend, "reduce");
    const graph = streamGraph(stream);
    const projection = project(graph, stream, undefined, {
      kind: "reduce",
      initial: 0,
      step: (accumulator, item) => {
        if (item === 9) throw new Error("bad item");
        return accumulator + item;
      },
    });
    backend.emit("reduce", 2);
    await flush();
    expect(projection.value.get()).toBe(2);
    backend.emit("reduce", 9);
    await flush();
    expect(projection.status.get()).toBe("failed");
    expect(projection.value.get()).toBe(2);
    expect((projection.error.get() as Error).message).toBe("bad item");
    graph.dispose();
  });

  it("rejects invalid accumulation bounds at construction", () => {
    const backend = testBackend();
    const stream = numberStream(backend, "bounds");
    const graph = streamGraph(stream);
    for (const max of [
      0,
      Number.POSITIVE_INFINITY,
      Number.NEGATIVE_INFINITY,
      Number.NaN,
      -1,
      1.5,
    ]) {
      try {
        project(graph, stream, undefined, { kind: "accumulate", max });
        throw new Error("project unexpectedly accepted an invalid maximum");
      } catch (error) {
        expect(error).toBeInstanceOf(Fault);
        expect((error as Fault).kind).toBe("contract");
      }
    }
    graph.dispose();
  });

  it("closes projections on graph disposal", () => {
    const backend = testBackend();
    const stream = numberStream(backend, "projection-dispose");
    const graph = streamGraph(stream);
    const projection = project(graph, stream, undefined, { kind: "accumulate", max: 2 });
    const call = backend.calls[0]!;
    graph.dispose();
    expect(call.aborted).toBe(true);
    expect(projection.status.get()).toBe("closed");
    projection.close();
  });
});
