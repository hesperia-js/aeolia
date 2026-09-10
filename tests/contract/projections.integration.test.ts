import { afterEach, beforeEach, describe, expect, it, jest } from "bun:test";
import {
  Fault,
  createGraph,
  defineContract,
  createStream,
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
