import { afterEach, beforeEach, expect, it, jest } from "bun:test";
import {
  bindStream,
  createBindingContext,
  createGraph,
  createStream,
  createPositionedStream,
  defineContract,
  project,
  subscribe,
  capturePositionedProjection,
  isPositionedCheckpointCurrent,
  type PositionedProjectionCheckpoint,
  type Readable,
  type Projection,
} from "../../src/index.ts";
import { testBackend } from "../../src/testing.ts";

beforeEach(() => {
  jest.useFakeTimers();
  jest.setSystemTime(0);
});
afterEach(() => {
  jest.clearAllTimers();
  jest.useRealTimers();
});

function until<T>(readable: Readable<T>, matches: (value: T) => boolean): Promise<void> {
  const ready = Promise.withResolvers<void>();
  const stop = subscribe(readable, (value) => {
    if (matches(value)) ready.resolve();
  });
  return ready.promise.finally(stop);
}

function setup() {
  const backend = testBackend();
  const stream = createStream({
    name: "shared-events",
    key: () => "shared-events/one",
    open: backend.stream<void, number>("shared-events"),
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "shared-projections", operations: { stream } }),
    idleMs: 10,
  });
  return { backend, graph, source: graph.api.stream() };
}

it("delivers shared close before nested cancellation without replacing the outer source reason", async () => {
  const backend = testBackend();
  const open = backend.stream<void, number>("nested-cancel");
  let sourceSignal: AbortSignal | undefined;
  let returns = 0;
  const stream = createStream({
    name: "nested-cancel",
    key: () => "nested-cancel/one",
    open: (input: void, options): AsyncIterable<number> => {
      sourceSignal = options.abortSignal;
      const iterable = open(input, options);
      return {
        [Symbol.asyncIterator]() {
          const iterator = iterable[Symbol.asyncIterator]();
          return {
            next: () => iterator.next(),
            return: () => {
              returns += 1;
              return iterator.return!();
            },
          };
        },
      };
    },
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "nested-cancel", operations: { stream } }),
  });
  const source = graph.api.stream(undefined);
  const history = project(source, { kind: "accumulate", max: 3 });
  const outerReason = new Error("outer cancellation");
  const innerReason = new Error("nested cancellation");
  let nestedCalls = 0;
  const stop = subscribe(history.status, (status) => {
    if (status !== "closed") return;
    nestedCalls += 1;
    graph.cancelPending(innerReason);
  });
  try {
    const received = until(history.value, (items) => items.length === 1);
    backend.emit("nested-cancel", 5);
    await received;
    graph.cancelPending(outerReason);
    expect(nestedCalls).toBe(1);
    expect(sourceSignal!.reason).toBe(outerReason);
    expect(returns).toBe(1);
    expect(source.status.get()).toBe("closed");
    expect(history.status.get()).toBe("closed");
    expect(history.error.get()).toBeUndefined();
    expect(history.value.get()).toEqual([5]);
    expect(source.value.get()).toBe(5);
    graph.cancelPending();
    expect(nestedCalls).toBe(1);
    expect(returns).toBe(1);
  } finally {
    stop();
    graph.dispose();
  }
});

it("projects every shared emission without reopening, replaying, or observing direct writes", async () => {
  const { backend, graph, source } = setup();
  try {
    const history = project(source, { kind: "accumulate", max: 10 });
    const count = project(source, { kind: "reduce", initial: 0, step: (n) => n + 1 });
    expect(backend.calls).toHaveLength(1);
    const first = until(history.value, (items) => items.length === 1);
    backend.emit("shared-events", 4);
    await first;
    const joined = project(source, { kind: "accumulate", max: 10 });
    expect(joined.value.get()).toStrictEqual([]);
    const second = until(history.value, (items) => items.length === 2);
    backend.emit("shared-events", 4);
    await second;
    expect(history.value.get()).toStrictEqual([4, 4]);
    expect(joined.value.get()).toStrictEqual([4]);
    expect(count.value.get()).toBe(2);
    source.set(99);
    expect(history.value.get()).toStrictEqual([4, 4]);
    history.close();
    expect(backend.calls[0]!.aborted).toBe(false);
    const third = until(joined.value, (items) => items.length === 2);
    backend.emit("shared-events", 5);
    await third;
    expect(history.value.get()).toStrictEqual([4, 4]);
    expect(joined.value.get()).toStrictEqual([4, 5]);
    expect(count.value.get()).toBe(3);
    expect(backend.calls).toHaveLength(1);
  } finally {
    graph.dispose();
  }
});

it("starts a positioned shared projection at the next event when it joins during delivery", async () => {
  const backend = testBackend();
  const stream = createPositionedStream<void, number, string, number, object>({
    name: "positioned-shared-join",
    key: () => "positioned-shared-join/one",
    source: {
      open: backend.stream<void, { position: number; value: number }>("positioned-shared-join"),
      validateReader: () => true,
      isReaderCurrent: () => true,
      acknowledge: () => undefined,
      releaseReader: () => undefined,
    },
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "positioned-shared-join", operations: { stream } }),
  });
  const source = graph.api.stream();
  const policy = {
    kind: "reduce" as const,
    initial: 0,
    step: (sum: number, item: number) => sum + item,
  };
  const first = project(source, policy);
  let late: Projection<number> | undefined;
  let lateAtJoin: PositionedProjectionCheckpoint<number, number> | undefined;
  const stop = subscribe(first.value, (value) => {
    if (value !== 1 || late !== undefined) return;
    late = project(source, policy);
    lateAtJoin = capturePositionedProjection<number, number>(late);
  });
  try {
    const firstDelivered = until(first.value, (value) => value === 1);
    backend.emit("positioned-shared-join", { position: 5, value: 1 });
    await firstDelivered;
    expect(lateAtJoin).toMatchObject({
      value: 0,
      hasPosition: false,
      position: undefined,
      revision: 0,
    });
    if (late === undefined || lateAtJoin === undefined)
      throw new Error("The projection did not join during delivery");
    expect(isPositionedCheckpointCurrent(lateAtJoin)).toBe(true);
    expect(capturePositionedProjection<number, number>(first)).toMatchObject({
      value: 1,
      hasPosition: true,
      position: 5,
      revision: 1,
    });
    const secondDelivered = until(late.value, (value) => value === 2);
    backend.emit("positioned-shared-join", { position: 6, value: 2 });
    await secondDelivered;
    expect(first.value.get()).toBe(3);
    expect(late.value.get()).toBe(2);
    expect(capturePositionedProjection<number, number>(late)).toMatchObject({
      value: 2,
      hasPosition: true,
      position: 6,
      revision: 1,
    });
    expect(isPositionedCheckpointCurrent(lateAtJoin)).toBe(false);
    expect(backend.calls).toHaveLength(1);
  } finally {
    stop();
    graph.dispose();
  }
});

it("retains a stream for a projection then collects after its final listener closes", async () => {
  const { backend, graph, source } = setup();
  try {
    const history = project(source, { kind: "accumulate", max: 2, onOverflow: "drop-oldest" });
    await Promise.resolve();
    jest.advanceTimersByTime(50);
    expect(backend.calls[0]!.aborted).toBe(false);
    const stopValue = subscribe(source.value, () => undefined);
    history.close();
    jest.advanceTimersByTime(50);
    expect(backend.calls[0]!.aborted).toBe(false);
    stopValue();
    await Promise.resolve();
    jest.advanceTimersByTime(10);
    expect(backend.calls[0]!.aborted).toBe(true);
  } finally {
    graph.dispose();
  }
});

it("fails independent projection state on overflow or gaps without aborting the shared source", async () => {
  const { backend, graph, source } = setup();
  try {
    const bounded = project(source, { kind: "accumulate", max: 1 });
    const sum = project(source, { kind: "reduce", initial: 0, step: (total, n) => total + n });
    const first = until(sum.value, (value) => value === 2);
    backend.emit("shared-events", 2);
    await first;
    const second = until(sum.value, (value) => value === 5);
    backend.emit("shared-events", 3);
    await second;
    expect(bounded.status.get()).toBe("failed");
    expect(bounded.value.get()).toStrictEqual([2]);
    expect(sum.status.get()).toBe("open");
    expect(backend.calls[0]!.aborted).toBe(false);
    backend.gap("shared-events");
    expect(sum.status.get()).toBe("failed");
    expect(sum.value.get()).toBe(5);
    expect(source.status.get()).toBe("stale");
    expect(backend.calls[0]!.aborted).toBe(false);
  } finally {
    graph.dispose();
  }
});

it("ends projections with their source session and does not reconnect them on reopen", async () => {
  const { backend, graph, source } = setup();
  try {
    const history = project(source, { kind: "accumulate", max: 10 });
    const closed = until(history.status, (status) => status === "closed");
    backend.end("shared-events");
    await closed;
    const reopened = graph.api.stream();
    expect(backend.calls).toHaveLength(2);
    const sum = project(reopened, { kind: "reduce", initial: 0, step: (total, n) => total + n });
    const first = until(sum.value, (value) => value === 1);
    backend.emit("shared-events", 1);
    await first;
    expect(history.value.get()).toStrictEqual([]);
    const reason = new Error("source failed");
    const failed = until(sum.status, (status) => status === "failed");
    backend.end("shared-events", reason);
    await failed;
    expect(sum.error.get()).toBe(reason);
    const late = project(reopened, { kind: "accumulate", max: 2 });
    expect(late.status.get()).toBe("failed");
    expect(late.error.get()).toBe(reason);
  } finally {
    graph.dispose();
  }
});

it.each(["end", "reset"] as const)(
  "keeps bound shared projections on A after %s and starts B explicitly",
  async (mode) => {
    const backend = testBackend();
    const stream = createStream({
      name: "fixed-projection",
      key: () => "one",
      open: backend.stream<void, number>("fixed-projection"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "fixed-projection", operations: { stream } }),
    });
    try {
      const binding = bindStream(graph, stream, graph.api.stream, {
        context: createBindingContext(graph),
      });
      const a = binding.member();
      const history = project(a, { kind: "accumulate", max: 3 });
      const receivedA = until(history.value, (items) => items.length === 1);
      backend.emit("fixed-projection", 5);
      await receivedA;
      let resetting: Promise<void> | undefined;
      if (mode === "reset") resetting = graph.reset();
      else backend.end("fixed-projection");
      await binding.session(a).detached;
      await resetting;
      expect(history.status.get()).toBe("closed");
      expect(backend.calls).toHaveLength(1);
      const prior = mode === "reset" ? [] : [5];
      expect(history.value.get()).toEqual(prior);
      const b = binding.member();
      const next = project(b, { kind: "accumulate", max: 3 });
      const receivedB = until(next.value, (items) => items.length === 1);
      backend.emit("fixed-projection", 9);
      await receivedB;
      expect(next.value.get()).toEqual([9]);
      expect(history.value.get()).toEqual(prior);
      expect(history.status.get()).toBe("closed");
      expect(backend.calls).toHaveLength(2);
      next.close();
    } finally {
      graph.dispose();
    }
  },
);
