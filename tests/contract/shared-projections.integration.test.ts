import { afterEach, beforeEach, expect, it, jest } from "bun:test";
import {
  createGraph,
  createStream,
  defineContract,
  project,
  subscribe,
  type Readable,
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
