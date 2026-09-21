import { afterEach, beforeEach, expect, it, jest } from "bun:test";
import { createGraph, createStream, defineContract, subscribe } from "../../src/index.ts";

beforeEach(() => {
  jest.useFakeTimers();
  jest.setSystemTime(0);
});

afterEach(() => {
  jest.clearAllTimers();
  jest.useRealTimers();
});

function setup() {
  let item = Promise.withResolvers<IteratorResult<number>>();
  let nextPull = Promise.withResolvers<void>();
  let sourceSignal: AbortSignal | undefined;
  let returns = 0;
  const stream = createStream({
    name: "retention-events",
    key: () => "retention-events/one",
    open: (_input: void, { abortSignal }) => {
      sourceSignal = abortSignal;
      return {
        [Symbol.asyncIterator]() {
          return {
            next() {
              nextPull.resolve();
              return item.promise;
            },
            return() {
              returns += 1;
              return Promise.resolve({ done: true as const, value: undefined });
            },
          };
        },
      };
    },
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "retention", operations: { stream } }),
    idleMs: 10,
  });
  const store = graph.api.stream();
  return {
    graph,
    store,
    signal: () => sourceSignal!,
    returns: () => returns,
    async emit(value: number) {
      const pending = item;
      item = Promise.withResolvers<IteratorResult<number>>();
      nextPull = Promise.withResolvers<void>();
      pending.resolve({ done: false, value });
      await nextPull.promise;
    },
    async emitAfterClosure(value: number) {
      item.resolve({ done: false, value });
      await item.promise;
    },
  };
}

it("collects an unobserved emitting stream from its last listener departure", async () => {
  const source = setup();
  try {
    const stop = subscribe(source.store.value, () => undefined);
    await source.emit(1);
    jest.advanceTimersByTime(20);
    expect(source.signal().aborted).toBe(false);
    stop();
    jest.advanceTimersByTime(6);
    await source.emit(2);
    // An untracked read is not a new listener or a renewal of stream retention.
    expect(source.store.value.get()).toBe(2);
    jest.advanceTimersByTime(3);
    await source.emit(3);
    expect(source.signal().aborted).toBe(false);
    jest.advanceTimersByTime(1);
    expect(source.signal().aborted).toBe(true);
    expect(source.returns()).toBe(1);
    await source.emitAfterClosure(99);
    expect(source.store.status.get()).toBe("empty");
    expect(source.store.value.get()).toBeUndefined();
  } finally {
    source.graph.dispose();
  }
});

it("starts a new complete retention period after a returning listener departs", async () => {
  const source = setup();
  try {
    const stopFirst = subscribe(source.store.value, () => undefined);
    await source.emit(1);
    stopFirst();
    jest.advanceTimersByTime(8);
    const stopSecond = subscribe(source.store.status, () => undefined);
    jest.advanceTimersByTime(50);
    expect(source.signal().aborted).toBe(false);
    stopSecond();
    await Promise.resolve();
    jest.advanceTimersByTime(9);
    expect(source.signal().aborted).toBe(false);
    jest.advanceTimersByTime(1);
    expect(source.signal().aborted).toBe(true);
  } finally {
    source.graph.dispose();
  }
});

it("expires a never-observed stream despite continuous source activity", async () => {
  const source = setup();
  try {
    jest.advanceTimersByTime(4);
    await source.emit(1);
    jest.advanceTimersByTime(4);
    await source.emit(2);
    jest.advanceTimersByTime(2);
    expect(source.signal().aborted).toBe(true);
    expect(source.returns()).toBe(1);
  } finally {
    source.graph.dispose();
  }
});
