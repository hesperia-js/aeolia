import { afterEach, beforeEach, expect, it, jest } from "bun:test";
import {
  adopt,
  bindStream,
  createBindingContext,
  inspectBoundAddress,
  createGraph,
  createStream,
  defineContract,
  snapshot,
  storeKey,
  subscribe,
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

function setup(bound = false) {
  let item = Promise.withResolvers<IteratorResult<number>>();
  let nextPull = Promise.withResolvers<void>();
  let sourceSignal: AbortSignal | undefined;
  let returns = 0;
  const stream = createStream({
    name: "retention-events",
    key: () => "retention-events/one",
    open: (_input: void, { abortSignal }) => {
      sourceSignal = abortSignal;
      nextPull = Promise.withResolvers<void>();
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
  const binding = bindStream(graph, stream, graph.api.stream, {
    context: createBindingContext(graph),
  });
  const store = bound ? binding.member() : graph.api.stream();
  return {
    graph,
    binding,
    store,
    signal: () => sourceSignal!,
    returns: () => returns,
    pulled: () => nextPull.promise,
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

it("does not let retired facets, metadata waiters or address inspection retain B or renew its idle period", async () => {
  const source = setup(true);
  const stopOld = subscribe(source.store.value, () => undefined);
  try {
    await source.emit(1);
    source.graph.cancelPending();
    const ticketA = source.binding.session(source.store);
    await ticketA.detached;
    const b = source.binding.member();
    await source.pulled();
    const ticketB = source.binding.session(b);
    const detachedB = ticketB.detached;
    jest.advanceTimersByTime(9);
    expect(source.signal().aborted).toBe(false);
    expect(source.binding.session(source.store)).toBe(ticketA);
    expect(source.store.value.get()).toBe(1);
    expect(inspectBoundAddress(source.binding, b.key)).toMatchObject({
      state: "occupied",
      present: false,
    });
    jest.advanceTimersByTime(1);
    const receipt = await detachedB;
    expect(source.signal().aborted).toBe(true);
    expect(source.returns()).toBe(2);
    expect(receipt.status).toBe("empty");
    expect(receipt.terminalCapture).toMatchObject({
      kind: "captured",
      capture: { present: false },
    });
    expect(source.store.value.get()).toBe(1);
  } finally {
    stopOld();
    source.graph.dispose();
  }
});

it("collects an imported stream base on its original idle deadline without renewing it on inspection", async () => {
  const source = setup(true);
  const reference = { module: "retention", member: "events" };
  const targetBackend = testBackend();
  const targetStream = createStream({
    name: "retention-events",
    key: () => "retention-events/one",
    open: targetBackend.stream<void, number>("retention-events"),
  });
  const targetGraph = createGraph({
    contract: defineContract({ namespace: "retention", operations: { stream: targetStream } }),
    idleMs: 10,
  });
  const targetBinding = bindStream(targetGraph, targetStream, targetGraph.api.stream, {
    context: createBindingContext(targetGraph),
  });
  try {
    source.store.set(7);
    const transfer = snapshot(source.graph, {
      version: 2,
      bindings: [{ binding: source.binding, reference }],
    });
    source.graph.dispose();

    adopt(targetGraph, transfer, {
      bindings: [{ binding: targetBinding, reference: { member: "events", module: "retention" } }],
    });
    await new Promise<void>((resolve) => queueMicrotask(resolve));
    expect(targetBackend.calls).toHaveLength(0);
    jest.advanceTimersByTime(6);
    expect(inspectBoundAddress(targetBinding, storeKey("retention-events/one"))).toMatchObject({
      state: "occupied",
      present: true,
      basePresent: true,
    });
    jest.advanceTimersByTime(4);
    expect(inspectBoundAddress(targetBinding, storeKey("retention-events/one"))).toMatchObject({
      kind: "stable",
      state: "empty",
      present: false,
      basePresent: false,
    });

    const fresh = targetBinding.member(undefined);
    expect([fresh.available.get(), fresh.value.get(), fresh.pending.get()]).toEqual([
      false,
      undefined,
      true,
    ]);
    expect(targetBackend.calls).toHaveLength(1);
  } finally {
    source.graph.dispose();
    targetGraph.dispose();
  }
});
