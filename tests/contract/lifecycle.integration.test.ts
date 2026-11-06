import { afterEach, beforeEach, expect, it, jest } from "bun:test";
import {
  computed,
  createGraph,
  createQuery,
  createStream,
  createMutation,
  affects,
  defineContract,
  project,
  subscribe,
  watch,
  Fault,
} from "../../src/index.ts";
import { testBackend } from "../../src/testing.ts";

beforeEach(() => {
  jest.useFakeTimers();
  jest.setSystemTime(100);
});
afterEach(() => {
  jest.clearAllTimers();
  jest.useRealTimers();
});
async function flush(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
}

it("disposes immediately when the graph lifetime signal is already aborted", () => {
  const backend = testBackend();
  const query = createQuery({
    name: "pre-aborted-query",
    key: () => "pre-aborted-query/one",
    fetch: backend.respond<void, number>("pre-aborted-query"),
  });
  const controller = new AbortController();
  controller.abort();
  const graph = createGraph({
    contract: defineContract({ namespace: "lifecycle", operations: { query } }),
    abortSignal: controller.signal,
  });

  expect(backend.calls).toHaveLength(0);
  try {
    graph.api.query();
    throw new Error("expected the graph API to be disposed");
  } catch (error) {
    expect(error).toBeInstanceOf(Fault);
    expect((error as Fault).kind).toBe("disposed");
  }
  graph.dispose();
});

it("disposes the graph and aborts in-flight work when its lifetime signal aborts", async () => {
  const backend = testBackend();
  const query = createQuery({
    name: "graph-abort-query",
    key: () => "graph-abort-query/one",
    fetch: backend.respond<void, number>("graph-abort-query"),
  });
  const stream = createStream({
    name: "graph-abort-stream",
    key: () => "graph-abort-stream/one",
    open: backend.stream<void, number>("graph-abort-stream"),
  });
  const controller = new AbortController();
  const graph = createGraph({
    contract: defineContract({ namespace: "lifecycle", operations: { query, stream } }),
    abortSignal: controller.signal,
  });
  const queryStore = graph.api.query();
  const streamStore = graph.api.stream();
  const queryCall = backend.calls[0]!;
  const streamCall = backend.calls[1]!;

  controller.abort();
  expect(queryCall.aborted).toBe(true);
  expect(streamCall.aborted).toBe(true);
  expect(() => queryStore.value.get()).toThrow("disposed");
  expect(() => streamStore.status.get()).toThrow("disposed");
  await flush();
  expect(() => graph.at("graph-abort-query/one")).toThrow("disposed");
  graph.dispose();
});

it("invalidates warmed query readables and their cached consumers on disposal", async () => {
  const backend = testBackend();
  const query = createQuery({
    name: "query",
    key: () => "one",
    fetch: backend.respond<void, number>("query"),
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "lifecycle", operations: { query } }),
  });
  const store = graph.api.query();
  backend.resolve(backend.calls[0]!, 42);
  await flush();
  const readables = [store.value, store.error, store.pending, store.status];
  const derived = computed(() => store.value.get());
  expect(derived.get()).toBe(42);
  for (const readable of readables) {
    readable.get();
    readable.peek();
  }
  graph.dispose();
  for (const readable of [...readables, derived]) {
    expect(() => readable.get()).toThrow("disposed");
    expect(() => readable.peek()).toThrow("disposed");
  }
});

it("waits for each stream session's first emission despite a seed or retained value", async () => {
  const backend = testBackend();
  const stream = createStream({
    name: "stream",
    key: () => "one",
    open: backend.stream<void, number>("stream"),
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "lifecycle", operations: { stream } }),
  });
  const store = graph.api.stream(undefined, { initial: 7 });
  backend.gap("stream");
  expect(store.status.get()).toBe("opening");
  expect(store.pending.get()).toBe(true);
  backend.emit("stream", 8);
  await flush();
  backend.gap("stream");
  expect(store.status.get()).toBe("stale");
  expect(store.pending.get()).toBe(false);
  backend.end("stream");
  await flush();
  const reopened = graph.api.stream();
  backend.gap("stream");
  expect(reopened.value.get()).toBe(8);
  expect(reopened.status.get()).toBe("opening");
  expect(reopened.pending.get()).toBe(true);
  for (const readable of [reopened.value, reopened.error, reopened.status, reopened.pending])
    readable.get();
  graph.dispose();
  for (const readable of [reopened.value, reopened.error, reopened.status, reopened.pending]) {
    expect(() => readable.get()).toThrow("disposed");
    expect(() => readable.peek()).toThrow("disposed");
  }
});

it("reports invalid effective freshness windows as contract faults before fetching", () => {
  const backend = testBackend();
  for (const invalid of [-1, NaN, Infinity, -Infinity]) {
    const query = createQuery({
      name: "query",
      key: () => "one",
      revalidateAfterMs: invalid,
      fetch: backend.respond<void, number>("query"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "lifecycle", operations: { query } }),
    });
    for (const run of [
      () => graph.api.query(),
      () => graph.api.query(undefined, { revalidateAfterMs: invalid }),
    ]) {
      try {
        run();
        throw new Error("expected rejection");
      } catch (error) {
        expect(error).toBeInstanceOf(Fault);
        expect((error as Fault).kind).toBe("contract");
      }
    }
    graph.dispose();
  }
  expect(backend.calls).toHaveLength(0);
});

it("collects eligible stores past a held oldest store and preserves the release deadline", async () => {
  const graph = createGraph({
    contract: defineContract({ namespace: "lifecycle", operations: {} }),
    idleMs: 10,
  });
  const held = graph.at("held");
  held.set(1);
  const release = watch(held.value, () => {});
  jest.advanceTimersByTime(2);
  const free = graph.at("free");
  free.set(2);
  await flush();
  jest.advanceTimersByTime(11);
  expect(free.value.get()).toBeUndefined();
  release();
  await flush();
  expect(held.value.get()).toBeUndefined();
  graph.dispose();
});

it("reorders touched deadlines even when the wall clock moves backwards", async () => {
  const graph = createGraph({
    contract: defineContract({ namespace: "lifecycle", operations: {} }),
    idleMs: 10,
  });
  const first = graph.at("first");
  first.set(1);
  const second = graph.at("second");
  second.set(2);
  jest.setSystemTime(50);
  second.value.get();
  await flush();
  jest.advanceTimersByTime(10);
  expect(second.value.get()).toBeUndefined();
  expect(first.value.get()).toBe(1);
  graph.dispose();
});

it("collects an overdue store when its last prediction settles without resetting idle time", async () => {
  const backend = testBackend();
  const query = createQuery({
    name: "query",
    key: () => "one",
    fetch: backend.respond<void, number>("query"),
  });
  const mutate = createMutation({
    name: "mutate",
    run: backend.perform<void, void>("mutate"),
    affects: [affects(query, { select: () => undefined, on: "invalidate", optimistic: () => 2 })],
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "lifecycle", operations: { query, mutate } }),
    idleMs: 10,
  });
  const store = graph.api.query(undefined, { initial: 1 });
  const pending = graph.api.mutate();
  await flush();
  jest.advanceTimersByTime(20);
  backend.reject(backend.calls[0]!, new Error("rejected"));
  await expect(pending).rejects.toThrow("rejected");
  await flush();
  expect(store.value.get()).toBeUndefined();
  graph.dispose();
});

it("preserves ordinary caller abort behavior for a mutation", async () => {
  const result = Promise.withResolvers<void>();
  let executionSignal: AbortSignal | undefined;
  const mutate = createMutation({
    name: "caller-abort.mutate",
    affects: [],
    run: (_input: void, { abortSignal }) => {
      executionSignal = abortSignal;
      return result.promise;
    },
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "caller-abort", operations: { mutate } }),
  });
  try {
    const caller = new AbortController();
    const pending = graph.api.mutate(undefined, { abortSignal: caller.signal });
    const reason = new Error("caller cancelled");
    caller.abort(reason);
    expect(executionSignal!.aborted).toBe(true);
    expect(executionSignal!.reason).not.toBe(reason);
    expect(executionSignal!.reason).toMatchObject({ name: "AbortError" });
    result.resolve();
    await pending;
  } finally {
    graph.dispose();
  }
});

for (const preAborted of [false, true]) {
  it(`retains readables and late mutation effects (pre-aborted: ${preAborted})`, async () => {
    const backend = testBackend();
    const signals: AbortSignal[] = [];
    const query = createQuery({
      name: "cancel.query",
      key: (input: string) => `cancel/${input}`,
      fetch: backend.respond<string, number>("cancel.query"),
    });
    const perform = backend.perform<void, string>("cancel.mutate");
    const mutate = createMutation({
      name: "cancel.mutate",
      run: (input: void, options) => {
        signals.push(options.abortSignal);
        return perform(input, options);
      },
      affects: [affects(query, { select: () => "cached", on: "revalidate" })],
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "cancel-lifecycle", operations: { query, mutate } }),
    });
    let stopCached = (): void => undefined;
    const observed: Array<number | undefined> = [];
    try {
      const cached = graph.api.query("cached", { initial: 7 });
      stopCached = subscribe(cached.value, (value) => observed.push(value));
      const pending = graph.api.query("pending");
      const ready = pending.ready;
      const value = cached.value;
      const derived = computed(() => value.get());
      expect(derived.get()).toBe(7);
      expect(observed).toEqual([7]);
      const controller = new AbortController();
      const earlierReason = new Error("caller already cancelled");
      if (preAborted) controller.abort(earlierReason);
      const mutation = graph.api.mutate(undefined, { abortSignal: controller.signal });
      expect(signals).toHaveLength(1);
      expect(signals[0]!.aborted).toBe(preAborted);
      const previousReason = signals[0]!.reason;
      if (preAborted) expect(previousReason).toMatchObject({ name: "AbortError" });
      const reason = new Error("request ended");

      graph.cancelPending(reason);

      expect(backend.calls[0]!.aborted).toBe(true);
      expect(signals[0]!.reason).toBe(preAborted ? previousReason : reason);
      await expect(ready).rejects.toBe(reason);
      expect(graph.resetVersion.get()).toBe(0);
      expect(cached.value).toBe(value);
      expect(value.get()).toBe(7);
      expect(derived.get()).toBe(7);
      expect(pending.error.get()).toBe(reason);
      expect(pending.pending.get()).toBe(false);

      backend.resolve(backend.calls[1]!, "accepted late");
      await expect(mutation).resolves.toBe("accepted late");
      expect(backend.calls).toHaveLength(3);
      expect(backend.calls[2]!.input).toBe("cached");
      expect(backend.calls[2]!.aborted).toBe(false);
      const refreshed = cached.ready;
      backend.resolve(backend.calls[2]!, 9);
      await refreshed;
      expect(derived.get()).toBe(9);
      expect(observed).toEqual([7, 9]);
      expect(graph.resetVersion.get()).toBe(0);
    } finally {
      stopCached();
      graph.dispose();
    }
    expect(() => graph.at("cancel/cached").value.get()).toThrow("disposed");
    expect(() => graph.cancelPending()).not.toThrow();
  });
}

it("retains prearmed freshness and collection timers after cancelling the entry work", async () => {
  const backend = testBackend();
  const query = createQuery({
    name: "cancel-timers.query",
    key: () => "cancel-timers/query",
    revalidateAfterMs: 10,
    fetch: backend.respond<void, number>("cancel-timers.query"),
  });
  const mutate = createMutation({
    name: "cancel-timers.mutate",
    affects: [],
    run: backend.perform<void, void>("cancel-timers.mutate"),
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "cancel-timers", operations: { query, mutate } }),
    idleMs: 15,
  });
  const cached = graph.api.query(undefined, { initial: 7 });
  const stop = subscribe(cached.value, () => undefined);
  const free = graph.at("cancel-timers/free");
  free.set(3);
  const mutation = graph.api.mutate();
  try {
    await expect(cached.ready).resolves.toBe(7);
    expect(backend.calls).toHaveLength(1);
    graph.cancelPending(new Error("cancel only admitted work"));
    expect(backend.calls[0]!.aborted).toBe(true);
    jest.advanceTimersByTime(10);
    expect(backend.calls).toHaveLength(2);
    expect(backend.calls[1]!.name).toBe("cancel-timers.query");
    expect(backend.calls[1]!.aborted).toBe(false);
    expect(cached.value.get()).toBe(7);
    jest.advanceTimersByTime(5);
    expect(free.value.get()).toBeUndefined();
    const ready = cached.ready;
    backend.resolve(backend.calls[1]!, 8);
    await expect(ready).resolves.toBe(8);
    backend.resolve(backend.calls[0]!, undefined);
    await mutation;
    expect(graph.resetVersion.get()).toBe(0);
  } finally {
    stop();
    graph.dispose();
  }
});

for (const closeFailure of ["accessor", "throw", "reject"] as const) {
  it(`attempts other captured cleanup and final disposal after an iterator return ${closeFailure}`, async () => {
    const failure = new Error(`iterator return ${closeFailure}`);
    const signals: AbortSignal[] = [];
    const returns: string[] = [];
    const rejected = Promise.withResolvers<IteratorResult<number>>();
    const stream = createStream({
      name: "cancel.cleanup",
      key: (input: string) => `cleanup/${input}`,
      open: (input, options): AsyncIterable<number> => {
        signals.push(options.abortSignal);
        return {
          [Symbol.asyncIterator]() {
            return {
              next: () => new Promise<IteratorResult<number>>(() => undefined),
              get return() {
                if (input === "first" && closeFailure === "accessor") {
                  returns.push(input);
                  throw failure;
                }
                return () => {
                  returns.push(input);
                  if (input !== "first")
                    return Promise.resolve({ done: true as const, value: undefined });
                  if (closeFailure === "throw") throw failure;
                  return rejected.promise;
                };
              },
            };
          },
        };
      },
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "cancel-cleanup", operations: { stream } }),
    });
    const first = graph.api.stream("first");
    const second = graph.api.stream("second");
    const projection = project(graph, stream, "projection", { kind: "accumulate", max: 2 });
    const reason = new Error("cancel all sources");
    try {
      let escaped: unknown;
      try {
        graph.cancelPending(reason);
      } catch (error) {
        escaped = error;
      }
      if (closeFailure === "accessor") {
        expect(escaped).toBeInstanceOf(AggregateError);
        expect((escaped as AggregateError).errors).toEqual([failure]);
      } else {
        expect(escaped).toBeUndefined();
      }
      expect(returns).toEqual(["first", "second", "projection"]);
      expect(signals.map((signal) => signal.reason)).toEqual([reason, reason, reason]);
      expect(first.status.get()).toBe("closed");
      expect(second.status.get()).toBe("closed");
      expect(projection.status.get()).toBe("closed");
      graph.cancelPending(new Error("repeat"));
      expect(returns).toEqual(["first", "second", "projection"]);
      if (closeFailure === "reject") {
        rejected.reject(failure);
        await expect(rejected.promise).rejects.toBe(failure);
      }
    } finally {
      graph.dispose();
    }
    expect(() => first.status.get()).toThrow("disposed");
    expect(() => second.value.get()).toThrow("disposed");
    expect(returns).toEqual(["first", "second", "projection"]);
  });
}
