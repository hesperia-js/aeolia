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
  withoutWrites,
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

for (const { label, preAborted, mode } of [
  { label: "supplied reason", preAborted: false, mode: "supplied" },
  { label: "pre-aborted caller", preAborted: true, mode: "supplied" },
  { label: "omitted reason", preAborted: false, mode: "omitted" },
  { label: "explicit undefined", preAborted: false, mode: "undefined" },
] as const) {
  it(`retains readables and late mutation effects (${label})`, async () => {
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
      const suppliedReason = new Error("request ended");

      if (mode === "supplied") graph.cancelPending(suppliedReason);
      else if (mode === "omitted") graph.cancelPending();
      else graph.cancelPending(undefined);

      const reason = mode === "supplied" ? suppliedReason : signals[0]!.reason;
      if (mode !== "supplied") {
        expect(reason).toBeInstanceOf(Error);
        expect(reason).toMatchObject({ name: "AbortError" });
      }

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

it("caches graph retirement before abort callouts and joins the original query", async () => {
  const backend = testBackend();
  let reentrant: Promise<void> | undefined;
  let admissionFailure: unknown;
  let reacquire: (() => void) | undefined;
  let retireOwner: (() => Promise<void>) | undefined;
  const query = createQuery({
    name: "retirement.query",
    key: () => "retirement/one",
    fetch: (input: void, options) => {
      options.abortSignal.addEventListener("abort", () => {
        try {
          reacquire?.();
        } catch (cause) {
          admissionFailure = cause;
        }
        reentrant = retireOwner?.();
      });
      return backend.respond<void, number>("retirement.query")(input, options);
    },
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "retirement", operations: { query } }),
  });
  reacquire = () => graph.api.query();
  retireOwner = () => graph.retire();
  const store = graph.api.query();
  const ready = store.ready.then(
    () => undefined,
    () => undefined,
  );
  const call = backend.calls[0]!;
  let resolved = false;
  try {
    const retirement = graph.retire();
    expect(reentrant).toBe(retirement);
    expect(graph.retire()).toBe(retirement);
    if (!(admissionFailure instanceof Fault))
      throw new Error("Expected a disposed admission fault");
    expect(admissionFailure.kind).toBe("disposed");
    expect(backend.calls).toHaveLength(1);
    await ready;
    expect(call.aborted).toBe(true);
    let settled = false;
    void retirement.then(
      () => {
        settled = true;
      },
      () => {
        settled = true;
      },
    );
    await flush();
    expect(settled).toBe(false);
    backend.resolve(call, 42);
    resolved = true;
    await retirement;
  } finally {
    if (!resolved) backend.resolve(call, 42);
    graph.dispose();
  }
});

it("joins the original stream read and its iterator return after graph retirement", async () => {
  let resolveRead!: (result: IteratorResult<number>) => void;
  let resolveReturn!: (result: IteratorResult<number>) => void;
  const read = new Promise<IteratorResult<number>>((resolve) => {
    resolveRead = resolve;
  });
  const returned = new Promise<IteratorResult<number>>((resolve) => {
    resolveReturn = resolve;
  });
  const stream = createStream({
    name: "retirement.stream",
    key: () => "retirement/stream",
    open: () => ({
      [Symbol.asyncIterator]: () => ({ next: () => read, return: () => returned }),
    }),
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "retirement-stream", operations: { stream } }),
  });
  graph.api.stream(undefined);
  const retirement = graph.retire();
  let settled = false;
  void retirement.then(
    () => {
      settled = true;
    },
    () => {
      settled = true;
    },
  );
  try {
    await flush();
    expect(settled).toBe(false);
    resolveRead({ done: true, value: undefined });
    await flush();
    expect(settled).toBe(false);
    resolveReturn({ done: true, value: undefined });
    await retirement;
  } finally {
    resolveRead({ done: true, value: undefined });
    resolveReturn({ done: true, value: undefined });
    graph.dispose();
  }
});

for (const reenter of [false, true]) {
  it(`recovers a detached stream return after reset interruption (reentrant retirement: ${reenter})`, async () => {
    let resolveRead!: (result: IteratorResult<number>) => void;
    let resolveReturn!: (result: IteratorResult<number>) => void;
    let resolveQuery!: (value: number) => void;
    const read = new Promise<IteratorResult<number>>((resolve) => {
      resolveRead = resolve;
    });
    const returned = new Promise<IteratorResult<number>>((resolve) => {
      resolveReturn = resolve;
    });
    const queryResult = new Promise<number>((resolve) => {
      resolveQuery = resolve;
    });
    let returnCalls = 0;
    const stream = createStream({
      name: "reset-retirement.stream",
      key: () => "reset-retirement/stream",
      open: () => ({
        [Symbol.asyncIterator]: () => ({
          next: () => read,
          return: () => {
            returnCalls += 1;
            return returned;
          },
        }),
      }),
    });
    const query = createQuery({
      name: "reset-retirement.query",
      key: () => "reset-retirement/query",
      fetch: () => queryResult,
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "reset-retirement", operations: { stream, query } }),
    });
    const detachFailure = new Error("caller abort listener removal failed");
    const controller = new AbortController();
    let failRemoval = true;
    let reentrantRetirement: Promise<void> | undefined;
    let returnCallsDuringDetach = -1;
    const callerSignal = new Proxy(controller.signal, {
      get(target, property) {
        if (property === "removeEventListener")
          return (...args: Parameters<AbortSignal["removeEventListener"]>) => {
            if (failRemoval) {
              failRemoval = false;
              if (reenter) {
                reentrantRetirement = graph.retire();
                returnCallsDuringDetach = returnCalls;
              }
              throw detachFailure;
            }
            target.removeEventListener(...args);
          };
        const value: unknown = Reflect.get(target, property, target);
        return typeof value === "function" ? value.bind(target) : value;
      },
    });
    graph.api.stream(undefined);
    graph.api.query(undefined, { abortSignal: callerSignal });

    const unhandled: unknown[] = [];
    const onUnhandled = (reason: unknown): void => {
      unhandled.push(reason);
    };
    process.on("unhandledRejection", onUnhandled);
    let resetError: unknown;
    try {
      graph.reset();
    } catch (error) {
      resetError = error;
    }

    try {
      expect(resetError).toBe(detachFailure);
      expect(returnCalls).toBe(reenter ? 1 : 0);
      if (reenter) expect(returnCallsDuringDetach).toBe(0);
      const retirement = graph.retire();
      if (reenter) expect(reentrantRetirement).toBe(retirement);
      let settled = false;
      const result = retirement.then(
        () => {
          settled = true;
          return undefined;
        },
        (error: unknown) => {
          settled = true;
          return error;
        },
      );
      expect(returnCalls).toBe(1);
      await flush();
      expect(settled).toBe(false);
      resolveRead({ done: true, value: undefined });
      resolveQuery(1);
      await flush();
      expect(settled).toBe(false);
      resolveReturn({ done: true, value: undefined });
      expect(await result).toBe(detachFailure);
      expect(returnCalls).toBe(1);
      jest.useRealTimers();
      try {
        await new Promise<void>((resolve) => setTimeout(resolve, 0));
      } finally {
        jest.useFakeTimers();
        jest.setSystemTime(100);
      }
      expect(unhandled).toEqual([]);
    } finally {
      process.off("unhandledRejection", onUnhandled);
      resolveRead({ done: true, value: undefined });
      resolveReturn({ done: true, value: undefined });
      resolveQuery(1);
      graph.dispose();
    }
  });
}

it("preserves reset failure and completes remaining retirement cleanup when reporting throws", async () => {
  let resolveFirstRead!: (result: IteratorResult<number>) => void;
  let resolveSecondRead!: (result: IteratorResult<number>) => void;
  let resolveFirstReturn!: (result: IteratorResult<number>) => void;
  let resolveSecondReturn!: (result: IteratorResult<number>) => void;
  let resolveQuery!: (value: number) => void;
  const firstRead = new Promise<IteratorResult<number>>((resolve) => {
    resolveFirstRead = resolve;
  });
  const secondRead = new Promise<IteratorResult<number>>((resolve) => {
    resolveSecondRead = resolve;
  });
  const firstReturn = new Promise<IteratorResult<number>>((resolve) => {
    resolveFirstReturn = resolve;
  });
  const secondReturn = new Promise<IteratorResult<number>>((resolve) => {
    resolveSecondReturn = resolve;
  });
  const queryResult = new Promise<number>((resolve) => {
    resolveQuery = resolve;
  });
  let firstReturnCalls = 0;
  let secondReturnCalls = 0;
  const first = createStream({
    name: "reset-reporting.first",
    key: () => "reset-reporting/first",
    open: () => ({
      [Symbol.asyncIterator]: () => ({
        next: () => firstRead,
        return: () => {
          firstReturnCalls += 1;
          return firstReturn;
        },
      }),
    }),
  });
  const second = createStream({
    name: "reset-reporting.second",
    key: () => "reset-reporting/second",
    open: () => ({
      [Symbol.asyncIterator]: () => ({
        next: () => secondRead,
        return: () => {
          secondReturnCalls += 1;
          return secondReturn;
        },
      }),
    }),
  });
  const query = createQuery({
    name: "reset-reporting.query",
    key: () => "reset-reporting/query",
    fetch: () => queryResult,
  });
  const unobserved: unknown[] = [];
  const graph = createGraph({
    contract: defineContract({
      namespace: "reset-reporting",
      operations: { first, second, query },
    }),
    onUnobservedFault: (error) => {
      unobserved.push(error);
    },
  });
  const resetFailure = new Error("query listener removal failed");
  const reportingFailure = new Error("cleanup diagnostic failed");
  const cleanupFailure = new Proxy(
    {},
    {
      getPrototypeOf() {
        throw reportingFailure;
      },
    },
  );
  const streamController = new AbortController();
  let failStreamRemoval = true;
  const streamSignal = new Proxy(streamController.signal, {
    get(target, property) {
      if (property === "removeEventListener")
        return (...args: Parameters<AbortSignal["removeEventListener"]>) => {
          if (failStreamRemoval) {
            failStreamRemoval = false;
            throw cleanupFailure;
          }
          target.removeEventListener(...args);
        };
      const value: unknown = Reflect.get(target, property, target);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
  const queryController = new AbortController();
  let failQueryRemoval = true;
  let reentrantRetirement: Promise<void> | undefined;
  const querySignal = new Proxy(queryController.signal, {
    get(target, property) {
      if (property === "removeEventListener")
        return (...args: Parameters<AbortSignal["removeEventListener"]>) => {
          if (failQueryRemoval) {
            failQueryRemoval = false;
            reentrantRetirement = graph.retire();
            throw resetFailure;
          }
          target.removeEventListener(...args);
        };
      const value: unknown = Reflect.get(target, property, target);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
  graph.api.first(undefined, { abortSignal: streamSignal });
  graph.api.second(undefined);
  graph.api.query(undefined, { abortSignal: querySignal });

  const unhandled: unknown[] = [];
  const onUnhandled = (reason: unknown): void => {
    unhandled.push(reason);
  };
  process.on("unhandledRejection", onUnhandled);
  try {
    let resetError: unknown;
    try {
      graph.reset();
    } catch (error) {
      resetError = error;
    }
    expect(resetError).toBe(resetFailure);
    expect(firstReturnCalls).toBe(1);
    expect(secondReturnCalls).toBe(1);
    const retirement = graph.retire();
    expect(reentrantRetirement).toBe(retirement);
    let settled = false;
    const result = retirement.then(
      () => {
        settled = true;
        return undefined;
      },
      (error: unknown) => {
        settled = true;
        return error;
      },
    );
    await flush();
    expect(settled).toBe(false);
    resolveFirstRead({ done: true, value: undefined });
    resolveSecondRead({ done: true, value: undefined });
    resolveQuery(1);
    resolveFirstReturn({ done: true, value: undefined });
    await flush();
    expect(settled).toBe(false);
    resolveSecondReturn({ done: true, value: undefined });
    const failure = await result;
    expect(failure).toBeInstanceOf(AggregateError);
    const causes: unknown[] = (failure as AggregateError).errors;
    expect(causes.some((cause) => cause === resetFailure)).toBe(true);
    expect(causes.some((cause) => cause === cleanupFailure)).toBe(true);
    expect(causes).toHaveLength(2);
    expect(causes.some((cause) => cause === reportingFailure)).toBe(false);
    expect(unobserved.some((error) => error === cleanupFailure)).toBe(true);
    expect(firstReturnCalls).toBe(1);
    expect(secondReturnCalls).toBe(1);
    jest.useRealTimers();
    try {
      await new Promise<void>((resolve) => setTimeout(resolve, 0));
    } finally {
      jest.useFakeTimers();
      jest.setSystemTime(100);
    }
    expect(unhandled).toEqual([]);
  } finally {
    process.off("unhandledRejection", onUnhandled);
    resolveFirstRead({ done: true, value: undefined });
    resolveSecondRead({ done: true, value: undefined });
    resolveFirstReturn({ done: true, value: undefined });
    resolveSecondReturn({ done: true, value: undefined });
    resolveQuery(1);
    graph.dispose();
  }
});

async function expectRetirementPending(retirement: Promise<void>): Promise<void> {
  let settled = false;
  void retirement.then(
    () => {
      settled = true;
    },
    () => {
      settled = true;
    },
  );
  await flush();
  expect(settled).toBe(false);
}

it("allows retirement after a write-restricted first attempt", async () => {
  const graph = createGraph({
    contract: defineContract({ namespace: "retirement-admission", operations: {} }),
  });
  let refusedPromise: Promise<void> | undefined;
  let refusal: unknown;
  try {
    try {
      withoutWrites(() => {
        refusedPromise = graph.retire();
        void refusedPromise.catch(() => undefined);
      });
    } catch (error) {
      refusal = error;
    }
    expect(refusal).toBeInstanceOf(Fault);
    expect((refusal as Fault).kind).toBe("write-forbidden");
    expect(refusedPromise).toBeUndefined();
    const writable = graph.at("still-open");
    writable.set(1);
    expect(writable.value.get()).toBe(1);
    const retirement = graph.retire();
    expect(graph.retire()).toBe(retirement);
    await retirement;
  } finally {
    graph.dispose();
    await refusedPromise?.catch(() => undefined);
  }
});

it("does not resolve a stream key after option validation retires the graph", async () => {
  let retirement: Promise<void> | undefined;
  let keys = 0;
  const stream = createStream({
    name: "retirement.validation-stream",
    key: () => {
      keys += 1;
      return "retirement/validation-stream";
    },
    open: (): AsyncIterable<number> => ({
      [Symbol.asyncIterator]: () => ({ next: () => new Promise(() => undefined) }),
    }),
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "retirement-validation-stream", operations: { stream } }),
  });
  try {
    expect(() =>
      graph.api.stream(undefined, {
        get equals() {
          retirement ??= graph.retire();
          return Object.is;
        },
      }),
    ).toThrow();
    expect(retirement).toBeDefined();
    expect(keys).toBe(0);
    await retirement;
  } finally {
    graph.dispose();
  }
});

for (const trigger of ["options", "seed", "signal"] as const) {
  it(`does not open a stream retired during ${trigger} setup`, async () => {
    let retirement: Promise<void> | undefined;
    let keyResolved = false;
    let opens = 0;
    let removes = 0;
    const controller = new AbortController();
    const stream = createStream({
      name: "retirement.reentrant-stream",
      key: () => {
        keyResolved = true;
        return "retirement/reentrant-stream";
      },
      open: (): AsyncIterable<number> => {
        opens += 1;
        return { [Symbol.asyncIterator]: () => ({ next: () => new Promise(() => undefined) }) };
      },
    });
    const graph = createGraph({
      contract: defineContract({
        namespace: "retirement-reentrant-stream",
        operations: { stream },
      }),
    });
    const retire = (): void => {
      retirement ??= graph.retire();
    };
    const callerSignal = new Proxy(controller.signal, {
      get(target, property) {
        if (property === "addEventListener")
          return (...args: Parameters<AbortSignal["addEventListener"]>) => {
            target.addEventListener(...args);
            if (trigger === "signal") retire();
          };
        if (property === "removeEventListener")
          return (...args: Parameters<AbortSignal["removeEventListener"]>) => {
            removes += 1;
            target.removeEventListener(...args);
          };
        const value: unknown = Reflect.get(target, property, target);
        return typeof value === "function" ? value.bind(target) : value;
      },
    });
    const options = {
      ...(trigger === "seed" ? { initial: 7 } : {}),
      ...(trigger === "signal" ? { abortSignal: callerSignal } : {}),
      get equals() {
        if (trigger === "options" && keyResolved) retire();
        return Object.is;
      },
    };
    const store = graph.at("retirement/reentrant-stream");
    const stop =
      trigger === "seed"
        ? subscribe(store.value, (value) => {
            if (value === 7) retire();
          })
        : () => {};
    let callError: unknown;
    try {
      try {
        graph.api.stream(undefined, options);
      } catch (error) {
        callError = error;
      }
      expect(retirement).toBeDefined();
      expect(opens).toBe(0);
      if (trigger === "signal") expect(removes).toBe(1);
      if (retirement === undefined) throw new Error("Expected retirement during stream setup.");
      await retirement;
      expect(graph.retire()).toBe(retirement);
      if (callError !== undefined) expect(callError).toMatchObject({ kind: "disposed" });
    } finally {
      stop();
      graph.dispose();
    }
  });
}

it("does not resolve a query key after a freshness option retires the graph", async () => {
  let optionValidated = false;
  let retirement: Promise<void> | undefined;
  let keys = 0;
  const query = createQuery({
    name: "retirement.query-window",
    key: () => {
      keys += 1;
      return "retirement/query-window";
    },
    fetch: async () => 1,
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "retirement-query-window", operations: { query } }),
  });
  try {
    expect(() =>
      graph.api.query(undefined, {
        get revalidateAfterMs() {
          if (optionValidated) retirement ??= graph.retire();
          optionValidated = true;
          return 1;
        },
      }),
    ).toThrow();
    expect(retirement).toBeDefined();
    expect(keys).toBe(0);
    await retirement;
  } finally {
    graph.dispose();
  }
});

it("does not read query cancellation after value options retire the graph", async () => {
  let retirement: Promise<void> | undefined;
  let keyResolved = false;
  let readAfterRetirement = false;
  const query = createQuery({
    name: "retirement.query-options",
    key: () => {
      keyResolved = true;
      return "retirement/query-options";
    },
    fetch: async () => 1,
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "retirement-query-options", operations: { query } }),
  });
  try {
    expect(() =>
      graph.api.query(undefined, {
        get equals() {
          if (keyResolved) retirement ??= graph.retire();
          return Object.is;
        },
        get abortSignal() {
          if (retirement !== undefined) readAfterRetirement = true;
          return undefined;
        },
      }),
    ).toThrow();
    expect(retirement).toBeDefined();
    expect(readAfterRetirement).toBe(false);
    await retirement;
  } finally {
    graph.dispose();
  }
});

it("does not reread cancellation options after revalidation retires the graph", async () => {
  let retirement: Promise<void> | undefined;
  let readAfterRetirement = false;
  const controller = new AbortController();
  const query = createQuery({
    name: "retirement.revalidate-options",
    key: () => "retirement/revalidate-options",
    fetch: async () => 1,
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "retirement-revalidate-options", operations: { query } }),
  });
  try {
    const store = graph.api.query(undefined, { initial: 1 });
    expect(() =>
      store.revalidate({
        get abortSignal() {
          if (retirement !== undefined) readAfterRetirement = true;
          retirement ??= graph.retire();
          return controller.signal;
        },
      }),
    ).toThrow();
    expect(retirement).toBeDefined();
    expect(readAfterRetirement).toBe(false);
    await retirement;
  } finally {
    graph.dispose();
  }
});

for (const mode of ["query", "stream", "mutation", "revalidate"] as const) {
  it(`does not continue ${mode} signal validation after retirement`, async () => {
    let retirement: Promise<void> | undefined;
    let readAfterRetirement = false;
    const controller = new AbortController();
    const query = createQuery({
      name: "retirement.signal-validation",
      key: () => "retirement/signal-validation",
      fetch: async () => 1,
    });
    const stream = createStream({
      name: "retirement.signal-validation-stream",
      key: () => "retirement/signal-validation-stream",
      open: (): AsyncIterable<number> => ({
        [Symbol.asyncIterator]: () => ({ next: () => new Promise(() => undefined) }),
      }),
    });
    const mutate = createMutation({
      name: "retirement.signal-validation-mutation",
      affects: [],
      run: async () => 1,
    });
    const graph = createGraph({
      contract: defineContract({
        namespace: "retirement-signal-validation",
        operations: { query, stream, mutate },
      }),
    });
    const signal = new Proxy(controller.signal, {
      get(target, property) {
        if (retirement !== undefined) readAfterRetirement = true;
        if (property === "aborted") retirement ??= graph.retire();
        const value: unknown = Reflect.get(target, property, target);
        return typeof value === "function" ? value.bind(target) : value;
      },
    });
    try {
      const store = mode === "revalidate" ? graph.api.query(undefined, { initial: 1 }) : undefined;
      expect(() => {
        if (mode === "query") graph.api.query(undefined, { abortSignal: signal });
        else if (mode === "stream") graph.api.stream(undefined, { abortSignal: signal });
        else if (mode === "mutation") graph.api.mutate(undefined, { abortSignal: signal });
        else store!.revalidate({ abortSignal: signal });
      }).toThrow();
      expect(retirement).toBeDefined();
      expect(readAfterRetirement).toBe(false);
      await retirement;
    } finally {
      graph.dispose();
    }
  });
}

for (const owner of ["query", "stream", "projection", "mutation-select", "mutation-key"] as const) {
  it(`does not call a ${owner} key or selector after its getter retires the graph`, async () => {
    let retirement: Promise<void> | undefined;
    let armed = false;
    let calledAfterRetirement = false;
    const query = {
      kind: "query" as const,
      name: "retirement.key-query",
      get key() {
        if (armed && (owner === "query" || owner === "mutation-key")) retirement ??= graph.retire();
        return () => {
          if (retirement !== undefined) calledAfterRetirement = true;
          return "retirement/key-query";
        };
      },
      fetch: async () => 1,
    };
    const stream = {
      kind: "stream" as const,
      name: "retirement.key-stream",
      get key() {
        if (armed && (owner === "stream" || owner === "projection")) retirement ??= graph.retire();
        return () => {
          if (retirement !== undefined) calledAfterRetirement = true;
          return "retirement/key-stream";
        };
      },
      open: (): AsyncIterable<number> => ({
        [Symbol.asyncIterator]: () => ({ next: async () => ({ done: true, value: undefined }) }),
      }),
    };
    const effect = affects(query, {
      select: () => {
        if (retirement !== undefined) calledAfterRetirement = true;
        return undefined;
      },
    });
    const selectedEffect = new Proxy(effect, {
      get(target, property, receiver) {
        if (property === "select" && armed && owner === "mutation-select")
          retirement ??= graph.retire();
        return Reflect.get(target, property, receiver);
      },
    });
    const mutate = createMutation({
      name: "retirement.key-mutation",
      affects: [selectedEffect],
      run: async () => 1,
    });
    const graph = createGraph({
      contract: defineContract({
        namespace: "retirement-key-dispatch",
        operations: { query, stream, mutate },
      }),
    });
    try {
      armed = true;
      try {
        if (owner === "query") graph.api.query(undefined);
        else if (owner === "stream") graph.api.stream(undefined);
        else if (owner === "projection")
          project(graph, stream, undefined, { kind: "accumulate", max: 2 });
        else void graph.api.mutate(undefined).catch(() => undefined);
      } catch (error) {
        expect(error).toMatchObject({ kind: "disposed" });
      }
      expect(retirement).toBeDefined();
      expect(calledAfterRetirement).toBe(false);
      await retirement;
    } finally {
      graph.dispose();
    }
  });
}

for (const owner of ["query", "stream"] as const) {
  it(`does not configure a ${owner} store after its key callback retires the graph`, async () => {
    let retirement: Promise<void> | undefined;
    let configuredAfterRetirement = false;
    const query = {
      kind: "query" as const,
      name: "retirement.key-call-query",
      key() {
        if (owner === "query") retirement ??= graph.retire();
        return "retirement/key-call-query";
      },
      get equals() {
        if (retirement !== undefined) configuredAfterRetirement = true;
        return Object.is;
      },
      fetch: async () => 1,
    };
    const stream = {
      kind: "stream" as const,
      name: "retirement.key-call-stream",
      key() {
        if (owner === "stream") retirement ??= graph.retire();
        return "retirement/key-call-stream";
      },
      get equals() {
        if (retirement !== undefined) configuredAfterRetirement = true;
        return Object.is;
      },
      open: (): AsyncIterable<number> => ({
        [Symbol.asyncIterator]: () => ({ next: async () => ({ done: true, value: undefined }) }),
      }),
    };
    const graph = createGraph({
      contract: defineContract({ namespace: "retirement-key-call", operations: { query, stream } }),
    });
    try {
      expect(() => {
        if (owner === "query") graph.api.query(undefined);
        else graph.api.stream(undefined);
      }).toThrow();
      expect(retirement).toBeDefined();
      expect(configuredAfterRetirement).toBe(false);
      await retirement;
    } finally {
      graph.dispose();
    }
  });
}

it("does not dispatch a pre-aborted query after its fetch getter retires the graph", async () => {
  let retirement: Promise<void> | undefined;
  let armed = false;
  let dispatched = false;
  const controller = new AbortController();
  controller.abort();
  const query = {
    kind: "query" as const,
    name: "retirement.pre-aborted-dispatch",
    key: () => "retirement/pre-aborted-dispatch",
    get fetch() {
      if (armed) retirement ??= graph.retire();
      return async () => {
        dispatched = true;
        return 1;
      };
    },
  };
  const graph = createGraph({
    contract: defineContract({
      namespace: "retirement-pre-aborted-dispatch",
      operations: { query },
    }),
  });
  try {
    armed = true;
    try {
      graph.api.query(undefined, { abortSignal: controller.signal });
    } catch (error) {
      expect(error).toMatchObject({ kind: "disposed" });
    }
    expect(retirement).toBeDefined();
    expect(dispatched).toBe(false);
    await retirement;
  } finally {
    graph.dispose();
  }
});

for (const owner of ["query", "mutation", "stream", "projection"] as const) {
  it(`does not dispatch a ${owner} provider after its getter retires the graph`, async () => {
    let retirement: Promise<void> | undefined;
    let armed = false;
    let dispatched = false;
    const query = {
      kind: "query" as const,
      name: "retirement.dispatch-query",
      key: () => "retirement/dispatch-query",
      get fetch() {
        if (armed && owner === "query") retirement ??= graph.retire();
        return async () => {
          dispatched = true;
          return 1;
        };
      },
    };
    const mutate = {
      kind: "mutation" as const,
      name: "retirement.dispatch-mutation",
      affects: [],
      get run() {
        if (armed && owner === "mutation") retirement ??= graph.retire();
        return async () => {
          dispatched = true;
          return 1;
        };
      },
    };
    const stream = {
      kind: "stream" as const,
      name: "retirement.dispatch-stream",
      key: () => "retirement/dispatch-stream",
      get open() {
        if (armed && (owner === "stream" || owner === "projection")) retirement ??= graph.retire();
        return (): AsyncIterable<number> => {
          dispatched = true;
          return {
            [Symbol.asyncIterator]: () => ({
              next: async () => ({ done: true, value: undefined }),
            }),
          };
        };
      },
    };
    const graph = createGraph({
      contract: defineContract({
        namespace: "retirement-dispatch",
        operations: { query, mutate, stream },
      }),
    });
    try {
      armed = true;
      try {
        if (owner === "query") graph.api.query(undefined);
        else if (owner === "mutation") void graph.api.mutate(undefined).catch(() => undefined);
        else if (owner === "stream") graph.api.stream(undefined);
        else project(graph, stream, undefined, { kind: "accumulate", max: 2 });
      } catch (error) {
        expect(error).toMatchObject({ kind: "disposed" });
      }
      expect(retirement).toBeDefined();
      expect(dispatched).toBe(false);
      await retirement;
    } finally {
      graph.dispose();
    }
  });
}

for (const owner of ["query", "mutation", "stream", "projection"] as const) {
  it(`preserves the live ${owner} provider and iterator receivers`, async () => {
    const pulled = Promise.withResolvers<void>();
    const query = {
      kind: "query" as const,
      name: "retirement.receiver-query",
      key(this: unknown) {
        expect(this).toBe(query);
        return "retirement/receiver-query";
      },
      async fetch(this: unknown) {
        expect(this).toBe(query);
        return 1;
      },
    };
    const mutate = {
      kind: "mutation" as const,
      name: "retirement.receiver-mutation",
      affects: [],
      async run(this: unknown) {
        expect(this).toBe(mutate);
        return 1;
      },
    };
    const iterator: AsyncIterator<number> = {
      async next() {
        expect(this).toBe(iterator);
        pulled.resolve();
        return { done: true, value: undefined };
      },
    };
    const iterable: AsyncIterable<number> = {
      [Symbol.asyncIterator]() {
        expect(this).toBe(iterable);
        return iterator;
      },
    };
    const stream = {
      kind: "stream" as const,
      name: "retirement.receiver-stream",
      key(this: unknown) {
        expect(this).toBe(stream);
        return "retirement/receiver-stream";
      },
      open(this: unknown) {
        expect(this).toBe(stream);
        return iterable;
      },
    };
    const graph = createGraph({
      contract: defineContract({
        namespace: "retirement-receivers",
        operations: { query, mutate, stream },
      }),
    });
    try {
      if (owner === "query") await graph.api.query(undefined).ready;
      else if (owner === "mutation") await graph.api.mutate(undefined);
      else {
        if (owner === "stream") graph.api.stream(undefined);
        else project(graph, stream, undefined, { kind: "accumulate", max: 2 });
        await pulled.promise;
      }
      await graph.retire();
    } finally {
      graph.dispose();
    }
  });
}

it("preserves the live mutation selector, key, and optimistic map receivers", async () => {
  const query = {
    kind: "query" as const,
    name: "retirement.receiver-target",
    key(this: unknown) {
      expect(this).toBe(query);
      return "retirement/receiver-target";
    },
    fetch: async () => 1,
  };
  const effect = affects(query, {
    select(this: unknown) {
      expect(this).toBe(effect);
      return undefined;
    },
  });
  const mutate = createMutation({
    name: "retirement.receiver-effect",
    affects: [effect],
    run: async () => 1,
  });
  const graph = createGraph({
    contract: defineContract({
      namespace: "retirement-receiver-effect",
      operations: { query, mutate },
    }),
  });
  const optimistic = new Map();
  const originalGet = optimistic.get;
  optimistic.get = function (key) {
    expect(this).toBe(optimistic);
    return Reflect.apply(originalGet, this, [key]);
  };
  try {
    graph.api.query(undefined, { initial: 1 });
    await graph.api.mutate(undefined, { optimistic });
    await graph.retire();
  } finally {
    graph.dispose();
  }
});

for (const owner of ["stream", "projection"] as const) {
  it(`does not pull a ${owner} iterator after its next getter retires the graph`, async () => {
    let retirement: Promise<void> | undefined;
    let pulled = false;
    let closed = false;
    const stream = createStream({
      name: `retirement.pull-${owner}`,
      key: () => `retirement/pull-${owner}`,
      open: (): AsyncIterable<number> => ({
        [Symbol.asyncIterator]() {
          return {
            get next() {
              retirement ??= graph.retire();
              return async () => {
                pulled = true;
                return { done: true as const, value: undefined };
              };
            },
            async return() {
              closed = true;
              return { done: true as const, value: undefined };
            },
          };
        },
      }),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: `retirement-pull-${owner}`, operations: { stream } }),
    });
    try {
      try {
        if (owner === "stream") graph.api.stream(undefined);
        else project(graph, stream, undefined, { kind: "accumulate", max: 2 });
      } catch (error) {
        expect(error).toMatchObject({ kind: "disposed" });
      }
      expect(retirement).toBeDefined();
      expect(pulled).toBe(false);
      expect(closed).toBe(true);
      await retirement;
    } finally {
      graph.dispose();
    }
  });
}

for (const owner of ["stream", "projection"] as const) {
  it(`closes a late ${owner} iterator acquired after its factory getter retires the graph`, async () => {
    let retirement: Promise<void> | undefined;
    let acquired = false;
    let pulled = false;
    let closed = false;
    let settled = false;
    const returned = Promise.withResolvers<IteratorResult<number>>();
    const iterable: AsyncIterable<number> = {
      get [Symbol.asyncIterator]() {
        retirement ??= graph.retire();
        return function (this: AsyncIterable<number>) {
          expect(this).toBe(iterable);
          acquired = true;
          return {
            next: async () => {
              pulled = true;
              return { done: true as const, value: undefined };
            },
            return: () => {
              closed = true;
              return returned.promise;
            },
          };
        };
      },
    };
    const stream = createStream({
      name: `retirement.late-factory-${owner}`,
      key: () => `retirement/late-factory-${owner}`,
      open: () => iterable,
    });
    const graph = createGraph({
      contract: defineContract({
        namespace: `retirement-late-factory-${owner}`,
        operations: { stream },
      }),
    });
    try {
      try {
        if (owner === "stream") graph.api.stream(undefined);
        else project(graph, stream, undefined, { kind: "accumulate", max: 2 });
      } catch (error) {
        expect(error).toMatchObject({ kind: "disposed" });
      }
      expect(retirement).toBeDefined();
      expect(acquired).toBe(true);
      expect(pulled).toBe(false);
      expect(closed).toBe(true);
      void retirement?.then(() => {
        settled = true;
      });
      await Promise.resolve();
      expect(settled).toBe(false);
      returned.resolve({ done: true, value: undefined });
      await retirement;
      expect(settled).toBe(true);
    } finally {
      returned.resolve({ done: true, value: undefined });
      graph.dispose();
    }
  });
}

for (const phase of [
  "size",
  "iterator",
  "factory",
  "factory-invalid",
  "next",
  "pull",
  "entry",
] as const) {
  it(`stops nested optimistic map validation when ${phase} retires the graph`, async () => {
    let retirement: Promise<void> | undefined;
    let readAfterRetirement = false;
    let pulledAfterRetirement = false;
    let closed = false;
    let delivered = false;
    let dispatched = false;
    const mutate = createMutation({
      name: "retirement.optimistic-validation",
      affects: [],
      run: async () => {
        dispatched = true;
        return 1;
      },
    });
    const graph = createGraph({
      contract: defineContract({
        namespace: "retirement-optimistic-validation",
        operations: { mutate },
      }),
    });
    const optimistic = new Proxy(new Map(), {
      get(target, property) {
        if (retirement !== undefined) readAfterRetirement = true;
        if (property === "size" && phase === "size") retirement ??= graph.retire();
        if (property === Symbol.iterator && phase === "iterator") retirement ??= graph.retire();
        if (
          property === Symbol.iterator &&
          ["factory", "factory-invalid", "next", "pull", "entry"].includes(phase)
        )
          return () => {
            if (phase === "factory" || phase === "factory-invalid") retirement ??= graph.retire();
            if (phase === "factory-invalid") return 0;
            return {
              get next() {
                if (phase === "next") retirement ??= graph.retire();
                return () => {
                  if (retirement !== undefined) pulledAfterRetirement = true;
                  if (phase === "pull") retirement ??= graph.retire();
                  if (phase === "entry" && !delivered) {
                    delivered = true;
                    const entry = new Proxy(["retirement/key", () => 1], {
                      get(target, key, receiver) {
                        if (retirement !== undefined) readAfterRetirement = true;
                        if (key === "length") retirement ??= graph.retire();
                        return Reflect.get(target, key, receiver);
                      },
                    });
                    return { done: false, value: entry };
                  }
                  return { done: true, value: undefined };
                };
              },
              return() {
                closed = true;
                return { done: true, value: undefined };
              },
            };
          };
        const value: unknown = Reflect.get(target, property, target);
        return typeof value === "function" ? value.bind(target) : value;
      },
    });
    try {
      let failure: unknown;
      try {
        graph.api.mutate(undefined, { optimistic });
      } catch (error) {
        failure = error;
      }
      expect(failure).toBeDefined();
      if (phase === "factory-invalid") expect(failure).toMatchObject({ kind: "disposed" });
      expect(retirement).toBeDefined();
      expect(readAfterRetirement).toBe(false);
      expect(pulledAfterRetirement).toBe(false);
      if (["factory", "next", "pull", "entry"].includes(phase)) expect(closed).toBe(true);
      expect(dispatched).toBe(false);
      await retirement;
    } finally {
      graph.dispose();
    }
  });
}

it("does not invoke optimistic lookup after its getter retires the graph", async () => {
  let retirement: Promise<void> | undefined;
  let validationFinished = false;
  let lookupAfterRetirement = false;
  const query = createQuery({
    name: "retirement.optimistic-key",
    key: () => "retirement/optimistic-key",
    fetch: async () => 1,
  });
  const mutate = createMutation({
    name: "retirement.optimistic-lookup",
    affects: [affects(query, { select: () => undefined })],
    run: async () => 1,
  });
  const graph = createGraph({
    contract: defineContract({
      namespace: "retirement-optimistic-lookup",
      operations: { query, mutate },
    }),
  });
  const optimistic = new Proxy(new Map(), {
    get(target, property) {
      if (property === "get" && validationFinished) {
        retirement ??= graph.retire();
        return () => {
          lookupAfterRetirement = true;
          return undefined;
        };
      }
      if (property === Symbol.iterator)
        return () => {
          const iterator = target[Symbol.iterator]();
          return {
            next() {
              const result = iterator.next();
              if (result.done) validationFinished = true;
              return result;
            },
          };
        };
      const value: unknown = Reflect.get(target, property, target);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
  try {
    graph.api.query(undefined, { initial: 1 });
    try {
      void graph.api.mutate(undefined, { optimistic }).catch(() => undefined);
    } catch (error) {
      expect(error).toMatchObject({ kind: "disposed" });
    }
    expect(retirement).toBeDefined();
    expect(lookupAfterRetirement).toBe(false);
    await retirement;
  } finally {
    graph.dispose();
  }
});

for (const rollbackFails of [false, true]) {
  it(`removes an optimistic prediction when mutation signal registration throws (rollback fails: ${rollbackFails})`, async () => {
    const registrationError = new Error("registration failed");
    const rollbackError = new Error("rollback listener failed");
    const controller = new AbortController();
    let ran = false;
    let sawPrediction = false;
    let stop = (): void => undefined;
    const signal = new Proxy(controller.signal, {
      get(target, property) {
        if (property === "addEventListener")
          return (...args: Parameters<AbortSignal["addEventListener"]>) => {
            target.addEventListener(...args);
            throw registrationError;
          };
        const value: unknown = Reflect.get(target, property, target);
        return typeof value === "function" ? value.bind(target) : value;
      },
    });
    const query = createQuery({
      name: "retirement.registration-prediction-query",
      key: () => "retirement/registration-prediction",
      fetch: async () => 1,
    });
    const mutate = createMutation({
      name: "retirement.registration-prediction-mutation",
      affects: [affects(query, { optimistic: () => 2 })],
      run: async () => {
        ran = true;
      },
    });
    const graph = createGraph({
      contract: defineContract({
        namespace: "retirement-registration-prediction",
        operations: { query, mutate },
      }),
    });
    try {
      const store = graph.api.query(undefined);
      await store.ready;
      expect(store.value.get()).toBe(1);
      stop = subscribe(store.value, (value) => {
        if (value === 2) sawPrediction = true;
        else if (value === 1 && sawPrediction && rollbackFails) throw rollbackError;
      });
      expect(() => graph.api.mutate(undefined, { abortSignal: signal })).toThrow(registrationError);
      expect(ran).toBe(false);
      expect(store.value.get()).toBe(1);
      stop();
      if (rollbackFails) await expect(graph.retire()).rejects.toBe(rollbackError);
      else await graph.retire();
    } finally {
      stop();
      graph.dispose();
    }
  });
}

for (const owner of ["query", "mutation", "stream"] as const) {
  it(`does not register a ${owner} listener after its getter retires the graph`, async () => {
    let retirement: Promise<void> | undefined;
    let validated = false;
    let registeredAfterRetirement = false;
    const controller = new AbortController();
    const signal = new Proxy(controller.signal, {
      get(target, property) {
        if (property === "addEventListener") {
          if (validated) retirement ??= graph.retire();
          return (...args: Parameters<AbortSignal["addEventListener"]>) => {
            if (retirement !== undefined) registeredAfterRetirement = true;
            target.addEventListener(...args);
          };
        }
        if (property === "throwIfAborted") validated = true;
        const value: unknown = Reflect.get(target, property, target);
        return typeof value === "function" ? value.bind(target) : value;
      },
    });
    const query = createQuery({
      name: "retirement.registration-getter-query",
      key: () => "retirement/registration-getter",
      fetch: async () => 1,
    });
    const mutate = createMutation({
      name: "retirement.registration-getter-mutation",
      affects: [],
      run: async () => 1,
    });
    const stream = createStream({
      name: "retirement.registration-getter-stream",
      key: () => "retirement/registration-getter-stream",
      open: (): AsyncIterable<number> => ({
        [Symbol.asyncIterator]: () => ({ next: async () => ({ done: true, value: undefined }) }),
      }),
    });
    const graph = createGraph({
      contract: defineContract({
        namespace: "retirement-registration-getter",
        operations: { query, mutate, stream },
      }),
    });
    try {
      try {
        if (owner === "query") graph.api.query(undefined, { abortSignal: signal });
        else if (owner === "mutation")
          void graph.api.mutate(undefined, { abortSignal: signal }).catch(() => undefined);
        else graph.api.stream(undefined, { abortSignal: signal });
      } catch (error) {
        expect(error).toMatchObject({ kind: "disposed" });
      }
      expect(retirement).toBeDefined();
      expect(registeredAfterRetirement).toBe(false);
      await retirement;
    } finally {
      graph.dispose();
    }
  });
}

for (const owner of ["query", "mutation", "stream"] as const) {
  for (const removalFails of [false, true]) {
    it(`owns ${owner} listener cleanup when registration attaches then throws (removal fails: ${removalFails})`, async () => {
      const registrationError = new Error("registration failed");
      const removalError = new Error("removal failed");
      let retirement: Promise<void> | undefined;
      const controller = new AbortController();
      let removed = false;
      const signal = new Proxy(controller.signal, {
        get(target, property) {
          if (property === "addEventListener")
            return (...args: Parameters<AbortSignal["addEventListener"]>) => {
              target.addEventListener(...args);
              if (removalFails) retirement ??= graph.retire();
              throw registrationError;
            };
          if (property === "removeEventListener")
            return (...args: Parameters<AbortSignal["removeEventListener"]>) => {
              target.removeEventListener(...args);
              removed = true;
              if (removalFails) throw removalError;
            };
          const value: unknown = Reflect.get(target, property, target);
          return typeof value === "function" ? value.bind(target) : value;
        },
      });
      const query = createQuery({
        name: "retirement.registration-query",
        key: () => "retirement/registration",
        fetch: async () => 1,
      });
      const mutate = createMutation({
        name: "retirement.registration-mutation",
        affects: [],
        run: async () => 1,
      });
      const stream = createStream({
        name: "retirement.registration-stream",
        key: () => "retirement/registration-stream",
        open: (): AsyncIterable<number> => ({
          [Symbol.asyncIterator]: () => ({ next: async () => ({ done: true, value: undefined }) }),
        }),
      });
      const graph = createGraph({
        contract: defineContract({
          namespace: "retirement-registration",
          operations: { query, mutate, stream },
        }),
      });
      try {
        let failure: unknown;
        try {
          if (owner === "query") graph.api.query(undefined, { abortSignal: signal });
          else if (owner === "mutation") void graph.api.mutate(undefined, { abortSignal: signal });
          else graph.api.stream(undefined, { abortSignal: signal });
        } catch (error) {
          failure = error;
        }
        expect(failure).toBe(registrationError);
        expect(removed).toBe(true);
        if (removalFails) await expect(retirement ?? graph.retire()).rejects.toBe(removalError);
        else await graph.retire();
      } finally {
        graph.dispose();
      }
    });
  }
}

it("removes query cancellation when retirement interrupts listener registration", async () => {
  let retirement: Promise<void> | undefined;
  let fetches = 0;
  let removes = 0;
  const controller = new AbortController();
  const query = createQuery({
    name: "retirement.reentrant-query",
    key: () => "retirement/reentrant-query",
    fetch: () => {
      fetches += 1;
      return new Promise<number>(() => undefined);
    },
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "retirement-reentrant-query", operations: { query } }),
  });
  const callerSignal = new Proxy(controller.signal, {
    get(target, property) {
      if (property === "addEventListener")
        return (...args: Parameters<AbortSignal["addEventListener"]>) => {
          target.addEventListener(...args);
          retirement ??= graph.retire();
        };
      if (property === "removeEventListener")
        return (...args: Parameters<AbortSignal["removeEventListener"]>) => {
          removes += 1;
          target.removeEventListener(...args);
        };
      const value: unknown = Reflect.get(target, property, target);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
  try {
    try {
      graph.api.query(undefined, { abortSignal: callerSignal });
    } catch (error) {
      expect(error).toMatchObject({ kind: "disposed" });
    }
    expect(retirement).toBeDefined();
    expect(fetches).toBe(0);
    expect(removes).toBe(1);
    await retirement;
  } finally {
    graph.dispose();
  }
});

it("does not resolve a mutation target after its selector retires the graph", async () => {
  let retirement: Promise<void> | undefined;
  let keys = 0;
  let runs = 0;
  const query = createQuery({
    name: "retirement.reentrant-target",
    key: () => {
      keys += 1;
      return "retirement/reentrant-target";
    },
    fetch: async () => 1,
  });
  const mutate = createMutation({
    name: "retirement.reentrant-mutation",
    affects: [
      affects(query, {
        select: () => {
          retirement ??= graph.retire();
          return undefined;
        },
      }),
    ],
    run: async () => {
      runs += 1;
      return 1;
    },
  });
  const graph = createGraph({
    contract: defineContract({
      namespace: "retirement-reentrant-mutation",
      operations: { mutate, query },
    }),
  });
  try {
    const result = graph.api.mutate(undefined);
    void result.catch(() => undefined);
    expect(retirement).toBeDefined();
    expect(keys).toBe(0);
    expect(runs).toBe(0);
    await result.catch(() => undefined);
    await retirement;
  } finally {
    graph.dispose();
  }
});

for (const owner of ["stream", "projection"] as const) {
  for (const cleanup of ["getter", "held"] as const) {
    it(`owns a late ${owner} iterator cleanup (${cleanup})`, async () => {
      const failure = new Error("late iterator return getter failed");
      const returned = Promise.withResolvers<IteratorResult<number>>();
      let retirement: Promise<void> | undefined;
      let returns = 0;
      let reads = 0;
      const stream = createStream({
        name: `retirement.late-iterator.${owner}.${cleanup}`,
        key: () => `retirement/late-iterator/${owner}/${cleanup}`,
        open: (): AsyncIterable<number> => ({
          [Symbol.asyncIterator]() {
            retirement = graph.retire();
            return {
              next: () => {
                reads += 1;
                return new Promise<IteratorResult<number>>(() => undefined);
              },
              get return() {
                if (cleanup === "getter") throw failure;
                return () => {
                  returns += 1;
                  return returned.promise;
                };
              },
            };
          },
        }),
      });
      const graph = createGraph({
        contract: defineContract({ namespace: "retirement-late-iterator", operations: { stream } }),
      });
      let callError: unknown;
      try {
        try {
          if (owner === "stream") graph.api.stream(undefined);
          else project(graph, stream, undefined, { kind: "accumulate", max: 2 });
        } catch (error) {
          callError = error;
        }
        expect(retirement).toBeDefined();
        const owned = retirement!;
        expect(graph.retire()).toBe(owned);
        expect(reads).toBe(0);
        if (cleanup === "getter") {
          await expect(owned).rejects.toBe(failure);
        } else {
          expect(returns).toBe(1);
          await expectRetirementPending(owned);
          returned.resolve({ done: true, value: undefined });
          await owned;
          expect(returns).toBe(1);
        }
        if (callError !== undefined) expect(callError).toMatchObject({ kind: "disposed" });
      } finally {
        returned.resolve({ done: true, value: undefined });
        graph.dispose();
      }
    });
  }
}

for (const owner of ["stream", "projection"] as const) {
  it(`records one return getter failure during graph retirement (${owner})`, async () => {
    const failure = new Error(`${owner} return getter failed`);
    const pull = Promise.withResolvers<IteratorResult<number>>();
    const started = Promise.withResolvers<void>();
    let getterCalls = 0;
    const stream = createStream({
      name: `retirement.getter.${owner}`,
      key: () => `retirement-getter/${owner}`,
      open: (): AsyncIterable<number> => ({
        [Symbol.asyncIterator]() {
          return {
            next: () => {
              started.resolve();
              return pull.promise;
            },
            get return(): never {
              getterCalls += 1;
              throw failure;
            },
          };
        },
      }),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "retirement-getter", operations: { stream } }),
    });
    try {
      if (owner === "stream") graph.api.stream(undefined);
      else project(graph, stream, undefined, { kind: "accumulate", max: 2 });
      await started.promise;
      const retirement = graph.retire();
      const outcome = retirement.then(
        () => undefined,
        (error: unknown) => error,
      );
      expect(getterCalls).toBe(1);
      await expectRetirementPending(retirement);
      pull.resolve({ done: true, value: undefined });
      expect(await outcome).toBe(failure);
      expect(getterCalls).toBe(1);
    } finally {
      pull.resolve({ done: true, value: undefined });
      graph.dispose();
    }
  });
}

for (const scenario of ["distinct", "same", "report"] as const) {
  it(`settles retirement after two cleanup failures (${scenario})`, async () => {
    const firstFailure = new Error("first return rejection");
    const secondFailure = scenario === "same" ? firstFailure : new Error("second return rejection");
    const reportFailure = new Error("cleanup report construction failed");
    const reads = [
      Promise.withResolvers<IteratorResult<number>>(),
      Promise.withResolvers<IteratorResult<number>>(),
    ];
    const returns = [
      Promise.withResolvers<IteratorResult<number>>(),
      Promise.withResolvers<IteratorResult<number>>(),
    ];
    const started = [Promise.withResolvers<void>(), Promise.withResolvers<void>()];
    const order: number[] = [];
    const stream = createStream({
      name: "retirement.ordered-stream",
      key: (input: number) => `retirement-ordered/${input}`,
      open: (input): AsyncIterable<number> => ({
        [Symbol.asyncIterator]() {
          return {
            next: () => {
              started[input]!.resolve();
              return reads[input]!.promise;
            },
            return: () => {
              order.push(input);
              return returns[input]!.promise;
            },
          };
        },
      }),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "retirement-ordered", operations: { stream } }),
    });
    try {
      graph.api.stream(0);
      graph.api.stream(1);
      await Promise.all(started.map((entry) => entry.promise));
      const retirement = graph.retire();
      expect(graph.retire()).toBe(retirement);
      let settlements = 0;
      let rejection: unknown;
      const outcome = retirement.then(
        () => {
          settlements += 1;
          return undefined;
        },
        (error: unknown) => {
          settlements += 1;
          rejection = error;
          return error;
        },
      );
      expect(order).toEqual([0, 1]);
      returns[1]!.reject(secondFailure);
      reads[1]!.resolve({ done: true, value: undefined });
      reads[0]!.resolve({ done: true, value: undefined });
      await expectRetirementPending(retirement);
      if (scenario === "report") {
        const descriptor = Object.getOwnPropertyDescriptor(globalThis, "AggregateError");
        if (descriptor === undefined) throw new Error("AggregateError is unavailable");
        let reportCalls = 0;
        try {
          Object.defineProperty(globalThis, "AggregateError", {
            ...descriptor,
            value: function () {
              reportCalls += 1;
              throw reportFailure;
            },
          });
          returns[0]!.reject(firstFailure);
          await flush();
          await flush();
        } finally {
          Object.defineProperty(globalThis, "AggregateError", descriptor);
        }
        expect(reportCalls).toBeGreaterThan(0);
        expect(settlements).toBe(1);
        expect(rejection).toBe(reportFailure);
      } else {
        returns[0]!.reject(firstFailure);
        const error = await outcome;
        expect(error).toBeInstanceOf(AggregateError);
        const causes: unknown[] = (error as AggregateError).errors;
        expect(causes).toHaveLength(2);
        expect(causes[0]).toBe(firstFailure);
        expect(causes[1]).toBe(secondFailure);
      }
      expect(graph.retire()).toBe(retirement);
      await flush();
      expect(settlements).toBe(1);
      expect(order).toEqual([0, 1]);
    } finally {
      for (const read of reads) read.resolve({ done: true, value: undefined });
      for (const pending of returns) pending.resolve({ done: true, value: undefined });
      graph.dispose();
    }
  });
}

it("waits for held mutation output after graph retirement", async () => {
  const original = Promise.withResolvers<string>();
  let signal: AbortSignal | undefined;
  const mutation = createMutation({
    name: "retirement.held-mutation",
    affects: [],
    run: (_input: void, options) => {
      signal = options.abortSignal;
      return original.promise;
    },
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "retirement-mutation", operations: { mutation } }),
  });
  try {
    const operation = graph.api.mutation();
    const retirement = graph.retire();
    expect(signal?.aborted).toBe(true);
    await expectRetirementPending(retirement);
    original.resolve("completed");
    await expect(operation).resolves.toBe("completed");
    await retirement;
  } finally {
    original.resolve("completed");
    graph.dispose();
  }
});

it("joins an independent projection pull and return during graph retirement", async () => {
  const pull = Promise.withResolvers<IteratorResult<number>>();
  const returned = Promise.withResolvers<IteratorResult<number>>();
  const started = Promise.withResolvers<void>();
  let returns = 0;
  const stream = createStream({
    name: "retirement.projection",
    key: () => "retirement/projection",
    open: (): AsyncIterable<number> => ({
      [Symbol.asyncIterator]() {
        return {
          next: () => {
            started.resolve();
            return pull.promise;
          },
          return: () => {
            returns += 1;
            return returned.promise;
          },
        };
      },
    }),
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "retirement-projection", operations: { stream } }),
  });
  try {
    const projection = project(graph, stream, undefined, { kind: "accumulate", max: 2 });
    await started.promise;
    const retirement = graph.retire();
    expect(returns).toBe(1);
    expect(projection.status.get()).toBe("closed");
    await expectRetirementPending(retirement);
    returned.resolve({ done: true, value: undefined });
    await expectRetirementPending(retirement);
    pull.resolve({ done: true, value: undefined });
    await retirement;
    expect(returns).toBe(1);
  } finally {
    pull.resolve({ done: true, value: undefined });
    returned.resolve({ done: true, value: undefined });
    graph.dispose();
  }
});

it.each(["Error", "undefined"] as const)(
  "resumes prior graph disposal with a %s lifetime-removal failure and reentrant retirement",
  async (failureKind) => {
    const failure = new Error("lifetime listener removal failed");
    const cause = failureKind === "Error" ? failure : undefined;
    const original = Promise.withResolvers<number>();
    const projectionPull = Promise.withResolvers<IteratorResult<number>>();
    const projectionReturn = Promise.withResolvers<IteratorResult<number>>();
    const pullStarted = Promise.withResolvers<void>();
    const lifetime = new AbortController();
    let retireDuringRemoval: (() => Promise<void>) | undefined;
    let reentrantRetirement: Promise<void> | undefined;
    let removalCalls = 0;
    let fetchCalls = 0;
    let queryAbortCalls = 0;
    let returnCalls = 0;
    let querySignal: AbortSignal | undefined;
    const abortSignal: AbortSignal = {
      get aborted() {
        return lifetime.signal.aborted;
      },
      get reason() {
        return lifetime.signal.reason;
      },
      get onabort() {
        return lifetime.signal.onabort;
      },
      set onabort(value) {
        lifetime.signal.onabort = value;
      },
      throwIfAborted() {
        lifetime.signal.throwIfAborted();
      },
      addEventListener(
        type: Parameters<AbortSignal["addEventListener"]>[0],
        listener: Parameters<AbortSignal["addEventListener"]>[1],
        options?: Parameters<AbortSignal["addEventListener"]>[2],
      ) {
        lifetime.signal.addEventListener(type, listener, options);
      },
      removeEventListener(
        type: Parameters<AbortSignal["removeEventListener"]>[0],
        listener: Parameters<AbortSignal["removeEventListener"]>[1],
        options?: Parameters<AbortSignal["removeEventListener"]>[2],
      ) {
        lifetime.signal.removeEventListener(type, listener, options);
        if (type !== "abort") return;
        removalCalls += 1;
        if (removalCalls === 1) reentrantRetirement = retireDuringRemoval?.();
        throw cause;
      },
      dispatchEvent(event) {
        return lifetime.signal.dispatchEvent(event);
      },
    };
    const query = createQuery({
      name: "prior-disposal.query",
      key: () => "prior-disposal/query",
      fetch: (_input: void, options) => {
        fetchCalls += 1;
        querySignal = options.abortSignal;
        options.abortSignal.addEventListener("abort", () => {
          queryAbortCalls += 1;
        });
        return original.promise;
      },
    });
    const stream = createStream({
      name: "prior-disposal.projection",
      key: () => "prior-disposal/projection",
      open: (): AsyncIterable<number> => ({
        [Symbol.asyncIterator]() {
          return {
            next: () => {
              pullStarted.resolve();
              return projectionPull.promise;
            },
            return: () => {
              returnCalls += 1;
              return projectionReturn.promise;
            },
          };
        },
      }),
    });
    const graph = createGraph({
      contract: defineContract({
        namespace: "prior-disposal-retirement",
        operations: { query, stream },
      }),
      abortSignal,
    });
    retireDuringRemoval = () => graph.retire();
    let readySettlements = 0;
    let ready: Promise<void> | undefined;
    try {
      const store = graph.api.query();
      ready = store.ready.then(
        () => {
          readySettlements += 1;
        },
        () => {
          readySettlements += 1;
        },
      );
      const projection = project(graph, stream, undefined, { kind: "accumulate", max: 2 });
      await pullStarted.promise;

      let disposalThrew = false;
      let disposalCause: unknown;
      try {
        graph.dispose();
      } catch (caught) {
        disposalThrew = true;
        disposalCause = caught;
      }
      expect(disposalThrew).toBe(true);
      expect(disposalCause).toBe(cause);

      const retirement = graph.retire();
      expect(reentrantRetirement).toBe(retirement);
      const outcome = retirement.then(
        () => ({ kind: "fulfilled" as const }),
        (rejection: unknown) => ({ kind: "rejected" as const, cause: rejection }),
      );
      expect(graph.retire()).toBe(retirement);

      expect(removalCalls).toBe(1);
      expect(fetchCalls).toBe(1);
      expect(querySignal?.aborted).toBe(true);
      expect(queryAbortCalls).toBe(1);
      expect(returnCalls).toBe(1);
      expect(projection.status.get()).toBe("closed");
      try {
        graph.api.query();
        throw new Error("Expected graph admission to be closed");
      } catch (admissionCause) {
        if (!(admissionCause instanceof Fault)) throw admissionCause;
        expect(admissionCause.kind).toBe("disposed");
      }
      await ready;
      expect(readySettlements).toBe(1);
      await expectRetirementPending(retirement);

      projectionReturn.resolve({ done: true, value: undefined });
      await expectRetirementPending(retirement);
      projectionPull.resolve({ done: true, value: undefined });
      await expectRetirementPending(retirement);
      original.resolve(42);

      const result = await outcome;
      expect(result.kind).toBe("rejected");
      if (result.kind !== "rejected") throw new Error("Expected the retained cleanup failure");
      expect(result.cause).toBe(cause);
      expect(removalCalls).toBe(1);
      expect(returnCalls).toBe(1);
    } finally {
      original.resolve(42);
      projectionPull.resolve({ done: true, value: undefined });
      projectionReturn.resolve({ done: true, value: undefined });
      try {
        graph.dispose();
      } catch {
        // Retirement retains the lifetime-removal cause.
      }
      await graph.retire().then(
        () => undefined,
        () => undefined,
      );
      await ready;
    }
  },
);

it("joins original queries after cancellation and prior void graph disposal", async () => {
  const backend = testBackend();
  const query = createQuery({
    name: "disposed-retirement.query",
    key: (input: string) => `disposed-retirement/${input}`,
    fetch: backend.respond<string, number>("disposed-retirement.query"),
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "disposed-retirement", operations: { query } }),
  });
  const cancelledReady = graph.api.query("cancelled").ready.then(
    () => undefined,
    () => undefined,
  );
  const cancelledCall = backend.calls[0]!;
  let disposedCall: (typeof backend.calls)[number] | undefined;
  let cancelledResolved = false;
  let disposedResolved = false;
  try {
    graph.cancelPending();
    const disposedReady = graph.api.query("disposed").ready.then(
      () => undefined,
      () => undefined,
    );
    disposedCall = backend.calls[1]!;
    graph.dispose();
    const retirement = graph.retire();
    await Promise.all([cancelledReady, disposedReady]);
    expect(cancelledCall.aborted).toBe(true);
    expect(disposedCall!.aborted).toBe(true);
    await expectRetirementPending(retirement);
    backend.resolve(disposedCall!, 84);
    disposedResolved = true;
    await expectRetirementPending(retirement);
    backend.resolve(cancelledCall, 42);
    cancelledResolved = true;
    await retirement;
  } finally {
    if (!cancelledResolved) backend.resolve(cancelledCall, 42);
    if (!disposedResolved && disposedCall !== undefined) backend.resolve(disposedCall, 84);
    graph.dispose();
  }
});
