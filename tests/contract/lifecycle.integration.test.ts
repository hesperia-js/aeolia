import { afterEach, beforeEach, expect, it, jest } from "bun:test";
import {
  computed,
  createGraph,
  createQuery,
  createStream,
  createMutation,
  affects,
  bindOperation,
  createBindingContext,
  createTransferScope,
  prepareTransferOperation,
  prepareTransferOperationRetirement,
  preparePassiveTransferClaim,
  prepareTransferScopeSuccessors,
  defineContract,
  project,
  subscribe,
  watch,
  Fault,
  withoutWrites,
  captureReactiveConstruction,
  prepareRequiredTransferCapture,
} from "../../src/index.ts";
import type { ClosedEncodedValue, RequiredTransferCodec } from "../../src/index.ts";
import { testBackend } from "../../src/testing.ts";

let ownsFakeTimers = false;
beforeEach(() => {
  jest.useFakeTimers();
  ownsFakeTimers = true;
  jest.setSystemTime(100);
});
afterEach(() => {
  if (ownsFakeTimers) jest.clearAllTimers();
  jest.useRealTimers();
  ownsFakeTimers = false;
});
async function flush(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
}

async function expectPending(promise: Promise<unknown>): Promise<void> {
  jest.useRealTimers();
  ownsFakeTimers = false;
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    const result = await Promise.race([
      promise.then(
        () => "settled",
        () => "settled",
      ),
      new Promise<"pending">((resolve) => {
        timer = setTimeout(() => resolve("pending"), 0);
      }),
    ]);
    expect(result).toBe("pending");
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}

async function expectSettled(promise: Promise<unknown>): Promise<void> {
  jest.useRealTimers();
  ownsFakeTimers = false;
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    const result = await Promise.race([
      promise.then(
        () => "settled",
        () => "rejected",
      ),
      new Promise<"pending">((resolve) => {
        timer = setTimeout(() => resolve("pending"), 0);
      }),
    ]);
    expect(result).toBe("settled");
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}

async function nativeResolveThenConstructorReads(): Promise<number> {
  const input = Promise.withResolvers<unknown>();
  const speciesHolder = { [Symbol.species]: Promise };
  let reads = 0;
  Object.defineProperty(input.promise, "constructor", {
    configurable: true,
    get() {
      reads += 1;
      return reads === 1 ? Promise : speciesHolder;
    },
  });
  const normalized = Reflect.apply(Promise.resolve, Promise, [input.promise]) as Promise<unknown>;
  const carrier = Reflect.apply(Promise.prototype.then, normalized, [
    (value: unknown) => value,
  ]) as Promise<unknown>;
  input.resolve("native control");
  await carrier;
  return reads;
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

it("removes a lifetime listener that attaches after reentrant disposal", async () => {
  const controller = new AbortController();
  const originalAdd = controller.signal.addEventListener.bind(controller.signal);
  const originalRemove = controller.signal.removeEventListener.bind(controller.signal);
  const attached = new Set<EventListenerOrEventListenerObject>();
  const removalPhases: string[] = [];
  let addCalls = 0;
  let reentered = false;
  const abortSignal = new Proxy(controller.signal, {
    get(target, property) {
      if (property === "addEventListener")
        return (...args: Parameters<AbortSignal["addEventListener"]>) => {
          const [type, listener] = args;
          if (type === "abort" && listener != null) {
            addCalls += 1;
            if (!reentered) {
              reentered = true;
              const event = new Event("abort");
              if (typeof listener === "function") listener.call(target, event);
              else listener.handleEvent(event);
            }
            originalAdd(...args);
            attached.add(listener);
            return;
          }
          return originalAdd(...args);
        };
      if (property === "removeEventListener")
        return (...args: Parameters<AbortSignal["removeEventListener"]>) => {
          const [type, listener] = args;
          if (type === "abort" && listener != null) {
            removalPhases.push(attached.has(listener) ? "after attach" : "before attach");
            attached.delete(listener);
          }
          return originalRemove(...args);
        };
      const value: unknown = Reflect.get(target, property, target);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
  const query = createQuery({
    name: "reentrant-lifetime.query",
    key: () => "reentrant-lifetime/one",
    fetch: async () => 1,
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "reentrant-lifetime", operations: { query } }),
    abortSignal,
  });
  try {
    expect(reentered).toBe(true);
    expect(addCalls).toBe(1);
    expect(removalPhases).toEqual(["before attach", "after attach"]);
    expect(attached.size).toBe(0);
    expect(() => graph.api.query()).toThrow(Fault);
    const retirement = graph.retire();
    expect(graph.retire()).toBe(retirement);
    await retirement;
  } finally {
    graph.dispose();
  }
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
      const reason = new Error("request ended");

      graph.cancelPending(reason);

      expect(backend.calls[0]!.aborted).toBe(true);
      const previousReason = signals[0]!.reason;
      if (preAborted) expect(previousReason).toMatchObject({ name: "AbortError" });
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
    await expectPending(retirement);
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

for (const trigger of ["retire", "throw", "invalid-entry"] as const) {
  for (const closeFailureAt of ["getter", "call"] as const) {
    for (const cleanupCause of [new Error("iterator close failed"), undefined]) {
      it(`retains ${closeFailureAt} cleanup failure ${String(cleanupCause)} after ${trigger}`, async () => {
        const semanticCause = new Error("iterator next failed");
        let retirement: Promise<void> | undefined;
        let closeReads = 0;
        let closeCalls = 0;
        let pulls = 0;
        let dispatches = 0;
        const mutate = createMutation({
          name: "retirement.optimistic-close",
          affects: [],
          run: async () => {
            dispatches += 1;
          },
        });
        const graph = createGraph({
          contract: defineContract({
            namespace: "retirement-optimistic-close",
            operations: { mutate },
          }),
        });
        const optimistic = new Proxy(new Map(), {
          get(target, property) {
            if (property === Symbol.iterator)
              return () => ({
                get next() {
                  if (trigger === "retire") {
                    retirement = graph.retire();
                    void retirement.catch(() => undefined);
                  }
                  if (trigger === "throw") throw semanticCause;
                  return () => {
                    pulls += 1;
                    return { done: false, value: ["invalid"] };
                  };
                },
                get return() {
                  closeReads += 1;
                  if (closeFailureAt === "getter") throw cleanupCause;
                  return () => {
                    closeCalls += 1;
                    throw cleanupCause;
                  };
                },
              });
            const value: unknown = Reflect.get(target, property, target);
            return typeof value === "function" ? value.bind(target) : value;
          },
        });
        try {
          let semanticFailure: unknown;
          let semanticThrew = false;
          try {
            void graph.api.mutate(undefined, { optimistic });
          } catch (error) {
            semanticThrew = true;
            semanticFailure = error;
          }
          expect(semanticThrew).toBe(true);
          if (trigger === "retire") expect(semanticFailure).toMatchObject({ kind: "disposed" });
          if (trigger === "throw") expect(semanticFailure).toBe(semanticCause);
          if (trigger === "invalid-entry") expect(semanticFailure).toBeInstanceOf(TypeError);
          expect(pulls).toBe(trigger === "invalid-entry" ? 1 : 0);
          expect(dispatches).toBe(0);
          expect(closeReads).toBe(1);
          expect(closeCalls).toBe(closeFailureAt === "call" ? 1 : 0);
          const result = await (retirement ?? graph.retire()).then(
            () => ({ kind: "fulfilled" as const }),
            (cause: unknown) => ({ kind: "rejected" as const, cause }),
          );
          expect(result.kind).toBe("rejected");
          if (result.kind === "rejected") expect(result.cause).toBe(cleanupCause);
        } finally {
          graph.dispose();
        }
      });
    }
  }
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
it("retires a context skipped by prior write-restricted graph disposal", async () => {
  const graph = createGraph({
    contract: defineContract({ namespace: "retirement-skipped-context", operations: {} }),
  });
  const context = createBindingContext(graph);
  let disposalThrew = false;
  let disposalCause: unknown;
  let retirementOutcome:
    | Promise<
        { readonly kind: "fulfilled" } | { readonly kind: "rejected"; readonly cause: unknown }
      >
    | undefined;
  try {
    expect(context.active.get()).toBe(true);
    try {
      withoutWrites(() => graph.dispose());
    } catch (cause) {
      disposalThrew = true;
      disposalCause = cause;
    }
    expect(disposalThrew).toBe(true);
    expect(context.active.get()).toBe(true);

    const retirement = graph.retire();
    retirementOutcome = retirement.then(
      () => ({ kind: "fulfilled" as const }),
      (cause: unknown) => ({ kind: "rejected" as const, cause }),
    );
    expect(graph.retire()).toBe(retirement);
    expect(context.active.get()).toBe(false);

    const outcome = retirementOutcome;
    if (outcome === undefined) throw new Error("Expected the cached retirement outcome");
    const result = await outcome;
    expect(result.kind).toBe("rejected");
    if (result.kind !== "rejected") throw new Error("Expected the retained disposal failure");
    expect(result.cause).toBe(disposalCause);
  } finally {
    try {
      context.dispose();
    } finally {
      graph.dispose();
      if (retirementOutcome !== undefined) await retirementOutcome;
    }
  }
});

it("joins the final mutation output adoption after abort-listener removal", async () => {
  const backend = testBackend();
  const finalization = Promise.withResolvers<void>();
  const listenerRemoved = Promise.withResolvers<void>();
  const controller = new AbortController();
  const nativeAdd = controller.signal.addEventListener.bind(controller.signal);
  const nativeRemove = controller.signal.removeEventListener.bind(controller.signal);
  let linkedListener: EventListenerOrEventListenerObject | null = null;
  let providerRead = false;
  let finalOutputAdoption = false;
  const signal = new Proxy(controller.signal, {
    get(target, property) {
      if (property === "addEventListener")
        return (
          type: string,
          listener: Parameters<typeof nativeAdd>[1],
          options?: Parameters<typeof nativeAdd>[2],
        ) => {
          nativeAdd(type, listener, options);
          if (type === "abort") linkedListener = listener;
        };
      if (property === "removeEventListener")
        return (
          type: string,
          listener: Parameters<typeof nativeRemove>[1],
          options?: Parameters<typeof nativeRemove>[2],
        ) => {
          nativeRemove(type, listener, options);
          if (type === "abort" && listener === linkedListener) {
            finalOutputAdoption = true;
            listenerRemoved.resolve();
          }
        };
      const value = Reflect.get(target, property, target);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
  const providerResult = Object.defineProperty({}, "then", {
    get() {
      if (!providerRead) {
        providerRead = true;
        return undefined;
      }
      if (!finalOutputAdoption) return undefined;
      return (resolve: (value: string) => void, reject: (reason: unknown) => void) => {
        finalization.promise.then(() => resolve("finalized"), reject);
      };
    },
  });
  const save = createMutation({
    name: "retirement-output.save",
    affects: [],
    run: backend.perform<void, unknown>("retirement-output.save"),
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "retirement-output", operations: { save } }),
  });
  let call: (typeof backend.calls)[number] | undefined;
  let providerResolved = false;
  try {
    const running = graph.api.save(undefined, { abortSignal: signal });
    call = backend.pending("retirement-output.save")[0]!;
    backend.resolve(call, providerResult);
    providerResolved = true;
    expect(providerRead).toBe(true);
    await expectSettled(listenerRemoved.promise);
    expect(finalOutputAdoption).toBe(true);

    const retirement = graph.retire();
    await expectPending(running);
    await expectPending(retirement);

    finalization.resolve();
    await expect(running).resolves.toBe("finalized");
    await retirement;
    expect(backend.calls.map((entry) => entry.name)).toEqual(["retirement-output.save"]);
  } finally {
    if (!providerResolved && call !== undefined) backend.resolve(call, providerResult);
    finalization.resolve();
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

it.each(["stateful constructor and species", "custom then observer"] as const)(
  "keeps retirement unacknowledged for a %s carrier",
  async (mode) => {
    const expectedConstructorReads = await nativeResolveThenConstructorReads();
    const provider = Promise.withResolvers<unknown>();
    const legacyCarrier = Promise.withResolvers<unknown>();
    const callbackSettled = Promise.withResolvers<void>();
    let callbackResult: unknown;
    let callbackInput: unknown;
    let callbackCalls = 0;
    let observerCalls = 0;
    let constructorReads = 0;
    let releaseLegacy: (() => void) | undefined;
    const heldSpecies = function (
      executor: (resolve: (value: unknown) => void, reject: (cause: unknown) => void) => void,
    ): Promise<unknown> {
      executor(
        (value) => {
          callbackResult = value;
          releaseLegacy = () => legacyCarrier.resolve(value);
          callbackSettled.resolve();
        },
        (cause) => {
          callbackResult = cause;
          releaseLegacy = () => legacyCarrier.reject(cause);
          callbackSettled.resolve();
        },
      );
      return legacyCarrier.promise;
    };
    const speciesHolder = {
      [Symbol.species]: mode === "stateful constructor and species" ? heldSpecies : Promise,
    };
    Object.defineProperty(provider.promise, "constructor", {
      configurable: true,
      get() {
        constructorReads += 1;
        if (constructorReads === 1) return Promise;
        if (constructorReads === 2) return speciesHolder;
        throw new Error(
          "Promise carrier constructor was read beyond native normalization and then",
        );
      },
    });
    const nativeThen = Promise.prototype.then;
    let customThenResult: string | undefined;
    if (mode === "custom then observer")
      Object.defineProperty(provider.promise, "then", {
        configurable: true,
        value: function (
          this: Promise<unknown>,
          onFulfilled: ((value: unknown) => unknown) | null | undefined,
          onRejected: ((cause: unknown) => unknown) | null | undefined,
        ): Promise<unknown> {
          observerCalls += 1;
          if (typeof onFulfilled !== "function")
            throw new Error("Expected the public mutation success continuation");
          const callbackCarrier = Reflect.apply(nativeThen, this, [
            (value: unknown) => {
              callbackCalls += 1;
              callbackInput = value;
              return onFulfilled(value);
            },
            onRejected,
          ]) as Promise<unknown>;
          void Reflect.apply(nativeThen, callbackCarrier, [
            (value: unknown) => {
              callbackResult = value;
              callbackSettled.resolve();
            },
            (cause: unknown) => {
              callbackResult = cause;
              callbackSettled.resolve();
            },
          ]);
          customThenResult = "independent custom result";
          return legacyCarrier.promise;
        },
      });
    const save = createMutation({
      name: `retirement.ambiguous.${mode}`,
      affects: [],
      run: () => provider.promise,
    });
    const graph = createGraph({
      contract: defineContract({
        namespace: `retirement-ambiguous-${mode}`,
        operations: { save },
      }),
    });
    let providerResolved = false;
    try {
      const running = graph.api.save(undefined);
      expect(running).toBe(legacyCarrier.promise);
      expect(constructorReads).toBe(expectedConstructorReads);
      expect(observerCalls).toBe(mode === "custom then observer" ? 1 : 0);

      const retirement = graph.retire();
      await expectPending(retirement);
      provider.resolve("provider result");
      providerResolved = true;
      await callbackSettled.promise;
      if (mode === "custom then observer") {
        expect(callbackInput).toBe("provider result");
        expect(callbackCalls).toBe(1);
        expect(callbackResult).toBe("provider result");
        releaseLegacy = () => legacyCarrier.resolve(customThenResult);
      } else {
        expect(callbackResult).toBe("provider result");
      }
      await expectPending(running);
      await expectPending(retirement);

      if (releaseLegacy === undefined)
        throw new Error("Expected the legacy carrier to retain its native completion");
      releaseLegacy();
      const expectedResult = mode === "custom then observer" ? customThenResult : "provider result";
      await expect(running).resolves.toBe(expectedResult);
      await expectPending(retirement);
    } finally {
      if (!providerResolved) provider.resolve("provider result");
      if (releaseLegacy === undefined) legacyCarrier.resolve("cleanup result");
      else releaseLegacy();
      graph.dispose();
    }
  },
);

it("keeps retirement pending after post-bootstrap Promise descriptor changes", async () => {
  const provider = Promise.withResolvers<unknown>();
  const control = Promise.withResolvers<unknown>();
  const originalConstructor = Object.getOwnPropertyDescriptor(Promise.prototype, "constructor");
  const originalSpecies = Object.getOwnPropertyDescriptor(Promise, Symbol.species);
  if (originalConstructor === undefined || originalSpecies === undefined)
    throw new Error("Expected native Promise constructor and species descriptors");
  const nativeResolve = Promise.resolve;
  const nativeThen = Promise.prototype.then;
  const reads = {
    beforeRun: { constructor: 0, species: 0 },
    baseline: { constructor: 0, species: 0 },
    operation: { constructor: 0, species: 0 },
  };
  let phase: "beforeRun" | "baseline" | "operation" = "beforeRun";
  let runCalls = 0;
  const note = (kind: "constructor" | "species"): void => {
    reads[phase][kind] += 1;
  };
  let baselineCarrier: Promise<unknown> | undefined;
  let running: Promise<unknown> | undefined;
  let baselineResolved = false;
  let providerResolved = false;
  const save = createMutation({
    name: "retirement.descriptor-mutation",
    affects: [],
    run: () => {
      runCalls += 1;
      phase = "operation";
      return provider.promise;
    },
  });
  const graph = createGraph({
    contract: defineContract({
      namespace: "retirement-descriptor-mutation",
      operations: { save },
    }),
  });
  try {
    try {
      Object.defineProperty(Promise.prototype, "constructor", {
        configurable: true,
        enumerable: originalConstructor.enumerable,
        get() {
          note("constructor");
          return Promise;
        },
      });
      Object.defineProperty(Promise, Symbol.species, {
        configurable: true,
        enumerable: originalSpecies.enumerable,
        get() {
          note("species");
          return Promise;
        },
      });

      phase = "beforeRun";
      running = graph.api.save(undefined);

      phase = "baseline";
      const normalizedControl = Reflect.apply(nativeResolve, Promise, [
        control.promise,
      ]) as Promise<unknown>;
      baselineCarrier = Reflect.apply(nativeThen, normalizedControl, [
        (value: unknown) => value,
      ]) as Promise<unknown>;
    } finally {
      Object.defineProperty(Promise.prototype, "constructor", originalConstructor);
      Object.defineProperty(Promise, Symbol.species, originalSpecies);
    }

    expect(reads.beforeRun).toEqual({ constructor: 0, species: 0 });
    expect(reads.operation.constructor).toBe(reads.baseline.constructor);
    expect(reads.operation.species).toBe(reads.baseline.species);
    expect(runCalls).toBe(1);
    if (running === undefined || baselineCarrier === undefined)
      throw new Error("Expected both native continuation carriers");

    const retirement = graph.retire();
    await expectPending(running);
    await expectPending(retirement);
    control.resolve("baseline result");
    baselineResolved = true;
    await expect(baselineCarrier).resolves.toBe("baseline result");
    provider.resolve("legacy descriptor result");
    providerResolved = true;
    await expect(running).resolves.toBe("legacy descriptor result");
    await expectPending(retirement);
  } finally {
    if (!baselineResolved) control.resolve("baseline result");
    if (!providerResolved) provider.resolve("legacy descriptor result");
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
    await expectPending(retirement);
    backend.resolve(disposedCall!, 84);
    disposedResolved = true;
    await expectPending(retirement);
    backend.resolve(cancelledCall, 42);
    cancelledResolved = true;
    await retirement;
  } finally {
    if (!cancelledResolved) backend.resolve(cancelledCall, 42);
    if (!disposedResolved && disposedCall !== undefined) backend.resolve(disposedCall, 84);
    graph.dispose();
  }
});

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

it("refuses retirement in the same write-restricted scope as a failed disposal", async () => {
  const graph = createGraph({
    contract: defineContract({ namespace: "retirement-disposal-retry", operations: {} }),
  });
  const context = createBindingContext(graph);
  let disposalCause: unknown;
  let retirementCause: unknown;
  let refusedPromise: Promise<void> | undefined;
  try {
    withoutWrites(() => {
      try {
        graph.dispose();
      } catch (cause) {
        disposalCause = cause;
      }
      try {
        refusedPromise = graph.retire();
        void refusedPromise.catch(() => undefined);
      } catch (cause) {
        retirementCause = cause;
      }
    });
    expect(disposalCause).toBeInstanceOf(Fault);
    expect((disposalCause as Fault).kind).toBe("write-forbidden");
    expect(retirementCause).toBeInstanceOf(Fault);
    expect((retirementCause as Fault).kind).toBe("write-forbidden");
    expect(refusedPromise).toBeUndefined();
    expect(context.active.get()).toBe(true);

    const retirement = graph.retire();
    expect(graph.retire()).toBe(retirement);
    expect(context.active.get()).toBe(false);
    await expect(retirement).rejects.toBe(disposalCause);
  } finally {
    context.dispose();
    graph.dispose();
    await refusedPromise?.catch(() => undefined);
  }
});

it("keeps retirement unstarted before and after disposal, then cleans up skipped contexts", async () => {
  const graph = createGraph({
    contract: defineContract({ namespace: "retirement-precondition", operations: {} }),
  });
  const openContext = createBindingContext(graph);
  let preDisposeRefusedRetirement: Promise<void> | undefined;
  let refusal: unknown;
  try {
    try {
      withoutWrites(() => {
        preDisposeRefusedRetirement = graph.retire();
      });
    } catch (cause) {
      refusal = cause;
    }
    void preDisposeRefusedRetirement?.catch(() => undefined);
    expect(refusal).toBeInstanceOf(Fault);
    expect((refusal as Fault).kind).toBe("write-forbidden");
    expect(preDisposeRefusedRetirement).toBeUndefined();
    expect(openContext.active.get()).toBe(true);
    const writable = graph.at("still-open");
    writable.set(1);
    expect(writable.value.get()).toBe(1);
    const retirement = graph.retire();
    expect(graph.retire()).toBe(retirement);
    expect(openContext.active.get()).toBe(false);
    await retirement;
  } finally {
    try {
      openContext.dispose();
    } finally {
      graph.dispose();
      await preDisposeRefusedRetirement?.catch(() => undefined);
    }
  }

  const priorGraph = createGraph({
    contract: defineContract({
      namespace: "retirement-prior-disposal-precondition",
      operations: {},
    }),
  });
  const context = createBindingContext(priorGraph);
  let disposalThrew = false;
  let disposalCause: unknown;
  let refusedRetirement: Promise<void> | undefined;
  let retirementRefusal: unknown;
  let retirement: Promise<void> | undefined;
  let retirementOutcome:
    | Promise<
        { readonly kind: "fulfilled" } | { readonly kind: "rejected"; readonly cause: unknown }
      >
    | undefined;
  try {
    try {
      withoutWrites(() => priorGraph.dispose());
    } catch (cause) {
      disposalThrew = true;
      disposalCause = cause;
    }
    expect(disposalThrew).toBe(true);
    if (!(disposalCause instanceof Fault))
      throw new Error("Expected the first write restriction fault");
    expect(disposalCause.kind).toBe("write-forbidden");
    expect(context.active.get()).toBe(true);
    expect(() => priorGraph.at("closed")).toThrow("disposed");

    try {
      withoutWrites(() => {
        refusedRetirement = priorGraph.retire();
      });
    } catch (cause) {
      retirementRefusal = cause;
    }
    void refusedRetirement?.catch(() => undefined);
    expect(retirementRefusal).toBeInstanceOf(Fault);
    expect((retirementRefusal as Fault).kind).toBe("write-forbidden");
    expect(refusedRetirement).toBeUndefined();
    expect(context.active.get()).toBe(true);
    expect(() => priorGraph.at("still-closed")).toThrow("disposed");

    retirement = priorGraph.retire();
    retirementOutcome = retirement.then(
      () => ({ kind: "fulfilled" as const }),
      (cause: unknown) => ({ kind: "rejected" as const, cause }),
    );
    expect(priorGraph.retire()).toBe(retirement);
    expect(context.active.get()).toBe(false);

    const outcome = retirementOutcome;
    if (outcome === undefined) throw new Error("Expected the cached retirement outcome");
    const result = await outcome;
    expect(result.kind).toBe("rejected");
    if (result.kind !== "rejected") throw new Error("Expected the retained disposal failure");
    expect(result.cause).toBe(disposalCause);
  } finally {
    try {
      context.dispose();
    } finally {
      priorGraph.dispose();
      await refusedRetirement?.catch(() => undefined);
      if (retirementOutcome !== undefined) await retirementOutcome;
    }
  }
});

it.each(["single Error", "single undefined", "multiple"] as const)(
  "retains cleanup causes in attempt order for %s after graph-owned work finishes",
  async (mode) => {
    const originalAggregate = new AggregateError([new Error("nested cause")], "first cleanup");
    const secondFailure = new Error("second cleanup");
    const failures: unknown[] =
      mode === "single Error"
        ? [new Error("iterator cleanup failed")]
        : mode === "single undefined"
          ? [undefined]
          : [originalAggregate, secondFailure];
    const activeCount = mode === "multiple" ? 2 : 1;
    const pulls = [
      Promise.withResolvers<IteratorResult<number>>(),
      Promise.withResolvers<IteratorResult<number>>(),
    ];
    const cleanups = [
      Promise.withResolvers<IteratorResult<number>>(),
      Promise.withResolvers<IteratorResult<number>>(),
    ];
    const pullStarted = [Promise.withResolvers<void>(), Promise.withResolvers<void>()];
    const returnStarted = [Promise.withResolvers<void>(), Promise.withResolvers<void>()];
    const returnOrder: number[] = [];
    const stream = (index: number) =>
      createStream({
        name: `retirement.cleanup.${index}`,
        key: () => `retirement-cleanup/${index}`,
        open: (): AsyncIterable<number> => ({
          [Symbol.asyncIterator]() {
            return {
              next: () => {
                pullStarted[index]!.resolve();
                return pulls[index]!.promise;
              },
              return: () => {
                returnOrder.push(index);
                returnStarted[index]!.resolve();
                return cleanups[index]!.promise;
              },
            };
          },
        }),
      });
    const first = stream(0);
    const second = stream(1);
    const graph = createGraph({
      contract: defineContract({
        namespace: "retirement-cleanup",
        operations: { first, second },
      }),
    });
    try {
      graph.api.first(undefined);
      if (activeCount === 2) graph.api.second(undefined);
      await Promise.all(pullStarted.slice(0, activeCount).map((started) => started.promise));
      graph.dispose();
      await Promise.all(returnStarted.slice(0, activeCount).map((started) => started.promise));
      expect(returnOrder).toEqual(activeCount === 2 ? [0, 1] : [0]);

      const retirement = graph.retire();
      expect(graph.retire()).toBe(retirement);
      const outcome = retirement.then(
        () => ({ kind: "fulfilled" as const }),
        (cause: unknown) => ({ kind: "rejected" as const, cause }),
      );
      if (activeCount === 2) {
        cleanups[1]!.reject(failures[1]);
        await expectPending(retirement);
        cleanups[0]!.reject(failures[0]);
      } else {
        cleanups[0]!.reject(failures[0]);
      }
      await expectPending(retirement);
      for (let index = activeCount - 1; index >= 0; index -= 1)
        pulls[index]!.resolve({ done: true, value: undefined });

      const result = await outcome;
      expect(result.kind).toBe("rejected");
      if (result.kind !== "rejected") throw new Error("Expected cleanup rejection");
      if (activeCount === 2) {
        if (!(result.cause instanceof AggregateError))
          throw new Error("Expected multiple cleanup causes to be aggregated");
        expect(result.cause.errors).toHaveLength(2);
        expect(result.cause.errors[0]).toBe(originalAggregate);
        expect(result.cause.errors[1]).toBe(secondFailure);
      } else {
        expect(result.cause).toBe(failures[0]);
      }
      expect(graph.retire()).toBe(retirement);
    } finally {
      for (const pull of pulls) pull.resolve({ done: true, value: undefined });
      for (const cleanup of cleanups) cleanup.resolve({ done: true, value: undefined });
      graph.dispose();
    }
  },
);

it("retires one binding context without cancelling or joining other scopes", async () => {
  type Scope = "first" | "second" | "raw";
  type Input = { readonly scope: Scope };
  const gates: Record<Scope, ReturnType<typeof Promise.withResolvers<number>>> = {
    first: Promise.withResolvers<number>(),
    second: Promise.withResolvers<number>(),
    raw: Promise.withResolvers<number>(),
  };
  const signals: Partial<Record<Scope, AbortSignal>> = {};
  const query = createQuery({
    name: "context-retirement.query",
    key: (input: Input) => input.scope,
    fetch: (input: Input, options) => {
      signals[input.scope] = options.abortSignal;
      return gates[input.scope].promise;
    },
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "context-retirement", operations: { query } }),
  });
  const firstContext = createBindingContext(graph);
  const secondContext = createBindingContext(graph);
  const first = bindOperation(graph, query, {
    context: firstContext,
    profile: {},
    fetch: query.fetch,
  });
  const second = bindOperation(graph, query, {
    context: secondContext,
    profile: {},
    fetch: query.fetch,
  });
  const firstReady = first.member({ scope: "first" }).ready.then(
    () => undefined,
    () => undefined,
  );
  const secondReady = second.member({ scope: "second" }).ready.then(
    () => undefined,
    () => undefined,
  );
  const rawReady = graph.api.query({ scope: "raw" }).ready.then(
    () => undefined,
    () => undefined,
  );
  try {
    expect(signals.first).toBeDefined();
    expect(signals.second).toBeDefined();
    expect(signals.raw).toBeDefined();
    firstContext.dispose();
    const retirement = firstContext.retire();
    expect(signals.first!.aborted).toBe(true);
    expect(signals.second!.aborted).toBe(false);
    expect(signals.raw!.aborted).toBe(false);
    await expectPending(retirement);
    gates.first.resolve(1);
    await expectSettled(retirement);
  } finally {
    gates.first.resolve(1);
    gates.second.resolve(2);
    gates.raw.resolve(3);
    await Promise.all([firstReady, secondReady, rawReady]);
    graph.dispose();
  }
});

it("drains later contexts when context.active reenters graph retirement and joins each original", async () => {
  type Scope = "first" | "second";
  type Input = { readonly scope: Scope };
  const originals: Record<Scope, ReturnType<typeof Promise.withResolvers<number>>> = {
    first: Promise.withResolvers<number>(),
    second: Promise.withResolvers<number>(),
  };
  const signals: Partial<Record<Scope, AbortSignal>> = {};
  const aborts: Record<Scope, number> = { first: 0, second: 0 };
  const closures: Record<Scope, number> = { first: 0, second: 0 };
  const query = createQuery({
    name: "context-phase-retirement.query",
    key: (input: Input) => input.scope,
    fetch: (input: Input, options) => {
      signals[input.scope] = options.abortSignal;
      options.abortSignal.addEventListener("abort", () => {
        aborts[input.scope] += 1;
      });
      return originals[input.scope].promise;
    },
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "context-phase-retirement", operations: { query } }),
  });
  const firstContext = createBindingContext(graph);
  const secondContext = createBindingContext(graph);
  let retirement: Promise<void> | undefined;
  let retirementOutcome:
    | Promise<
        { readonly kind: "fulfilled" } | { readonly kind: "rejected"; readonly cause: unknown }
      >
    | undefined;
  let firstReady: Promise<void> | undefined;
  let secondReady: Promise<void> | undefined;
  let stopFirst = (): void => undefined;
  let stopSecond = (): void => undefined;
  const startRetirement = (): void => {
    if (retirement !== undefined) return;
    retirement = graph.retire();
    retirementOutcome = retirement.then(
      () => ({ kind: "fulfilled" as const }),
      (cause: unknown) => ({ kind: "rejected" as const, cause }),
    );
  };
  try {
    const first = bindOperation(graph, query, {
      context: firstContext,
      profile: {},
      fetch: query.fetch,
    });
    const second = bindOperation(graph, query, {
      context: secondContext,
      profile: {},
      fetch: query.fetch,
    });
    firstReady = first.member({ scope: "first" }).ready.then(
      () => undefined,
      () => undefined,
    );
    secondReady = second.member({ scope: "second" }).ready.then(
      () => undefined,
      () => undefined,
    );
    stopFirst = subscribe(firstContext.active, (active) => {
      if (active) return;
      closures.first += 1;
      startRetirement();
    });
    stopSecond = subscribe(secondContext.active, (active) => {
      if (!active) closures.second += 1;
    });
    if (signals.first === undefined || signals.second === undefined)
      throw new Error("Expected both context queries to start");

    graph.dispose();

    const retirementPromise = retirement;
    const outcome = retirementOutcome;
    if (retirementPromise === undefined || outcome === undefined)
      throw new Error("Expected context closure to reenter graph retirement");
    expect(graph.retire()).toBe(retirementPromise);
    expect(firstContext.active.get()).toBe(false);
    expect(secondContext.active.get()).toBe(false);
    expect(closures).toEqual({ first: 1, second: 1 });
    expect(aborts).toEqual({ first: 1, second: 1 });
    expect(signals.first.aborted).toBe(true);
    expect(signals.second.aborted).toBe(true);
    await Promise.all([firstReady, secondReady]);
    await expectPending(retirementPromise);

    originals.first.resolve(1);
    await expectPending(retirementPromise);
    originals.second.resolve(2);
    const result = await outcome;
    expect(result.kind).toBe("fulfilled");
    if (result.kind !== "fulfilled") throw new Error("Expected both originals to retire cleanly");
  } finally {
    originals.first.resolve(1);
    originals.second.resolve(2);
    try {
      graph.dispose();
    } finally {
      stopFirst();
      stopSecond();
      const finalRetirement = retirement ?? graph.retire();
      await finalRetirement.then(
        () => undefined,
        () => undefined,
      );
      if (firstReady !== undefined) await firstReady;
      if (secondReady !== undefined) await secondReady;
    }
  }
});

it("keeps overlapping mutation coverage until both original carriers retire", async () => {
  const backend = testBackend();
  const query = createQuery({
    name: "transfer-overlap.query",
    key: () => "transfer-overlap/one",
    fetch: backend.respond<void, number>("transfer-overlap.query"),
  });
  const mutate = createMutation({
    name: "transfer-overlap.mutate",
    run: backend.perform<object, void>("transfer-overlap.mutate"),
    affects: [],
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "transfer-overlap", operations: { query, mutate } }),
  });
  const codec: RequiredTransferCodec = {
    encodeValue: (value) => value as ClosedEncodedValue,
    encodeFailure: (cause) => String(cause),
    decodeValue: (value) => value,
    decodeFailure: (failure) => new Error(String(failure)),
  };
  try {
    const store = graph.api.query();
    const ready = store.ready;
    backend.resolve(backend.calls[0]!, 1);
    await ready;
    const value = computed(() => store.value.get());
    expect(value.get()).toBe(1);
    const construction = captureReactiveConstruction(value);
    const captureOptions = {
      graphs: [graph],
      roots: [{ source: value, construction }],
      codec,
    } as const;
    const scope = createTransferScope(graph, graph.api.query, store);
    const firstInput = Object.freeze({ request: "first" });
    const secondInput = Object.freeze({ request: "second" });
    const firstOperation = prepareTransferOperation(
      [scope],
      [{ role: "raw", source: graph.api.mutate, input: firstInput }],
    );
    const firstCommit = firstOperation.seal();
    if (firstCommit === undefined) throw new Error("The first operation did not seal");
    firstCommit.publish();
    firstCommit.notifications.deliver();
    const secondOperation = prepareTransferOperation(
      [scope],
      [{ role: "raw", source: graph.api.mutate, input: secondInput }],
    );
    const secondCommit = secondOperation.seal();
    if (secondCommit === undefined) throw new Error("The overlapping operation did not seal");
    secondCommit.publish();
    secondCommit.notifications.deliver();

    const firstResult = graph.api.mutate(firstInput);
    const secondResult = graph.api.mutate(secondInput);
    const firstCall = backend.calls[1]!;
    const secondCall = backend.calls[2]!;
    expect(firstCall.input).toBe(firstInput);
    expect(secondCall.input).toBe(secondInput);
    expect(() => prepareRequiredTransferCapture(captureOptions)).toThrow(TypeError);

    backend.resolve(firstCall, undefined);
    backend.resolve(secondCall, undefined);
    await Promise.all([firstResult, secondResult]);

    const firstRetirement = prepareTransferOperationRetirement(firstOperation.operation);
    const firstRetirementCommit = firstRetirement.seal();
    if (firstRetirementCommit === undefined)
      throw new Error("The first operation retirement did not seal");
    firstRetirementCommit.publish();
    firstRetirementCommit.notifications.deliver();
    await firstRetirement.retired;
    expect(() => prepareRequiredTransferCapture(captureOptions)).toThrow(TypeError);

    const secondRetirement = prepareTransferOperationRetirement(secondOperation.operation);
    const secondRetirementCommit = secondRetirement.seal();
    if (secondRetirementCommit === undefined)
      throw new Error("The second operation retirement did not seal");
    secondRetirementCommit.publish();
    secondRetirementCommit.notifications.deliver();
    await secondRetirement.retired;

    const availableCapture = prepareRequiredTransferCapture(captureOptions);
    availableCapture.discard();
    await availableCapture.retired;
  } finally {
    graph.dispose();
  }
});

it("retains predecessor and successor address coverage for the same original operation until explicit retirement", async () => {
  type QueryInput = { readonly key: string };
  type MutationInput = { readonly request: string };
  type CommitPort = {
    readonly publish: () => void;
    readonly notifications: { readonly deliver: () => void };
  };
  type PublicationPort = {
    readonly retired: Promise<void>;
    readonly seal: () => CommitPort | undefined;
    readonly discard: () => void;
  };
  type HeldPublication = {
    readonly publication: PublicationPort;
    phase: "pending" | "sealed" | "published" | "discarded";
    commit: CommitPort | undefined;
    sealAttempted: boolean;
    deliveryAttempted: boolean;
    joined: boolean;
  };
  type PassiveClaim = ReturnType<typeof preparePassiveTransferClaim>;
  type HeldClaim = {
    readonly claim: PassiveClaim;
    discarded: boolean;
    joined: boolean;
  };
  type RefusalCheck =
    | { readonly failed: false }
    | { readonly failed: true; readonly error: unknown };

  const backend = testBackend();
  const queryName = "transfer-successor.query";
  const mutationName = "transfer-successor.mutate";
  const respondQuery = backend.respond<QueryInput, number>(queryName);
  let queryFetchCalls = 0;
  const fetchQuery = (
    input: QueryInput,
    options: Parameters<typeof respondQuery>[1],
  ): Promise<number> => {
    queryFetchCalls += 1;
    return respondQuery(input, options);
  };
  const performMutation = backend.perform<MutationInput, void>(mutationName);
  let mutationDispatches = 0;
  const runMutation = (
    input: MutationInput,
    options: Parameters<typeof performMutation>[1],
  ): Promise<void> => {
    mutationDispatches += 1;
    return performMutation(input, options);
  };
  const query = createQuery({
    name: queryName,
    key: (input: QueryInput) => input.key,
    fetch: fetchQuery,
  });
  const mutation = createMutation({
    name: mutationName,
    run: runMutation,
    affects: [],
  });
  const graph = createGraph({
    contract: defineContract({
      namespace: "transfer-successor",
      operations: { query, mutation },
    }),
  });

  const publications: HeldPublication[] = [];
  const claims: HeldClaim[] = [];
  const cleanupFailures: unknown[] = [];
  let contextAForCleanup: { readonly retire: () => Promise<void> } | undefined;
  let contextBForCleanup: { readonly retire: () => Promise<void> } | undefined;
  let queryCallRecord: (typeof backend.calls)[number] | undefined;
  let mutationCallRecord: (typeof backend.calls)[number] | undefined;
  let predecessorReady: Promise<unknown> | undefined;
  let predecessorReadyJoined = false;
  let mutationResult: Promise<unknown> | undefined;
  let mutationResultJoined = false;
  let operationHandle: ReturnType<typeof prepareTransferOperation>["operation"] | undefined;
  let operationPublication: HeldPublication | undefined;
  let retirementPublication: HeldPublication | undefined;
  let bodyFailed = false;
  let bodyFailure: unknown;

  const retainClaim = (claim: PassiveClaim): HeldClaim => {
    const held = { claim, discarded: false, joined: false };
    claims.push(held);
    return held;
  };
  const discardClaim = (held: HeldClaim): void => {
    if (held.discarded) return;
    held.discarded = true;
    held.claim.discard();
  };
  const joinClaim = async (held: HeldClaim): Promise<void> => {
    try {
      await held.claim.retired;
    } finally {
      held.joined = true;
    }
  };
  const discardAndJoinClaim = async (held: HeldClaim): Promise<void> => {
    discardClaim(held);
    await joinClaim(held);
  };
  const retainPublication = (publication: PublicationPort): HeldPublication => {
    const held: HeldPublication = {
      publication,
      phase: "pending",
      commit: undefined,
      sealAttempted: false,
      deliveryAttempted: false,
      joined: false,
    };
    publications.push(held);
    return held;
  };
  const sealPublishAndDeliver = (held: HeldPublication): void => {
    held.sealAttempted = true;
    const commit = held.publication.seal();
    if (commit === undefined) throw new Error("The transfer publication did not seal");
    held.commit = commit;
    held.phase = "sealed";
    commit.publish();
    held.phase = "published";
    held.deliveryAttempted = true;
    commit.notifications.deliver();
  };
  const deliverPublished = (held: HeldPublication): void => {
    if (held.phase !== "published" || held.commit === undefined || held.deliveryAttempted) return;
    held.deliveryAttempted = true;
    held.commit.notifications.deliver();
  };
  const discardPending = (held: HeldPublication): void => {
    if (held.phase !== "pending") return;
    held.phase = "discarded";
    held.publication.discard();
  };
  const joinPublication = async (held: HeldPublication): Promise<void> => {
    try {
      await held.publication.retired;
    } finally {
      held.joined = true;
    }
  };
  const joinPredecessorReady = async (): Promise<void> => {
    if (predecessorReady === undefined || predecessorReadyJoined) return;
    try {
      await predecessorReady;
    } finally {
      predecessorReadyJoined = true;
    }
  };
  const joinMutationResult = async (): Promise<void> => {
    if (mutationResult === undefined || mutationResultJoined) return;
    try {
      await mutationResult;
    } finally {
      mutationResultJoined = true;
    }
  };
  const cleanupAttempt = async (action: () => void | Promise<void>): Promise<void> => {
    try {
      await action();
    } catch (error) {
      cleanupFailures.push(error);
    }
  };

  try {
    try {
      const profile = {};
      const input = Object.freeze({ key: "transfer-successor/one" });
      const contextA = createBindingContext(graph);
      const contextB = createBindingContext(graph);
      contextAForCleanup = contextA;
      contextBForCleanup = contextB;
      const bindingA = bindOperation(graph, query, {
        context: contextA,
        profile,
        fetch: fetchQuery,
      });
      const bindingB = bindOperation(graph, query, {
        context: contextB,
        profile,
        fetch: fetchQuery,
      });

      const predecessorStore = bindingA.member(input);
      const queryCall = backend.pending(queryName)[0];
      if (queryCall === undefined)
        throw new Error("The predecessor query did not reach the controlled backend");
      queryCallRecord = queryCall;
      predecessorReady = predecessorStore.ready;
      if (!queryCall.settled) backend.resolve(queryCall, 1);
      await joinPredecessorReady();
      expect(queryCall.settled).toBe(true);
      expect(queryFetchCalls).toBe(1);
      expect(backend.calls).toHaveLength(1);

      const controlA = retainClaim(preparePassiveTransferClaim(graph, bindingA, input));
      const controlB = retainClaim(preparePassiveTransferClaim(graph, bindingB, input));
      expect(controlA.claim.store.key).toBe(controlB.claim.store.key);
      expect(controlA.claim.store.identity).not.toBe(controlB.claim.store.identity);
      expect(queryFetchCalls).toBe(1);
      expect(mutationDispatches).toBe(0);
      await discardAndJoinClaim(controlA);
      await discardAndJoinClaim(controlB);
      expect(queryFetchCalls).toBe(1);
      expect(mutationDispatches).toBe(0);
      expect(backend.calls).toHaveLength(1);

      const predecessorScope = createTransferScope(graph, bindingA, predecessorStore);
      const carrier = Object.freeze({ request: "external-query-refresh" });
      const operationPublicationValue = prepareTransferOperation(
        [predecessorScope],
        [{ role: "raw", source: graph.api.mutation, input: carrier }],
      );
      const operation = operationPublicationValue.operation;
      operationHandle = operation;
      const operationPublicationHeld = retainPublication(operationPublicationValue);
      operationPublication = operationPublicationHeld;
      sealPublishAndDeliver(operationPublicationHeld);
      await joinPublication(operationPublicationHeld);

      mutationResult = graph.api.mutation(carrier);
      const mutationCall = backend.pending(mutationName)[0];
      if (mutationCall === undefined)
        throw new Error("The original mutation did not reach the controlled backend");
      const originalMutationCall = mutationCall;
      mutationCallRecord = originalMutationCall;
      expect(originalMutationCall.input).toBe(carrier);
      expect(originalMutationCall.settled).toBe(false);
      expect(mutationDispatches).toBe(1);
      expect(backend.calls).toHaveLength(2);
      let unexpectedRetirement: ReturnType<typeof prepareTransferOperationRetirement> | undefined;
      try {
        expect(() => {
          unexpectedRetirement = prepareTransferOperationRetirement(operation);
        }).toThrow(TypeError);
      } finally {
        if (unexpectedRetirement !== undefined) retainPublication(unexpectedRetirement);
      }

      const successorPublicationValue = prepareTransferScopeSuccessors([
        {
          operation,
          predecessor: predecessorScope,
          graph,
          source: bindingB,
          input,
        },
      ]);
      const successorPublication = retainPublication(successorPublicationValue);
      sealPublishAndDeliver(successorPublication);
      await joinPublication(successorPublication);
      expect(successorPublicationValue.scopes).toHaveLength(1);
      expect(queryFetchCalls).toBe(1);
      expect(mutationDispatches).toBe(1);
      expect(backend.calls).toHaveLength(2);

      if (!originalMutationCall.settled) backend.resolve(originalMutationCall, undefined);
      await joinMutationResult();
      expect(originalMutationCall.settled).toBe(true);
      const retirementValue = prepareTransferOperationRetirement(operation);
      const retirementHeld = retainPublication(retirementValue);
      retirementPublication = retirementHeld;

      const expectClaimRefused = (source: typeof bindingA): RefusalCheck => {
        let unexpectedClaim: PassiveClaim | undefined;
        try {
          expect(() => {
            unexpectedClaim = preparePassiveTransferClaim(graph, source, input);
          }).toThrow(TypeError);
          return { failed: false };
        } catch (error) {
          return { failed: true, error };
        } finally {
          if (unexpectedClaim !== undefined) retainClaim(unexpectedClaim);
        }
      };
      const predecessorRefusal = expectClaimRefused(bindingA);
      const successorRefusal = expectClaimRefused(bindingB);
      const refusalFailures: unknown[] = [];
      if (predecessorRefusal.failed) refusalFailures.push(predecessorRefusal.error);
      if (successorRefusal.failed) refusalFailures.push(successorRefusal.error);
      if (refusalFailures.length === 1) throw refusalFailures[0];
      if (refusalFailures.length > 1)
        throw new AggregateError(
          refusalFailures,
          "Both covered addresses must refuse passive claims",
        );

      expect(queryFetchCalls).toBe(1);
      expect(mutationDispatches).toBe(1);
      expect(backend.calls).toHaveLength(2);

      sealPublishAndDeliver(retirementHeld);
      await joinPublication(retirementHeld);

      const releasedA = retainClaim(preparePassiveTransferClaim(graph, bindingA, input));
      const releasedB = retainClaim(preparePassiveTransferClaim(graph, bindingB, input));
      expect(releasedA.claim.store.key).toBe(releasedB.claim.store.key);
      expect(releasedA.claim.store.identity).not.toBe(releasedB.claim.store.identity);
      await discardAndJoinClaim(releasedA);
      await discardAndJoinClaim(releasedB);
      expect(queryFetchCalls).toBe(1);
      expect(mutationDispatches).toBe(1);
      expect(backend.calls).toHaveLength(2);
    } catch (error) {
      bodyFailed = true;
      bodyFailure = error;
    }
  } finally {
    if (queryCallRecord === undefined)
      queryCallRecord = backend.calls.find((call) => call.name === queryName);
    if (mutationCallRecord === undefined)
      mutationCallRecord = backend.calls.find((call) => call.name === mutationName);
    const queryCallRecords = new Set(backend.calls.filter((call) => call.name === queryName));
    const mutationCallRecords = new Set(backend.calls.filter((call) => call.name === mutationName));
    if (queryCallRecord !== undefined) queryCallRecords.add(queryCallRecord);
    if (mutationCallRecord !== undefined) mutationCallRecords.add(mutationCallRecord);
    for (const call of queryCallRecords)
      if (!call.settled) await cleanupAttempt(() => backend.resolve(call, 1));
    for (const call of mutationCallRecords)
      if (!call.settled) await cleanupAttempt(() => backend.resolve(call, undefined));
    if (predecessorReady !== undefined && !predecessorReadyJoined) {
      if (queryCallRecord !== undefined) await cleanupAttempt(joinPredecessorReady);
      else
        cleanupFailures.push(
          new Error("The predecessor ready promise has no controlled CallRecord to settle"),
        );
    }
    if (mutationResult !== undefined && !mutationResultJoined) {
      if (mutationCallRecord !== undefined) await cleanupAttempt(joinMutationResult);
      else
        cleanupFailures.push(
          new Error("The original mutation result has no controlled CallRecord to settle"),
        );
    }

    for (const claim of claims) {
      if (!claim.discarded) await cleanupAttempt(() => discardClaim(claim));
    }
    for (const claim of claims) {
      if (!claim.joined) await cleanupAttempt(() => joinClaim(claim));
    }

    if (
      operationPublication?.phase === "published" &&
      operationHandle !== undefined &&
      retirementPublication === undefined
    ) {
      await cleanupAttempt(() => {
        retirementPublication = retainPublication(
          prepareTransferOperationRetirement(operationHandle!),
        );
      });
    }
    if (
      retirementPublication !== undefined &&
      retirementPublication.phase === "pending" &&
      !retirementPublication.sealAttempted &&
      operationPublication?.phase === "published"
    )
      await cleanupAttempt(() => sealPublishAndDeliver(retirementPublication!));

    for (const publication of publications) {
      if (publication.phase === "pending") await cleanupAttempt(() => discardPending(publication));
    }
    for (const publication of publications) {
      if (publication.phase === "published" && !publication.deliveryAttempted)
        await cleanupAttempt(() => deliverPublished(publication));
      if (publication.phase === "sealed")
        cleanupFailures.push(
          new Error("A sealed transfer publication could not be publicly completed"),
        );
    }
    for (const publication of publications) {
      if (
        !publication.joined &&
        (publication.phase === "published" || publication.phase === "discarded")
      )
        await cleanupAttempt(() => joinPublication(publication));
    }

    const ownerRetirements: Promise<void>[] = [];
    const startRetirement = (owner: { readonly retire: () => Promise<void> } | undefined): void => {
      if (owner === undefined) return;
      try {
        ownerRetirements.push(owner.retire());
      } catch (error) {
        cleanupFailures.push(error);
      }
    };
    startRetirement(contextAForCleanup);
    startRetirement(contextBForCleanup);
    startRetirement(graph);
    for (const retirement of ownerRetirements) await cleanupAttempt(async () => retirement);
  }
  if (bodyFailed) {
    if (cleanupFailures.length === 0) throw bodyFailure;
    throw new AggregateError(
      [bodyFailure, ...cleanupFailures],
      "Successor coverage test and cleanup failed",
    );
  }
  if (cleanupFailures.length === 1) throw cleanupFailures[0];
  if (cleanupFailures.length > 1)
    throw new AggregateError(cleanupFailures, "Successor coverage cleanup failed");
});
