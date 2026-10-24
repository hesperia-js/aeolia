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
  defineContract,
  project,
  subscribe,
  watch,
  Fault,
  withoutWrites,
} from "../../src/index.ts";
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
      expect(signals[0]!.reason).toBe(preAborted ? earlierReason : reason);
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

it("caches a rejected retirement without closing admission in a write-restricted scope", async () => {
  const graph = createGraph({
    contract: defineContract({ namespace: "retirement-precondition", operations: {} }),
  });
  try {
    const retirement = withoutWrites(() => graph.retire());
    const writable = graph.at("still-open");
    writable.set(1);
    expect(writable.value.get()).toBe(1);
    await expect(retirement).rejects.toThrow("write-forbidden");
    expect(graph.retire()).toBe(retirement);
  } finally {
    graph.dispose();
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
      addEventListener(type, listener, options) {
        lifetime.signal.addEventListener(type, listener, options);
      },
      removeEventListener(type, listener, options) {
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
      await expectPending(retirement);

      projectionReturn.resolve({ done: true, value: undefined });
      await expectPending(retirement);
      projectionPull.resolve({ done: true, value: undefined });
      await expectPending(retirement);
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
