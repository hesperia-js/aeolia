import { afterEach, beforeEach, describe, expect, it, jest } from "bun:test";
import {
  affects,
  createGraph,
  createMutation,
  createQuery,
  createStore,
  createStream,
  createPositionedStream,
  createBindingContext,
  createTransferScope,
  prepareTransferOperation,
  prepareTransferOperationRetirement,
  bindStream,
  capturePositionedStore,
  isPositionedStoreCheckpointCurrent,
  preparePositionedStoreRestore,
  defineContract,
  project,
} from "../../src/contract/index.ts";
import { Fault } from "../../src/fault.ts";
import { computed, subscribe, withoutWrites } from "../../src/reactive.ts";
import { testBackend } from "../../src/testing.ts";
import {
  adopt,
  observeStore,
  peekStore,
  snapshot,
  subscribe as subscribeStore,
  captureReactiveConstruction,
  preparePassiveTransferClaim,
  prepareRequiredTransferAdoption,
  prepareRequiredTransferCapture,
} from "../../src/index.ts";
import type {
  ClosedEncodedValue,
  PositionedStoreRestoration,
  RequiredTransferCodec,
  StreamStore,
} from "../../src/index.ts";
import type { PositionedStreamSource } from "../../src/contract/index.ts";

const providerClockNow = Date.now;

beforeEach(() => {
  jest.useFakeTimers();
  jest.setSystemTime(0);
});

afterEach(() => {
  jest.clearAllTimers();
  jest.useRealTimers();
});

it("preflights every reset generation before changing an earlier store or the graph epoch", async () => {
  const left = createStore({ name: "left", initial: 1, snapshot: true });
  const right = createStore({ name: "right", initial: 2, snapshot: true });
  const targetLeft = createStore({ name: "left", initial: 10, snapshot: true });
  const targetRight = createStore({ name: "right", initial: 20, snapshot: true });
  const source = createGraph({
    contract: defineContract({ namespace: "reset-counter", operations: { left, right } }),
  });
  const target = createGraph({
    contract: defineContract({
      namespace: "reset-counter",
      operations: { left: targetLeft, right: targetRight },
    }),
  });
  try {
    source.api.left();
    source.api.right();
    const first = target.api.left();
    const second = target.api.right();
    const encoded = snapshot(source, { version: 2 });
    const incoming = {
      ...encoded,
      entries: encoded.entries.map((row) => ({
        ...row,
        generation: row.address.key === "right" ? Number.MAX_SAFE_INTEGER - 1 : 10,
      })),
    } as unknown as typeof encoded;
    adopt(target, incoming, {});
    const before = [peekStore(first), peekStore(second)];
    const epoch = target.resetVersion.get();
    let threw = false;
    let reason: unknown;
    let reset: Promise<void> | undefined;
    try {
      reset = target.reset();
    } catch (error) {
      threw = true;
      reason = error;
    }
    if (reset !== undefined) await reset;

    expect(threw).toBe(true);
    expect(reason).toBeInstanceOf(RangeError);
    expect(target.resetVersion.get()).toBe(epoch);
    expect([peekStore(first), peekStore(second)]).toEqual(before);
    expect([first.value.get(), second.value.get()]).toEqual([1, 2]);
  } finally {
    source.dispose();
    target.dispose();
  }
});

async function flush(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
}

async function nextTurn(): Promise<void> {
  await new Promise<void>((resolve) => setImmediate(resolve));
}

describe("Graph.reset", () => {
  it("rejects a reset cancelled in its first refresh, stops dispatch, and preserves reentrant raw work", async () => {
    const reason = new Error("cancel running reset");
    const requests: Array<{
      input: string;
      signal: AbortSignal;
      result: ReturnType<typeof Promise.withResolvers<number>>;
    }> = [];
    let startSuccessor = (): void => undefined;
    const query = createQuery({
      name: "cancel-reset.query",
      key: (input: string) => `cancel-reset/${input}`,
      fetch: (input, options) => {
        const result = Promise.withResolvers<number>();
        requests.push({ input, signal: options.abortSignal, result });
        if (input === "first") {
          options.abortSignal.addEventListener("abort", () => startSuccessor(), { once: true });
          graph.cancelPending(reason);
        }
        return result.promise;
      },
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "cancel-reset", operations: { query } }),
    });
    const first = graph.api.query("first", { initial: 1 });
    const second = graph.api.query("second", { initial: 2 });
    const stopFirst = subscribe(first.status, () => undefined);
    const stopSecond = subscribe(second.status, () => undefined);
    let successor: typeof first | undefined;
    startSuccessor = () => {
      successor = graph.api.query("successor");
    };
    try {
      const reset = graph.reset();
      await expect(reset).rejects.toBe(reason);
      expect(requests.map((request) => request.input)).toEqual(["first", "successor"]);
      expect(requests[0]!.signal.reason).toBe(reason);
      expect(requests[1]!.signal.aborted).toBe(false);
      await expect(first.ready).rejects.toBe(reason);
      expect(graph.resetVersion.get()).toBe(1);
      expect(first.value.get()).toBeUndefined();
      expect(second.value.get()).toBeUndefined();
      expect(second.status.get()).toBe("empty");
      expect(successor).toBeDefined();
      const ready = successor!.ready;
      requests[1]!.result.resolve(3);
      await ready;
      expect(successor!.value.get()).toBe(3);
      requests[0]!.result.resolve(99);
      await requests[0]!.result.promise;
      expect(first.value.get()).toBeUndefined();
      expect(successor!.value.get()).toBe(3);
    } finally {
      stopFirst();
      stopSecond();
      graph.dispose();
    }
  });

  it("preserves graph and store handles, restores declarations, and accepts fresh undefined data", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "reset.undefined",
      key: (input: string) => `reset/${input}`,
      fetch: backend.respond<string, number | undefined>("reset.undefined"),
    });
    const declared = createStore({ name: "settings", initial: "default" });
    const graph = createGraph({
      contract: defineContract({ namespace: "reset", operations: { query, declared } }),
    });
    const api = graph.api;
    const declaredStore = graph.store(declared);
    const declaredValue = declaredStore.value;
    const queryStore = graph.api.query("cached");
    const queryValue = queryStore.value;
    const versions: number[] = [];
    const stopVersion = subscribe(graph.resetVersion, (version) => versions.push(version));

    declaredStore.set("changed");
    backend.resolve(backend.calls[0]!, undefined);
    await flush();
    await expect(queryStore.ready).resolves.toBeUndefined();
    expect(queryStore.value.get()).toBeUndefined();

    const stopQuery = subscribe(queryStore.value, () => undefined);
    const reset = graph.reset();
    expect(graph.api).toBe(api);
    expect(graph.resetVersion.get()).toBe(1);
    expect(versions).toEqual([0, 1]);
    expect(graph.store(declared)).toBe(declaredStore);
    expect(declaredStore.value).toBe(declaredValue);
    expect(declaredStore.value.get()).toBe("default");
    expect(queryStore.value).toBe(queryValue);
    expect(queryStore.value.get()).toBeUndefined();
    expect(backend.calls).toHaveLength(2);

    backend.resolve(backend.calls[1]!, undefined);
    await expect(reset).resolves.toBeUndefined();
    await expect(queryStore.ready).resolves.toBeUndefined();
    expect(queryStore.value).toBe(queryValue);

    stopQuery();
    stopVersion();
    graph.dispose();
  });

  it("refreshes a status-only live query and clears an inactive query without fetching it", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "reset.live",
      key: (input: string) => `reset/${input}`,
      fetch: backend.respond<string, number>("reset.live"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "reset-live", operations: { query } }),
    });
    const live = graph.api.query("live");
    const inactive = graph.api.query("inactive");
    const priorFailure = new Error("prior query failed");
    backend.reject(backend.calls[0]!, priorFailure);
    backend.resolve(backend.calls[1]!, 10);
    await flush();
    expect(live.status.get()).toBe("failed");
    expect(live.error.get()).toBe(priorFailure);
    const stopStatus = subscribe(live.status, () => undefined);

    const reset = graph.reset();
    expect(backend.calls).toHaveLength(3);
    expect(live.value.get()).toBeUndefined();
    expect(live.error.get()).toBeUndefined();
    expect(inactive.value.get()).toBeUndefined();
    expect(live.status.get()).toBe("fetching");
    backend.resolve(backend.calls[2]!, 2);
    await expect(reset).resolves.toBeUndefined();
    expect(live.value.get()).toBe(2);
    expect(backend.calls).toHaveLength(3);

    stopStatus();
    graph.dispose();
  });

  it("refuses reset in a write-restricted scope without superseding active work", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "reset.write-restricted",
      key: () => "reset/write-restricted",
      fetch: backend.respond<void, number>("reset.write-restricted"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "reset-write-restricted", operations: { query } }),
    });
    const store = graph.api.query(undefined);
    backend.resolve(backend.calls[0]!, 5);
    await expect(store.ready).resolves.toBe(5);

    const revalidation = store.revalidate();
    const activeCall = backend.calls[1]!;
    expect(store.status.get()).toBe("revalidating");
    expect(() => withoutWrites(() => graph.reset())).toThrow("write-forbidden");
    expect(graph.resetVersion.get()).toBe(0);
    expect(activeCall.aborted).toBe(false);
    expect(store.value.get()).toBe(5);
    expect(store.status.get()).toBe("revalidating");

    backend.resolve(activeCall, 6);
    await expect(revalidation).resolves.toBeUndefined();
    expect(store.value.get()).toBe(6);
    graph.dispose();
  });

  it("keeps an existing ready wait active across reset", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "reset.ready-waiter",
      key: () => "reset/ready-waiter",
      fetch: backend.respond<void, number>("reset.ready-waiter"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "reset-ready-waiter", operations: { query } }),
    });
    const store = graph.api.query(undefined);
    const ready = store.ready;
    const previous = backend.calls[0]!;

    const reset = graph.reset();
    expect(previous.aborted).toBe(true);
    expect(backend.calls).toHaveLength(2);
    backend.resolve(backend.calls[1]!, 8);
    await expect(reset).resolves.toBeUndefined();
    await expect(ready).resolves.toBe(8);

    graph.dispose();
  });

  it("clears inactive cached data without starting a refresh when no query is live", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "reset.inactive",
      key: () => "reset/inactive",
      fetch: backend.respond<void, number>("reset.inactive"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "reset-inactive", operations: { query } }),
    });
    const store = graph.api.query(undefined);
    backend.resolve(backend.calls[0]!, 7);
    await flush();
    expect(store.value.get()).toBe(7);

    const reset = graph.reset();
    expect(graph.resetVersion.get()).toBe(1);
    expect(store.value.get()).toBeUndefined();
    expect(backend.calls).toHaveLength(1);
    await expect(reset).resolves.toBeUndefined();
    expect(backend.calls).toHaveLength(1);

    graph.dispose();
  });

  for (const facet of ["store", "value", "available", "issue"] as const) {
    it(`distinguishes ${facet} activation after an unobserved query reset`, async () => {
      const backend = testBackend();
      const query = createQuery({
        name: "reset.reactivate",
        key: () => "reset/reactivate",
        revalidateAfterMs: 10,
        fetch: backend.respond<void, number>("reset.reactivate"),
      });
      const graph = createGraph({
        contract: defineContract({ namespace: "reset-reactivate", operations: { query } }),
      });
      const store = graph.api.query();
      const initial = store.ready;
      backend.resolve(backend.calls[0]!, 4);
      expect(await initial).toBe(4);

      await expect(graph.reset()).resolves.toBeUndefined();
      expect(store.value.get()).toBeUndefined();
      expect(backend.calls).toHaveLength(1);

      const seen: unknown[] = [];
      const stop =
        facet === "store"
          ? subscribeStore(store, (value) => {
              seen.push(value);
            })
          : facet === "value"
            ? subscribe(store.value, (value) => {
                seen.push(value);
              })
            : facet === "available"
              ? subscribe(store.available, (value) => {
                  seen.push(value);
                })
              : subscribe(observeStore(graph, store).issue, (value) => {
                  seen.push(value);
                });
      if (facet === "store" || facet === "value") {
        expect(backend.calls).toHaveLength(2);
        if (facet === "store") expect(seen).toEqual([]);
        const refreshed = store.ready;
        backend.resolve(backend.calls[1]!, 5);
        expect(await refreshed).toBe(5);
        expect(seen.at(-1)).toBe(5);
      } else {
        expect(backend.calls).toHaveLength(1);
        expect(seen).toStrictEqual([facet === "available" ? false : undefined]);
      }

      stop();
      graph.dispose();
    });
  }

  it("keeps shared whole-store activation until the last subscriber leaves and releases it afterward", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "reset.shared-store",
      key: () => "reset/shared-store",
      revalidateAfterMs: 10,
      fetch: backend.respond<void, number>("reset.shared-store"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "reset-shared-store", operations: { query } }),
    });
    try {
      const store = graph.api.query();
      const initial = store.ready;
      backend.resolve(backend.calls[0]!, 4);
      await initial;
      await graph.reset();
      const first: number[] = [];
      const second: number[] = [];
      const stopFirst = subscribeStore(store, (value) => {
        first.push(value);
      });
      const stopSecond = subscribeStore(store, (value) => {
        second.push(value);
      });
      expect(backend.calls).toHaveLength(2);
      const refreshed = store.ready;
      backend.resolve(backend.calls[1]!, 5);
      await refreshed;
      expect(first).toEqual([5]);
      expect(second).toEqual([5]);
      stopFirst();
      jest.advanceTimersByTime(10);
      expect(backend.calls).toHaveLength(3);
      const fresh = store.ready;
      backend.resolve(backend.calls[2]!, 6);
      await fresh;
      expect(second).toEqual([5, 6]);
      stopSecond();
      jest.advanceTimersByTime(20);
      expect(backend.calls).toHaveLength(3);
      await graph.reset();
      expect(backend.calls).toHaveLength(3);
      const stopMetadata = subscribe(store.available, () => undefined);
      expect(backend.calls).toHaveLength(3);
      stopMetadata();
      const next: number[] = [];
      const stopNext = subscribeStore(store, (value) => {
        next.push(value);
      });
      expect(backend.calls).toHaveLength(4);
      const nextReady = store.ready;
      backend.resolve(backend.calls[3]!, 7);
      await nextReady;
      expect(next).toEqual([7]);
      expect(first).toEqual([5]);
      expect(second).toEqual([5, 6]);
      stopNext();
    } finally {
      graph.dispose();
    }
  });

  it("allows a keyed initial seed after reset only while empty and idle", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "reset.reseed",
      key: (input: string) => `reset/reseed/${input}`,
      fetch: backend.respond<string, number>("reset.reseed"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "reset-reseed", operations: { query } }),
    });
    const store = graph.api.query("one", { initial: 3 });
    const previousFetch = store.revalidate();
    backend.resolve(backend.calls[0]!, 4);
    await previousFetch;
    await expect(graph.reset()).resolves.toBeUndefined();

    const reseeded = graph.api.query("one", { initial: 9 });
    expect(reseeded.value.get()).toBe(9);
    expect(backend.calls).toHaveLength(1);

    const liveStop = subscribe(reseeded.status, () => undefined);
    const reset = graph.reset();
    expect(backend.calls).toHaveLength(2);
    expect(() => graph.api.query("one", { initial: 10 })).toThrow(Fault);
    expect(backend.calls[1]!.aborted).toBe(false);
    backend.resolve(backend.calls[1]!, 12);
    await expect(reset).resolves.toBeUndefined();
    expect(reseeded.value.get()).toBe(12);
    expect(() => graph.api.query("one", { initial: 10 })).toThrow(Fault);
    expect(reseeded.value.get()).toBe(12);

    liveStop();
    graph.dispose();
  });

  it("does not wait for a query that becomes live after reset starts", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "reset.late-live",
      key: (input: string) => `reset/${input}`,
      fetch: backend.respond<string, number>("reset.late-live"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "reset-late-live", operations: { query } }),
    });
    const required = graph.api.query("required");
    let later = graph.api.query("later");
    backend.resolve(backend.calls[0]!, 1);
    backend.resolve(backend.calls[1]!, 2);
    await flush();
    const stopRequired = subscribe(required.status, () => undefined);
    let stopLater: (() => void) | undefined;
    const stopVersion = subscribe(graph.resetVersion, (version) => {
      if (version === 0) return;
      later = graph.api.query("later");
      stopLater = subscribe(later.status, () => undefined);
    });

    let settled = false;
    const outcome = graph.reset().then(
      () => {
        settled = true;
        return { ok: true as const };
      },
      (error: unknown) => {
        settled = true;
        return { ok: false as const, error };
      },
    );
    expect(backend.calls).toHaveLength(4);
    expect(later.status.get()).toBe("fetching");
    expect(settled).toBe(false);
    backend.resolve(backend.calls[3]!, 3);
    expect(await outcome).toEqual({ ok: true });
    expect(settled).toBe(true);
    expect(backend.calls[2]!.aborted).toBe(false);

    backend.resolve(backend.calls[2]!, 4);
    await flush();
    expect(later.value.get()).toBe(4);
    stopRequired();
    stopLater?.();
    stopVersion();
    graph.dispose();
  });

  it("adopts the current same-key refresh after the reset-owned request is superseded", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "reset.adopted-refresh",
      key: () => "reset/adopted-refresh",
      fetch: backend.respond<void, number>("reset.adopted-refresh"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "reset-adopted-refresh", operations: { query } }),
    });
    const store = graph.api.query(undefined);
    backend.resolve(backend.calls[0]!, 1);
    await flush();
    const stop = subscribe(store.status, () => undefined);

    let settled = false;
    const reset = graph.reset().then(() => {
      settled = true;
    });
    const obsolete = backend.calls[1]!;
    const current = store.revalidate();
    expect(backend.calls).toHaveLength(3);
    expect(obsolete.aborted).toBe(true);

    backend.resolve(backend.calls[2]!, 2);
    await current;
    await nextTurn();
    expect(settled).toBe(true);
    await expect(reset).resolves.toBeUndefined();
    expect(store.value.get()).toBe(2);

    backend.resolve(obsolete, 99);
    await flush();
    expect(store.value.get()).toBe(2);
    stop();
    graph.dispose();
  });

  it("does not let an obsolete cooperative abort reject reset ahead of its replacement", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "reset.adopted-abort",
      key: () => "reset/adopted-abort",
      fetch: backend.respond<void, number>("reset.adopted-abort"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "reset-adopted-abort", operations: { query } }),
    });
    const store = graph.api.query(undefined);
    backend.resolve(backend.calls[0]!, 1);
    await flush();
    const stop = subscribe(store.status, () => undefined);

    let settled = false;
    const result = graph.reset().then(
      () => {
        settled = true;
        return { ok: true as const };
      },
      (error: unknown) => {
        settled = true;
        return { ok: false as const, error };
      },
    );
    const obsolete = backend.calls[1]!;
    const replacement = store.revalidate();
    backend.reject(obsolete, new DOMException("obsolete fetch was aborted", "AbortError"));
    await flush();
    expect(settled).toBe(false);

    backend.resolve(backend.calls[2]!, 2);
    await expect(result).resolves.toEqual({ ok: true });
    expect(store.value.get()).toBe(2);
    await replacement;
    stop();
    graph.dispose();
  });

  it("does not finish from an obsolete success while its same-key replacement is pending", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "reset.obsolete-success",
      key: () => "reset/obsolete-success",
      fetch: backend.respond<void, number>("reset.obsolete-success"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "reset-obsolete-success", operations: { query } }),
    });
    const store = graph.api.query(undefined);
    backend.resolve(backend.calls[0]!, 1);
    await flush();
    const stop = subscribe(store.status, () => undefined);

    let settled = false;
    const reset = graph.reset().then(() => {
      settled = true;
    });
    const obsolete = backend.calls[1]!;
    store.revalidate();
    const replacement = backend.calls[2]!;
    backend.resolve(obsolete, 99);
    await nextTurn();
    expect(settled).toBe(false);

    backend.resolve(replacement, 2);
    await expect(reset).resolves.toBeUndefined();
    expect(settled).toBe(true);
    expect(store.value.get()).toBe(2);
    stop();
    graph.dispose();
  });

  it("forwards the reset outcome through reentrant same-key replacements", async () => {
    const requests: Array<{
      readonly abortSignal: AbortSignal;
      readonly resolve: (value: number) => void;
      readonly reject: (error: unknown) => void;
    }> = [];
    const query = createQuery({
      name: "reset.reentrant-replacements",
      key: () => "reset/reentrant-replacements",
      fetch: (_input, { abortSignal }) =>
        new Promise<number>((resolve, reject) => {
          requests.push({ abortSignal, resolve, reject });
        }),
    });
    const graph = createGraph({
      contract: defineContract({
        namespace: "reset-reentrant-replacements",
        operations: { query },
      }),
    });
    const store = graph.api.query(undefined);
    requests[0]!.resolve(1);
    await flush();
    const stop = subscribe(store.status, () => undefined);

    let settled = false;
    const reset = graph.reset().then(() => {
      settled = true;
    });
    const obsolete = requests[1]!;
    let reentrant: Promise<void> | undefined;
    obsolete.abortSignal.addEventListener("abort", () => {
      reentrant = store.revalidate();
    });
    store.revalidate();
    expect(requests).toHaveLength(4);
    expect(reentrant).toBeDefined();
    const reentrantCall = requests[2]!;
    const finalCall = requests[3]!;

    reentrantCall.resolve(3);
    obsolete.resolve(99);
    await nextTurn();
    expect(settled).toBe(false);
    finalCall.resolve(4);
    await expect(reset).resolves.toBeUndefined();
    await expect(reentrant).resolves.toBeUndefined();
    expect(store.value.get()).toBe(4);
    stop();
    graph.dispose();
  });

  it("rejects a sole required query failure with its original reason, including undefined", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "reset.single-failure",
      key: () => "reset/single-failure",
      fetch: backend.respond<void, number>("reset.single-failure"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "reset-single-failure", operations: { query } }),
    });
    const store = graph.api.query(undefined);
    const stop = subscribe(store.status, () => undefined);
    backend.resolve(backend.calls[0]!, 1);
    await flush();

    const reset = graph.reset().then(
      () => ({ ok: true as const }),
      (error: unknown) => ({ ok: false as const, error }),
    );
    expect(backend.calls).toHaveLength(2);
    backend.reject(backend.calls[1]!, undefined);
    const outcome = await reset;
    expect(outcome).toEqual({ ok: false, error: undefined });
    expect(store.status.get()).toBe("failed");
    expect(store.error.get()).toBeUndefined();

    stop();
    graph.dispose();
  });

  it("waits for every active query outcome and aggregates original failures", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "reset.all",
      key: (input: string) => `reset/${input}`,
      fetch: backend.respond<string, number>("reset.all"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "reset-all", operations: { query } }),
    });
    const first = graph.api.query("first");
    const second = graph.api.query("second");
    const third = graph.api.query("third");
    backend.resolve(backend.calls[0]!, 1);
    backend.resolve(backend.calls[1]!, 2);
    backend.resolve(backend.calls[2]!, 3);
    await flush();
    const stopFirst = subscribe(first.status, () => undefined);
    const stopSecond = subscribe(second.status, () => undefined);
    const stopThird = subscribe(third.status, () => undefined);

    let settled = false;
    const reset = graph.reset().then(
      () => {
        settled = true;
        return undefined;
      },
      (error: unknown) => {
        settled = true;
        throw error;
      },
    );
    const firstFailure = undefined;
    const secondFailure = new Error("second refresh failed");
    backend.reject(backend.calls[3]!, firstFailure);
    backend.resolve(backend.calls[5]!, 4);
    await flush();
    expect(settled).toBe(false);
    expect(third.value.get()).toBe(4);
    backend.reject(backend.calls[4]!, secondFailure);
    const resetError = await reset.then(
      () => undefined,
      (error: unknown) => error,
    );
    expect(resetError).toBeInstanceOf(AggregateError);
    expect((resetError as AggregateError).errors).toEqual([firstFailure, secondFailure]);
    expect(third.value.get()).toBe(4);

    stopFirst();
    stopSecond();
    stopThird();
    graph.dispose();
  });

  it("rejects a superseded reset immediately and ignores its late query landing", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "reset.overlap",
      key: () => "reset/one",
      fetch: backend.respond<void, number>("reset.overlap"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "reset-overlap", operations: { query } }),
    });
    const store = graph.api.query(undefined);
    const stop = subscribe(store.status, () => undefined);

    const firstReset = graph.reset();
    const firstResult = firstReset.then(
      () => undefined,
      (error: unknown) => error,
    );
    const secondReset = graph.reset();
    const firstError = await firstResult;
    expect(firstError).toMatchObject({ name: "AbortError" });
    expect(backend.calls[0]!.aborted).toBe(true);
    expect(backend.calls[1]!.aborted).toBe(true);
    expect(backend.calls).toHaveLength(3);

    backend.resolve(backend.calls[0]!, 88);
    backend.resolve(backend.calls[1]!, 99);
    await flush();
    expect(store.value.get()).toBeUndefined();
    backend.resolve(backend.calls[2]!, 3);
    await expect(secondReset).resolves.toBeUndefined();
    expect(store.value.get()).toBe(3);
    expect(graph.resetVersion.get()).toBe(2);

    stop();
    graph.dispose();
  });

  it("ignores a prior-epoch mutation success without retiring new predictions or running effects", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "reset.mutation-query",
      key: (input: { readonly id: string }) => `item/${input.id}`,
      fetch: backend.respond<{ readonly id: string }, number>("reset.mutation-query"),
    });
    const mutation = createMutation({
      name: "reset.mutation",
      affects: [
        affects(query, {
          select: (input: { readonly id: string; readonly amount: number }) => ({ id: input.id }),
          optimistic: (current, input: { readonly id: string; readonly amount: number }) =>
            (current ?? 0) + input.amount,
        }),
      ],
      run: backend.perform<{ readonly id: string; readonly amount: number }, void>(
        "reset.mutation",
      ),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "reset-mutation", operations: { query, mutation } }),
    });
    const store = graph.api.query({ id: "one" });
    const stop = subscribe(store.value, () => undefined);
    backend.resolve(backend.calls[0]!, 10);
    await flush();

    const oldMutation = graph.api.mutation({ id: "one", amount: 1 });
    const oldFailureMutation = graph.api.mutation({ id: "one", amount: 3 });
    const oldMutationCall = backend.calls[1]!;
    const oldFailureCall = backend.calls[2]!;
    const oldFailure = new Error("old mutation failed");
    const oldFailureOutcome = oldFailureMutation.then(
      () => undefined,
      (error: unknown) => error,
    );
    expect(store.value.get()).toBe(14);
    const reset = graph.reset();
    expect(oldMutationCall.aborted).toBe(true);
    expect(oldFailureCall.aborted).toBe(true);
    backend.resolve(backend.calls[3]!, 20);
    await expect(reset).resolves.toBeUndefined();

    const newMutation = graph.api.mutation({ id: "one", amount: 2 });
    expect(store.value.get()).toBe(22);
    backend.resolve(oldMutationCall, undefined);
    await oldMutation;
    backend.reject(oldFailureCall, oldFailure);
    expect(await oldFailureOutcome).toBe(oldFailure);
    await flush();
    expect(store.value.get()).toBe(22);
    expect(backend.calls.filter((call) => call.name === "reset.mutation-query")).toHaveLength(2);

    backend.resolve(backend.calls[4]!, undefined);
    await newMutation;
    stop();
    graph.dispose();
  });

  it("does not dispatch a mutation when its optimistic predictor resets the graph", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "reset.reentrant-query",
      key: () => "reset/reentrant",
      fetch: backend.respond<void, number>("reset.reentrant-query"),
    });
    let startReset = (): Promise<void> => Promise.resolve();
    let reset: Promise<void> | undefined;
    const mutation = createMutation({
      name: "reset.reentrant-mutation",
      affects: [
        affects(query, {
          optimistic: (current: number | undefined) => {
            reset ??= startReset();
            return (current ?? 0) + 1;
          },
        }),
      ],
      run: backend.perform<void, void>("reset.reentrant-mutation"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "reset-reentrant", operations: { query, mutation } }),
    });
    startReset = () => graph.reset();
    const store = graph.api.query(undefined);
    const stop = subscribe(store.value, () => undefined);
    backend.resolve(backend.calls[0]!, 10);
    await flush();

    const outcome = graph.api.mutation(undefined).then(
      () => ({ ok: true as const }),
      (error: unknown) => ({ ok: false as const, error }),
    );
    expect(await outcome).toMatchObject({ ok: false, error: { name: "AbortError" } });
    expect(backend.calls.filter((call) => call.name === "reset.reentrant-mutation")).toHaveLength(
      0,
    );
    expect(store.value.get()).toBeUndefined();
    expect(reset).toBeDefined();
    backend.resolve(backend.calls[1]!, 20);
    await expect(reset).resolves.toBeUndefined();
    expect(store.value.get()).toBe(20);

    stop();
    graph.dispose();
  });

  it("keeps a mutation promise result when reset aborts its callback signal", async () => {
    const backend = testBackend();
    const mutation = createMutation({
      name: "reset.mutation-result",
      affects: [],
      run: backend.perform<void, string>("reset.mutation-result"),
    });
    const graph = createGraph({
      contract: defineContract({
        namespace: "reset-mutation-result",
        operations: { mutation },
      }),
    });
    const task = graph.api.mutation();
    const call = backend.calls[0]!;

    await expect(graph.reset()).resolves.toBeUndefined();
    expect(call.aborted).toBe(true);
    backend.resolve(call, "saved");
    await expect(task).resolves.toBe("saved");
    graph.dispose();
  });

  it("restarts a live stream and rejects emissions from its retired session", async () => {
    const sessions: Array<{
      readonly abortSignal: AbortSignal;
      readonly pending: Array<(result: IteratorResult<number>) => void>;
    }> = [];
    const stream = createStream({
      name: "reset.stream",
      key: () => "reset/stream",
      open: (_input, { abortSignal }) => {
        const session = {
          abortSignal,
          pending: [] as Array<(result: IteratorResult<number>) => void>,
        };
        sessions.push(session);
        const iterator: AsyncIterator<number> = {
          next: () => new Promise((resolve) => session.pending.push(resolve)),
          return: async () => ({ done: true, value: undefined }),
        };
        return { [Symbol.asyncIterator]: () => iterator };
      },
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "reset-stream", operations: { stream } }),
    });
    const store = graph.api.stream(undefined);
    const stop = subscribe(store.value, () => undefined);
    expect(sessions).toHaveLength(1);

    const reset = graph.reset();
    expect(sessions).toHaveLength(2);
    expect(sessions[0]!.abortSignal.aborted).toBe(true);
    expect(store.value.get()).toBeUndefined();
    await expect(reset).resolves.toBeUndefined();

    sessions[0]!.pending[0]!({ done: false, value: 99 });
    await flush();
    expect(store.value.get()).toBeUndefined();
    sessions[1]!.pending[0]!({ done: false, value: 7 });
    await flush();
    expect(store.value.get()).toBe(7);

    stop();
    graph.dispose();
  });

  it("reset invalidates pending and published positioned transfer claims", async () => {
    type Input = { readonly room: string };
    type Event = { readonly position: number; readonly value: number };
    type Reservation = {
      readonly input: Input;
      readonly after: number;
      readonly seed: unknown;
      readonly controller: AbortController;
      readonly expiresAt: number;
    };
    const makeProvider = () => {
      const reservations = new WeakMap<object, Reservation>();
      const readers = new WeakMap<object, (event: Event) => void>();
      const releases = new WeakMap<object, number>();
      const openings: Array<{ readonly reader: object | undefined }> = [];
      let emitLive: ((event: Event) => void) | undefined;
      const makeChannel = () => {
        const waiters: Array<(result: IteratorResult<Event>) => void> = [];
        let ended = false;
        return {
          iterable: {
            [Symbol.asyncIterator]() {
              return {
                next: () => {
                  if (ended) return Promise.resolve({ done: true as const, value: undefined });
                  return new Promise<IteratorResult<Event>>((resolve) => waiters.push(resolve));
                },
                return: () => {
                  ended = true;
                  for (const waiter of waiters.splice(0)) waiter({ done: true, value: undefined });
                  return Promise.resolve({ done: true as const, value: undefined });
                },
              };
            },
          } as AsyncIterable<Event>,
          emit(event: Event) {
            const waiter = waiters.shift();
            if (waiter === undefined)
              throw new Error("The positioned stream has no pending next call");
            waiter({ done: false, value: event });
          },
        };
      };
      const source: PositionedStreamSource<Input, number, number, object> = {
        open: (_input, options) => {
          const channel = makeChannel();
          openings.push({ reader: options.reader });
          if (options.reader === undefined) emitLive = channel.emit;
          else readers.set(options.reader, channel.emit);
          return channel.iterable;
        },
        readerLifetime: (_input, reader, after, seed) => {
          const reservation = reservations.get(reader);
          if (
            reservation === undefined ||
            reservation.input !== _input ||
            reservation.after !== after ||
            reservation.seed !== seed
          )
            throw new TypeError("The reader lifetime does not match its exact reservation");
          return { signal: reservation.controller.signal, expiresAt: reservation.expiresAt };
        },
        validateReader: (input, reader, after, _checkpoint, seed) => {
          const reservation = reservations.get(reader);
          return (
            reservation !== undefined &&
            reservation.input === input &&
            reservation.after === after &&
            reservation.seed === seed
          );
        },
        isReaderCurrent: (input, reader, after, seed) => {
          const reservation = reservations.get(reader);
          return (
            reservation !== undefined &&
            reservation.input === input &&
            reservation.after === after &&
            reservation.seed === seed
          );
        },
        acknowledge: () => undefined,
        releaseReader: (_input, reader) => {
          releases.set(reader, (releases.get(reader) ?? 0) + 1);
        },
      };
      return {
        source,
        openings,
        reserve(input: Input, after: number, seed: unknown) {
          const reader = Object.freeze({});
          reservations.set(reader, {
            input,
            after,
            seed,
            controller: new AbortController(),
            expiresAt: providerClockNow() + 60_000,
          });
          return reader;
        },
        emitLive(event: Event) {
          if (emitLive === undefined) throw new Error("The source positioned Store did not open");
          emitLive(event);
        },
        releaseCount(reader: object) {
          return releases.get(reader) ?? 0;
        },
      };
    };
    const sourceProvider = makeProvider();
    const makeDefinition = (provider: ReturnType<typeof makeProvider>) =>
      createPositionedStream<Input, number, string, number, object>({
        name: "reset-required-positioned",
        key: (input) => input.room,
        source: provider.source,
      });
    const namespace = "reset-required-positioned";
    const sourceDefinition = makeDefinition(sourceProvider);
    const sourceGraph = createGraph({
      contract: defineContract({ namespace, operations: { events: sourceDefinition } }),
    });
    const input = Object.freeze({ room: "one" });
    const policy = {
      kind: "reduce" as const,
      initial: 0,
      step: (sum: number, event: number) => sum + event,
    };
    const source = sourceGraph.api.events(input);
    const projection = project(source, policy);
    let capture: ReturnType<typeof prepareRequiredTransferCapture> | undefined;
    let pendingGraph: typeof sourceGraph | undefined;
    let pendingClaim:
      | {
          readonly store: StreamStore<number>;
          readonly restoration: PositionedStoreRestoration<number, readonly [number]>;
          readonly retired: Promise<void>;
          discard(): void;
        }
      | undefined;
    let pendingAdoption: ReturnType<typeof prepareRequiredTransferAdoption> | undefined;
    let pendingReader: object | undefined;
    let pendingProvider: ReturnType<typeof makeProvider> | undefined;
    let activeGraph: typeof sourceGraph | undefined;
    let activeClaim:
      | {
          readonly store: StreamStore<number>;
          readonly restoration: PositionedStoreRestoration<number, readonly [number]>;
          readonly retired: Promise<void>;
          discard(): void;
        }
      | undefined;
    let activeAdoption: ReturnType<typeof prepareRequiredTransferAdoption> | undefined;
    let activeReader: object | undefined;
    let activeProvider: ReturnType<typeof makeProvider> | undefined;
    let capturePublished = false;
    let activePublished = false;
    try {
      const sourceLanding = new Promise<void>((resolve) => {
        let stop: () => void = () => undefined;
        stop = subscribe(projection.value, (value) => {
          if (value !== 3) return;
          stop();
          resolve();
        });
      });
      sourceProvider.emitLive({ position: 5, value: 3 });
      await sourceLanding;
      const checkpoint = capturePositionedStore(source, [projection] as const);
      expect(checkpoint.present).toBe(true);
      expect(checkpoint.hasPosition).toBe(true);
      expect(checkpoint.position).toBe(5);
      const seed = {
        boundary: { kind: "event" as const, after: 5 },
        base: { present: true as const, value: 3 },
        values: checkpoint.values,
      };
      const seedId = "p0-seed";
      const decodedSeeds = new Map<string, unknown>([[seedId, seed]]);
      const codec: RequiredTransferCodec = {
        encodeValue: (value) =>
          value !== null &&
          typeof value === "object" &&
          Object.hasOwn(value, "boundary") &&
          Object.hasOwn(value, "base")
            ? ({ $ref: seedId } as ClosedEncodedValue)
            : (value as ClosedEncodedValue),
        encodeFailure: (cause) => String(cause),
        decodeValue: (value) => {
          if (
            value !== null &&
            typeof value === "object" &&
            Object.hasOwn(value, "$ref") &&
            (value as { readonly $ref?: unknown }).$ref === seedId
          ) {
            const decoded = decodedSeeds.get(seedId);
            if (decoded === undefined) throw new Error("The positioned seed reference is missing");
            return decoded;
          }
          return value;
        },
        decodeFailure: (failure) => new Error(String(failure)),
      };
      const sourceRoot = computed(() => projection.value.get());
      expect(sourceRoot.get()).toBe(3);
      capture = prepareRequiredTransferCapture({
        graphs: [sourceGraph],
        roots: [{ source: sourceRoot, construction: captureReactiveConstruction(sourceRoot) }],
        positioned: [checkpoint],
        codec,
      });
      const image = capture.image;
      const captureCommit = capture.seal();
      if (captureCommit === undefined)
        throw new Error("The source positioned capture did not seal");
      captureCommit.publish();
      capturePublished = true;
      captureCommit.notifications.deliver();
      await capture.retired;

      const pendingTargetProvider = makeProvider();
      pendingProvider = pendingTargetProvider;
      const pendingDefinition = makeDefinition(pendingTargetProvider);
      pendingGraph = createGraph({
        contract: defineContract({ namespace, operations: { events: pendingDefinition } }),
      });
      const pendingReaderLocal = pendingTargetProvider.reserve(input, 5, seed);
      pendingReader = pendingReaderLocal;
      const pendingClaimValue = preparePassiveTransferClaim(
        pendingGraph,
        pendingGraph.api.events,
        input,
        undefined,
        { policies: [policy] as const, seed, reader: pendingReaderLocal },
      );
      pendingClaim = pendingClaimValue;
      const pendingTarget = computed(() =>
        pendingClaimValue.restoration.projections[0]!.value.get(),
      );
      pendingAdoption = prepareRequiredTransferAdoption(image, {
        graphs: [pendingGraph],
        sources: [{ slot: "s0", claim: pendingClaimValue }],
        positioned: [{ slot: "p0", claim: pendingClaimValue, seed }],
        reactive: [
          {
            slot: image.roots[0]!,
            target: pendingTarget,
            construction: captureReactiveConstruction(pendingTarget),
          },
        ],
        codec,
        mappedStateWrites: [],
      });
      await pendingGraph.reset();
      expect(pendingAdoption.seal()).toBeUndefined();
      expect(pendingTargetProvider.openings).toHaveLength(0);
      pendingAdoption.discard();
      pendingClaimValue.discard();
      await Promise.all([pendingClaimValue.retired, pendingClaimValue.restoration.retired]);
      expect(pendingTargetProvider.openings).toHaveLength(0);
      expect(pendingTargetProvider.releaseCount(pendingReaderLocal)).toBe(1);
      expect(() => pendingClaimValue.restoration.activate()).toThrow(TypeError);

      const activeProviderLocal = makeProvider();
      activeProvider = activeProviderLocal;
      const activeDefinition = makeDefinition(activeProviderLocal);
      activeGraph = createGraph({
        contract: defineContract({ namespace, operations: { events: activeDefinition } }),
      });
      const activeReaderLocal = activeProviderLocal.reserve(input, 5, seed);
      activeReader = activeReaderLocal;
      const activeClaimValue = preparePassiveTransferClaim(
        activeGraph,
        activeGraph.api.events,
        input,
        { initial: 3 },
        { policies: [policy] as const, seed, reader: activeReaderLocal },
      );
      activeClaim = activeClaimValue;
      expect(activeClaimValue.retired).not.toBe(activeClaimValue.restoration.retired);
      const activeTarget = computed(() => activeClaimValue.restoration.projections[0]!.value.get());
      activeAdoption = prepareRequiredTransferAdoption(image, {
        graphs: [activeGraph],
        sources: [{ slot: "s0", claim: activeClaimValue }],
        positioned: [{ slot: "p0", claim: activeClaimValue, seed }],
        reactive: [
          {
            slot: image.roots[0]!,
            target: activeTarget,
            construction: captureReactiveConstruction(activeTarget),
          },
        ],
        codec,
        mappedStateWrites: [],
      });
      const activeCommit = activeAdoption.seal();
      if (activeCommit === undefined)
        throw new Error("The active positioned adoption did not seal");
      activeCommit.publish();
      activePublished = true;
      activeCommit.notifications.deliver();
      await Promise.all([activeAdoption.retired, activeClaimValue.retired]);
      expect(activeProviderLocal.openings).toHaveLength(0);

      await activeGraph.reset();
      expect(activeProviderLocal.openings).toHaveLength(0);
      await activeClaimValue.restoration.retired;
      expect(activeProviderLocal.releaseCount(activeReaderLocal)).toBe(1);
      expect(() => activeClaimValue.restoration.activate()).toThrow(TypeError);
      expect(activeClaimValue.store.available.get()).toBe(true);
      expect(activeClaimValue.store.value.get()).toBe(3);
      expect(activeClaimValue.restoration.projections[0]!.value.get()).toBe(0);
    } finally {
      if (!capturePublished) capture?.discard();
      pendingAdoption?.discard();
      pendingClaim?.discard();
      if (!activePublished) activeAdoption?.discard();
      if (activePublished) activeClaim?.restoration.discard();
      else activeClaim?.discard();
      projection.close();
      sourceGraph.dispose();
      pendingGraph?.dispose();
      activeGraph?.dispose();
      if (
        pendingReader !== undefined &&
        pendingClaim === undefined &&
        pendingProvider !== undefined &&
        pendingProvider.releaseCount(pendingReader) === 0
      )
        pendingProvider.source.releaseReader(input, pendingReader);
      if (
        activeReader !== undefined &&
        activeClaim === undefined &&
        activeProvider !== undefined &&
        activeProvider.releaseCount(activeReader) === 0
      )
        activeProvider.source.releaseReader(input, activeReader);
      if (capture !== undefined)
        await capture.retired.then(
          () => undefined,
          () => undefined,
        );
      if (pendingAdoption !== undefined)
        await pendingAdoption.retired.then(
          () => undefined,
          () => undefined,
        );
      if (activeAdoption !== undefined)
        await activeAdoption.retired.then(
          () => undefined,
          () => undefined,
        );
      if (pendingClaim !== undefined) {
        await pendingClaim.retired.then(
          () => undefined,
          () => undefined,
        );
        await pendingClaim.restoration.retired.then(
          () => undefined,
          () => undefined,
        );
      }
      if (activeClaim !== undefined) {
        await activeClaim.retired.then(
          () => undefined,
          () => undefined,
        );
        await activeClaim.restoration.retired.then(
          () => undefined,
          () => undefined,
        );
      }
    }
  });

  it("reset invalidates a dormant Store reader and restores its fold seed without reopening it", async () => {
    type Input = { readonly room: string };
    type Event = { position: number; value: number };
    const reservations = new WeakMap<object, { input: Input; after: number; seed: unknown }>();
    const releases = new WeakMap<object, number>();
    const openings: Array<{ reader: object | undefined; after: number | undefined }> = [];
    let emitLive: ((event: Event) => void) | undefined;

    const makeChannel = () => {
      const queued: Event[] = [];
      const waiters: Array<(result: IteratorResult<Event>) => void> = [];
      let ended = false;
      const emit = (event: Event): void => {
        if (ended) return;
        const waiter = waiters.shift();
        if (waiter !== undefined) waiter({ done: false, value: event });
        else queued.push(event);
      };
      const iterable: AsyncIterable<Event> = {
        [Symbol.asyncIterator]() {
          return {
            next: () => {
              const event = queued.shift();
              if (event !== undefined)
                return Promise.resolve({ done: false as const, value: event });
              if (ended) return Promise.resolve({ done: true as const, value: undefined });
              return new Promise<IteratorResult<Event>>((resolve) => waiters.push(resolve));
            },
            return: () => {
              ended = true;
              for (const waiter of waiters.splice(0)) waiter({ done: true, value: undefined });
              return Promise.resolve({ done: true as const, value: undefined });
            },
          };
        },
      };
      return { iterable, emit };
    };

    const reserve = (input: Input, after: number, seed: unknown): object => {
      const reader = Object.freeze({});
      reservations.set(reader, { input, after, seed });
      return reader;
    };
    const releaseCount = (reader: object): number => releases.get(reader) ?? 0;
    const releaseReader = (reader: object): void => {
      releases.set(reader, releaseCount(reader) + 1);
    };
    const stream = createPositionedStream<Input, number, string, number, object>({
      name: "reset-reader-store",
      key: (input) => input.room,
      source: {
        open: (input, options) => {
          const channel = makeChannel();
          openings.push({ reader: options.reader, after: options.after });
          if (options.reader === undefined) emitLive = channel.emit;
          return channel.iterable;
        },
        validateReader: (input, reader, after, checkpoint, seed) => {
          const reservation = reservations.get(reader);
          return (
            checkpoint === undefined &&
            reservation !== undefined &&
            reservation.input === input &&
            reservation.after === after &&
            reservation.seed === seed
          );
        },
        isReaderCurrent: (input, reader, after, seed) => {
          const reservation = reservations.get(reader);
          return (
            reservation !== undefined &&
            reservation.input === input &&
            reservation.after === after &&
            reservation.seed === seed
          );
        },
        acknowledge: () => undefined,
        releaseReader: (_input, reader) => releaseReader(reader),
      },
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "reset-reader-store", operations: { stream } }),
    });
    const rawGraph = createGraph({
      contract: defineContract({ namespace: "reset-reader-store-raw", operations: { stream } }),
    });
    const context = createBindingContext(graph);
    const binding = bindStream(graph, stream, graph.api.stream, { context });
    const input = Object.freeze({ room: "one" });
    const source = binding.member(input);
    const policy = {
      kind: "reduce" as const,
      initial: 10,
      step: (sum: number, event: number) => sum + event,
    };
    const projection = project(source, policy);
    let reader: object | undefined;
    let rawReader: object | undefined;
    let readerTransferred = false;
    let rawReaderTransferred = false;

    try {
      const folded = new Promise<void>((resolve) => {
        let stop = (): void => undefined;
        stop = subscribe(projection.value, (value) => {
          if (value !== 12) return;
          stop();
          resolve();
        });
      });
      if (emitLive === undefined) throw new Error("The real source Store did not open");
      emitLive({ position: 5, value: 2 });
      await folded;

      const checkpoint = capturePositionedStore(source, [projection] as const);
      if (!checkpoint.present || !checkpoint.hasPosition || typeof checkpoint.position !== "number")
        throw new Error("The source did not capture a positioned landing");
      const after: number = checkpoint.position;
      const seed = { after, latest: checkpoint.latest, values: checkpoint.values };
      reader = reserve(input, after, seed);
      rawReader = reserve(input, after, seed);
      const restoration = preparePositionedStoreRestore(
        binding,
        input,
        [policy] as const,
        seed,
        reader,
        { initial: 99 },
      );
      readerTransferred = true;
      const rawRestoration = preparePositionedStoreRestore(
        rawGraph.api.stream,
        input,
        [policy] as const,
        seed,
        rawReader,
        { initial: 99 },
      );
      rawReaderTransferred = true;
      const restoredSession = binding.session(restoration.source);
      expect(restoration.source.available.get()).toBe(true);
      expect(restoration.source.value.get()).toBe(2);
      expect(restoration.projections[0].value.get()).toBe(12);
      expect(rawRestoration.source.available.get()).toBe(true);
      expect(rawRestoration.source.value.get()).toBe(2);
      expect(rawRestoration.projections[0].value.get()).toBe(12);
      expect(openings).toHaveLength(1);

      await graph.reset();
      const detached = await restoredSession.detached;

      expect(openings).toHaveLength(1);
      expect(detached.status).toBe("closed");
      expect(detached.terminalCapture).toMatchObject({
        kind: "captured",
        capture: { kind: "stable", present: true, value: 2 },
      });
      expect(restoration.source.available.get()).toBe(true);
      expect(restoration.source.value.get()).toBe(2);
      expect(restoration.source.status.get()).toBe("closed");
      expect(restoration.source.pending.get()).toBe(false);
      expect(restoration.projections[0].value.get()).toBe(10);
      expect(isPositionedStoreCheckpointCurrent(checkpoint)).toBe(false);
      expect(releaseCount(reader)).toBe(1);
      expect(() => restoration.activate()).toThrow(TypeError);

      await rawGraph.reset();
      expect(rawRestoration.source.available.get()).toBe(true);
      expect(rawRestoration.source.value.get()).toBe(99);
      expect(rawRestoration.projections[0].value.get()).toBe(10);
      expect(releaseCount(rawReader)).toBe(1);
      expect(() => rawRestoration.activate()).toThrow(TypeError);
      expect(openings).toHaveLength(1);
    } finally {
      if (reader !== undefined && !readerTransferred && releaseCount(reader) === 0)
        releaseReader(reader);
      if (rawReader !== undefined && !rawReaderTransferred && releaseCount(rawReader) === 0)
        releaseReader(rawReader);
      projection.close();
      context.dispose();
      graph.dispose();
      rawGraph.dispose();
    }
  });

  it("clears retained values and errors from terminal projections without reopening them", async () => {
    const backend = testBackend();
    let resetDuringFinish: Promise<void> | undefined;
    let requestResetDuringFinish = (): void => undefined;
    const completedStream = createStream({
      name: "reset.completed-projection",
      key: () => "reset/completed-projection",
      open: backend.stream<void, number>("reset.completed-projection"),
    });
    const failedStream = createStream({
      name: "reset.failed-projection",
      key: () => "reset/failed-projection",
      open: backend.stream<void, number>("reset.failed-projection"),
    });
    const reentrantOpen = backend.stream<void, number>("reset.reentrant-projection-failure");
    const reentrantStream = createStream({
      name: "reset.reentrant-projection-failure",
      key: () => "reset/reentrant-projection-failure",
      open: (input: void, options) => {
        options.abortSignal?.addEventListener("abort", requestResetDuringFinish, { once: true });
        return reentrantOpen(input, options);
      },
    });
    const graph = createGraph({
      contract: defineContract({
        namespace: "reset-terminal-projections",
        operations: { completedStream, failedStream, reentrantStream },
      }),
    });
    requestResetDuringFinish = () => {
      resetDuringFinish = graph.reset();
    };
    const completedReadables = (() => {
      const projection = project(graph, completedStream, undefined, {
        kind: "accumulate",
        max: 4,
      });
      return { value: projection.value, status: projection.status, error: projection.error };
    })();
    const failedReadables = (() => {
      const projection = project(graph, failedStream, undefined, {
        kind: "reduce",
        initial: 10,
        step: (total, item) => total + item,
      });
      return { value: projection.value, status: projection.status, error: projection.error };
    })();
    const waitForStatus = (
      status: typeof completedReadables.status,
      expected: "closed" | "failed",
    ): Promise<void> =>
      new Promise<void>((resolve) => {
        let stop = (): void => undefined;
        stop = subscribe(status, (value) => {
          if (value !== expected) return;
          stop();
          resolve();
        });
      });
    const failure = new Error("terminal projection source failed");
    const completedStatus = waitForStatus(completedReadables.status, "closed");
    const failedStatus = waitForStatus(failedReadables.status, "failed");

    backend.emit("reset.completed-projection", 2);
    backend.emit("reset.completed-projection", 3);
    backend.end("reset.completed-projection");
    backend.emit("reset.failed-projection", 4);
    backend.end("reset.failed-projection", failure);
    await Promise.all([completedStatus, failedStatus]);
    expect(completedReadables.value.get()).toEqual([2, 3]);
    expect(completedReadables.status.get()).toBe("closed");
    expect(completedReadables.error.get()).toBeUndefined();
    expect(failedReadables.value.get()).toBe(14);
    expect(failedReadables.status.get()).toBe("failed");
    expect(failedReadables.error.get()).toBe(failure);
    expect(backend.calls).toHaveLength(2);

    await expect(graph.reset()).resolves.toBeUndefined();
    expect(completedReadables.value.get()).toEqual([]);
    expect(completedReadables.status.get()).toBe("closed");
    expect(completedReadables.error.get()).toBeUndefined();
    expect(failedReadables.value.get()).toBe(10);
    expect(failedReadables.status.get()).toBe("failed");
    expect(failedReadables.error.get()).toBeUndefined();
    expect(backend.calls).toHaveLength(2);

    const reentrantReadables = (() => {
      const projection = project(graph, reentrantStream, undefined, {
        kind: "reduce",
        initial: 20,
        step: (total, item) => total + item,
      });
      return { value: projection.value, status: projection.status, error: projection.error };
    })();
    const reentrantFailure = new Error("terminal reset should clear this failure");
    const reentrantStatus = waitForStatus(reentrantReadables.status, "failed");
    const previousResetVersion = graph.resetVersion.get();
    backend.emit("reset.reentrant-projection-failure", 5);
    backend.end("reset.reentrant-projection-failure", reentrantFailure);
    await reentrantStatus;
    expect(resetDuringFinish).toBeDefined();
    await expect(resetDuringFinish!).resolves.toBeUndefined();
    expect(graph.resetVersion.get()).toBe(previousResetVersion + 1);
    expect(reentrantReadables.value.get()).toBe(20);
    expect(reentrantReadables.status.get()).toBe("failed");
    expect(reentrantReadables.error.get()).toBeUndefined();
    expect(backend.calls).toHaveLength(3);

    graph.dispose();
  });

  it("restarts observed streams that ended or failed before reset", async () => {
    const backend = testBackend();
    const stream = createStream({
      name: "reset.terminal-stream",
      key: () => "reset/terminal-stream",
      open: backend.stream<void, number>("reset.terminal-stream"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "reset-terminal-stream", operations: { stream } }),
    });
    const store = graph.api.stream(undefined);
    const stop = subscribe(store.status, () => undefined);

    backend.emit("reset.terminal-stream", 1);
    await flush();
    backend.end("reset.terminal-stream");
    await flush();
    expect(store.status.get()).toBe("closed");
    expect(store.value.get()).toBe(1);

    const endedReset = graph.reset();
    expect(backend.calls).toHaveLength(2);
    expect(store.status.get()).toBe("opening");
    expect(store.value.get()).toBeUndefined();
    await expect(endedReset).resolves.toBeUndefined();

    const failure = new Error("stream failed before reset");
    backend.end("reset.terminal-stream", failure);
    await flush();
    expect(store.status.get()).toBe("failed");
    expect(store.error.get()).toBe(failure);

    const failedReset = graph.reset();
    expect(backend.calls).toHaveLength(3);
    expect(store.status.get()).toBe("opening");
    expect(store.error.get()).toBeUndefined();
    expect(store.value.get()).toBeUndefined();
    await expect(failedReset).resolves.toBeUndefined();

    stop();
    graph.dispose();
  });

  it("does not create an iterator when stream.open resets its graph", async () => {
    let startReset = (): Promise<void> => Promise.resolve();
    let nestedReset: Promise<void> | undefined;
    let openCalls = 0;
    let iteratorCalls = 0;
    const stream = createStream({
      name: "reset.reentrant-open",
      key: () => "reset/reentrant-open",
      open: () => {
        openCalls += 1;
        if (openCalls === 2) nestedReset = startReset();
        return {
          [Symbol.asyncIterator]: () => {
            iteratorCalls += 1;
            return {
              next: () => new Promise<IteratorResult<number>>(() => undefined),
              return: async () => ({ done: true, value: undefined }),
            };
          },
        };
      },
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "reset-reentrant-open", operations: { stream } }),
    });
    startReset = () => graph.reset();
    const store = graph.api.stream(undefined);
    const stop = subscribe(store.status, () => undefined);
    expect(iteratorCalls).toBe(1);

    const firstReset = graph.reset();
    const firstResult = firstReset.then(
      () => ({ ok: true as const }),
      (error: unknown) => ({ ok: false as const, error }),
    );
    expect(await firstResult).toMatchObject({ ok: false, error: { name: "AbortError" } });
    await expect(nestedReset).resolves.toBeUndefined();
    expect(openCalls).toBe(3);
    expect(iteratorCalls).toBe(2);

    stop();
    graph.dispose();
  });

  it("retires an iterator whose factory resets its graph before consumption", async () => {
    let startReset = (): Promise<void> => Promise.resolve();
    let nestedReset: Promise<void> | undefined;
    let openCalls = 0;
    let iteratorCalls = 0;
    let staleIteratorReturns = 0;
    const stream = createStream({
      name: "reset.reentrant-iterator",
      key: () => "reset/reentrant-iterator",
      open: () => {
        openCalls += 1;
        return {
          [Symbol.asyncIterator]: () => {
            iteratorCalls += 1;
            if (iteratorCalls === 2) nestedReset = startReset();
            return {
              next: () => new Promise<IteratorResult<number>>(() => undefined),
              return: async () => {
                staleIteratorReturns += 1;
                return { done: true, value: undefined };
              },
            };
          },
        };
      },
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "reset-reentrant-iterator", operations: { stream } }),
    });
    startReset = () => graph.reset();
    const store = graph.api.stream(undefined);
    const stop = subscribe(store.status, () => undefined);

    const firstReset = graph.reset();
    const firstResult = firstReset.then(
      () => ({ ok: true as const }),
      (error: unknown) => ({ ok: false as const, error }),
    );
    expect(await firstResult).toMatchObject({ ok: false, error: { name: "AbortError" } });
    await expect(nestedReset).resolves.toBeUndefined();
    expect(openCalls).toBe(3);
    expect(iteratorCalls).toBe(3);
    expect(staleIteratorReturns).toBe(2);

    stop();
    graph.dispose();
  });

  it("clears and reconnects a shared projection handle through the restarted session", async () => {
    const backend = testBackend();
    const stream = createStream({
      name: "reset.projection-stream",
      key: () => "reset/projection",
      open: backend.stream<void, number>("reset.projection-stream"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "reset-projection", operations: { stream } }),
    });
    const source = graph.api.stream(undefined);
    const projection = project(source, { kind: "accumulate", max: 5 });
    backend.emit("reset.projection-stream", 1);
    await flush();
    expect(projection.value.get()).toEqual([1]);

    const reset = graph.reset();
    await expect(reset).resolves.toBeUndefined();
    expect(projection.value.get()).toEqual([]);
    expect(backend.calls).toHaveLength(2);
    backend.emit("reset.projection-stream", 2);
    await flush();
    expect(projection.value.get()).toEqual([2]);

    projection.close();
    graph.dispose();
  });

  it("reopens an independent projection and ignores its retired iterator", async () => {
    const sessions: Array<{
      readonly abortSignal: AbortSignal;
      readonly pending: Array<(result: IteratorResult<number>) => void>;
    }> = [];
    const stream = createStream({
      name: "reset.independent-projection",
      key: () => "reset/independent-projection",
      open: (_input, { abortSignal }) => {
        const session = {
          abortSignal,
          pending: [] as Array<(result: IteratorResult<number>) => void>,
        };
        sessions.push(session);
        const iterator: AsyncIterator<number> = {
          next: () => new Promise((resolve) => session.pending.push(resolve)),
          return: async () => ({ done: true, value: undefined }),
        };
        return { [Symbol.asyncIterator]: () => iterator };
      },
    });
    const graph = createGraph({
      contract: defineContract({
        namespace: "reset-independent-projection",
        operations: { stream },
      }),
    });
    const projection = project(graph, stream, undefined, { kind: "accumulate", max: 5 });
    expect(sessions).toHaveLength(1);
    sessions[0]!.pending[0]!({ done: false, value: 1 });
    await flush();
    expect(projection.value.get()).toEqual([1]);

    const reset = graph.reset();
    expect(sessions).toHaveLength(2);
    expect(sessions[0]!.abortSignal.aborted).toBe(true);
    expect(projection.status.get()).toBe("open");
    expect(projection.value.get()).toEqual([]);
    await expect(reset).resolves.toBeUndefined();

    expect(sessions[0]!.pending).toHaveLength(2);
    sessions[0]!.pending[1]!({ done: false, value: 99 });
    await flush();
    expect(projection.value.get()).toEqual([]);
    sessions[1]!.pending[0]!({ done: false, value: 2 });
    await flush();
    expect(projection.value.get()).toEqual([2]);

    projection.close();
    graph.dispose();
  });

  it("does not publish a projection reducer result after the reducer resets its graph", async () => {
    const sessions: Array<{
      readonly abortSignal: AbortSignal;
      readonly pending: Array<(result: IteratorResult<number>) => void>;
    }> = [];
    const stream = createStream({
      name: "reset.reentrant-projection",
      key: () => "reset/reentrant-projection",
      open: (_input, { abortSignal }) => {
        const session = {
          abortSignal,
          pending: [] as Array<(result: IteratorResult<number>) => void>,
        };
        sessions.push(session);
        const iterator: AsyncIterator<number> = {
          next: () => new Promise((resolve) => session.pending.push(resolve)),
          return: async () => ({ done: true, value: undefined }),
        };
        return { [Symbol.asyncIterator]: () => iterator };
      },
    });
    let startReset = (): Promise<void> => Promise.resolve();
    let reset: Promise<void> | undefined;
    let resetFromReducer = false;
    const graph = createGraph({
      contract: defineContract({
        namespace: "reset-reentrant-projection",
        operations: { stream },
      }),
    });
    startReset = () => graph.reset();
    const projection = project(graph, stream, undefined, {
      kind: "reduce",
      initial: 0,
      step: (sum, item) => {
        if (!resetFromReducer) {
          resetFromReducer = true;
          reset = startReset();
        }
        return sum + item;
      },
    });
    sessions[0]!.pending[0]!({ done: false, value: 5 });
    await flush();
    await expect(reset).resolves.toBeUndefined();
    expect(sessions).toHaveLength(2);
    expect(sessions[0]!.abortSignal.aborted).toBe(true);
    expect(projection.value.get()).toBe(0);

    sessions[1]!.pending[0]!({ done: false, value: 3 });
    await flush();
    expect(projection.value.get()).toBe(3);

    projection.close();
    graph.dispose();
  });

  it("clears a terminal projection when only its status and error readables are held", async () => {
    const backend = testBackend();
    const stream = createStream({
      name: "reset.held-terminal-readables",
      key: () => "reset/held-terminal-readables",
      open: backend.stream<void, number>("reset.held-terminal-readables"),
    });
    const graph = createGraph({
      contract: defineContract({
        namespace: "reset-held-terminal-readables",
        operations: { stream },
      }),
    });
    const held = (() => {
      const projection = project(graph, stream, undefined, {
        kind: "reduce",
        initial: 10,
        step: (total, item) => total + item,
      });
      return { status: projection.status, error: projection.error };
    })();
    const failure = new Error("terminal projection failed");

    backend.end("reset.held-terminal-readables", failure);
    await flush();
    expect(held.status.get()).toBe("failed");
    expect(held.error.get()).toBe(failure);

    await expect(graph.reset()).resolves.toBeUndefined();
    expect(held.status.get()).toBe("failed");
    expect(held.error.get()).toBeUndefined();
    graph.dispose();
  });

  it("releases terminal projection registry entries after their readables are collected", async () => {
    const contractModule = new URL("../../src/contract/index.ts", import.meta.url).href;
    const source = `
      const { createGraph, createStream, defineContract, project } = await import(${JSON.stringify(contractModule)});
      const retainedError = new Error("retained terminal projection");
      const endedStream = createStream({
        name: "reset.projection-finalizer-ended",
        key: () => "reset/projection-finalizer-ended",
        open: async function* () {},
      });
      const failedStream = createStream({
        name: "reset.projection-finalizer-failed",
        key: () => "reset/projection-finalizer-failed",
        open: () => { throw retainedError; },
      });
      const graph = createGraph({
        contract: defineContract({
          namespace: "reset-projection-finalizer",
          operations: { endedStream, failedStream },
        }),
      });
      const retained = project(graph, failedStream, undefined, { kind: "accumulate", max: 2 });
      if (retained.status.get() !== "failed" || retained.error.get() !== retainedError) {
        throw new Error("retained terminal projection did not fail as expected");
      }
      const registry = graph.__runtime.projections;
      async function makeCloseAndDropProjection() {
        const projection = project(graph, endedStream, undefined, { kind: "accumulate", max: 2 });
        await new Promise((resolve) => setImmediate(resolve));
        if (projection.status.get() !== "closed") throw new Error("projection did not close");
        projection.close();
      }
      try {
        for (let index = 0; index < 24; index += 1) await makeCloseAndDropProjection();
        if (registry.size <= 1) throw new Error("test did not register dropped projections");
        for (let attempt = 0; attempt < 24 && registry.size > 1; attempt += 1) {
          Bun.gc(true);
          await new Promise((resolve) => setImmediate(resolve));
        }
        if (registry.size !== 1) throw new Error("expected one retained terminal projection, found " + registry.size);
        await graph.reset();
        if (retained.status.get() !== "failed" || retained.error.get() !== undefined) {
          throw new Error("reset did not clear the retained terminal projection error");
        }
      } finally {
        graph.dispose();
      }
    `;
    const child = Bun.spawnSync([process.execPath, "-e", source], {
      stdout: "pipe",
      stderr: "pipe",
    });
    const childError = new TextDecoder().decode(child.stderr);
    if (child.exitCode !== 0) throw new Error(childError || `GC child exited ${child.exitCode}`);
  });

  it("does not make a computed that closes a projection depend on resetVersion", async () => {
    const stream = createStream({
      name: "reset.close-tracking",
      key: () => "reset/close-tracking",
      open: () => ({
        [Symbol.asyncIterator]: () => ({
          next: () => new Promise<IteratorResult<number>>(() => undefined),
          return: async () => ({ done: true, value: undefined }),
        }),
      }),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "reset-close-tracking", operations: { stream } }),
    });
    const projection = project(graph, stream, undefined, { kind: "accumulate", max: 4 });
    let evaluations = 0;
    const closeAndCount = computed(() => {
      evaluations += 1;
      projection.close();
      return evaluations;
    });

    expect(closeAndCount.get()).toBe(1);
    await graph.reset();
    expect(closeAndCount.get()).toBe(1);
    expect(evaluations).toBe(1);

    graph.dispose();
  });
});

it("keeps a published transfer exclusion across reset until its original mutation retires", async () => {
  const backend = testBackend();
  const query = createQuery({
    name: "transfer-reset.query",
    key: (input: string) => input,
    fetch: backend.respond<string, number>("transfer-reset.query"),
  });
  const mutate = createMutation({
    name: "transfer-reset.mutate",
    run: backend.perform<object, void>("transfer-reset.mutate"),
    affects: [],
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "transfer-reset", operations: { query, mutate } }),
  });
  const codec: RequiredTransferCodec = {
    encodeValue: (value) => value as ClosedEncodedValue,
    encodeFailure: (cause) => String(cause),
    decodeValue: (value) => value,
    decodeFailure: (failure) => new Error(String(failure)),
  };
  let mutationResult: Promise<unknown> | undefined;
  let mutationResolved = false;
  let mutationCall: (typeof backend.calls)[number] | undefined;
  try {
    const store = graph.api.query("one");
    const initialReady = store.ready;
    backend.resolve(backend.calls[0]!, 1);
    await initialReady;

    const scope = createTransferScope(graph, graph.api.query, store);
    const carrier = Object.freeze({ request: "held" });
    const operation = prepareTransferOperation(
      [scope],
      [{ role: "raw", source: graph.api.mutate, input: carrier }],
    );
    const operationCommit = operation.seal();
    if (operationCommit === undefined) throw new Error("The transfer operation did not seal");
    operationCommit.publish();
    operationCommit.notifications.deliver();

    mutationResult = graph.api.mutate(carrier);
    mutationCall = backend.calls[1]!;
    expect(mutationCall.input).toBe(carrier);
    const reset = graph.reset();
    expect(mutationCall.aborted).toBe(true);
    await reset;

    const resetStore = graph.api.query("one");
    const refreshed = resetStore.ready;
    const refreshCall = backend.calls[2]!;
    backend.resolve(refreshCall, 2);
    await refreshed;
    const value = computed(() => resetStore.value.get());
    expect(value.get()).toBe(2);
    const construction = captureReactiveConstruction(value);
    const captureOptions = {
      graphs: [graph],
      roots: [{ source: value, construction }],
      codec,
    } as const;

    backend.resolve(mutationCall, undefined);
    mutationResolved = true;
    await mutationResult;
    expect(() => prepareRequiredTransferCapture(captureOptions)).toThrow(TypeError);

    const retirement = prepareTransferOperationRetirement(operation.operation);
    const retirementCommit = retirement.seal();
    if (retirementCommit === undefined) throw new Error("The transfer retirement did not seal");
    retirementCommit.publish();
    retirementCommit.notifications.deliver();
    await retirement.retired;

    const capture = prepareRequiredTransferCapture(captureOptions);
    capture.discard();
    await capture.retired;
  } finally {
    if (mutationCall !== undefined && mutationResult !== undefined && !mutationResolved) {
      backend.resolve(mutationCall, undefined);
      mutationResolved = true;
      await mutationResult.then(
        () => undefined,
        () => undefined,
      );
    }
    graph.dispose();
  }
});
