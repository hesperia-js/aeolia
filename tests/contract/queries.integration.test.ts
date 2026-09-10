import { afterEach, beforeEach, describe, expect, it, jest } from "bun:test";
import { Fault, onFault } from "../../src/fault.ts";
import { watch } from "../../src/reactive.ts";
import { createGraph } from "../../src/contract/index.ts";
import { snapshot } from "../../src/realm.ts";
import {
  affects,
  defineContract,
  createMutation,
  createQuery,
  createStore,
} from "../../src/contract/index.ts";
import { testBackend } from "../../src/testing.ts";

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
}

function userQuery(backend: ReturnType<typeof testBackend>) {
  return createQuery({
    name: "users.get",
    key: (input: { readonly id: string }) => `users/${input.id}`,
    fetch: backend.respond<{ readonly id: string }, { readonly id: string }>("users.get"),
  });
}

describe("query stores and freshness", () => {
  it("uses member calls as the fetch trigger and keeps reads inert", async () => {
    const backend = testBackend();
    const query = userQuery(backend);
    const graph = createGraph({
      contract: defineContract({ namespace: "operations", operations: { query } }),
    });
    const reached = graph.at("users/read-only");
    expect(reached.value.get()).toBeUndefined();
    expect(reached.value.peek()).toBeUndefined();
    await flush();
    expect(backend.calls).toHaveLength(0);

    const first = graph.api.query({ id: "read-only" });
    expect(backend.calls).toHaveLength(1);
    expect(first.status.get()).toBe("fetching");
    expect(first.pending.get()).toBe(true);
    expect(first.value.get()).toBeUndefined();
    expect(backend.calls).toHaveLength(1);
    backend.resolve(backend.calls[0]!, { id: "read-only" });
    await flush();
    expect(first.value.get()).toEqual({ id: "read-only" });
    expect(first.status.get()).toBe("ready");
    graph.dispose();
  });

  it("shares one store/request while retaining per-caller status and pending", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "users.get",
      key: (input: { id: string }) => `users/${input.id}`,
      revalidateAfterMs: 50,
      fetch: backend.respond("users.get"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "operations", operations: { query } }),
    });
    const short = graph.api.query({ id: "7" }, { revalidateAfterMs: 5 });
    const long = graph.api.query({ id: "7" }, { revalidateAfterMs: 50 });
    expect(short.value).toBe(long.value);
    expect(short.key).toBe(long.key);
    expect(short.pending).not.toBe(long.pending);
    expect(short.status).not.toBe(long.status);
    expect(backend.calls).toHaveLength(1);
    backend.resolve(backend.calls[0]!, { id: "7" });
    await flush();
    jest.advanceTimersByTime(6);
    expect(short.status.get()).toBe("stale");
    expect(short.pending.get()).toBe(false);
    expect(long.status.get()).toBe("ready");
    // Reading the shared value does not choose a caller or issue a request.
    expect(long.value.get()).toEqual({ id: "7" });
    expect(backend.calls).toHaveLength(1);
    const refresh = short.revalidate();
    expect(short.status.get()).toBe("revalidating");
    expect(short.pending.get()).toBe(true);
    expect(long.status.get()).toBe("revalidating");
    expect(long.pending.get()).toBe(true);
    backend.resolve(backend.calls[1]!, { id: "8" });
    await refresh;
    graph.dispose();
  });

  it("retains different windows when two callers reuse the same input object", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "identity-window.query",
      key: (input: { id: string }) => input.id,
      fetch: backend.respond("identity-window.query"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "operations", operations: { query } }),
    });
    const input = { id: "same-object" };
    const slow = graph.api.query(input, { revalidateAfterMs: 100 });
    const fast = graph.api.query(input, { revalidateAfterMs: 10 });
    backend.resolve(backend.calls[0]!, { id: "base" });
    await flush();
    const stop = watch(slow.value, () => undefined);

    jest.advanceTimersByTime(11);

    expect(backend.calls).toHaveLength(2);
    expect(fast.status.get()).toBe("revalidating");
    stop();
    graph.dispose();
  });

  it("publishes each caller window without fetching when value is not live", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "window-status.query",
      key: () => "window-status",
      fetch: backend.respond<undefined, number>("window-status.query"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "operations", operations: { query } }),
    });
    const short = graph.api.query(undefined, { revalidateAfterMs: 5 });
    const long = graph.api.query(undefined, { revalidateAfterMs: 50 });
    backend.resolve(backend.calls[0]!, 1);
    await flush();

    jest.advanceTimersByTime(6);
    expect(short.status.get()).toBe("stale");
    expect(long.status.get()).toBe("ready");
    expect(backend.calls).toHaveLength(1);

    jest.advanceTimersByTime(44);
    expect(long.status.get()).toBe("stale");
    expect(backend.calls).toHaveLength(1);
    graph.dispose();
  });

  it("treats omitted value identity options as no opinion", () => {
    const backend = testBackend();
    const query = createQuery({
      name: "identity.query",
      key: () => "identity",
      fetch: backend.respond("identity.query"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "operations", operations: { query } }),
    });
    graph.api.query(undefined, { initial: 1 });
    expect(() => graph.api.query(undefined, { snapshot: false })).not.toThrow();
    expect(snapshot(graph).entries).not.toHaveProperty("identity");
    graph.dispose();
  });

  it("rejects a first caller that contradicts query-level value identity", () => {
    const backend = testBackend();
    const declaredEquals = (left: number, right: number) => left === right;
    const callerEquals = (left: number, right: number) => left === right;
    const query = createQuery({
      name: "identity-conflict.query",
      key: () => "identity-conflict",
      equals: declaredEquals,
      snapshot: true,
      fetch: backend.respond<undefined, number>("identity-conflict.query"),
    });
    const contract = defineContract({ namespace: "operations", operations: { query } });

    const equalsGraph = createGraph({ contract });
    expect(() => equalsGraph.api.query(undefined, { equals: callerEquals })).toThrow("equals");
    equalsGraph.dispose();

    const snapshotGraph = createGraph({ contract });
    expect(() => snapshotGraph.api.query(undefined, { snapshot: false })).toThrow("snapshot");
    snapshotGraph.dispose();
  });

  it("allows Store.update as a watcher write", () => {
    const local = createStore({ name: "watcher-update", initial: 0 });
    const graph = createGraph({
      contract: defineContract({ namespace: "operations", operations: { local } }),
    });
    const store = graph.api.local();
    let updates = 0;
    const stop = watch(store.value, () => {
      if (updates > 0) return;
      updates += 1;
      store.update((current) => (current ?? 0) + 1);
    });

    store.set(1);

    expect(store.value.get()).toBe(2);
    expect(updates).toBe(1);
    stop();
    graph.dispose();
  });

  it("hands Store.update the committed value beneath an outstanding prediction", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "counter.get",
      key: () => "counter",
      fetch: backend.respond<undefined, number>("counter.get"),
    });
    const mutation = createMutation({
      name: "counter.increment",
      affects: [
        affects(query, {
          select: (_input: undefined) => undefined,
          on: "invalidate",
          optimistic: (current) => (current ?? 0) + 1,
        }),
      ],
      run: backend.perform<undefined, void>("counter.increment"),
    });

    const graph = createGraph({
      contract: defineContract({ namespace: "operations", operations: { query, mutation } }),
    });
    const store = graph.api.query(undefined);
    store.set(1);
    const settlement = graph.api.mutation(undefined);
    expect(store.value.get()).toBe(2);
    let received: number | undefined;

    store.update((current) => {
      received = current;
      return (current ?? 0) + 10;
    });

    expect(received).toBe(1);
    expect(store.value.get()).toBe(12);
    backend.reject(
      backend.calls.find((call) => call.name === "counter.increment")!,
      new Error("not committed"),
    );
    await expect(settlement).rejects.toThrow("not committed");
    expect(store.value.get()).toBe(11);
    graph.dispose();
  });

  it("uses the most recent fresh member call as the timer fetch source", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "pages.get",
      key: (_input: { page: number }) => "pages/shared",
      revalidateAfterMs: 10,
      fetch: backend.respond<{ page: number }, number>("pages.get"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "operations", operations: { query } }),
    });
    const first = graph.api.query({ page: 1 }, { initial: 0 });
    graph.api.query({ page: 2 });
    expect(backend.calls).toHaveLength(0);
    const stop = watch(first.value, () => undefined);

    jest.advanceTimersByTime(11);

    expect(backend.calls).toHaveLength(1);
    expect(backend.calls[0]?.input).toEqual({ page: 2 });
    stop();
    graph.dispose();
  });

  it("revalidates on a value watch at the shortest live window and uses the latest source", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "users.get",
      key: (input: { id: string }) => `users/${input.id}`,
      revalidateAfterMs: 10,
      fetch: backend.respond("users.get"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "operations", operations: { query } }),
    });
    const first = graph.api.query({ id: "first" });
    backend.resolve(backend.calls[0]!, { id: "first" });
    await flush();
    const stop = watch(first.value, () => undefined);
    await flush();
    jest.advanceTimersByTime(11);
    expect(backend.calls).toHaveLength(2);
    expect(backend.calls[1]?.input).toEqual({ id: "first" });
    backend.resolve(backend.calls[1]!, { id: "fresh" });
    await flush();
    expect(first.value.get()).toEqual({ id: "fresh" });
    jest.advanceTimersByTime(11);
    expect(backend.calls).toHaveLength(3);
    expect(backend.calls[2]?.input).toEqual({ id: "first" });
    stop();
    graph.dispose();
  });

  it("revalidates immediately when a live query's freshness window is zero", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "zero-window",
      key: () => "zero-window",
      revalidateAfterMs: 0,
      fetch: backend.respond<undefined, number>("zero-window"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "operations", operations: { query } }),
    });
    const store = graph.api.query(undefined);
    expect(backend.calls).toHaveLength(1);
    const stop = watch(store.value, () => undefined);

    backend.resolve(backend.calls[0]!, 1);
    await flush();
    jest.advanceTimersByTime(0);

    expect(backend.calls).toHaveLength(2);
    expect(store.status.get()).toBe("revalidating");

    stop();
    graph.dispose();
  });

  it("drops an idle query's contents but keeps its Store and held QueryStore usable", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "users.get",
      key: (input: { id: string }) => `users/${input.id}`,
      fetch: backend.respond("users.get"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "operations", operations: { query } }),
      idleMs: 10,
    });
    const held = graph.api.query({ id: "collect" });
    const base = graph.at("users/collect");
    backend.resolve(backend.calls[0]!, { id: "collect" });
    await flush();
    const stop = watch(held.value, () => undefined);
    stop();
    await flush();
    jest.advanceTimersByTime(11);
    expect(held.value.get()).toBeUndefined();
    expect(graph.at("users/collect")).toBe(base);

    const revalidation = held.revalidate();
    expect(backend.calls).toHaveLength(2);
    backend.resolve(backend.calls[1]!, { id: "refilled" });
    await revalidation;
    await flush();
    expect(held.value.get()).toEqual({ id: "refilled" });
    graph.dispose();
  });

  it("revalidation supersedes, aborts, and discards an older request", async () => {
    const backend = testBackend();
    const query = userQuery(backend);
    const graph = createGraph({
      contract: defineContract({ namespace: "operations", operations: { query } }),
    });
    const store = graph.api.query({ id: "supersede" });
    const first = backend.calls[0]!;
    const firstRevalidate = store.revalidate();
    await flush();
    const second = backend.calls[1]!;
    expect(first.aborted).toBe(true);
    backend.reject(first, new Error("superseded"));
    await flush();
    expect(store.status.get()).toBe("fetching");
    backend.resolve(second, { id: "new" });
    await firstRevalidate;
    await flush();
    expect(store.value.get()).toEqual({ id: "new" });
    graph.dispose();
  });

  it("fulfils explicit revalidation even when its backend call fails", async () => {
    const backend = testBackend();
    const query = userQuery(backend);
    const graph = createGraph({
      contract: defineContract({ namespace: "operations", operations: { query } }),
    });
    const store = graph.api.query({ id: "failure" });
    const initial = backend.calls[0]!;
    backend.resolve(initial, { id: "base" });
    await flush();
    const revalidation = store.revalidate();
    const call = backend.calls[1]!;
    const error = new Error("offline");
    backend.reject(call, error);
    await expect(revalidation).resolves.toBeUndefined();
    expect(store.error.get()).toBe(error);
    expect(store.status.get()).toBe("failed");
    expect(store.value.peek()).toEqual({ id: "base" });
    const retry = store.revalidate();
    expect(store.status.get()).toBe("revalidating");
    expect(store.pending.get()).toBe(true);
    backend.resolve(backend.calls[2]!, { id: "recovered" });
    await retry;
    expect(store.status.get()).toBe("ready");
    graph.dispose();
  });

  it("keeps an equal direct write over a late response without notifying value watchers", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "equal-direct",
      key: () => "equal-direct",
      revalidateAfterMs: 10,
      fetch: backend.respond<undefined, { readonly version: number }>("equal-direct"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "operations", operations: { query } }),
    });
    const local = { version: 1 };
    const store = graph.api.query(undefined, { initial: local });
    let notifications = 0;
    const stop = watch(store.value, () => {
      notifications += 1;
    });
    const refresh = store.revalidate();
    const call = backend.calls[0]!;
    store.set(local);
    expect(call.aborted).toBe(false);
    expect(notifications).toBe(0);
    expect(store.value.get()).toBe(local);
    expect(store.status.get()).toBe("ready");

    backend.resolve(call, { version: 2 });
    await refresh;

    expect(store.value.get()).toBe(local);
    expect(notifications).toBe(0);
    stop();
    graph.dispose();
  });

  it("restarts a live value's refetch deadline after a direct write", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "users.direct-deadline",
      key: (input: { id: string }) => input.id,
      revalidateAfterMs: 10,
      fetch: backend.respond("users.direct-deadline"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "operations", operations: { query } }),
    });
    const store = graph.api.query({ id: "direct-deadline" });
    backend.resolve(backend.calls[0]!, { id: "remote" });
    await flush();
    const stop = watch(store.value, () => undefined);

    jest.advanceTimersByTime(5);
    store.set({ id: "local" });
    jest.advanceTimersByTime(6);
    expect(backend.calls).toHaveLength(1);
    expect(store.value.get()).toEqual({ id: "local" });

    jest.advanceTimersByTime(4);
    expect(backend.calls).toHaveLength(2);
    expect(store.status.get()).toBe("revalidating");

    stop();
    graph.dispose();
  });

  it("aborts an unsettled request moved past by a later stale member call", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "users.moved-past",
      key: (input: { id: string }) => input.id,
      revalidateAfterMs: 10,
      fetch: backend.respond("users.moved-past"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "operations", operations: { query } }),
    });
    const store = graph.api.query({ id: "moved-past" });
    const first = backend.calls[0]!;
    store.set({ id: "local" });
    jest.setSystemTime(11);
    const refreshed = graph.api.query({ id: "moved-past" }, { revalidateAfterMs: 10 });
    expect(backend.calls).toHaveLength(2);
    expect(first.aborted).toBe(true);
    backend.resolve(first, { id: "late" });
    backend.resolve(backend.calls[1]!, { id: "refetched" });
    await flush();
    expect(refreshed.value.get()).toEqual({ id: "refetched" });
    graph.dispose();
  });

  it("routes a watcher-read fault from an async landing and continues later watchers", async () => {
    const backend = testBackend();
    const query = userQuery(backend);
    const graph = createGraph({
      contract: defineContract({ namespace: "operations", operations: { query } }),
      idleMs: 10,
    });
    const store = graph.api.query({ id: "watcher-read" });
    const faults: Fault[] = [];
    let laterWatcherCalls = 0;
    const stopFaults = onFault(graph, (fault) => faults.push(fault));
    const stop = watch(store.value, () => store.value.get());
    const stopSecond = watch(store.value, () => {
      laterWatcherCalls += 1;
    });

    jest.setSystemTime(7);
    backend.resolve(backend.calls[0]!, { id: "landed" });
    await flush();

    expect(faults.filter((fault) => fault.kind === "watcher-read")).toHaveLength(1);
    expect(laterWatcherCalls).toBe(1);
    expect(store.value.get()).toEqual({ id: "landed" });
    expect(store.status.get()).toBe("ready");
    stop();
    stopSecond();
    jest.advanceTimersByTime(9);
    await flush();
    expect(store.value.get()).toEqual({ id: "landed" });
    jest.advanceTimersByTime(10);
    await flush();
    expect(store.value.get()).toBeUndefined();
    stopFaults();
    graph.dispose();
  });

  it("reports raw watcher throws from async landings without wrapping or splitting them", async () => {
    const backend = testBackend();
    const query = userQuery(backend);
    const reported: unknown[] = [];
    const graph = createGraph({
      contract: defineContract({ namespace: "operations", operations: { query } }),
      onUnobservedFault: (error) => reported.push(error),
    });
    const store = graph.api.query({ id: "raw-watcher-throw" });
    const first = { watcher: 1 };
    const second = { watcher: 2 };
    const stopFirst = watch(store.value, () => {
      throw first;
    });
    const stopSecond = watch(store.value, () => {
      throw second;
    });

    backend.resolve(backend.calls[0]!, { id: "landed" });
    await flush();

    expect(reported).toHaveLength(1);
    expect(reported[0]).toBeInstanceOf(AggregateError);
    expect([...(reported[0] as AggregateError).errors]).toEqual([first, second]);

    stopFirst();
    stopSecond();
    graph.dispose();
  });
});
