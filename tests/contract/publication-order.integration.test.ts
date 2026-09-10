import { afterEach, beforeEach, describe, expect, it, jest } from "bun:test";
import {
  adopt,
  affects,
  createGraph,
  defineContract,
  createMutation,
  createQuery,
  createStore,
  createStream,
  snapshot,
  watch,
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

async function flush(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
}

describe("state-layer golden paths", () => {
  it("publishes Store.set and Store.update synchronously through value", () => {
    const count = createStore({ name: "count", initial: 0 });
    const graph = createGraph({
      contract: defineContract({ namespace: "store-write", operations: { count } }),
    });
    const store = graph.store(count);
    let notifications = 0;
    const stop = watch(store.value, () => {
      notifications += 1;
    });

    store.set(1);
    expect(store.value.get()).toBe(1);
    expect(notifications).toBe(1);

    store.update((committed) => (committed ?? 0) + 1);
    expect(store.value.get()).toBe(2);
    expect(notifications).toBe(2);

    store.set(2);
    expect(notifications).toBe(2);
    stop();
    graph.dispose();
  });

  it("recreates committed graph state by adopting a deterministic snapshot", () => {
    const alpha = createStore({ name: "alpha", initial: 0, snapshot: true });
    const zeta = createStore({ name: "zeta", initial: 0, snapshot: true });
    const contract = defineContract({
      namespace: "snapshot-round-trip",
      operations: { zeta, alpha },
    });
    const source = createGraph({ contract });
    source.store(zeta).set(9);
    source.store(alpha).set(1);

    const state = snapshot(source);
    expect(Object.keys(state.entries)).toEqual(["alpha", "zeta"]);

    const target = createGraph({ contract });
    const report = adopt(target, state);
    expect(report.skipped).toEqual([]);
    expect(target.store(alpha).value.get()).toBe(1);
    expect(target.store(zeta).value.get()).toBe(9);

    source.dispose();
    target.dispose();
  });

  it("invalidates or revalidates query stores only after successful mutations", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "items.get",
      key: (id: string) => `items/${id}`,
      fetch: backend.respond<string, { readonly id: string }>("items.get"),
    });
    const invalidate = createMutation({
      name: "items.invalidate",
      affects: [affects(query, { select: (id: string) => id, on: "invalidate" })],
      run: backend.perform<string, void>("items.invalidate"),
    });
    const revalidate = createMutation({
      name: "items.revalidate",
      affects: [affects(query, { select: (id: string) => id, on: "revalidate" })],
      run: backend.perform<string, void>("items.revalidate"),
    });
    const graph = createGraph({
      contract: defineContract({
        namespace: "mutation-effects",
        operations: { query, invalidate, revalidate },
      }),
    });
    const store = graph.api.query("one");
    backend.resolve(backend.calls[0]!, { id: "one" });
    await flush();

    const invalidation = graph.api.invalidate("one");
    backend.resolve(backend.calls[1]!, undefined);
    await invalidation;
    expect(store.status.get()).toBe("stale");
    expect(backend.calls).toHaveLength(2);

    const refresh = graph.api.revalidate("one");
    backend.resolve(backend.calls[2]!, undefined);
    await refresh;
    expect(backend.calls).toHaveLength(4);
    expect(store.status.get()).toBe("revalidating");
    backend.resolve(backend.calls[3]!, { id: "one" });
    await flush();
    expect(store.status.get()).toBe("ready");
    graph.dispose();
  });

  it("does not reactivate collected query shells through mutation effects", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "collected.get",
      key: (id: string) => `collected/${id}`,
      fetch: backend.respond<string, number>("collected.get"),
    });
    const invalidate = createMutation({
      name: "collected.invalidate",
      affects: [
        affects(query, {
          select: (id: string) => id,
          on: "invalidate",
          optimistic: (current) => (current ?? 0) + 1,
        }),
      ],
      run: backend.perform<string, void>("collected.invalidate"),
    });
    const revalidate = createMutation({
      name: "collected.revalidate",
      affects: [affects(query, { select: (id: string) => id, on: "revalidate" })],
      run: backend.perform<string, void>("collected.revalidate"),
    });
    const graph = createGraph({
      contract: defineContract({
        namespace: "collected-effects",
        operations: { query, invalidate, revalidate },
      }),
      idleMs: 5,
    });
    const held = graph.api.query("one");
    backend.resolve(backend.calls[0]!, 1);
    await flush();
    jest.advanceTimersByTime(5);
    await flush();
    expect(held.value.get()).toBeUndefined();

    const invalidation = graph.api.invalidate("one");
    expect(held.value.get()).toBeUndefined();
    backend.resolve(backend.calls[1]!, undefined);
    await invalidation;
    expect(backend.calls).toHaveLength(2);
    expect(held.value.get()).toBeUndefined();

    const refresh = graph.api.revalidate("one");
    backend.resolve(backend.calls[2]!, undefined);
    await refresh;
    expect(backend.calls).toHaveLength(3);
    expect(held.value.get()).toBeUndefined();
    graph.dispose();
  });

  it("does not create a missing query store through mutation effects", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "missing.query",
      key: (input: { id: string }) => `missing/${input.id}`,
      fetch: backend.respond("missing.query"),
    });
    const mutation = createMutation({
      name: "missing.mutation",
      affects: [
        affects(query, {
          select: (input: { id: string }) => input,
          on: "invalidate",
          optimistic: () => ({ id: "optimistic" }),
        }),
      ],
      run: backend.perform<{ id: string }, string>("missing.mutation"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "state-layer", operations: { query, mutation } }),
    });

    const settlement = graph.api.mutation({ id: "7" });
    const reached = graph.at("missing/7");
    expect(reached.value.get()).toBeUndefined();

    backend.resolve(backend.calls[0]!, "ok");
    await settlement;
    expect(reached.value.get()).toBeUndefined();
    graph.dispose();
  });

  it("re-arms freshness from a direct write and clears it when collection drops the store", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "clock.get",
      key: () => "clock",
      revalidateAfterMs: 10,
      fetch: backend.respond<void, number>("clock.get"),
    });
    const contract = defineContract({ namespace: "timer", operations: { query } });

    const liveGraph = createGraph({ contract, idleMs: 100 });
    const liveStore = liveGraph.api.query();
    backend.resolve(backend.calls[0]!, 1);
    await flush();
    const stop = watch(liveStore.value, () => undefined);
    jest.advanceTimersByTime(5);
    liveStore.set(2);
    jest.advanceTimersByTime(9);
    expect(backend.calls).toHaveLength(1);
    jest.advanceTimersByTime(1);
    expect(backend.calls).toHaveLength(2);
    stop();
    liveGraph.dispose();

    backend.clear();
    const collectedGraph = createGraph({ contract, idleMs: 5 });
    const collectedStore = collectedGraph.api.query();
    backend.resolve(backend.calls[0]!, 1);
    await flush();
    jest.advanceTimersByTime(6);
    await flush();
    expect(collectedStore.value.get()).toBeUndefined();
    jest.advanceTimersByTime(20);
    expect(backend.calls).toHaveLength(1);
    collectedGraph.dispose();
  });

  it("publishes timer staleness before starting the timer-driven revalidation", () => {
    const backend = testBackend();
    const query = createQuery({
      name: "timer-order.get",
      key: () => "timer-order",
      fetch: backend.respond<void, number>("timer-order.get"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "timer-order", operations: { query } }),
    });
    const store = graph.api.query(undefined, { initial: 1, revalidateAfterMs: 10 });
    const stopValue = watch(store.value, () => undefined);
    let statusNotifications = 0;
    const stopStatus = watch(store.status, () => {
      statusNotifications += 1;
    });

    jest.advanceTimersByTime(10);
    expect(backend.calls).toHaveLength(1);
    expect(store.status.get()).toBe("revalidating");
    expect(statusNotifications).toBe(2);

    stopStatus();
    stopValue();
    graph.dispose();
  });

  it("notifies a landed query value before its caller status", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "landing-order.get",
      key: () => "landing-order",
      fetch: backend.respond<void, number>("landing-order.get"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "landing-order", operations: { query } }),
    });
    const store = graph.api.query();
    const events: string[] = [];
    const stopValue = watch(store.value, () => {
      events.push("value");
    });
    const stopStatus = watch(store.status, () => {
      events.push("status");
    });

    backend.resolve(backend.calls[0]!, 1);
    await flush();
    expect(events).toEqual(["value", "status"]);
    expect(store.value.get()).toBe(1);
    expect(store.status.get()).toBe("ready");

    stopStatus();
    stopValue();
    graph.dispose();
  });

  it("applies stream emissions in source order and exposes the shared lifecycle", async () => {
    const backend = testBackend();
    const stream = createStream({
      name: "events",
      key: () => "events",
      open: backend.stream<void, number>("events"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "stream-order", operations: { stream } }),
    });
    const first = graph.api.stream();
    const second = graph.api.stream();
    expect(first.value).toBe(second.value);
    expect(first.status.get()).toBe("opening");

    backend.emit("events", 1);
    await flush();
    expect(first.value.get()).toBe(1);
    expect(first.status.get()).toBe("live");
    expect(second.status.get()).toBe("live");

    backend.emit("events", 2);
    backend.emit("events", 3);
    await flush();
    expect(first.value.get()).toBe(3);
    expect(first.pending.get()).toBe(false);
    graph.dispose();
  });
});
