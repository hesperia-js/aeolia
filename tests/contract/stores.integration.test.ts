import { afterEach, beforeEach, describe, expect, it, jest } from "bun:test";
import { Fault, onFault } from "../../src/fault.ts";
import { computed, watch } from "../../src/reactive.ts";
import { createGraph } from "../../src/contract/index.ts";
import { defineContract, createQuery, createStore } from "../../src/contract/index.ts";
import { testBackend } from "../../src/testing.ts";

beforeEach(() => {
  jest.useFakeTimers();
  jest.setSystemTime(0);
});

afterEach(() => {
  jest.clearAllTimers();
  jest.useRealTimers();
});

function contractWithStore<T, N extends string>(definition: ReturnType<typeof createStore<T, N>>) {
  return defineContract({ namespace: "graph-tests", operations: { local: definition } });
}

async function flush(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
}

describe("Aeolia graph and stores", () => {
  it("rejects invalid graph bounds as contract faults", () => {
    const contract = defineContract({ namespace: "graph-tests", operations: {} });
    for (const options of [
      { contract, idleMs: Number.POSITIVE_INFINITY },
      { contract, maxPredictions: 0 },
    ]) {
      try {
        createGraph(options);
        throw new Error("expected createGraph to reject its bound");
      } catch (error) {
        expect(error).toBeInstanceOf(Fault);
        expect((error as Fault).kind).toBe("contract");
      }
    }
  });

  it("shares declared stores through the API and graph.at", () => {
    const definition = createStore({ name: "cart", initial: ["seed"] as string[] });
    const graph = createGraph({ contract: contractWithStore(definition) });
    const fromApi = graph.api.local();
    const fromKey = graph.at("cart");

    expect(fromKey).toBe(fromApi);
    expect(String(fromApi.key)).toBe("cart");
    expect(fromApi.graph).toBe(graph.id);
    expect(fromApi.value.get()).toEqual(["seed"]);
    fromApi.update((current) => [...(current ?? []), "second"]);
    expect(fromApi.value.get()).toEqual(["seed", "second"]);
    graph.dispose();
  });

  it("creates an unwatched key store without fetching from reads", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "users.get",
      key: (input: { id: string }) => `users/${input.id}`,
      fetch: backend.respond("users.get"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "graph-tests", operations: { query } }),
    });
    const reached = graph.at("users/7");

    expect(reached.value.get()).toBeUndefined();
    expect(reached.value.peek()).toBeUndefined();
    await flush();
    expect(backend.calls).toHaveLength(0);
    reached.set({ id: "7" });
    expect(reached.value.get()).toEqual({ id: "7" });
    expect(graph.at("users/7")).toBe(reached);
    graph.dispose();
  });

  it("treats undefined boundaries as changes without calling custom equality", () => {
    let comparisons = 0;
    const definition = createStore<string | undefined, "optional">({
      name: "optional",
      initial: undefined,
      equals: (left, right) => {
        comparisons += 1;
        return left === right;
      },
    });
    const graph = createGraph({ contract: contractWithStore(definition) });
    const store = graph.store(definition);

    expect(store.value.get()).toBeUndefined();
    store.set("defined");
    expect(store.value.get()).toBe("defined");
    store.set(undefined);
    expect(store.value.get()).toBeUndefined();
    expect(comparisons).toBe(0);

    graph.dispose();
  });

  it("collects an idle shell by sweep while preserving Store identity", async () => {
    const graph = createGraph({
      contract: defineContract({ namespace: "graph-tests", operations: {} }),
      idleMs: 10,
    });
    const store = graph.at("temporary");
    store.set("value");
    await flush();
    jest.advanceTimersByTime(11);
    expect(store.value.get()).toBeUndefined();
    expect(graph.at("temporary")).toBe(store);
    graph.dispose();
  });

  it("does not collect a store with a live value binding", async () => {
    const definition = createStore({ name: "live", initial: 1 });
    const graph = createGraph({ contract: contractWithStore(definition), idleMs: 1 });
    const store = graph.store(definition);
    const stop = watch(store.value, () => undefined);
    await flush();
    jest.advanceTimersByTime(100);
    expect(store.value.get()).toBe(1);
    stop();
    graph.dispose();
  });

  it("keeps graph identities isolated and rejects mixed-graph computed reads", () => {
    const definition = createStore({ name: "local", initial: 0 });
    const contract = contractWithStore(definition);
    const left = createGraph({ contract });
    const right = createGraph({ contract });
    const leftStore = left.store(definition);
    const rightStore = right.store(definition);
    leftStore.set(7);

    expect(rightStore.value.get()).toBe(0);
    expect(leftStore.graph).not.toBe(rightStore.graph);
    expect(computed(() => leftStore.value.get()).get()).toBe(7);
    expect(() =>
      computed(() => Number(leftStore.value.get()) + Number(rightStore.value.get())).get(),
    ).toThrow("cross-graph");
    left.dispose();
    right.dispose();
  });

  it("routes equals faults to the graph channel and continues the write", () => {
    const definition = createStore({
      name: "faulty",
      initial: 1,
      equals: () => {
        throw new Error("bad comparator");
      },
    });
    const graph = createGraph({ contract: contractWithStore(definition) });
    const faults: Fault[] = [];
    const stopFaults = onFault(graph, (fault) => faults.push(fault));
    const store = graph.store(definition);
    store.set(2);

    expect(store.value.get()).toBe(2);
    expect(faults).toHaveLength(1);
    expect(faults[0]?.kind).toBe("equals");
    stopFaults();
    graph.dispose();
  });

  it("continues after a throwing graph fault observer and reports the observer error", () => {
    const marker = { observer: "threw" };
    const reported: unknown[] = [];
    let laterObserverRuns = 0;
    const local = createStore({
      name: "fault-observer",
      initial: 0,
      equals: () => {
        throw new Error("equals failed");
      },
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "graph-tests", operations: { local } }),
      onUnobservedFault: (error) => {
        reported.push(error);
      },
    });
    onFault(graph, () => {
      throw marker;
    });
    onFault(graph, () => {
      laterObserverRuns += 1;
    });

    graph.api.local().set(1);
    graph.api.local().set(2);

    expect(reported).toEqual([marker, marker]);
    expect(laterObserverRuns).toBe(2);
    graph.dispose();
  });

  it("disposes terminally, aborts calls, and faults held store reads", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "users.get",
      key: (input: { id: string }) => input.id,
      fetch: backend.respond("users.get"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "graph-tests", operations: { query } }),
    });
    const store = graph.api.query({ id: "7" });
    const call = backend.calls[0]!;
    graph.dispose();
    graph.dispose();

    expect(call.aborted).toBe(true);
    expect(() => store.value.get()).toThrow("disposed");
    expect(() => store.status.get()).toThrow("disposed");
    expect(() => store.pending.get()).toThrow("disposed");
    expect(() => store.error.get()).toThrow("disposed");
    expect(() => store.set({ id: "local" })).toThrow("disposed");
    backend.resolve(call, { id: "late" });
    await flush();
    expect(() => store.value.peek()).toThrow("disposed");
  });

  it("restarts collection after a moved-past request finally settles", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "collection.pending",
      key: (input: { id: string }) => input.id,
      fetch: backend.respond("collection.pending"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "graph-tests", operations: { query } }),
      idleMs: 10,
    });
    const store = graph.api.query({ id: "moved" });
    const call = backend.calls[0]!;
    store.set({ id: "local" });
    jest.advanceTimersByTime(5);
    backend.resolve(call, { id: "late" });
    await flush();
    jest.advanceTimersByTime(5);
    expect(store.value.get()).toBeUndefined();
    graph.dispose();
  });

  it("writes through a status-only chain without starting a value refresh", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "status.query",
      key: (input: { id: string }) => input.id,
      revalidateAfterMs: 10,
      fetch: backend.respond("status.query"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "graph-tests", operations: { query } }),
      idleMs: 1,
    });
    const store = graph.api.query({ id: "status" });
    const call = backend.calls[0]!;
    backend.resolve(call, "ready");
    await flush();
    const derived = computed(() => store.status.get());
    let notifications = 0;
    const stop = watch(derived, () => {
      notifications += 1;
    });
    expect(derived.get()).toBe("ready");
    jest.advanceTimersByTime(11);
    expect(derived.get()).toBe("stale");
    expect(notifications).toBe(1);
    expect(backend.calls).toHaveLength(1);
    stop();
    graph.dispose();
  });

  it("waits for a live-readable release instead of polling collection", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "collection-live.query",
      key: () => "collection-live",
      fetch: backend.respond<undefined, number>("collection-live.query"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "graph-tests", operations: { query } }),
      idleMs: 10,
    });
    const store = graph.api.query(undefined);
    backend.resolve(backend.calls[0]!, 1);
    await flush();
    const stop = watch(store.pending, () => undefined);

    jest.advanceTimersByTime(25);
    stop();
    await flush();
    expect(store.value.get()).toBeUndefined();
    graph.dispose();
  });
});
