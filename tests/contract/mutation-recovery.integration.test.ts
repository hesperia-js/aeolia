import { afterEach, beforeEach, expect, jest, test } from "bun:test";
import {
  affects,
  createGraph,
  createMutation,
  createQuery,
  defineContract,
} from "../../src/contract/index.ts";
import { watch } from "../../src/reactive.ts";
import { testBackend } from "../../src/testing.ts";

beforeEach(() => {
  jest.useFakeTimers();
  jest.setSystemTime(0);
});
afterEach(() => {
  jest.clearAllTimers();
  jest.useRealTimers();
});

async function failedQuery(on: "invalidate" | "revalidate") {
  const backend = testBackend();
  const query = createQuery({
    name: "profile",
    key: (id: string) => `profile/${id}`,
    fetch: backend.respond<string, number>("profile"),
  });
  const save = createMutation({
    name: "save",
    affects: [affects<string, string, number>(query, { select: (id) => id, on })],
    run: backend.perform<string, string>("save"),
  });
  const faults: unknown[] = [];
  const graph = createGraph({
    contract: defineContract({ namespace: "mutation-recovery", operations: { query, save } }),
    onUnobservedFault: (error) => faults.push(error),
  });
  const store = graph.api.query("one", { revalidateAfterMs: 1 });
  const stop = watch(store.value, () => undefined);
  const initial = store.ready.catch((error: unknown) => error);
  backend.reject(backend.calls[0]!, new Error("initial failure"));
  await initial;
  jest.advanceTimersByTime(2);
  const recovery = store.ready.catch((error: unknown) => error);
  backend.reject(backend.calls[1]!, new Error("recovery failure"));
  await recovery;
  jest.advanceTimersByTime(20);
  expect(backend.calls).toHaveLength(2);
  expect(store.status.get()).toBe("failed");
  return {
    backend,
    graph,
    store,
    faults,
    dispose() {
      stop();
      graph.dispose();
    },
    async save() {
      const operation = graph.api.save("one");
      backend.resolve(backend.pending("save")[0]!, "saved");
      expect(await operation).toBe("saved");
      expect(store.status.get()).toBe("fetching");
      expect(store.error.get()).toBeUndefined();
      return backend.pending("profile").at(-1)!;
    },
  };
}

test.each(["invalidate", "revalidate"] as const)(
  "a successful %s mutation refreshes a live disarmed query and a successful landing re-arms its timer",
  async (on) => {
    const fixture = await failedQuery(on);
    const { backend, store } = fixture;
    try {
      const request = await fixture.save();
      const ready = store.ready;
      backend.resolve(request, 42);
      expect(await ready).toBe(42);
      expect(store.value.get()).toBe(42);
      jest.advanceTimersByTime(2);
      expect(backend.pending("profile")).toHaveLength(1);
      expect(store.status.get()).toBe("revalidating");
    } finally {
      fixture.dispose();
    }
  },
);

test.each(["invalidate", "revalidate"] as const)(
  "a failed %s mutation refresh leaves the query disarmed for every consumer",
  async (on) => {
    const fixture = await failedQuery(on);
    const { backend, store, graph, faults } = fixture;
    try {
      const request = await fixture.save();
      const failure = new Error("still offline after save");
      const ready = store.ready.catch((error: unknown) => error);
      backend.reject(request, failure);
      expect(await ready).toBe(failure);
      expect(store.error.get()).toBe(failure);
      jest.advanceTimersByTime(20);
      expect(backend.pending("profile")).toHaveLength(0);
      graph.api.query("one", { revalidateAfterMs: 1 });
      expect(backend.calls.filter((call) => call.name === "profile")).toHaveLength(3);
      expect(faults).toHaveLength(2);
    } finally {
      fixture.dispose();
    }
  },
);

test.each(["invalidate", "revalidate"] as const)(
  "a newer %s mutation refresh preserves bounded recovery and rejects stale query landings",
  async (on) => {
    const fixture = await failedQuery(on);
    const { backend, store } = fixture;
    try {
      const first = await fixture.save();
      const second = await fixture.save();
      expect(first.aborted).toBe(true);
      backend.resolve(first, 99);
      const failure = new Error("newest refresh failed");
      const ready = store.ready.catch((error: unknown) => error);
      backend.reject(second, failure);
      expect(await ready).toBe(failure);
      expect(store.value.get()).toBeUndefined();
      jest.advanceTimersByTime(20);
      expect(backend.pending("profile")).toHaveLength(0);
    } finally {
      fixture.dispose();
    }
  },
);
