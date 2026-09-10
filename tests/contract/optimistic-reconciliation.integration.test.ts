import { expect, test } from "bun:test";
import { affects, createGraph, createMutation, createQuery, subscribe } from "../../src/index.ts";
import { testBackend } from "../../src/testing.ts";

async function fixture(on: "invalidate" | "revalidate" = "revalidate") {
  const backend = testBackend();
  const query = createQuery({
    name: "counter.get",
    key: () => "counter",
    fetch: backend.respond<void, number>("counter.get"),
  });
  const mutation = createMutation({
    name: "counter.add",
    run: backend.perform<number, void>("counter.add"),
    affects: [
      affects(query, {
        select: (_delta: number) => undefined,
        on,
        optimistic: (current, delta) => (current ?? 0) + delta,
      }),
    ],
  });
  const graph = createGraph({
    contract: { namespace: "reconciliation", operations: { query, mutation } },
  });
  const store = graph.api.query(undefined);
  const values: Array<number | undefined> = [];
  const stop = subscribe(store.value, (value) => values.push(value));
  const ready = store.ready;
  backend.resolve(backend.calls[0]!, 0);
  await ready;
  return {
    backend,
    graph,
    store,
    values,
    close: () => {
      stop();
      graph.dispose();
    },
  };
}

test("reconciles equal and corrected results without exposing the old base or applying a prediction twice", async () => {
  const f = await fixture();
  try {
    const mutation = f.graph.api.mutation(42);
    expect(f.values).toEqual([undefined, 0, 42]);
    f.backend.resolve(f.backend.calls[1]!, undefined);
    await mutation;
    expect(f.store.status.get()).toBe("revalidating");
    expect(f.values).toEqual([undefined, 0, 42]);
    const ready = f.store.ready;
    f.backend.resolve(f.backend.calls[2]!, 42);
    expect(await ready).toBe(42);
    expect(f.values).toEqual([undefined, 0, 42]);

    const corrected = f.graph.api.mutation(1);
    f.backend.resolve(f.backend.calls[3]!, undefined);
    await corrected;
    const correctedReady = f.store.ready;
    f.backend.resolve(f.backend.calls[4]!, 44);
    expect(await correctedReady).toBe(44);
    expect(f.values).toEqual([undefined, 0, 42, 43, 44]);
  } finally {
    f.close();
  }
});

test("retains an unconfirmed prediction on refresh failure and reconciles it on retry", async () => {
  const f = await fixture();
  try {
    const mutation = f.graph.api.mutation(42);
    f.backend.resolve(f.backend.calls[1]!, undefined);
    await mutation;
    const failure = new Error("refresh unavailable");
    const ready = f.store.ready;
    f.backend.reject(f.backend.calls[2]!, failure);
    await expect(ready).rejects.toBe(failure);
    expect(f.store.error.get()).toBe(failure);
    expect(f.store.status.get()).toBe("failed");
    expect(f.store.pending.get()).toBe(false);
    expect(f.values).toEqual([undefined, 0, 42]);
    const retry = f.store.revalidate();
    const retriedReady = f.store.ready;
    f.backend.resolve(f.backend.calls[3]!, 42);
    await retry;
    expect(await retriedReady).toBe(42);
    expect(f.store.error.get()).toBeUndefined();
    expect(f.values).toEqual([undefined, 0, 42]);
  } finally {
    f.close();
  }
});

test("a refresh retires its covered prediction but preserves a newer pending mutation", async () => {
  const f = await fixture();
  try {
    const first = f.graph.api.mutation(1);
    f.backend.resolve(f.backend.calls[1]!, undefined);
    await first;
    const second = f.graph.api.mutation(10);
    const ready = f.store.ready;
    f.backend.resolve(f.backend.calls[2]!, 1);
    expect(await ready).toBe(1);
    expect(f.store.value.get()).toBe(11);
    expect(f.values).toEqual([undefined, 0, 1, 11]);
    const failure = new Error("mutation refused");
    f.backend.reject(f.backend.calls[3]!, failure);
    await expect(second).rejects.toBe(failure);
    expect(f.values).toEqual([undefined, 0, 1, 11, 1]);
  } finally {
    f.close();
  }
});

test("invalidation retains a successful prediction until an explicitly started refresh", async () => {
  const f = await fixture("invalidate");
  try {
    const mutation = f.graph.api.mutation(42);
    f.backend.resolve(f.backend.calls[1]!, undefined);
    await mutation;
    expect(f.backend.calls).toHaveLength(2);
    expect(f.store.status.get()).toBe("stale");
    expect(f.values).toEqual([undefined, 0, 42]);
    const refresh = f.store.revalidate();
    const ready = f.store.ready;
    f.backend.resolve(f.backend.calls[2]!, 42);
    await refresh;
    expect(await ready).toBe(42);
    expect(f.values).toEqual([undefined, 0, 42]);
  } finally {
    f.close();
  }
});

test("a superseded refresh cannot retire predictions owned by the newer reconciliation", async () => {
  const f = await fixture();
  try {
    const first = f.graph.api.mutation(1);
    f.backend.resolve(f.backend.calls[1]!, undefined);
    await first;
    const obsoleteRefresh = f.backend.calls[2]!;
    const second = f.graph.api.mutation(10);
    f.backend.resolve(f.backend.calls[3]!, undefined);
    await second;
    expect(obsoleteRefresh.aborted).toBe(true);
    const ready = f.store.ready;
    f.backend.resolve(obsoleteRefresh, 1);
    await Promise.resolve();
    expect(f.store.status.get()).toBe("revalidating");
    expect(f.values).toEqual([undefined, 0, 1, 11]);
    f.backend.resolve(f.backend.calls[4]!, 11);
    expect(await ready).toBe(11);
    expect(f.values).toEqual([undefined, 0, 1, 11]);
  } finally {
    f.close();
  }
});

test("a request started before mutation success cannot retire its later successful prediction", async () => {
  const f = await fixture("invalidate");
  try {
    const oldRefresh = f.store.revalidate();
    const mutation = f.graph.api.mutation(42);
    f.backend.resolve(f.backend.calls[2]!, undefined);
    await mutation;
    f.backend.resolve(f.backend.calls[1]!, 0);
    await oldRefresh;
    expect(f.values).toEqual([undefined, 0, 42]);

    const reconciliation = f.store.revalidate();
    f.backend.resolve(f.backend.calls[3]!, 42);
    await reconciliation;
    expect(f.values).toEqual([undefined, 0, 42]);
    expect(await f.store.ready).toBe(42);
  } finally {
    f.close();
  }
});
