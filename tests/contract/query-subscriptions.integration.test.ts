import { expect, test } from "bun:test";
import { affects, createGraph, createMutation, createQuery, subscribe } from "../../src/index.ts";
import { testBackend } from "../../src/testing.ts";

test("waits for initial query data and observes mutation-driven refresh without a second initial fetch", async () => {
  const backend = testBackend();
  const query = createQuery({
    name: "items.get",
    key: (index: number) => `items/${index}`,
    fetch: backend.respond<number, string>("items.get"),
  });
  const mutation = createMutation({
    name: "items.set",
    run: backend.perform<{ index: number; value: number }, void>("items.set"),
    affects: [
      affects(query, {
        select: (input: { index: number; value: number }) => input.index,
        on: "revalidate",
      }),
    ],
  });
  const graph = createGraph({
    contract: { namespace: "query-observation", operations: { query, mutation } },
  });
  const store = graph.api.query(0);
  const values: Array<string | undefined> = [];
  const stop = subscribe(store.value, (value) => {
    expect(store.value.peek()).toBe(value);
    values.push(value);
  });

  try {
    const initial = store.ready;
    expect(backend.calls).toHaveLength(1);
    expect(values).toEqual([undefined]);
    backend.resolve(backend.calls[0]!, "0");
    expect(await initial).toBe("0");
    expect(values).toEqual([undefined, "0"]);

    const changed = graph.api.mutation({ index: 0, value: 42 });
    backend.resolve(backend.calls[1]!, undefined);
    await changed;
    expect(backend.calls).toHaveLength(3);
    expect(backend.calls[2]!.input).toBe(0);
    expect(store.status.peek()).toBe("revalidating");
    expect(store.pending.peek()).toBe(true);
    let refreshSettled = false;
    const refreshed = store.ready.then((value) => {
      refreshSettled = true;
      return value;
    });
    await Promise.resolve();
    expect(refreshSettled).toBe(false);
    expect(store.value.peek()).toBe("0");
    expect(backend.calls).toHaveLength(3);

    backend.resolve(backend.calls[2]!, "42");
    expect(await refreshed).toBe("42");
    expect(store.pending.peek()).toBe(false);
    expect(store.status.peek()).toBe("ready");
    expect(values).toEqual([undefined, "0", "42"]);
    expect(await store.ready).toBe("42");

    const retry = store.revalidate();
    const failure = new Error("refresh failed");
    const waitingForRetry = store.ready;
    backend.reject(backend.calls[3]!, failure);
    await expect(waitingForRetry).rejects.toBe(failure);
    await retry;
    expect(store.status.peek()).toBe("failed");
    expect(store.pending.peek()).toBe(false);
    expect(store.error.peek()).toBe(failure);
    expect(store.value.peek()).toBe("42");
  } finally {
    stop();
    graph.dispose();
  }
});
