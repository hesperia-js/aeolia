import { expect, jest, test } from "bun:test";
import {
  adopt,
  affects,
  createGraph,
  createMutation,
  createQuery,
  createStore,
  createStream,
  project,
  snapshot,
  subscribe,
} from "../../src/index.ts";
import { testBackend } from "../../src/testing.ts";

async function flush() {
  for (let turn = 0; turn < 5; turn++) await Promise.resolve();
}

function fixture(idleMs?: number) {
  const backend = testBackend();
  const stream = createStream({
    name: "values",
    key: (key: string) => `values/${key}`,
    open: backend.stream<string, number | undefined>("values"),
  });
  const graph = createGraph({
    contract: { namespace: "presence", operations: { stream } },
    idleMs,
  });
  return { backend, stream, graph };
}

test("store subscription skips absence but delivers a real undefined and retained values", async () => {
  const { backend, graph } = fixture();
  const store = graph.api.stream("one");
  const values: Array<number | undefined> = [];
  const raw: Array<number | undefined> = [];
  const stop = subscribe(store, (value) => values.push(value));
  const stopRaw = subscribe(store.value, (value) => raw.push(value));
  try {
    expect(values).toStrictEqual([]);
    expect(raw).toStrictEqual([undefined]);
    backend.emit("values", undefined);
    await flush();
    expect(values).toStrictEqual([undefined]);
    backend.emit("values", 1);
    await flush();
    backend.emit("values", 1);
    await flush();
    expect(values).toStrictEqual([undefined, 1]);
    const late: Array<number | undefined> = [];
    const stopLate = subscribe(store, (value) => late.push(value));
    expect(late).toStrictEqual([1]);
    stopLate();
    stop();
    backend.emit("values", 2);
    await flush();
    expect(values).toStrictEqual([undefined, 1]);
  } finally {
    stop();
    stopRaw();
    graph.dispose();
  }
});

test("explicit initial undefined is present even while the stream is waiting", () => {
  const { graph } = fixture();
  const store = graph.api.stream("seed", { initial: undefined });
  const values: Array<number | undefined> = [];
  const stop = subscribe(store, (value) => values.push(value));
  expect(values).toStrictEqual([undefined]);
  expect(store.pending.get()).toBe(true);
  stop();
  graph.dispose();
});

test("closing or failing a stream without an emission does not fabricate a value", async () => {
  for (const reason of [undefined, new Error("source failed")]) {
    const { backend, graph } = fixture();
    const store = graph.api.stream("empty");
    const values: Array<number | undefined> = [];
    const stop = subscribe(store, (value) => values.push(value));
    backend.end("values", reason);
    await flush();
    expect(values).toStrictEqual([]);
    expect(store.pending.get()).toBe(false);
    expect(store.status.get()).toBe(reason === undefined ? "closed" : "failed");
    stop();
    graph.dispose();
  }
});

test("query results and direct writes establish presence without placeholder callbacks", async () => {
  const backend = testBackend();
  const query = createQuery({
    name: "get",
    key: () => "query",
    fetch: backend.respond<void, number | undefined>("get"),
  });
  const local = createStore({ name: "local", initial: undefined as number | undefined });
  const graph = createGraph({ contract: { namespace: "presence", operations: { query, local } } });
  const store = graph.api.query(undefined);
  const values: Array<number | undefined> = [];
  const stop = subscribe(store, (value) => values.push(value));
  const localValues: Array<number | undefined> = [];
  const stopLocal = subscribe(graph.api.local(), (value) => localValues.push(value));
  try {
    expect(localValues).toStrictEqual([undefined]);
    expect(values).toStrictEqual([]);
    const ready = store.ready;
    backend.resolve(backend.calls[0]!, undefined);
    await ready;
    expect(values).toStrictEqual([undefined]);
    store.set(2);
    store.set(2);
    expect(values).toStrictEqual([undefined, 2]);
  } finally {
    stop();
    stopLocal();
    graph.dispose();
  }
});

test("adoption establishes presence for a held empty stream store", () => {
  const source = fixture();
  const seeded = source.graph.api.stream("adopt", { initial: undefined, snapshot: true });
  const encoded = snapshot(source.graph);
  const target = fixture();
  const store = target.graph.api.stream("adopt", { snapshot: true });
  const values: Array<number | undefined> = [];
  const stop = subscribe(store, (value) => values.push(value));
  expect(values).toStrictEqual([]);
  adopt(target.graph, encoded);
  expect(values).toStrictEqual([undefined]);
  expect(seeded.value.get()).toBeUndefined();
  stop();
  target.graph.dispose();
  source.graph.dispose();
});

test("projections retain seeds and consume repeated source events independently of store equality", async () => {
  const { graph, backend, stream } = fixture();
  const accumulation = project(graph, stream, "projection", { kind: "accumulate", max: 5 });
  const reduction = project(graph, stream, "projection", {
    kind: "reduce",
    initial: 0,
    step: (count) => count + 1,
  });
  const arrays: Array<readonly (number | undefined)[]> = [];
  const counts: number[] = [];
  const stopArray = subscribe(accumulation, (value) => arrays.push(value));
  const stopCount = subscribe(reduction, (value) => counts.push(value));
  try {
    expect(arrays).toStrictEqual([[]]);
    expect(counts).toEqual([0]);
    backend.emit("values", 1);
    await flush();
    backend.emit("values", 1);
    await flush();
    expect(arrays).toStrictEqual([[], [1], [1, 1]]);
    expect(counts).toEqual([0, 1, 2]);
  } finally {
    stopArray();
    stopCount();
    graph.dispose();
  }
});

test("store subscription respects forced same-reference changes and callback writes", () => {
  const item = { count: 0 };
  const local = createStore({ name: "item", initial: item, equals: () => false });
  const graph = createGraph({ contract: { namespace: "presence", operations: { local } } });
  const store = graph.api.local();
  const counts: number[] = [];
  const stop = subscribe(store, (value) => {
    counts.push(value.count);
    if (value.count === 1) store.set({ count: 2 });
  });
  item.count = 1;
  store.set(item);
  expect(counts).toEqual([0, 1, 2]);
  stop();
  store.set({ count: 3 });
  expect(counts).toEqual([0, 1, 2]);
  graph.dispose();
});

test("an empty held store can become present through a direct undefined write", () => {
  const { graph } = fixture();
  const store = graph.api.stream("direct");
  const values: Array<number | undefined> = [];
  const stop = subscribe(store, (value) => values.push(value));
  expect(values).toStrictEqual([]);
  store.set(undefined);
  expect(values).toStrictEqual([undefined]);
  store.set(undefined);
  expect(values).toStrictEqual([undefined]);
  stop();
  graph.dispose();
});

test("store subscriptions retain live streams and release them after unsubscribe or setup failure", async () => {
  jest.useFakeTimers();
  jest.setSystemTime(0);
  const { graph, backend } = fixture(1);
  try {
    const store = graph.api.stream("live");
    const stop = subscribe(store, () => undefined);
    await flush();
    jest.advanceTimersByTime(5);
    expect(backend.calls[0]!.aborted).toBe(false);
    stop();
    await flush();
    jest.advanceTimersByTime(1);
    expect(backend.calls[0]!.aborted).toBe(true);

    const seeded = graph.api.stream("throwing", { initial: 3 });
    const failure = new Error("observer failed");
    expect(() =>
      subscribe(seeded, () => {
        throw failure;
      }),
    ).toThrow(failure);
    await flush();
    jest.advanceTimersByTime(2);
    expect(backend.calls[1]!.aborted).toBe(true);
  } finally {
    graph.dispose();
    jest.clearAllTimers();
    jest.useRealTimers();
  }
});

test("an optimistic value is observable before the first committed query result", async () => {
  const backend = testBackend();
  const query = createQuery({
    name: "get",
    key: () => "item",
    fetch: backend.respond<void, number>("get"),
  });
  const mutation = createMutation({
    name: "set",
    run: backend.perform<number, void>("set"),
    affects: [
      affects(query, {
        select: (_value: number) => undefined,
        on: "revalidate",
        optimistic: (_old, value) => value,
      }),
    ],
  });
  const graph = createGraph({
    contract: { namespace: "presence", operations: { query, mutation } },
  });
  const store = graph.api.query(undefined);
  const values: number[] = [];
  const stop = subscribe(store, (value) => values.push(value));
  try {
    const changed = graph.api.mutation(42);
    expect(values).toStrictEqual([42]);
    backend.resolve(backend.calls[1]!, undefined);
    await changed;
    const ready = store.ready;
    backend.resolve(backend.calls[2]!, 42);
    expect(await ready).toBe(42);
    expect(values).toStrictEqual([42]);
  } finally {
    stop();
    graph.dispose();
  }
});

test("an empty store rejects a non-callable observer at registration", () => {
  const { graph } = fixture();
  try {
    const store = graph.api.stream("invalid-observer");
    expect(() => subscribe(store, null as never)).toThrow(TypeError);
  } finally {
    graph.dispose();
  }
});
