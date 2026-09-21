import { expect, test } from "bun:test";
import {
  afterPropagation,
  createGraph,
  createObserver,
  createQuery,
  defineContract,
  withoutWrites,
} from "../../src/index.ts";

test("a framework observer sees a managed query settlement as one complete update", async () => {
  const response = Promise.withResolvers<number>();
  const query = createQuery({
    name: "observer-query",
    key: () => "observer-query/one",
    fetch: (_input: void) => response.promise,
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "observer-query", operations: { query } }),
  });
  const store = graph.api.query();
  const snapshots: unknown[][] = [];
  const read = () => {
    snapshots.push([store.value.get(), store.pending.get(), store.error.get(), store.status.get()]);
  };
  const observer = createObserver({
    notify() {
      afterPropagation(() => {
        if (observer.check()) observer.track(read);
      });
    },
  });
  try {
    observer.track(read);
    snapshots.length = 0;
    response.resolve(7);
    await store.ready;
    expect(snapshots).toStrictEqual([[7, false, undefined, "ready"]]);
  } finally {
    observer.dispose();
    graph.dispose();
  }
});

test("forbidden store writes leave an in-flight query eligible to publish", async () => {
  const response = Promise.withResolvers<number>();
  const query = createQuery({
    name: "guarded-query",
    key: () => "guarded-query/one",
    fetch: (_input: void) => response.promise,
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "guarded-query", operations: { query } }),
  });
  const store = graph.api.query();
  let updaterRan = false;
  try {
    withoutWrites(() => {
      expect(() => store.set(99)).toThrow("write-forbidden");
      expect(() =>
        store.update(() => {
          updaterRan = true;
          return 98;
        }),
      ).toThrow("write-forbidden");
    });
    expect(updaterRan).toBe(false);
    expect(store.value.get()).toBeUndefined();
    response.resolve(7);
    expect(await store.ready).toBe(7);
    expect(store.value.get()).toBe(7);
  } finally {
    graph.dispose();
  }
});
