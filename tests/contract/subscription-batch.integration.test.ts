import { expect, test } from "bun:test";
import {
  adopt,
  affects,
  createGraph,
  createMutation,
  createQuery,
  createStore,
  observeQueryProgress,
  peekStore,
  snapshot,
  subscribe,
  type Store,
  type StoreCapture,
  type Unsubscribe,
} from "../../src/index.ts";

test("a whole-Store subscriber joining from V2 interruption receives the adopted value", async () => {
  const held = Promise.withResolvers<number>();
  const query = createQuery({
    name: "query",
    key: (key: string) => key,
    fetch: () => held.promise,
  });
  const contract = { namespace: "adoption-subscription", operations: { query } };
  const source = createGraph({ contract });
  const receiver = createGraph({ contract });
  source.api.query("key", { initial: 17 });
  const store = receiver.api.query("key", { initial: 1 });
  const existing: Array<number | undefined> = [];
  const joined: number[] = [];
  let during: StoreCapture<number> | undefined;
  let stopJoined: Unsubscribe | undefined;
  const stopExisting = subscribe(store.value, (value) => existing.push(value));
  const stopProgress = observeQueryProgress(store).subscribe(({ kind, snapshot }) => {
    if (kind !== "lifecycle" || snapshot.status !== "interrupted") return;
    stopJoined = subscribe(store, (value) => joined.push(value));
    during = peekStore(store);
  });
  try {
    const pending = store.revalidate();
    adopt(receiver, snapshot(source, { version: 2 }), {});
    held.resolve(99);
    await pending;
    expect(during).toMatchObject({ kind: "stable", present: true, value: 17 });
    expect(joined.at(-1)).toBe(17);
    store.set(23);
    expect([
      [17, 23],
      [1, 17, 23],
    ]).toContainEqual(joined);
    expect(existing).toEqual([1, 17, 23]);
  } finally {
    held.resolve(99);
    stopJoined?.();
    stopProgress();
    stopExisting();
    await Promise.all([source.retire(), receiver.retire()]);
  }
});

test.each(["whole Store", "value"] as const)(
  "a %s subscriber joining from equality receives the nested write",
  async (subscription) => {
    const existing: Array<number | undefined> = [];
    const joined: Array<number | undefined> = [];
    let stopJoined: Unsubscribe | undefined;
    let nestedWrite: (() => void) | undefined;
    const entry = createStore({
      name: "entry",
      initial: 0,
      equals(previous, proposed) {
        if (previous === 0 && proposed === 1) nestedWrite?.();
        return Object.is(previous, proposed);
      },
    });
    const graph = createGraph({
      contract: { namespace: "equality-subscription", operations: { entry } },
    });
    const store = graph.api.entry();
    const stopExisting = subscribe(store.value, (value) => existing.push(value));
    let during: StoreCapture<number> | undefined;
    nestedWrite = () => {
      nestedWrite = undefined;
      store.set(2);
      const onValue = (value: number | undefined) => joined.push(value);
      stopJoined =
        subscription === "whole Store"
          ? subscribe(store, onValue)
          : subscribe(store.value, onValue);
      during = peekStore(store);
    };
    try {
      store.set(1);
      expect(during).toStrictEqual(peekStore(store));
      expect(joined.at(-1)).toBe(2);
      store.set(3);
      const allowed: Array<Array<number | undefined>> = [
        [2, 3],
        [0, 2, 3],
      ];
      expect(allowed).toContainEqual(joined);
      expect(existing).toEqual([0, 2, 3]);
    } finally {
      stopJoined?.();
      stopExisting();
      await graph.retire();
    }
  },
);

for (const read of ["none", "ordinary", "capture", "available"] as const) {
  test.each([1, 3])(
    `a ${read} read preserves settled delivery when a batch ends at %i`,
    async (final) => {
      const entry = createStore({ name: "entry", initial: 1 });
      let target: Store<number>;
      let inside: unknown;
      const trigger = createStore({
        name: "trigger",
        initial: 0,
        equals(previous, proposed) {
          if (previous === 0 && proposed === 1) {
            target.set(2);
            if (read === "ordinary") inside = target.value.peek();
            if (read === "capture") inside = peekStore(target);
            if (read === "available") inside = target.available.get();
            target.set(final);
          }
          return Object.is(previous, proposed);
        },
      });
      const graph = createGraph({
        contract: { namespace: "settled-subscription", operations: { entry, trigger } },
      });
      target = graph.api.entry();
      const values: Array<number | undefined> = [];
      const stop = subscribe(target.value, (value) => values.push(value));
      try {
        graph.api.trigger().set(1);
        if (read === "capture")
          expect(inside).toMatchObject({
            kind: "stable",
            present: true,
            value: 2,
            valueRevision: 2,
          });
        if (read === "available") expect(inside).toBe(true);
        expect(values).toEqual(final === 1 ? [1] : [1, 3]);
        target.set(2);
        target.set(1);
        expect(values.slice(-2)).toEqual([2, 1]);
      } finally {
        stop();
        await graph.retire();
      }
    },
  );
}

test("native reads and the settled subscriber share the borrowed prediction for one revision", async () => {
  const work = Promise.withResolvers<void>();
  const query = createQuery({
    name: "query",
    key: (_input: void) => "target",
    fetch: async () => ({ amount: 0 }),
  });
  const mutation = createMutation({
    name: "prediction",
    run: (_input: void) => work.promise,
    affects: [
      affects(query, {
        select: () => undefined,
        on: "invalidate",
        optimistic: (value) => ({ amount: (value?.amount ?? 0) * 10 }),
      }),
    ],
  });
  let write: (() => void) | undefined;
  const trigger = createStore({
    name: "trigger",
    initial: 0,
    equals(previous, proposed) {
      if (previous === 0 && proposed === 1) write?.();
      return Object.is(previous, proposed);
    },
  });
  const graph = createGraph({
    contract: { namespace: "batch-prediction", operations: { query, mutation, trigger } },
  });
  const store = graph.api.query(undefined, { initial: { amount: 1 } });
  const pending = graph.api.mutation();
  const values: Array<{ amount: number } | undefined> = [];
  const stop = subscribe(store.value, (value) => values.push(value));
  let first: StoreCapture<{ amount: number }> | undefined;
  let second: StoreCapture<{ amount: number }> | undefined;
  write = () => {
    write = undefined;
    store.set({ amount: 2 });
    first = peekStore(store);
    second = peekStore(store);
  };
  try {
    graph.api.trigger().set(1);
    if (first?.kind !== "stable" || !first.present || second?.kind !== "stable" || !second.present)
      throw new Error("Expected both in-batch captures to be present");
    expect(first.value).toEqual({ amount: 20 });
    expect(second.valueRevision).toBe(first.valueRevision);
    expect(second.value).toBe(first.value);
    expect(values.map((value) => value?.amount)).toEqual([10, 20]);
    expect(values[1]).toBe(first.value);
  } finally {
    stop();
    work.resolve();
    await pending;
    await graph.retire();
  }
});
