import { expect, test } from "bun:test";
import {
  adopt,
  affects,
  computed,
  signal,
  createGraph,
  createMutation,
  createQuery,
  createStore,
  observeQueryProgress,
  peekStore,
  snapshot,
  subscribe,
  watch,
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

test.each(["distinct", "equal"] as const)(
  "the settled value reuses the borrowed prediction when configured equality says %s",
  async (settled) => {
    const work = Promise.withResolvers<void>();
    const setting = signal(false);
    const policy = computed(() => setting.get(), { equals: () => true });
    const stopPolicy = subscribe(policy, () => {});
    const query = createQuery({
      name: "query",
      key: (_input: void) => "target",
      fetch: async () => ({ amount: 0 }),
      equals(previous, proposed) {
        return previous.amount >= 10 && proposed.amount >= 10
          ? policy.get()
          : Object.is(previous, proposed);
      },
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
      if (settled === "equal") setting.set(true);
    };

    try {
      graph.api.trigger().set(1);

      if (
        first?.kind !== "stable" ||
        !first.present ||
        second?.kind !== "stable" ||
        !second.present
      )
        throw new Error("Expected both in-batch captures to be present");

      expect(first.value).toEqual({ amount: 20 });
      expect(second.valueRevision).toBe(first.valueRevision);
      expect(second.value).toBe(first.value);
      expect(store.value.peek()).toBe(first.value);
      expect(values.map((value) => value?.amount)).toEqual(settled === "equal" ? [10] : [10, 20]);
      if (settled === "distinct") expect(values[1]).toBe(first.value);
    } finally {
      stop();
      stopPolicy();
      work.resolve();
      await pending;
      await graph.retire();
    }
  },
);

for (const suppressPolicyNotification of [false, true]) {
  test.each(["without capture", "with capture"] as const)(
    `settled equality follows the current policy %s when the policy suppresses notifications: ${suppressPolicyNotification}`,
    async (mode) => {
      const setting = signal(false);
      const policy = computed(
        () => setting.get(),
        suppressPolicyNotification ? { equals: () => true } : undefined,
      );
      const stopPolicy = subscribe(policy, () => {});
      const entry = createStore({
        name: "entry",
        initial: 1,
        equals(previous, proposed) {
          return previous === 1 && proposed === 2 ? policy.get() : Object.is(previous, proposed);
        },
      });
      let inside: StoreCapture<number> | undefined;
      const trigger = createStore({
        name: "trigger",
        initial: 0,
        equals(previous, proposed) {
          if (previous === 0 && proposed === 1) {
            target.set(2);
            setting.set(true);
            if (mode === "with capture") inside = peekStore(target);
          }

          return Object.is(previous, proposed);
        },
      });
      const graph = createGraph({
        contract: { namespace: "settled-equality-policy", operations: { entry, trigger } },
      });
      const target = graph.api.entry();
      const delivered: Array<number | undefined> = [];
      const stop = subscribe(target.value, (value) => delivered.push(value));

      try {
        graph.api.trigger().set(1);

        if (mode === "with capture") {
          expect(inside).toMatchObject({
            kind: "stable",
            present: true,
            value: 2,
            valueRevision: 2,
          });
        }

        expect(target.value.peek()).toBe(2);
        expect(delivered).toEqual([1]);
      } finally {
        stop();
        stopPolicy();
        await graph.retire();
      }
    },
  );
}

test.each(["without capture", "with capture"] as const)(
  "a later equality dependency can replace an earlier Store input %s",
  async (mode) => {
    let target: Store<number>;
    let inside: StoreCapture<number> | undefined;
    const side = signal(false);
    const controller = computed(() => {
      if (side.get()) target.set(3);
      return 0;
    });
    const entry = createStore({
      name: "target",
      initial: 1,
      snapshot: true,
      equals(previous, proposed) {
        if (previous === 1 && proposed === 2) controller.get();
        return Object.is(previous, proposed);
      },
    });
    const trigger = createStore({
      name: "trigger",
      initial: 0,
      equals(previous, proposed) {
        if (previous === 0 && proposed === 1) {
          target.set(2);
          if (mode === "with capture") inside = peekStore(target);
          side.set(true);
        }

        return Object.is(previous, proposed);
      },
    });
    const graph = createGraph({
      contract: { namespace: "subscription-validation", operations: { entry, trigger } },
    });

    target = graph.api.entry();
    const stopController = watch(controller, () => {});
    const delivered: Array<number | undefined> = [];
    const stop = subscribe(target.value, (value) => delivered.push(value));

    try {
      graph.api.trigger().set(1);

      if (mode === "with capture") {
        expect(inside).toMatchObject({ kind: "stable", present: true, value: 2, valueRevision: 2 });
      }

      expect(snapshot(graph).entries.target?.value).toBe(3);
      expect(target.value.peek()).toBe(3);
      expect(peekStore(target)).toMatchObject({
        kind: "stable",
        present: true,
        value: 3,
        valueRevision: 3,
      });
      expect(delivered.at(-1)).toBe(3);
    } finally {
      stop();
      stopController();
      await graph.retire();
    }
  },
);
