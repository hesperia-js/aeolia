import { expect, test } from "bun:test";
import {
  createGraph,
  createStream,
  peekStore,
  project,
  subscribe,
  type StoreCapture,
} from "../../src/index.ts";

test("raw first landing exposes value, presence and revision together during notifications", async () => {
  const graph = createGraph({ contract: { namespace: "raw-landing", operations: {} } });
  const store = graph.at("one");
  const seen: unknown[] = [];
  const stop = subscribe(store.value, (value) => {
    if (value === 1 || value === 2) {
      seen.push([value, store.available.get(), peekStore(store)]);
      if (value === 1) store.set(2);
    }
  });
  store.set(1);
  expect(seen).toHaveLength(2);
  expect(seen[0]).toMatchObject([
    1,
    true,
    { kind: "stable", present: true, value: 1, valueRevision: 1 },
  ]);
  expect(seen[1]).toMatchObject([
    2,
    true,
    { kind: "stable", present: true, value: 2, valueRevision: 2 },
  ]);
  const empty = graph.at("undefined");
  empty.set(undefined);
  expect(peekStore(empty)).toMatchObject({
    kind: "stable",
    present: true,
    value: undefined,
    valueRevision: 1,
  });
  empty.set(undefined);
  expect(peekStore(empty)).toMatchObject({
    kind: "stable",
    present: true,
    value: undefined,
    valueRevision: 2,
  });
  stop();
  await graph.retire();
});

test("a revoked raw stream getter cannot advance its successor and the successor can land", async () => {
  const first = Promise.withResolvers<IteratorResult<number>>();
  const second = Promise.withResolvers<IteratorResult<number>>();
  const secondOpened = Promise.withResolvers<void>();
  let opens = 0;
  let reads = 0;
  let reset: Promise<unknown> | undefined;
  let during: StoreCapture<number> | undefined;
  const stream = createStream<void, number, string>({
    name: "s",
    key: () => "k",
    open: () => {
      const next = ++opens === 1 ? first : second;
      if (opens === 2) secondOpened.resolve();
      return {
        [Symbol.asyncIterator]() {
          const tail = Promise.withResolvers<IteratorResult<number>>();
          let nextRead = next.promise;
          return {
            next() {
              const read = nextRead;
              nextRead = tail.promise;
              return read;
            },
            async return() {
              const done = { done: true as const, value: undefined };
              next.resolve(done);
              tail.resolve(done);
              return done;
            },
          };
        },
      };
    },
  });
  const graph = createGraph({ contract: { namespace: "raw-getter", operations: { stream } } });
  const store = graph.api.stream();
  const projection = project(store, { kind: "reduce", initial: 0, step: (sum, n) => sum + n });
  const stop = subscribe(projection, () => undefined);
  const landed = Promise.withResolvers<void>();
  const stopValue = subscribe(store.value, (value) => {
    if (value === 7) landed.resolve();
  });
  try {
    first.resolve({
      done: false,
      get value() {
        reads += 1;
        reset = graph.reset();
        store.set(42);
        during = peekStore(store);
        return 99;
      },
    });
    await secondOpened.promise;
    await reset;
    expect(reads).toBe(1);
    expect(opens).toBe(2);
    if (during?.kind !== "stable")
      throw new Error("The predecessor getter did not capture a stable replacement");
    expect(peekStore(store)).toStrictEqual(during);
    second.resolve({ done: false, value: 7 });
    await landed.promise;
    expect(peekStore(store)).toMatchObject({
      kind: "stable",
      present: true,
      value: 7,
      valueRevision: during.valueRevision + 1,
    });
  } finally {
    stop();
    stopValue();
    await graph.retire();
  }
});
