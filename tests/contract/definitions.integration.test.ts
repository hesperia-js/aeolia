import { expect, test } from "bun:test";
import {
  affects,
  defineContract,
  createMutation,
  createQuery,
  createStore,
  createStream,
  storeKey,
  type CallContext,
  type FetchOptions,
} from "../../src/contract/index.ts";

test("contract definitions preserve kinds and reject duplicate names globally", () => {
  const query = createQuery({
    name: "users.get",
    key: (input: { uid: string }) => `users/${input.uid}`,
    fetch: async (_input: { uid: string }, _options: FetchOptions) => ({ uid: "7" }),
  });
  const mutation = createMutation({
    name: "users.rename",
    affects: [
      affects(query, {
        select: (input: { uid: string; name: string }) => ({ uid: input.uid }),
        on: "revalidate",
      }),
    ],
    run: async (_input: { uid: string; name: string }, _options: CallContext) => undefined,
  });
  const stream = createStream({
    name: "users.events",
    key: (input: { uid: string }) => `users/${input.uid}/events`,
    open: (_input: { uid: string }) => ({
      async *[Symbol.asyncIterator](): AsyncIterator<number> {
        yield 1;
      },
    }),
  });
  const local = createStore({ name: "cart", initial: [] as string[] });
  const contract = defineContract({
    namespace: "test",
    operations: { users: { query, mutation, stream }, cart: local },
  });

  expect(contract.operations.users.query.kind).toBe("query");
  expect(contract.operations.users.mutation.kind).toBe("mutation");
  expect(contract.operations.users.stream.kind).toBe("stream");
  expect(contract.operations.cart.kind).toBe("store");

  expect(() =>
    defineContract({
      namespace: "test",
      operations: {
        first: createQuery({
          name: "same",
          key: () => "first",
          fetch: async () => 1,
        }),
        nested: {
          second: createMutation({
            name: "same",
            affects: [],
            run: async () => undefined,
          }),
        },
      },
    }),
  ).toThrow("contract: same");

  expect(() =>
    defineContract({
      namespace: "test",
      operations: {
        one: createStore({ name: "shared", initial: 1 }),
        nested: { two: createStore({ name: "shared", initial: 2 }) },
      },
    }),
  ).toThrow("contract: shared");
});

test("creation functions retain ownership of definition discriminants", () => {
  const storeInput = { kind: "query", name: "local", initial: 1 } as const;
  const queryInput = {
    kind: "stream",
    name: "items.get",
    key: () => "items",
    fetch: async () => 1,
  } as const;
  const streamInput = {
    kind: "mutation",
    name: "items.events",
    key: () => "item-events",
    open: async function* () {
      yield 1;
    },
  } as const;
  const mutationInput = {
    kind: "store",
    name: "items.update",
    affects: [],
    run: async () => undefined,
  } as const;

  expect(createStore(storeInput).kind).toBe("store");
  expect(createQuery(queryInput).kind).toBe("query");
  expect(createStream(streamInput).kind).toBe("stream");
  expect(createMutation(mutationInput).kind).toBe("mutation");
});

test("contract validation rejects aliases, cycles, and orphan affected queries", () => {
  const query = createQuery({
    name: "aliased.query",
    key: () => "aliased",
    fetch: async () => 1,
  });
  expect(() =>
    defineContract({
      namespace: "test",
      operations: { first: query, second: query },
    }),
  ).toThrow("contract alias: aliased.query");

  const store = createStore({ name: "aliased.store", initial: 1 });
  expect(() =>
    defineContract({
      namespace: "test",
      operations: { first: store, second: store },
    }),
  ).toThrow("contract alias: aliased.store");

  const cyclic: Record<string, unknown> = {};
  cyclic.self = cyclic;
  expect(() =>
    defineContract({
      namespace: "test",
      operations: cyclic as never,
    }),
  ).toThrow("contract cycle");

  const orphanQuery = createQuery({
    name: "orphan.query",
    key: (input: { id: string }) => `orphan/${input.id}`,
    fetch: async () => 1,
  });
  const mutation = createMutation({
    name: "orphan.mutation",
    affects: [
      affects(orphanQuery, {
        select: (input: { id: string }) => input,
        on: "invalidate",
      }),
    ],
    run: async () => undefined,
  });
  expect(() => defineContract({ namespace: "test", operations: { mutation } })).toThrow(
    "contract: orphan.query",
  );
});

test("store keys are branded at the boundary", () => {
  expect(String(storeKey("users/7"))).toBe("users/7");
  expect(() => storeKey("")).toThrow();
});
