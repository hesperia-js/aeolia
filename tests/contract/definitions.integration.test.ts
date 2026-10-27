import { expect, test } from "bun:test";
import {
  affects,
  createGraph,
  defineContract,
  createMutation,
  createQuery,
  createStore,
  createStream,
  createPositionedStream,
  storeKey,
  type CallContext,
  type FetchOptions,
} from "../../src/contract/index.ts";
import { testBackend } from "../../src/testing.ts";

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

  let opens = 0;
  const positioned = createPositionedStream({
    name: "items.positioned",
    key: (input: { readonly id: string }) => input.id,
    source: {
      open: async function* (_input: { readonly id: string }) {
        opens += 1;
        yield { position: 1, value: 1 };
      },
      validateReader: () => true,
      isReaderCurrent: () => true,
      acknowledge: () => undefined,
      releaseReader: () => undefined,
    },
  });
  expect(positioned.kind).toBe("positioned-stream");
  const graph = createGraph({
    contract: defineContract({ namespace: "positioned-factory", operations: { positioned } }),
  });
  try {
    expect(typeof graph.api.positioned).toBe("function");
    const structuralCopy = { ...positioned };
    expect(() =>
      createGraph({
        contract: defineContract({
          namespace: "positioned-copy",
          operations: { positioned: structuralCopy },
        }),
      }),
    ).toThrow(TypeError);
    expect(opens).toBe(0);
  } finally {
    graph.dispose();
  }
});

test("query declarations capture and validate acquisition policy", () => {
  const input = {
    name: "acquisition.query",
    acquisition: "fetch" as "cache" | "fetch",
    key: () => "acquisition",
    fetch: async () => 1,
  };
  const query = createQuery(input);
  input.acquisition = "cache";
  expect(query.acquisition).toBe("fetch");
  const defaultQuery = createQuery({
    name: "default.query",
    key: () => "default",
    fetch: async () => 1,
  });
  expect(defaultQuery.acquisition).toBe("cache");
  expect(
    createQuery({
      name: "undefined.query",
      acquisition: undefined,
      key: () => "undefined",
      fetch: async () => 1,
    }).acquisition,
  ).toBe("cache");

  let getterCalled = false;
  const accessor = Object.defineProperty(
    {
      name: "accessor.query",
      key: () => "accessor",
      fetch: async () => 1,
    },
    "acquisition",
    {
      get() {
        getterCalled = true;
        return "fetch";
      },
    },
  );
  expect(() => createQuery(accessor as never)).toThrow(TypeError);
  expect(getterCalled).toBe(false);

  const inherited = Object.assign(Object.create({ acquisition: "fetch" }), {
    name: "inherited.query",
    key: () => "inherited",
    fetch: async () => 1,
  });
  expect(() => createQuery(inherited as never)).toThrow(TypeError);

  for (const acquisition of [false, null, "network"] as const) {
    expect(() =>
      createQuery({
        name: "invalid.query",
        acquisition,
        key: () => "invalid",
        fetch: async () => 1,
      } as never),
    ).toThrow(TypeError);
  }
});

test("a graph retains the acquisition captured from a structural query", async () => {
  const backend = testBackend();
  const definition = {
    kind: "query" as const,
    name: "structural.acquisition",
    acquisition: "fetch" as "cache" | "fetch",
    key: (input: string) => input,
    fetch: backend.respond<string, number>("structural.acquisition"),
  };
  let descriptorReads = 0;
  const query = new Proxy(definition, {
    getOwnPropertyDescriptor(target, property) {
      if (property === "acquisition") {
        descriptorReads += 1;
        return {
          configurable: true,
          enumerable: true,
          writable: true,
          value: descriptorReads === 1 ? "fetch" : "cache",
        };
      }
      return Reflect.getOwnPropertyDescriptor(target, property);
    },
  });
  const operations = {
    get query() {
      descriptorReads = 0;
      return query;
    },
  };
  const graph = createGraph({
    contract: defineContract({ namespace: "structural-acquisition", operations }),
  });
  definition.acquisition = "cache";

  try {
    const first = graph.api.query("structural-key");
    backend.resolve(backend.calls[0]!, 1);
    await expect(first.ready).resolves.toBe(1);

    const second = graph.api.query("structural-key");
    expect(backend.calls).toHaveLength(2);
    backend.resolve(backend.calls[1]!, 2);
    await expect(second.ready).resolves.toBe(2);
  } finally {
    graph.dispose();
  }
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
