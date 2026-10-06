import { describe, expect, it } from "bun:test";
import {
  createGraph,
  createMutation,
  createQuery,
  affects,
  defineContract,
  queryOptions,
  storeKey,
} from "../../src/contract/index.ts";
import type { QueryStore, StoreKey } from "../../src/contract/index.ts";
import { testBackend } from "../../src/testing.ts";

describe("branded operation options", () => {
  it("keeps branded options ahead of input normalization and merges second-argument options", async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "options.query",
      key: () => "options/query",
      fetch: backend.respond<void, number>("options.query"),
    });
    const oneArgumentQuery = createQuery({
      name: "options.one-argument-query",
      key: () => "options/one-argument-query",
      fetch: backend.respond<void, number>("options.one-argument-query"),
    });
    const mutation = createMutation({
      name: "options.mutation",
      affects: [],
      run: backend.perform<void, string>("options.mutation"),
    });
    const graph = createGraph({
      contract: defineContract({
        namespace: "operation-options",
        operations: { query, oneArgumentQuery, mutation },
      }),
    });
    const api = graph.api as unknown as {
      readonly query: (...args: unknown[]) => QueryStore<number>;
      readonly oneArgumentQuery: (...args: unknown[]) => QueryStore<number>;
      readonly mutation: (...args: unknown[]) => Promise<string>;
    };

    const oneArgumentStore = api.oneArgumentQuery(queryOptions({ initial: 7 }));
    expect(oneArgumentStore.value.get()).toBe(7);

    const firstQuerySignal = new AbortController();
    const queryOverrideSignal = new AbortController();
    const store = api.query(queryOptions({ initial: 1, abortSignal: firstQuerySignal.signal }), {
      initial: 2,
      abortSignal: queryOverrideSignal.signal,
    });
    expect(store.value.get()).toBe(2);
    expect(backend.calls).toHaveLength(0);

    const oneArgumentMutation = api.mutation(queryOptions({}));
    const oneArgumentCall = backend.calls[0]!;
    expect(oneArgumentCall.input).toBeUndefined();
    backend.resolve(oneArgumentCall, "saved once");
    await expect(oneArgumentMutation).resolves.toBe("saved once");

    const firstMutationSignal = new AbortController();
    const mutationOverrideSignal = new AbortController();
    const mutationTask = api.mutation(queryOptions({ abortSignal: firstMutationSignal.signal }), {
      abortSignal: mutationOverrideSignal.signal,
    });
    const call = backend.calls[1]!;
    expect(call.input).toBeUndefined();
    firstMutationSignal.abort();
    expect(call.aborted).toBe(false);
    mutationOverrideSignal.abort();
    expect(call.aborted).toBe(true);
    backend.resolve(call, "saved");
    await expect(mutationTask).resolves.toBe("saved");

    graph.dispose();
  });

  it("accepts structural cancellation and readonly maps while preserving explicit operation input", async () => {
    const backend = testBackend();
    const controller = new AbortController();
    const signal: AbortSignal = {
      get aborted() {
        return controller.signal.aborted;
      },
      get reason() {
        return controller.signal.reason;
      },
      onabort: null,
      addEventListener: controller.signal.addEventListener.bind(controller.signal),
      removeEventListener: controller.signal.removeEventListener.bind(controller.signal),
      dispatchEvent: controller.signal.dispatchEvent.bind(controller.signal),
      throwIfAborted: controller.signal.throwIfAborted.bind(controller.signal),
    };
    const producers = new Map<StoreKey, (current: unknown, input: unknown) => unknown>([
      [storeKey("options/custom"), () => 9],
    ]);
    const optimistic: ReadonlyMap<StoreKey, (current: unknown, input: unknown) => unknown> = {
      size: producers.size,
      get: producers.get.bind(producers),
      has: producers.has.bind(producers),
      entries: producers.entries.bind(producers),
      keys: producers.keys.bind(producers),
      values: producers.values.bind(producers),
      forEach: producers.forEach.bind(producers),
      [Symbol.iterator]: producers[Symbol.iterator].bind(producers),
    };
    const query = createQuery({
      name: "custom.query",
      key: () => "options/custom",
      fetch: backend.respond<void, number>("custom.query"),
    });
    const mutation = createMutation({
      name: "custom.mutation",
      affects: [affects(query, {})],
      run: backend.perform<{ optimistic: string }, string>("custom.mutation"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "custom-options", operations: { query, mutation } }),
    });
    try {
      const options = Object.assign(Object.create(null), {
        initial: 1,
        equals: undefined,
        extra: "ignored",
      });
      const store = graph.api.query(queryOptions(options));
      const input = { optimistic: "application data" };
      const task = graph.api.mutation(input, { abortSignal: signal, optimistic });
      const call = backend.calls[0]!;
      expect(call.input).toBe(input);
      expect(store.value.get()).toBe(9);
      controller.abort();
      expect(call.aborted).toBe(true);
      backend.resolve(call, "saved");
      await expect(task).resolves.toBe("saved");
    } finally {
      graph.dispose();
    }
  });
});
