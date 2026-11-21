import { describe, expect, it } from "bun:test";
import {
  bindOperation,
  createBindingContext,
  predictionFor,
  profileEffect,
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

describe("bound mutation options and capability validation", () => {
  it("normalizes branded empty/options-only forms and preserves explicit option-like business input", async () => {
    const backend = testBackend();
    const empty = createMutation({
      name: "empty",
      run: backend.perform<void, string>("empty"),
      affects: [],
    });
    const business = createMutation({
      name: "business",
      run: backend.perform<{ readonly optimistic: string }, string>("business"),
      affects: [],
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "bound-options", operations: { empty, business } }),
    });
    try {
      const context = createBindingContext(graph);
      const inputless = bindOperation(graph, empty, {
        context,
        profile: {},
        run: empty.run,
        effects: [],
      });
      const required = bindOperation(graph, business, {
        context,
        profile: {},
        run: business.run,
        effects: [],
      });
      const first = inputless.member(queryOptions({}));
      expect(backend.pending("empty")[0]!.input).toBeUndefined();
      backend.resolve(backend.pending("empty")[0]!, "one");
      await first;
      const original = new AbortController();
      const selected = new AbortController();
      const second = inputless.member(queryOptions({ abortSignal: original.signal }), {
        abortSignal: selected.signal,
      });
      const call = backend.pending("empty")[0]!;
      original.abort();
      expect(call.aborted).toBe(false);
      selected.abort();
      expect(call.aborted).toBe(true);
      backend.resolve(call, "two");
      await second;
      const data = { optimistic: "business data" };
      const third = required.member(data, {});
      expect(backend.pending("business")[0]!.input).toBe(data);
      backend.resolve(backend.pending("business")[0]!, "three");
      await third;
      const structural: AbortSignal = {
        aborted: false,
        onabort: null,
        reason: undefined,
        addEventListener: selected.signal.addEventListener.bind(selected.signal),
        removeEventListener: selected.signal.removeEventListener.bind(selected.signal),
        dispatchEvent: selected.signal.dispatchEvent.bind(selected.signal),
        throwIfAborted: () => undefined,
      };
      const fourth = inputless.member({ abortSignal: structural });
      expect(backend.pending("empty")[0]!.aborted).toBe(false);
      backend.resolve(backend.pending("empty")[0]!, "four");
      await fourth;
    } finally {
      graph.dispose();
    }
  });

  it.each(["inherited", "non-enumerable"] as const)(
    "reads actual %s option fields once for cancellation and physical overrides",
    async (shape) => {
      const backend = testBackend();
      let selections = 0;
      let producerCalls = 0;
      const reads: Record<string, number> = {};
      const structural = <T extends object>(data: T): T => {
        const owner = {};
        for (const field of Object.keys(data))
          Object.defineProperty(owner, field, {
            enumerable: shape === "inherited",
            get: () => {
              reads[field] = (reads[field] ?? 0) + 1;
              return Reflect.get(data, field);
            },
          });
        return (shape === "inherited" ? Object.create(owner) : owner) as T;
      };
      const query = createQuery({ name: "query", key: () => "same", fetch: async () => 1 });
      const mutation = createMutation({
        name: "save",
        run: backend.perform<void, string>("save"),
        affects: [
          affects(query, {
            select: () => {
              selections += 1;
              return undefined;
            },
          }),
        ],
      });
      const graph = createGraph({
        contract: defineContract({
          namespace: "structural-bound-options",
          operations: { query, mutation },
        }),
      });
      try {
        const context = createBindingContext(graph);
        const first = bindOperation(graph, query, { context, profile: {}, fetch: query.fetch });
        const second = bindOperation(graph, query, { context, profile: {}, fetch: query.fetch });
        const a = first.member(undefined, { initial: 1 });
        const b = second.member(undefined, { initial: 2 });
        const effect = profileEffect(mutation, 0, query, {
          prepare: (phase) => phase.existing(undefined).map((target) => phase.prepare(target)),
          settle: () => [],
        });
        const bound = bindOperation(graph, mutation, {
          context,
          profile: {},
          effects: [effect],
          run: mutation.run,
        });
        const override = predictionFor(mutation, first.target(a)!, () => {
          producerCalls += 1;
          return 10;
        });
        const canceled = new AbortController();
        canceled.abort();
        const denied = bound.member(
          undefined,
          structural({ abortSignal: canceled.signal, optimistic: [override] }),
        );
        expect(backend.calls).toHaveLength(0);
        expect(selections).toBe(0);
        expect(producerCalls).toBe(0);
        await expect(denied).rejects.toMatchObject({ name: "AbortError" });
        expect(reads).toEqual({ abortSignal: 1, optimistic: 1 });
        expect([a.value.get(), b.value.get()]).toEqual([1, 2]);
        const live = new AbortController();
        const running = bound.member(
          undefined,
          structural({ abortSignal: live.signal, optimistic: [override] }),
        );
        expect(reads).toEqual({ abortSignal: 2, optimistic: 2 });
        expect([a.value.get(), b.value.get()]).toEqual([10, 2]);
        expect(producerCalls).toBeGreaterThan(0);
        expect(backend.calls).toHaveLength(1);
        backend.resolve(backend.pending("save")[0]!, "saved");
        await expect(running).resolves.toBe("saved");
        expect(selections).toBe(2);
      } finally {
        graph.dispose();
      }
    },
  );

  it("rejects fabricated, foreign and duplicate overrides before selectors or run, while equal keys remain distinct", async () => {
    let selections = 0;
    let runs = 0;
    const query = createQuery({ name: "query", key: () => "same", fetch: async () => 1 });
    const mutation = createMutation({
      name: "mutation",
      affects: [
        affects(query, {
          select: () => {
            selections += 1;
            return undefined;
          },
        }),
      ],
      run: async () => {
        runs += 1;
        return "ok";
      },
    });
    const other = createMutation({
      name: "other",
      affects: [affects(query, {})],
      run: async () => "other",
    });
    const graph = createGraph({
      contract: defineContract({
        namespace: "checked-overrides",
        operations: { query, mutation, other },
      }),
    });
    try {
      const context = createBindingContext(graph);
      const first = bindOperation(graph, query, { context, profile: {}, fetch: query.fetch });
      const second = bindOperation(graph, query, { context, profile: {}, fetch: query.fetch });
      const foreign = bindOperation(graph, query, {
        context: createBindingContext(graph),
        profile: {},
        fetch: query.fetch,
      });
      const a = first.member(undefined, { initial: 1 });
      const b = second.member(undefined, { initial: 2 });
      const c = foreign.member(undefined, { initial: 3 });
      const effect = profileEffect(mutation, 0, query, {
        prepare: (phase) => phase.existing(undefined).map((target) => phase.prepare(target)),
        settle: () => [],
      });
      const profile = {};
      const options = { context, profile, run: mutation.run, effects: [effect] };
      const bound = bindOperation(graph, mutation, options);
      const invoke = bound.member as (...args: unknown[]) => Promise<unknown>;
      const p = predictionFor(mutation, first.target(a)!, () => 10);
      const q = predictionFor(mutation, second.target(b)!, () => 20);
      const invalid: unknown[] = [
        {},
        Object.create(p),
        predictionFor(other, first.target(a)!, () => 4),
        predictionFor(mutation, foreign.target(c)!, () => 4),
      ];
      for (const optimistic of invalid)
        expect(() => invoke(undefined, { optimistic: [optimistic] })).toThrow(TypeError);
      expect(() => invoke(undefined, { optimistic: [p, p] })).toThrow(TypeError);
      expect(() => invoke(undefined, { optimistic: new Map() })).toThrow(TypeError);
      expect(() => invoke(undefined, { abortSignal: {} })).toThrow(TypeError);
      expect(() => predictionFor(mutation, Object.create(first.target(a)!), () => 8)).toThrow(
        TypeError,
      );
      expect(() => bindOperation(graph, mutation, options)).toThrow(TypeError);
      expect(() =>
        bindOperation(graph, mutation, { ...options, profile: {}, effects: [] }),
      ).toThrow(TypeError);
      expect(() =>
        profileEffect(mutation, 1, query, { prepare: () => [], settle: () => [] }),
      ).toThrow(TypeError);
      expect([selections, runs]).toEqual([0, 0]);
      await bound.member(undefined, { optimistic: [p, q] });
      expect([a.value.get(), b.value.get(), c.value.get()]).toEqual([10, 20, 3]);
      expect([selections, runs]).toEqual([2, 1]);
    } finally {
      graph.dispose();
    }
  });
});
