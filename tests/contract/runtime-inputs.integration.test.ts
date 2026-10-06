import { expect, it } from "bun:test";
import { Fault } from "../../src/fault.ts";
import {
  createGraph,
  createMutation,
  createQuery,
  createStream,
  createStore,
  affects,
  defineContract,
  project,
  queryOptions,
  storeKey,
} from "../../src/contract/index.ts";

function fixture() {
  const calls: string[] = [];
  const query = createQuery({
    name: "checked-query",
    key: () => {
      calls.push("query key");
      return "checked/query";
    },
    fetch: async () => {
      calls.push("fetch");
      return 1;
    },
  });
  const stream = createStream({
    name: "checked-stream",
    key: () => {
      calls.push("stream key");
      return "checked/stream";
    },
    open: async function* () {
      calls.push("open");
      yield 1;
    },
  });
  const mutation = createMutation({
    name: "checked-mutation",
    affects: [],
    run: async () => {
      calls.push("run");
      return 1;
    },
  });
  const contract = defineContract({
    namespace: "runtime-inputs",
    operations: { query, stream, mutation },
  });
  return { calls, stream, contract };
}

const invalidValueOptions = [
  { snapshot: "yes" },
  { equals: true },
  { abortSignal: {} },
  { abortSignal: null },
  { abortSignal: { aborted: false, addEventListener: false, removeEventListener() {} } },
];

it.each(invalidValueOptions)(
  "rejects malformed value options before query or stream callbacks: %j",
  (options) => {
    const { calls, contract } = fixture();
    const graph = createGraph({ contract });
    try {
      const invokeQuery = graph.api.query as (...args: unknown[]) => unknown;
      const invokeStream = graph.api.stream as (...args: unknown[]) => unknown;
      expect(() => invokeQuery(options)).toThrow(TypeError);
      expect(() => invokeQuery(queryOptions(options as never))).toThrow(TypeError);
      expect(() => invokeQuery(undefined, options)).toThrow(TypeError);
      expect(() => invokeStream(undefined, options)).toThrow(TypeError);
      expect(calls).toStrictEqual([]);
      expect(graph.api.query({ initial: 7 }).value.get()).toBe(7);
    } finally {
      graph.dispose();
    }
  },
);

it.each([null, false, "options", [], () => undefined].map((value) => [value]))(
  "rejects non-object explicit query and stream options: %j",
  (options) => {
    const { calls, contract } = fixture();
    const graph = createGraph({ contract });
    try {
      expect(() =>
        (graph.api.query as (...args: unknown[]) => unknown)(undefined, options),
      ).toThrow(TypeError);
      expect(() =>
        (graph.api.stream as (...args: unknown[]) => unknown)(undefined, options),
      ).toThrow(TypeError);
      expect(calls).toStrictEqual([]);
    } finally {
      graph.dispose();
    }
  },
);

it.each([
  { optimistic: {} },
  { optimistic: new Map([["checked/query", false]]) },
  { optimistic: new Map([[42, () => 1]]) },
  { optimistic: new Map([["", () => 1]]) },
  { abortSignal: {} },
  { abortSignal: null },
])("rejects malformed mutation options before dispatch: %j", (options) => {
  const { calls, contract } = fixture();
  const graph = createGraph({ contract });
  try {
    const invoke = graph.api.mutation as (...args: unknown[]) => unknown;
    expect(() => invoke(options)).toThrow(TypeError);
    expect(() => invoke(queryOptions(options as never))).toThrow(TypeError);
    expect(() => invoke(undefined, options)).toThrow(TypeError);
    expect(calls).toStrictEqual([]);
  } finally {
    graph.dispose();
  }
});

it("rejects malformed revalidation options without changing the ready store", () => {
  const { calls, contract } = fixture();
  const graph = createGraph({ contract });
  try {
    const store = graph.api.query({ initial: 7 });
    calls.length = 0;
    expect(() => store.revalidate({ abortSignal: {} as AbortSignal })).toThrow(TypeError);
    expect(store.status.get()).toBe("ready");
    expect(store.value.get()).toBe(7);
    expect(calls).toStrictEqual([]);
  } finally {
    graph.dispose();
  }
});

it.each([{ abortSignal: {} }, { abortSignal: null }, { onUnobservedFault: true }])(
  "rejects invalid graph lifecycle options at creation: %j",
  (options) => {
    const { contract } = fixture();
    let graph: ReturnType<typeof createGraph> | undefined;
    try {
      expect(() => {
        graph = createGraph({ contract, ...options } as never);
      }).toThrow(TypeError);
    } finally {
      graph?.dispose();
    }
  },
);

it.each([
  { kind: "unknown", max: 1 },
  { kind: "accumulate", max: 1, onOverflow: "ignore" },
  { kind: "reduce", initial: 0, step: false },
])("rejects malformed projection policies before opening a source: %j", (policy) => {
  const { calls, contract, stream } = fixture();
  const graph = createGraph({ contract });
  try {
    expect(() => project(graph, stream, undefined, policy as never)).toThrow(TypeError);
    expect(calls).toStrictEqual([]);
    const shared = graph.api.stream(undefined);
    expect(() => project(shared, policy as never)).toThrow(TypeError);
  } finally {
    graph.dispose();
  }
});

it.each([42, {}, [], null, undefined].map((value) => [value]))(
  "rejects non-string store identities: %j",
  (key) => {
    expect(() => storeKey(key as string)).toThrow(TypeError);
    const { calls, contract } = fixture();
    const graph = createGraph({ contract });
    try {
      expect(() => graph.at(key as string)).toThrow(TypeError);
      expect(calls).toStrictEqual([]);
    } finally {
      graph.dispose();
    }
  },
);

it("rejects malformed declarations and effect callbacks without executing author code", () => {
  const { calls, contract } = fixture();
  const query = contract.operations.query;
  const stream = contract.operations.stream;
  const mutation = contract.operations.mutation;
  expect(() => createStore({ name: "bad", initial: 0, equals: true } as never)).toThrow(TypeError);
  expect(() => createQuery({ ...query, fetch: false } as never)).toThrow(TypeError);
  expect(() => createStream({ ...stream, key: false } as never)).toThrow(TypeError);
  expect(() => createMutation({ ...mutation, run: false } as never)).toThrow(TypeError);
  expect(() => createMutation({ ...mutation, affects: [null] } as never)).toThrow(TypeError);
  for (const spec of [{ on: "retry" }, { select: false }, { optimistic: 1 }, { on: null }]) {
    expect(() => affects(query, spec as never)).toThrow(TypeError);
  }
  expect(calls).toStrictEqual([]);
});

it("validates directly supplied contract definitions before creating a graph", () => {
  const { calls, contract } = fixture();
  const malformed = {
    namespace: "malformed",
    operations: { query: { ...contract.operations.query, fetch: false } },
  };
  expect(() => defineContract(malformed as never)).toThrow(TypeError);
  let graph: ReturnType<typeof createGraph> | undefined;
  try {
    expect(() => {
      graph = createGraph({ contract: malformed } as never);
    }).toThrow(TypeError);
    expect(calls).toStrictEqual([]);
  } finally {
    graph?.dispose();
  }
});

it.each([-1, Infinity, NaN])(
  "rejects invalid freshness before configuring a keyed store: %j",
  (revalidateAfterMs) => {
    const { calls, contract } = fixture();
    const graph = createGraph({ contract });
    try {
      expect(() => graph.api.query({ initial: 1, snapshot: false, revalidateAfterMs })).toThrow(
        Fault,
      );
      expect(calls).toStrictEqual([]);
      expect(graph.api.query({ initial: 2, snapshot: true }).value.get()).toBe(2);
    } finally {
      graph.dispose();
    }
  },
);

it.each([{ idleMs: null }, { maxPredictions: null }])(
  "rejects null graph bounds instead of applying defaults: %j",
  (options) => {
    const { contract } = fixture();
    let graph: ReturnType<typeof createGraph> | undefined;
    try {
      expect(() => {
        graph = createGraph({ contract, ...options } as never);
      }).toThrow(Fault);
    } finally {
      graph?.dispose();
    }
  },
);
