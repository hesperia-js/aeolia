import { afterEach, beforeEach, describe, expect, it, jest } from "bun:test";
import { Fault, onFault } from "../../src/fault.ts";
import { createGraph } from "../../src/contract/index.ts";
import {
  affects,
  defineContract,
  createMutation,
  createQuery,
  storeKey,
  type FetchOptions,
  type CallContext,
} from "../../src/contract/index.ts";
import { testBackend } from "../../src/testing.ts";

beforeEach(() => {
  jest.useFakeTimers();
  jest.setSystemTime(0);
});

afterEach(() => {
  jest.clearAllTimers();
  jest.useRealTimers();
});

async function flush(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
}

function userQuery(backend: ReturnType<typeof testBackend>) {
  return createQuery({
    name: "users.get",
    key: (input: { readonly id: string }) => `users/${input.id}`,
    fetch: backend.respond<{ readonly id: string }, { readonly id: string }>("users.get"),
  });
}

describe("mutation effects and failure routing", () => {
  it("applies optimistic predictions and invalidates only after successful mutation", async () => {
    const backend = testBackend();
    const query = userQuery(backend);
    const rename = createMutation({
      name: "users.rename",
      affects: [
        affects(query, {
          select: (input: { readonly id: string; readonly name: string }) => ({ id: input.id }),
          on: "invalidate",
          optimistic: (current, input: { readonly id: string; readonly name: string }) => ({
            id: `${current?.id ?? input.id}-optimistic`,
          }),
        }),
      ],
      run: backend.perform<{ readonly id: string; readonly name: string }, string>("users.rename"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "operations", operations: { query, rename } }),
    });
    const store = graph.api.query({ id: "optimistic" });
    backend.resolve(backend.calls[0]!, { id: "base" });
    await flush();
    const mutation = graph.api.rename({ id: "optimistic", name: "next" });
    expect(store.value.get()).toEqual({ id: "base-optimistic" });
    backend.resolve(backend.calls[1]!, "ok");
    await mutation;
    await flush();
    expect(store.value.get()).toEqual({ id: "base-optimistic" });
    expect(store.status.get()).toBe("stale");
    graph.dispose();
  });

  it("removes predictions on mutation failure and routes prediction reads to the fault channel", async () => {
    const backend = testBackend();
    const query = userQuery(backend);
    const failing = createMutation({
      name: "users.failing",
      affects: [
        affects(query, {
          select: (input: { readonly id: string }) => input,
          on: "invalidate",
          optimistic: (current) => ({ id: `${current?.id ?? "none"}-optimistic` }),
        }),
      ],
      run: backend.perform<{ readonly id: string }, string>("users.failing"),
    });
    const reads = createMutation({
      name: "users.reads",
      affects: [
        affects(query, { select: (input: { readonly id: string }) => input, on: "invalidate" }),
      ],
      run: backend.perform<{ readonly id: string }, string>("users.reads"),
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "operations", operations: { query, failing, reads } }),
    });
    const store = graph.api.query({ id: "prediction" });
    store.set({ id: "base" });
    const failure = graph.api.failing({ id: "prediction" });
    expect(store.value.get()).toEqual({ id: "base-optimistic" });
    backend.reject(
      backend.calls.find((call) => call.name === "users.failing")!,
      new Error("rejected"),
    );
    await expect(failure).rejects.toThrow("rejected");
    await flush();
    expect(store.value.get()).toEqual({ id: "base" });

    const faults: Fault[] = [];
    const stop = onFault(graph, (fault) => faults.push(fault));
    const read = graph.api.reads(
      { id: "prediction" },
      {
        optimistic: new Map([[storeKey("users/prediction"), () => store.value.get()]]),
      },
    );
    expect(store.value.get()).toEqual({ id: "base" });
    expect(faults.some((fault) => fault.kind === "prediction")).toBe(true);
    backend.resolve(
      backend.calls.find((call) => call.name === "users.reads")!,
      "ok",
    );
    await read;
    stop();
    graph.dispose();
  });

  it("sends public graph/key context to query and mutation callbacks", async () => {
    const calls: Array<{ readonly graph: unknown; readonly key?: unknown }> = [];
    const query = createQuery({
      name: "context.query",
      key: () => "context/query",
      fetch: async (_input: undefined, options: FetchOptions) => {
        calls.push({ graph: options.graph, key: options.key });
        return 1;
      },
    });
    const mutation = createMutation({
      name: "context.mutation",
      affects: [],
      run: async (_input: undefined, options: CallContext) => {
        calls.push({ graph: options.graph });
        return "ok";
      },
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "operations", operations: { query, mutation } }),
    });
    graph.api.query(undefined);
    await flush();
    expect(calls[0]?.graph).toBe(graph.id);
    expect(calls[0]?.key).toBe(storeKey("context/query"));
    await graph.api.mutation(undefined);
    expect(calls[1]).toEqual({ graph: graph.id });
    graph.dispose();
  });

  it("continues after a settled-effect selector fault and reports it on the graph channel", async () => {
    const backend = testBackend();
    const query = userQuery(backend);
    let firstSelection = true;
    const broken = createMutation({
      name: "effects.broken",
      affects: [
        affects(query, {
          select: (input: { readonly id: string }) => {
            if (firstSelection) {
              firstSelection = false;
              return input;
            }
            throw new Error("bad selector");
          },
          on: "invalidate",
        }),
        affects(query, {
          select: (input: { readonly id: string }) => input,
          on: "invalidate",
        }),
      ],
      run: backend.perform<{ readonly id: string }, string>("effects.broken"),
    });
    const contract = defineContract({ namespace: "operations", operations: { query, broken } });
    const graph = createGraph({ contract });
    const store = graph.api.query({ id: "effects" });
    store.set({ id: "effects" });
    const faults: Fault[] = [];
    const stop = onFault(graph, (fault) => faults.push(fault));
    const promise = graph.api.broken({ id: "effects" });
    backend.resolve(
      backend.calls.find((call) => call.name === "effects.broken")!,
      "ok",
    );
    await promise;
    expect(faults.some((fault) => fault.kind === "contract")).toBe(true);
    expect(store.status.get()).toBe("stale");
    stop();
    graph.dispose();
  });
});
