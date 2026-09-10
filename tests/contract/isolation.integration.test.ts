import { afterEach, beforeEach, describe, expect, it, jest } from "bun:test";
import {
  createGraph,
  defineContract,
  createQuery,
  watch,
  type Graph,
  type QueryStore,
} from "../../src/index.ts";
import { testBackend, type CallRecord, type TestBackend } from "../../src/testing.ts";

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

type User = Readonly<{ id: string; revision: number }>;

type Trace = Readonly<{
  calls: readonly Readonly<{
    graph: "A";
    name: string;
    key: string | undefined;
    input: unknown;
    aborted: boolean;
    settled: boolean;
  }>[];
  values: readonly (User | undefined)[];
}>;

function callFor(backend: TestBackend, graph: Graph<any>, index: number): CallRecord {
  const calls = backend.calls.filter((call) => call.graph === graph.id);
  const call = calls[index];
  if (call === undefined) throw new Error(`missing call ${index}`);
  return call;
}

async function runTrace(interleave: boolean): Promise<Trace> {
  const backend = testBackend();
  const query = createQuery({
    name: "users.get",
    key: (input: { id: string }) => `users/${input.id}`,
    revalidateAfterMs: 10,
    fetch: backend.respond<{ id: string }, User>("users.get"),
  });
  const contract = defineContract({
    namespace: "isolation-fixture",
    operations: { users: { get: query } },
  });
  const graphA = createGraph({ contract, idleMs: 1_000 });
  const graphB = interleave ? createGraph({ contract, idleMs: 1_000 }) : undefined;
  const graphC = interleave ? createGraph({ contract, idleMs: 1_000 }) : undefined;
  const storeA = graphA.api.users.get({ id: "a" });
  const releaseA = watch(storeA.value, () => {});
  const values: (User | undefined)[] = [storeA.value.get()];

  let storeB: QueryStore<User> | undefined;
  let storeC: QueryStore<User> | undefined;
  if (graphB != null && graphC != null) {
    storeB = graphB.api.users.get({ id: "b" });
    storeC = graphC.api.users.get({ id: "c" });
  }

  backend.resolve(callFor(backend, graphA, 0), { id: "a", revision: 1 });
  if (graphB != null && graphC != null) {
    backend.resolve(callFor(backend, graphC, 0), { id: "c", revision: 1 });
    backend.resolve(callFor(backend, graphB, 0), { id: "b", revision: 1 });
  }
  await flush();
  values.push(storeA.value.get());

  if (graphB != null && graphC != null) {
    const revalidationC = storeC!.revalidate();
    backend.resolve(callFor(backend, graphC, 1), { id: "c", revision: 2 });
    await revalidationC;
  }
  jest.advanceTimersByTime(11);
  await flush();
  expect(callFor(backend, graphA, 1).settled).toBe(false);
  values.push(storeA.value.get());

  const revalidationA = storeA.revalidate();
  if (graphB != null && graphC != null) {
    const revalidationB = storeB!.revalidate();
    backend.resolve(callFor(backend, graphB, 1), { id: "b", revision: 2 });
    await revalidationB;
  }
  expect(callFor(backend, graphA, 1).aborted).toBe(true);
  backend.resolve(callFor(backend, graphA, 1), { id: "a", revision: -1 });
  backend.resolve(callFor(backend, graphA, 2), { id: "a", revision: 2 });
  await revalidationA;
  await flush();
  values.push(storeA.value.get());
  expect(storeA.value.get()).toEqual({ id: "a", revision: 2 });

  jest.advanceTimersByTime(7);
  await flush();
  jest.advanceTimersByTime(4);
  await flush();
  expect(callFor(backend, graphA, 3).settled).toBe(false);
  backend.resolve(callFor(backend, graphA, 3), { id: "a", revision: 3 });
  await flush();
  values.push(storeA.value.get());
  expect(storeA.value.get()).toEqual({ id: "a", revision: 3 });

  const calls = backend.calls
    .filter((call) => call.graph === graphA.id)
    .map((call) => ({
      graph: "A" as const,
      name: call.name,
      key: call.key === undefined ? undefined : String(call.key),
      input: call.input,
      aborted: call.aborted,
      settled: call.settled,
    }));

  releaseA();
  graphA.dispose();
  graphB?.dispose();
  graphC?.dispose();
  return { calls, values };
}

describe("graph differential isolation", () => {
  it("leaves graph A unchanged when B and C interleave at every settle and time advance", async () => {
    const solo = await runTrace(false);
    jest.clearAllTimers();
    jest.setSystemTime(0);
    const interleaved = await runTrace(true);

    expect(interleaved).toEqual(solo);
    expect(solo.calls).toHaveLength(4);
  });
});
