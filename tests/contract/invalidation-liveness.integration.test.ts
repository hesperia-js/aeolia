import { afterEach, beforeEach, expect, jest, test } from "bun:test";
import { affects, createGraph, createMutation, createQuery, subscribe } from "../../src/index.ts";
import { testBackend } from "../../src/testing.ts";

beforeEach(() => {
  jest.useFakeTimers();
  jest.setSystemTime(0);
});

afterEach(() => {
  jest.clearAllTimers();
  jest.useRealTimers();
});

test.each(["observed", "unobserved", "unsubscribed", "metadata", "eager"] as const)(
  "mutation invalidation respects current value liveness: %s",
  async (mode) => {
    const backend = testBackend();
    const list = createQuery({
      name: "todos.list",
      key: () => "todos",
      fetch: backend.respond<void, string[]>("todos.list"),
    });
    const create = createMutation({
      name: "todos.create",
      run: backend.perform<string, void>("todos.create"),
      affects: [affects(list, { on: mode === "eager" ? "revalidate" : "invalidate" })],
    });
    const graph = createGraph({ contract: { namespace: "todos", operations: { list, create } } });
    const values: Array<string[] | undefined> = [];
    const store = graph.api.list();
    const stop =
      mode === "observed" || mode === "unsubscribed"
        ? subscribe(store.value, (value) => values.push(value))
        : mode === "metadata"
          ? subscribe(store.status, () => undefined)
          : () => undefined;
    try {
      const initial = store.ready;
      backend.resolve(backend.calls[0]!, []);
      await initial;
      if (mode === "unsubscribed") stop();
      const mutation = graph.api.create("Write a test");
      expect(backend.calls.filter((call) => call.name === "todos.list")).toHaveLength(1);
      backend.resolve(backend.calls[1]!, undefined);
      await mutation;
      const refreshes = mode === "observed" || mode === "eager";
      expect(backend.calls.filter((call) => call.name === "todos.list")).toHaveLength(
        refreshes ? 2 : 1,
      );
      expect(store.status.get()).toBe(refreshes ? "revalidating" : "stale");
      expect(store.pending.get()).toBe(refreshes);
      expect(store.value.get()).toStrictEqual([]);
      if (refreshes) {
        const ready = store.ready;
        backend.resolve(backend.calls[2]!, ["Write a test"]);
        expect(await ready).toStrictEqual(["Write a test"]);
        expect(store.value.get()).toStrictEqual(["Write a test"]);
        if (mode === "observed") expect(values).toStrictEqual([undefined, [], ["Write a test"]]);
      }
    } finally {
      stop();
      graph.dispose();
    }
  },
);

test.each(["restore", "leave-again", "fail"] as const)(
  "an invalidated query resumes refresh with returning value observers: %s",
  async (mode) => {
    const backend = testBackend();
    const list = createQuery({
      name: "todos.list",
      key: () => "todos",
      fetch: backend.respond<void, string[]>("todos.list"),
    });
    const save = createMutation({
      name: "todos.save",
      run: backend.perform<void, void>("todos.save"),
      affects: [affects(list, {})],
    });
    const graph = createGraph({ contract: { namespace: "todos", operations: { list, save } } });
    const store = graph.api.list();
    const values: Array<string[] | undefined> = [];
    let stop = subscribe(store.value, (value) => values.push(value));
    let stopSecond = () => {};
    let stopStatus = () => {};
    try {
      const initial = store.ready;
      backend.resolve(backend.calls[0]!, ["Pending"]);
      await initial;
      stop();
      const mutation = graph.api.save();
      backend.resolve(backend.calls[1]!, undefined);
      await mutation;
      expect(store.status.get()).toBe("stale");
      expect(store.value.get()).toStrictEqual(["Pending"]);
      stopStatus = subscribe(store.status, () => {});
      jest.advanceTimersByTime(100);
      expect(backend.calls).toHaveLength(2);

      stop = subscribe(store.value, (value) => values.push(value));
      stopSecond = subscribe(store.value, () => {});
      expect(backend.calls).toHaveLength(2);
      if (mode === "leave-again") {
        stop();
        stopSecond();
        jest.advanceTimersByTime(1);
        expect(backend.calls).toHaveLength(2);
        expect(store.status.get()).toBe("stale");
        return;
      }

      jest.advanceTimersByTime(1);
      expect(backend.calls).toHaveLength(3);
      expect(store.status.get()).toBe("revalidating");
      expect(store.value.get()).toStrictEqual(["Pending"]);
      stop();
      stopSecond();
      stop = subscribe(store.value, (value) => values.push(value));
      jest.advanceTimersByTime(1);
      expect(backend.calls).toHaveLength(3);
      const ready = store.ready.catch((error: unknown) => error);
      const failure = new Error("backend unavailable");
      if (mode === "fail") backend.reject(backend.calls[2]!, failure);
      else backend.resolve(backend.calls[2]!, ["Completed"]);
      expect(await ready).toStrictEqual(mode === "fail" ? failure : ["Completed"]);
      expect(store.status.get()).toBe(mode === "fail" ? "failed" : "ready");
      expect(values.at(-1)).toStrictEqual(mode === "fail" ? ["Pending"] : ["Completed"]);

      stop();
      stop = subscribe(store.value, () => {});
      jest.advanceTimersByTime(100);
      expect(backend.calls).toHaveLength(3);
    } finally {
      stop();
      stopSecond();
      stopStatus();
      graph.dispose();
    }
  },
);
