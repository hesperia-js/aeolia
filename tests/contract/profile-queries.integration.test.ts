import { afterEach, beforeEach, describe, expect, it, jest } from "bun:test";
import {
  adopt,
  bindOperation,
  createBindingContext,
  createGraph,
  createQuery,
  defineContract,
  Fault,
  queryOptions,
  snapshot,
  subscribe,
} from "../../src/index.ts";
import { testBackend } from "../../src/testing.ts";

beforeEach(() => {
  jest.useFakeTimers();
  jest.setSystemTime(0);
});

afterEach(() => {
  jest.clearAllTimers();
  jest.useRealTimers();
});

function fixture(idleMs = 100) {
  const backend = testBackend();
  let keyCalls = 0;
  const query = createQuery({
    name: "count",
    key: (input: { id: string }) => {
      keyCalls += 1;
      return input.id;
    },
    fetch: backend.respond<{ id: string }, { count: number }>("count"),
  });
  const other = createQuery({ ...query, name: "other" });
  const contract = defineContract({ namespace: "profiles", operations: { query, other } });
  const graph = createGraph({ contract, idleMs });
  return { backend, query, other, contract, graph, keyCalls: () => keyCalls };
}

function trackedCallerSignal(onLink: () => void) {
  const controller = new AbortController();
  const links = new Set<EventListenerOrEventListenerObject>();
  let added = 0;
  let removed = 0;
  const signal = new Proxy(controller.signal, {
    get(target, property) {
      if (property === "addEventListener")
        return (
          type: string,
          listener: EventListenerOrEventListenerObject,
          options?: boolean | AddEventListenerOptions,
        ) => {
          target.addEventListener(type, listener, options);
          if (type === "abort") {
            links.add(listener);
            added += 1;
            onLink();
          }
        };
      if (property === "removeEventListener")
        return (
          type: string,
          listener: EventListenerOrEventListenerObject,
          options?: boolean | EventListenerOptions,
        ) => {
          target.removeEventListener(type, listener, options);
          if (type === "abort") {
            links.delete(listener);
            removed += 1;
          }
        };
      const value: unknown = Reflect.get(target, property, target);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
  return {
    controller,
    signal,
    links,
    get added() {
      return added;
    },
    get removed() {
      return removed;
    },
  };
}

describe("context-bound query entries", () => {
  it("removes the caller link when authored registration consumes the last generation", async () => {
    const source = fixture();
    const target = fixture();
    const sourceBinding = bindOperation(source.graph, source.query, {
      context: createBindingContext(source.graph),
      profile: {},
      fetch: source.query.fetch,
    });
    const targetBinding = bindOperation(target.graph, target.query, {
      context: createBindingContext(target.graph),
      profile: {},
      fetch: target.query.fetch,
    });
    const reference = { role: "counter" };
    const handle = targetBinding.member({ id: "one" }, { initial: { count: 0 } });
    const caller = trackedCallerSignal(() => handle.set({ count: 100 }));
    try {
      sourceBinding.member({ id: "one" }, { initial: { count: 4 } });
      const encoded = snapshot(source.graph, {
        version: 2,
        bindings: [{ binding: sourceBinding, reference }],
      });
      const incoming = {
        ...encoded,
        entries: encoded.entries.map((row) => ({
          ...row,
          generation: Number.MAX_SAFE_INTEGER - 2,
        })),
      } as unknown as typeof encoded;
      adopt(target.graph, incoming, { bindings: [{ binding: targetBinding, reference }] });
      let refresh: Promise<void> | undefined;
      let threw = false;
      let reason: unknown;
      try {
        refresh = handle.revalidate({ abortSignal: caller.signal });
      } catch (error) {
        threw = true;
        reason = error;
      }
      const dispatched = [...target.backend.calls];
      for (const call of dispatched) target.backend.resolve(call, { count: 1 });
      if (refresh !== undefined) await refresh;

      expect(threw).toBe(true);
      expect(reason).toBeInstanceOf(RangeError);
      expect(dispatched).toHaveLength(0);
      expect(caller.added).toBe(1);
      expect(caller.removed).toBe(1);
      expect(caller.links.size).toBe(0);
      expect(handle.value.get()).toEqual({ count: 100 });
      expect(
        Number(
          snapshot(target.graph, {
            version: 2,
            bindings: [{ binding: targetBinding, reference }],
          }).entries[0]?.generation,
        ),
      ).toBe(Number.MAX_SAFE_INTEGER);
    } finally {
      caller.controller.abort();
      source.graph.dispose();
      target.graph.dispose();
    }
  });

  it("shares one native entry per full address while preserving raw logical keys", async () => {
    const { backend, query, other, graph } = fixture();
    const context = createBindingContext(graph);
    const secondContext = createBindingContext(graph);
    const profile = {};
    const first = bindOperation(graph, query, {
      context,
      profile,
      fetch: async (input, options) => (await query.fetch(input, options)).count,
    });
    const sibling = bindOperation(graph, query, {
      context,
      profile: {},
      fetch: async () => "label",
    });
    const anotherOperation = bindOperation(graph, other, {
      context,
      profile,
      fetch: async () => "other",
    });
    const anotherContext = bindOperation(graph, query, {
      context: secondContext,
      profile,
      fetch: async () => "context",
    });
    const one = first.member({ id: "arbitrary/profile:raw" });
    const two = first.member({ id: "arbitrary/profile:raw" });
    const label = sibling.member({ id: one.key });
    const operation = anotherOperation.member({ id: one.key });
    const environment = anotherContext.member({ id: one.key });
    const raw = graph.api.query({ id: one.key });
    expect(one.identity).toBe(two.identity);
    expect(
      new Set([
        one.identity,
        label.identity,
        operation.identity,
        environment.identity,
        raw.identity,
      ]).size,
    ).toBe(5);
    expect([label.key, operation.key, environment.key, raw.key]).toEqual([
      one.key,
      one.key,
      one.key,
      one.key,
    ]);
    expect(graph.at(one.key).identity).toBe(raw.identity);
    expect(backend.calls).toHaveLength(2);
    expect(backend.calls[0]).toMatchObject({
      graph: graph.id,
      key: one.key,
      input: { id: one.key },
    });
    backend.resolve(backend.calls[0]!, { count: 7 });
    backend.resolve(backend.calls[1]!, { count: 9 });
    expect(await one.ready).toBe(7);
    expect(await two.ready).toBe(7);
    expect(await label.ready).toBe("label");
    expect(await operation.ready).toBe("other");
    expect(await environment.ready).toBe("context");
    expect(await raw.ready).toEqual({ count: 9 });
    graph.dispose();
  });

  it("checks exact registration provenance before key computation or dispatch", () => {
    const { backend, query, contract, graph, keyCalls } = fixture();
    const context = createBindingContext(graph);
    const foreign = createGraph({ contract });
    const profile = {};
    const configuration = { context, profile, fetch: async () => ({ count: 1 }) };
    const binding = bindOperation(graph, query, configuration);
    expect(backend.calls).toHaveLength(0);
    expect(keyCalls()).toBe(0);
    expect(() => bindOperation(graph, query, configuration)).toThrow(TypeError);
    expect(() => bindOperation(foreign, query, configuration)).toThrow(TypeError);
    const lookalike = createQuery({ ...query });
    expect(() => bindOperation(graph, lookalike, { ...configuration, profile: {} })).toThrow(
      TypeError,
    );
    expect(keyCalls()).toBe(0);
    const raw = graph.api.query({ id: "one" });
    expect(() => binding.target(raw)).toThrow(TypeError);
    const own = binding.member({ id: "one" });
    expect(() => binding.target(Object.create(own))).toThrow(TypeError);
    expect(binding.target(own)?.forBinding(binding)).toBeDefined();
    graph.dispose();
    foreign.dispose();
  });

  it("retains completed fetch semantics for refresh and native failure", async () => {
    const { backend, query, graph } = fixture();
    const failure = { validation: "negative" };
    const binding = bindOperation(graph, query, {
      context: createBindingContext(graph),
      profile: {},
      fetch: async (input, options) => {
        const result = await query.fetch(input, options);
        if (result.count < 0) throw failure;
        return `count:${result.count}`;
      },
    });
    const handle = binding.member({ id: "one" });
    backend.resolve(backend.calls[0]!, { count: 2 });
    expect(await handle.ready).toBe("count:2");
    const refresh = handle.revalidate();
    backend.resolve(backend.calls[1]!, { count: -1 });
    await refresh;
    expect(handle.status.get()).toBe("failed");
    expect(handle.error.get()).toBe(failure);
    expect(handle.value.get()).toBe("count:2");
    const recovery = handle.revalidate();
    backend.resolve(backend.calls[2]!, { count: 3 });
    await recovery;
    expect(handle.value.get()).toBe("count:3");
    graph.dispose();
  });

  it("retains fixed initial defaults and fresh tickets for callers joining across live reset", async () => {
    const { backend, query, graph } = fixture();
    const binding = bindOperation(graph, query, {
      context: createBindingContext(graph),
      profile: {},
      fetch: async (input, options) => (await query.fetch(input, options)).count,
      value: { initial: 0 },
    });
    const handle = binding.member({ id: "one" });
    expect(handle.value.get()).toBe(0);
    expect(() => binding.member({ id: "one" }, { initial: 9 })).toThrow(TypeError);
    const initialRefresh = handle.revalidate();
    backend.resolve(backend.calls[0]!, { count: 1 });
    await initialRefresh;
    await handle.ready;
    const stop = subscribe(handle, () => {});
    const identity = handle.identity;
    const old = binding.target(handle)!;
    const reset = graph.reset();
    const fresh = binding.target(handle);
    expect(fresh).toBeDefined();
    expect(fresh?.identity).toBe(identity);
    expect(old.forBinding(binding)).toBeUndefined();
    expect(handle.value.get()).toBeUndefined();
    const during = binding.member({ id: "one" });
    expect(during.identity).toBe(identity);
    expect(during.value.get()).toBeUndefined();
    expect(() => binding.member({ id: "one" }, { initial: 0 })).toThrow(Fault);
    expect(backend.calls).toHaveLength(2);
    backend.resolve(backend.calls[1]!, { count: 4 });
    await reset;
    expect(handle.value.get()).toBe(4);
    const after = binding.member({ id: "one" });
    expect(after.identity).toBe(identity);
    expect(after.value.get()).toBe(4);
    expect(() => binding.member({ id: "one" }, { initial: 0 })).toThrow(Fault);
    expect(backend.calls).toHaveLength(2);
    expect(binding.target(handle)?.identity).toBe(identity);
    expect(old.forBinding(binding)).toBeUndefined();
    stop();
    graph.dispose();
  });

  it.each(["member", "revalidate"] as const)(
    "keeps collection authority separate through %s, failure and reset",
    async (operation) => {
      const { backend, query, graph, keyCalls } = fixture(10);
      const context = createBindingContext(graph);
      const binding = bindOperation(graph, query, {
        context,
        profile: {},
        fetch: async (input, options) => (await query.fetch(input, options)).count,
      });
      const handle = binding.member({ id: "one" });
      backend.resolve(backend.calls[0]!, { count: 1 });
      await handle.ready;
      const old = binding.target(handle)!;
      const stop = subscribe(context.active, () => {});
      jest.advanceTimersByTime(6);
      expect(binding.target(handle)).toBeDefined();
      expect(old.forBinding(binding)).toBe(old);
      jest.advanceTimersByTime(5);
      expect(binding.target(handle)).toBeUndefined();
      expect(old.forBinding(binding)).toBeUndefined();
      const waiting = handle.ready.then(
        () => undefined,
        (error: unknown) => error,
      );
      expect(binding.target(handle)).toBeUndefined();
      await graph.reset();
      expect(binding.target(handle)).toBeUndefined();
      expect(backend.calls).toHaveLength(1);
      const next = operation === "member" ? binding.member({ id: "one" }) : handle;
      const revalidation = operation === "revalidate" ? handle.revalidate() : undefined;
      const reacquired = binding.target(next)!;
      expect(next.identity).toBe(handle.identity);
      expect(reacquired).toBeDefined();
      expect(binding.target(handle)?.identity).toBe(next.identity);
      expect(next.status.get()).toBe("fetching");
      expect(keyCalls()).toBe(operation === "member" ? 2 : 1);
      expect(old.forBinding(binding)).toBeUndefined();
      const failure = { rejected: "reacquired" };
      const failedReady = next.ready.then(
        () => undefined,
        (error: unknown) => error,
      );
      backend.reject(backend.calls[1]!, failure);
      if (revalidation !== undefined) await revalidation;
      expect(await failedReady).toBe(failure);
      expect(await waiting).toBe(failure);
      expect(next.status.get()).toBe("failed");
      expect(reacquired.forBinding(binding)).toBe(reacquired);
      expect(binding.target(next)).toBeDefined();
      const stopStatus = subscribe(next.status, () => {});
      const reset = graph.reset();
      expect(reacquired.forBinding(binding)).toBeUndefined();
      expect(old.forBinding(binding)).toBeUndefined();
      expect(binding.target(next)).toBeDefined();
      expect(next.status.get()).toBe("fetching");
      expect(backend.calls).toHaveLength(3);
      backend.resolve(backend.calls[2]!, { count: 3 });
      await reset;
      expect(next.value.get()).toBe(3);
      expect(reacquired.forBinding(binding)).toBeUndefined();
      stopStatus();
      stop();
      graph.dispose();
    },
  );

  it("does not compute a key or dispatch after a bound options getter revokes its context", () => {
    const { backend, query, graph, keyCalls } = fixture();
    const context = createBindingContext(graph);
    const binding = bindOperation(graph, query, {
      context,
      profile: {},
      fetch: query.fetch,
    });
    const options = {
      snapshot: false,
      get metadata() {
        context.dispose();
        return "captured";
      },
    };
    expect(() => binding.member({ id: "one" }, options)).toThrow(Fault);
    expect(context.active.peek()).toBe(false);
    expect(keyCalls()).toBe(0);
    expect(backend.calls).toHaveLength(0);
    graph.api.query({ id: "one" });
    expect(keyCalls()).toBe(1);
    expect(backend.calls).toHaveLength(1);
    graph.dispose();
  });

  it("does not publish a bound write after its equality callback resets that incarnation", async () => {
    const { query, graph } = fixture();
    let reset: Promise<void> | undefined;
    const binding = bindOperation(graph, query, {
      context: createBindingContext(graph),
      profile: {},
      fetch: async () => 1,
      value: {
        equals: () => {
          reset = graph.reset();
          return false;
        },
      },
    });
    const handle = binding.member({ id: "one" });
    await handle.ready;
    const old = binding.target(handle)!;
    handle.set(2);
    await reset;
    expect(handle.value.get()).toBeUndefined();
    expect(handle.status.get()).toBe("empty");
    expect(old.forBinding(binding)).toBeUndefined();
    expect(binding.target(handle)).toBeDefined();
    graph.dispose();
  });

  it("revokes only its own context before abort callbacks and rejects pending readiness", async () => {
    const { backend, query, graph } = fixture();
    const context = createBindingContext(graph);
    const otherContext = createBindingContext(graph);
    let lateAdmission: unknown;
    let reenter = (_input: { id: string }): void => {};
    const first = bindOperation(graph, query, {
      context,
      profile: {},
      fetch: (input, options) => {
        options.abortSignal.addEventListener("abort", () => reenter(input), { once: true });
        return query.fetch(input, options);
      },
    });
    reenter = (input) => {
      try {
        first.member(input);
      } catch (error) {
        lateAdmission = error;
      }
    };
    const second = bindOperation(graph, query, {
      context: otherContext,
      profile: {},
      fetch: query.fetch,
    });
    const own = first.member({ id: "one" });
    const waiting = own.ready.then(
      () => undefined,
      (error: unknown) => error,
    );
    const separate = second.member({ id: "one" });
    const raw = graph.api.query({ id: "one" });
    context.dispose();
    expect(context.active.peek()).toBe(false);
    expect(otherContext.active.peek()).toBe(true);
    expect(await waiting).toMatchObject({ kind: "disposed" });
    expect(lateAdmission).toBeInstanceOf(Fault);
    expect(backend.calls.map((call) => call.aborted)).toEqual([true, false, false]);
    expect(() => own.value.get()).toThrow(Fault);
    expect(() => first.target(own)).toThrow(Fault);
    backend.resolve(backend.calls[1]!, { count: 8 });
    backend.resolve(backend.calls[2]!, { count: 9 });
    expect(await separate.ready).toEqual({ count: 8 });
    expect(await raw.ready).toEqual({ count: 9 });
    context.dispose();
    graph.dispose();
  });

  it.each(["predecessor", "caller-link", "setup"] as const)(
    "releases refresh ownership when context retires at %s",
    async (phase) => {
      const { backend, query, graph } = fixture();
      const context = createBindingContext(graph);
      const binding = bindOperation(graph, query, {
        context,
        profile: {},
        fetch: (input, options) => {
          options.abortSignal.addEventListener(
            "abort",
            () => {
              if (phase === "predecessor") context.dispose();
            },
            { once: true },
          );
          return query.fetch(input, options);
        },
      });
      const handle = binding.member({ id: "one" });
      let waiting: Promise<unknown> | undefined;
      if (phase === "predecessor")
        waiting = handle.ready.then(
          () => undefined,
          (error: unknown) => error,
        );
      else {
        backend.resolve(backend.calls[0]!, { count: 1 });
        await handle.ready;
      }
      const caller = trackedCallerSignal(() => {
        if (phase === "caller-link") context.dispose();
      });
      const stop =
        phase === "setup"
          ? subscribe(handle.pending, (pending) => {
              if (pending) {
                waiting = handle.ready.then(
                  () => undefined,
                  (error: unknown) => error,
                );
                context.dispose();
              }
            })
          : undefined;
      if (phase === "setup") await handle.revalidate({ abortSignal: caller.signal });
      else expect(() => handle.revalidate({ abortSignal: caller.signal })).toThrow(Fault);
      expect(context.active.peek()).toBe(false);
      if (phase !== "caller-link") {
        expect(waiting).toBeDefined();
        expect(await waiting).toMatchObject({ kind: "disposed" });
      }
      expect(backend.calls).toHaveLength(1);
      expect(caller.links.size).toBe(0);
      expect(caller.added).toBe(phase === "predecessor" ? 0 : 1);
      expect(caller.removed).toBe(caller.added);
      expect(() => handle.value.get()).toThrow(Fault);
      caller.controller.abort();
      context.dispose();
      graph.cancelPending();
      expect(caller.links.size).toBe(0);
      expect(backend.calls).toHaveLength(1);
      if (!backend.calls[0]!.settled) backend.resolve(backend.calls[0]!, { count: 1 });
      stop?.();
      graph.dispose();
    },
  );

  it("refuses selected bound values in v1 while excluded profiles leave raw snapshots truthful", async () => {
    const { backend, query, graph } = fixture();
    const context = createBindingContext(graph);
    const excluded = bindOperation(graph, query, {
      context,
      profile: {},
      fetch: async () => "excluded",
      value: { snapshot: false },
    });
    await excluded.member({ id: "one" }).ready;
    const raw = graph.api.query({ id: "one" });
    backend.resolve(backend.calls[0]!, { count: 5 });
    await raw.ready;
    expect(Object.keys(snapshot(graph).entries)).toEqual(["one"]);
    const selected = bindOperation(graph, query, {
      context,
      profile: {},
      fetch: async () => "selected",
    });
    await selected.member({ id: "one" }).ready;
    expect(() => snapshot(graph)).toThrow(Fault);
    context.dispose();
    expect(Object.keys(snapshot(graph).entries)).toEqual(["one"]);
    expect(raw.value.get()).toEqual({ count: 5 });
    graph.dispose();
  });

  it("uses native branded options and second-argument precedence for inputless bindings", async () => {
    const inputs: unknown[] = [];
    const query = createQuery({ name: "ping", key: () => "ping" as const, fetch: async () => 0 });
    const graph = createGraph({
      contract: defineContract({ namespace: "ping", operations: { query } }),
    });
    const binding = bindOperation(graph, query, {
      context: createBindingContext(graph),
      profile: {},
      fetch: async (input) => {
        inputs.push(input);
        return 3;
      },
    });
    const handle = binding.member(queryOptions({ initial: 1 }), { initial: 2 });
    expect(handle.value.get()).toBe(2);
    expect(inputs).toEqual([]);
    await handle.revalidate();
    expect(await handle.ready).toBe(3);
    expect(inputs).toEqual([undefined]);
    graph.dispose();
  });
});
