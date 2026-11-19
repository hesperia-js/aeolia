import { afterEach, beforeEach, describe, expect, it, jest } from "bun:test";
import {
  affects,
  bindOperation,
  createBindingContext,
  createGraph,
  createMutation,
  createQuery,
  defineContract,
  Fault,
  observeStore,
  onFault,
  peekStore,
  predictionFor,
  profileEffect,
  signal,
  subscribe,
} from "../../src/index.ts";
import type { PreparedEffect, ProfileEffect, QueryStore } from "../../src/index.ts";
import { testBackend } from "../../src/testing.ts";

type Input = { readonly id: string; readonly revision?: number };
type Change = Input & { readonly value: number };

let ownsFakeTimers = false;
beforeEach(() => {
  jest.useFakeTimers();
  ownsFakeTimers = true;
  jest.setSystemTime(0);
});
afterEach(() => {
  if (ownsFakeTimers) jest.clearAllTimers();
  jest.useRealTimers();
  ownsFakeTimers = false;
});

function fixture(on: "invalidate" | "revalidate" = "revalidate", maxPredictions = 64) {
  const backend = testBackend();
  const faults: unknown[] = [];
  const selections: Change[] = [];
  const query = createQuery({
    name: "read",
    key: (input: Input) => input.id,
    fetch: backend.respond<Input, number>("read"),
  });
  const other = createQuery({ ...query, name: "other" });
  const save = createMutation({
    name: "save",
    run: backend.perform<Change, string>("save"),
    affects: [
      affects(query, {
        select: (input: Change) => {
          selections.push(input);
          return { id: input.id, revision: input.revision };
        },
        on,
        optimistic: (_value: number | undefined, input: Change) => input.value,
      }),
    ],
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "bound-effects", operations: { query, other, save } }),
    idleMs: 100,
    maxPredictions,
    onUnobservedFault: (error) => faults.push(error),
  });
  const context = createBindingContext(graph);
  const native = bindOperation(graph, query, { context, profile: {}, fetch: query.fetch });
  const labels = bindOperation(graph, query, {
    context,
    profile: {},
    fetch: async (input, call) => "label:" + (await query.fetch(input, call)),
  });
  const membership = () =>
    profileEffect(save, 0, query, {
      prepare: (phase) =>
        phase.existing(phase.selectedInput).map((target) => phase.prepare(target)),
      settle: (phase) =>
        phase
          .existing(phase.selectedInput)
          .map((target) => phase.selected(target, phase.selectedInput)),
    });
  const binding = (effect: ProfileEffect<typeof save> = membership()) =>
    bindOperation(graph, save, {
      context,
      profile: {},
      effects: [effect],
      run: async (input, call) => ({ receipt: await save.run(input, call) }),
    });
  const completeSave = (value = "saved") => backend.resolve(backend.pending("save")[0]!, value);
  return {
    backend,
    faults,
    selections,
    query,
    other,
    save,
    graph,
    context,
    native,
    labels,
    membership,
    binding,
    completeSave,
  };
}

function visible<T>(handle: QueryStore<T>): T | undefined {
  return handle.value.get();
}

async function expectPending(promise: Promise<unknown>): Promise<void> {
  jest.useRealTimers();
  ownsFakeTimers = false;
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    const result = await Promise.race([
      promise.then(
        () => "settled",
        () => "settled",
      ),
      new Promise<"pending">((resolve) => {
        timer = setTimeout(() => resolve("pending"), 0);
      }),
    ]);
    expect(result).toBe("pending");
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}

describe("context-bound mutation effects", () => {
  it("closes a live prediction-only subscription when its context is disposed", () => {
    const f = fixture();
    const handle = f.native.member({ id: "one" });
    const values: number[] = [];
    const stop = subscribe(handle, (value) => values.push(value));
    try {
      const prediction = predictionFor(f.save, f.native.target(handle)!, () => 42);
      const pending = f.binding().member({ id: "one", value: 42 }, { optimistic: [prediction] });
      void pending.catch(() => undefined);
      expect(values).toStrictEqual([42]);
      f.context.dispose();
      expect(values).toStrictEqual([42]);
      expect(f.faults).toStrictEqual([]);
      expect(() => handle.value.get()).toThrow("disposed");
      expect(() => handle.available.get()).toThrow("disposed");
    } finally {
      stop();
      f.graph.dispose();
    }
  });

  for (const arrayKind of ["effects", "optimistic"] as const) {
    for (const owner of ["context", "graph"] as const) {
      for (const phase of ["iterator getter", "factory", "next getter"] as const) {
        it(`stops ${arrayKind} iteration when ${owner} retires in the ${phase}`, async () => {
          const f = fixture();
          const binding = arrayKind === "optimistic" ? f.binding() : undefined;
          const values = arrayKind === "effects" ? [f.membership()] : [];
          let retirement: Promise<void> | undefined;
          let factories = 0;
          let pulls = 0;
          let closes = 0;
          let factoryReceiver: unknown;
          let closeReceiver: unknown;
          const retire = (): void => {
            retirement = owner === "context" ? f.context.retire() : f.graph.retire();
            void retirement.catch(() => undefined);
          };
          const iterator = {
            get next() {
              if (phase === "next getter") retire();
              return () => {
                pulls += 1;
                return { done: true, value: undefined };
              };
            },
            return(this: unknown) {
              closeReceiver = this;
              closes += 1;
              return { done: true, value: undefined };
            },
          };
          Object.defineProperty(values, Symbol.iterator, {
            get() {
              if (phase === "iterator getter") retire();
              return function (this: unknown) {
                factoryReceiver = this;
                factories += 1;
                if (phase === "factory") retire();
                return iterator;
              };
            },
          });
          try {
            let semanticError: unknown;
            try {
              if (arrayKind === "effects")
                bindOperation(f.graph, f.save, {
                  context: f.context,
                  profile: {},
                  effects: values as never,
                  run: f.save.run,
                });
              else binding!.member({ id: "one", value: 1 }, { optimistic: values as never });
            } catch (error) {
              semanticError = error;
            }
            expect(semanticError).toMatchObject({ kind: "disposed" });
            expect(factories).toBe(phase === "iterator getter" ? 0 : 1);
            expect(pulls).toBe(0);
            expect(closes).toBe(phase === "iterator getter" ? 0 : 1);
            if (phase !== "iterator getter") {
              expect(factoryReceiver).toBe(values);
              expect(closeReceiver).toBe(iterator);
            }
            expect(f.backend.calls.filter((call) => call.name === "save")).toHaveLength(0);
            expect(retirement).toBeDefined();
            await expect(retirement!).resolves.toBeUndefined();
          } finally {
            f.graph.dispose();
            await retirement?.catch(() => undefined);
          }
        });
      }

      for (const failure of ["return getter", "return call", "invalid return"] as const) {
        it(`retains ${arrayKind} ${failure} cleanup failure in ${owner} retirement`, async () => {
          const f = fixture();
          const binding = arrayKind === "optimistic" ? f.binding() : undefined;
          const values = arrayKind === "effects" ? [f.membership()] : [];
          const semanticError = new Error("iterator next getter failed");
          const cleanupError = new Error("iterator return failed");
          let retirement: Promise<void> | undefined;
          let returnReads = 0;
          let returnCalls = 0;
          const iterator = {
            get next() {
              retirement = owner === "context" ? f.context.retire() : f.graph.retire();
              void retirement.catch(() => undefined);
              throw semanticError;
            },
            get return() {
              returnReads += 1;
              if (failure === "return getter") throw cleanupError;
              return function (this: unknown) {
                expect(this).toBe(iterator);
                returnCalls += 1;
                if (failure === "return call") throw cleanupError;
                return 1;
              };
            },
          };
          Object.defineProperty(values, Symbol.iterator, {
            value() {
              return iterator;
            },
          });
          try {
            let observed: unknown;
            try {
              if (arrayKind === "effects")
                bindOperation(f.graph, f.save, {
                  context: f.context,
                  profile: {},
                  effects: values as never,
                  run: f.save.run,
                });
              else binding!.member({ id: "one", value: 1 }, { optimistic: values as never });
            } catch (error) {
              observed = error;
            }
            expect(observed).toBe(semanticError);
            expect(returnReads).toBe(1);
            expect(returnCalls).toBe(failure === "return getter" ? 0 : 1);
            expect(f.backend.calls.filter((call) => call.name === "save")).toHaveLength(0);
            expect(retirement).toBeDefined();
            if (failure === "invalid return")
              await expect(retirement!).rejects.toBeInstanceOf(TypeError);
            else await expect(retirement!).rejects.toBe(cleanupError);
          } finally {
            f.graph.dispose();
            await retirement?.catch(() => undefined);
          }
        });
      }
    }

    it(`preserves a live ${arrayKind} iterator error without closing it`, () => {
      const f = fixture();
      const binding = arrayKind === "optimistic" ? f.binding() : undefined;
      const values = arrayKind === "effects" ? [f.membership()] : [];
      const failure = new Error("iterator failed");
      let factoryReceiver: unknown;
      let nextReceiver: unknown;
      let closes = 0;
      const iterator = {
        next(this: unknown): never {
          nextReceiver = this;
          throw failure;
        },
        return() {
          closes += 1;
          return { done: true, value: undefined };
        },
      };
      Object.defineProperty(values, Symbol.iterator, {
        value(this: unknown) {
          factoryReceiver = this;
          return iterator;
        },
      });
      try {
        let observed: unknown;
        try {
          if (arrayKind === "effects")
            bindOperation(f.graph, f.save, {
              context: f.context,
              profile: {},
              effects: values as never,
              run: f.save.run,
            });
          else binding!.member({ id: "one", value: 1 }, { optimistic: values as never });
        } catch (error) {
          observed = error;
        }
        expect(observed).toBe(failure);
        expect(factoryReceiver).toBe(values);
        expect(nextReceiver).toBe(iterator);
        expect(closes).toBe(0);
        expect(f.backend.calls.filter((call) => call.name === "save")).toHaveLength(0);
        expect(f.context.active.get()).toBe(true);
      } finally {
        f.graph.dispose();
      }
    });
  }

  it("preserves the selector receiver and refuses it after its getter retires the context", async () => {
    const query = createQuery({
      name: "receiver-query",
      key: (id: string) => id,
      fetch: async (id) => id,
    });
    const receivers: unknown[] = [];
    const affected = affects(query, {
      select(this: unknown, input: string) {
        receivers.push(this);
        return input;
      },
    });
    let armed = false;
    let retirement: Promise<void> | undefined;
    let context!: ReturnType<typeof createBindingContext>;
    const observed = new Proxy(affected, {
      get(target, property, receiver) {
        if (armed && property === "select") retirement ??= context.retire();
        return Reflect.get(target, property, receiver);
      },
    });
    const mutation = createMutation({
      name: "receiver-mutation",
      affects: [observed],
      run: async (input: string) => input,
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "selector-receiver", operations: { query, mutation } }),
    });
    context = createBindingContext(graph);
    try {
      const effect = profileEffect(mutation, 0, query, { prepare: () => [], settle: () => [] });
      const binding = bindOperation(graph, mutation, {
        context,
        profile: {},
        effects: [effect],
        run: mutation.run,
      });
      await expect(binding.member("live")).resolves.toBe("live");
      expect(receivers.length).toBeGreaterThan(0);
      expect(receivers.every((receiver) => receiver === observed)).toBe(true);
      const previousCalls = receivers.length;
      armed = true;
      await expect(binding.member("closed")).rejects.toMatchObject({ name: "AbortError" });
      expect(retirement).toBeDefined();
      expect(receivers).toHaveLength(previousCalls);
      await retirement;
    } finally {
      graph.dispose();
    }
  });

  it("invokes a preparation thenable with its receiver without reading the callable's call property", async () => {
    const query = createQuery({
      name: "then-query",
      key: (id: string) => id,
      fetch: async (id) => id,
    });
    const mutation = createMutation({
      name: "then-mutation",
      affects: [affects(query, { select: (input: string) => input })],
      run: async (input: string) => input,
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "then-receiver", operations: { query, mutation } }),
    });
    const context = createBindingContext(graph);
    let callPropertyRead = false;
    let receiver: unknown;
    let retirement: Promise<void> | undefined;
    const then = new Proxy(
      function (this: unknown, resolve: (value: readonly PreparedEffect[]) => void) {
        receiver = this;
        resolve([]);
      },
      {
        get(target, property, actualReceiver) {
          if (property === "call") {
            callPropertyRead = true;
            retirement ??= context.retire();
          }
          return Reflect.get(target, property, actualReceiver);
        },
      },
    );
    const thenable = { then } as unknown as PromiseLike<readonly PreparedEffect[]>;
    try {
      const effect = profileEffect(mutation, 0, query, {
        prepare: () => thenable,
        settle: () => [],
      });
      const binding = bindOperation(graph, mutation, {
        context,
        profile: {},
        effects: [effect],
        run: mutation.run,
      });
      const outcome = await binding.member("live").then(
        (value) => ({ kind: "fulfilled" as const, value }),
        (reason: unknown) => ({ kind: "rejected" as const, reason }),
      );
      expect(callPropertyRead).toBe(false);
      expect(receiver).toBe(thenable);
      expect(outcome).toEqual({ kind: "fulfilled", value: "live" });
      if (retirement !== undefined) await retirement;
    } finally {
      graph.dispose();
    }
  });

  it("uses the executing plan and physical override identity across existing same-context profiles", async () => {
    const f = fixture();
    try {
      const numbers = f.native.member({ id: "one" }, { initial: 1 });
      const labels = f.labels.member({ id: "one" }, { initial: "label:1" });
      const raw = f.graph.api.query({ id: "one" }, { initial: 3 });
      const foreign = bindOperation(f.graph, f.query, {
        context: createBindingContext(f.graph),
        profile: {},
        fetch: f.query.fetch,
      }).member({ id: "one" }, { initial: 4 });
      const other = bindOperation(f.graph, f.other, {
        context: f.context,
        profile: {},
        fetch: f.other.fetch,
      }).member({ id: "one" }, { initial: 5 });
      const unrelated = f.native.member({ id: "unrelated" }, { initial: 6 });
      const sameValue = bindOperation(f.graph, f.query, {
        context: f.context,
        profile: {},
        fetch: f.query.fetch,
      }).member({ id: "one" }, { initial: 8 });
      let declaredCalls = 0;
      const effect = profileEffect(f.save, 0, f.query, {
        prepare: (phase) =>
          phase.existing(phase.selectedInput).map((target) => {
            const native = target.forBinding(f.native);
            if (native !== undefined) return phase.original(native);
            const label = target.forBinding(f.labels);
            return label === undefined
              ? phase.prepare(target)
              : phase.predict(label, (_current, input) => {
                  declaredCalls += 1;
                  return "plan:" + input.value;
                });
          }),
        settle: (phase) =>
          phase
            .existing(phase.selectedInput)
            .map((target) => phase.selected(target, phase.selectedInput)),
      });
      const first = f.binding(effect);
      const second = f.binding(
        profileEffect(f.save, 0, f.query, {
          prepare: (phase) =>
            phase.existing(phase.selectedInput).map((target) => {
              const label = target.forBinding(f.labels);
              return label === undefined
                ? phase.prepare(target)
                : phase.predict(label, () => "second-plan");
            }),
          settle: () => [],
        }),
      );
      expect(f.backend.calls).toHaveLength(0);
      expect(f.selections).toHaveLength(0);
      const override = predictionFor(f.save, f.native.target(numbers)!, () => 20);
      const extra = predictionFor(f.save, f.native.target(unrelated)!, () => 99);
      const running = first.member({ id: "one", value: 7 }, { optimistic: [override, extra] });
      expect([
        visible(numbers),
        visible(labels),
        visible(raw),
        visible(foreign),
        visible(other),
        visible(unrelated),
        visible(sameValue),
      ]).toEqual([20, "plan:7", 3, 4, 5, 6, 8]);
      expect(declaredCalls).toBeGreaterThan(0);
      f.completeSave();
      await expect(running).resolves.toEqual({ receipt: "saved" });
      expect(f.selections).toHaveLength(2);
      expect(f.backend.pending("read")).toHaveLength(3);
      for (const call of f.backend.pending("read")) f.backend.resolve(call, 8);
      await Promise.all([numbers.ready, labels.ready, sameValue.ready]);
      expect([visible(numbers), visible(labels)]).toEqual([8, "label:8"]);
      const again = second.member({ id: "one", value: 9 });
      expect(visible(labels)).toBe("second-plan");
      f.completeSave();
      await again;
      expect(visible(raw)).toBe(3);
    } finally {
      f.graph.dispose();
    }
  });

  it("completes all preparation before publication and dispatch, preserving a later resolver failure", async () => {
    const backend = testBackend();
    const query = createQuery({ name: "query", key: () => "one", fetch: async () => 1 });
    const cause = new Error("default validation failed");
    const gate = Promise.withResolvers<readonly PreparedEffect[]>();
    const entered = Promise.withResolvers<void>();
    const save = createMutation({
      name: "save",
      run: backend.perform<void, string>("save"),
      affects: [affects(query, {}), affects(query, {})],
    });
    const graph = createGraph({
      contract: defineContract({ namespace: "preparation", operations: { query, save } }),
    });
    try {
      const context = createBindingContext(graph);
      const binding = bindOperation(graph, query, { context, profile: {}, fetch: query.fetch });
      const handle = binding.member(undefined, { initial: 1 });
      const values: unknown[] = [];
      const stop = subscribe(handle, (value) => values.push(value));
      const mutation = bindOperation(graph, save, {
        context,
        profile: {},
        run: save.run,
        effects: [
          profileEffect(save, 0, query, {
            prepare: (phase) =>
              phase
                .existing(undefined)
                .map((target) => phase.predict(target.forBinding(binding)!, () => 10)),
            settle: () => [],
          }),
          profileEffect(save, 1, query, {
            prepare: () => {
              entered.resolve();
              return gate.promise;
            },
            settle: () => [],
          }),
        ],
      });
      const running = mutation.member();
      const outcome = running.then(
        () => undefined,
        (error: unknown) => error,
      );
      await entered.promise;
      expect(visible(handle)).toBe(1);
      expect(values).toEqual([1]);
      expect(backend.calls).toHaveLength(0);
      gate.reject(cause);
      expect(await outcome).toBe(cause);
      expect(values).toEqual([1]);
      expect(backend.calls).toHaveLength(0);
      stop();
    } finally {
      graph.dispose();
    }
  });

  it("preserves synchronous preparation and consumes a genuine thenable before run", async () => {
    const f = fixture();
    try {
      const handle = f.native.member({ id: "one" }, { initial: 1 });
      const order: string[] = [];
      const effect = profileEffect(f.save, 0, f.query, {
        prepare: (phase) => {
          order.push("prepare");
          const instructions = phase
            .existing(phase.selectedInput)
            .map((target) => phase.original(target.forBinding(f.native)!));
          const thenable: PromiseLike<readonly PreparedEffect[]> = {
            then(resolve, reject) {
              order.push("then");
              return Promise.resolve(instructions).then(resolve, reject);
            },
          };
          return thenable;
        },
        settle: () => {
          order.push("settle");
          return [];
        },
      });
      const started = Promise.withResolvers<void>();
      const binding = bindOperation(f.graph, f.save, {
        context: f.context,
        profile: {},
        effects: [effect],
        run: (input, call) => {
          order.push("run");
          started.resolve();
          return f.save.run(input, call);
        },
      });
      const running = binding.member({ id: "one", value: 2 });
      expect(order).toEqual(["prepare", "then"]);
      await started.promise;
      expect(order).toEqual(["prepare", "then", "run"]);
      expect(visible(handle)).toBe(2);
      f.completeSave();
      await running;
      expect(order).toEqual(["prepare", "then", "run", "settle"]);
    } finally {
      f.graph.dispose();
    }
  });

  it.each([
    "caller",
    "graph",
    "context",
    "reset",
    "dispose",
    "graph.retire",
    "context.retire",
    "graph.self-retire",
    "context.self-retire",
    "thenable.revoked",
  ] as const)(
    "revokes suspended preparation through %s without publishing or dispatching",
    async (mode) => {
      const f = fixture();
      const gate = Promise.withResolvers<readonly PreparedEffect[]>();
      const finalizer = Promise.withResolvers<void>();
      const finalized = Promise.withResolvers<void>();
      const nativeOriginal = gate.promise.then(async (effects) => {
        await finalizer.promise;
        finalized.resolve();
        return effects;
      });
      const caller = new AbortController();
      let retirement: Promise<void> | undefined;
      let prepareCalls = 0;
      let thenCalls = 0;
      const revokedThenable = {
        then(resolve: (value: readonly PreparedEffect[]) => unknown) {
          thenCalls += 1;
          return Promise.resolve(resolve([]));
        },
      } as PromiseLike<readonly PreparedEffect[]>;
      try {
        const handle = f.native.member({ id: "one" }, { initial: 1 });
        const mutation = f.binding(
          profileEffect(f.save, 0, f.query, {
            prepare: () => {
              prepareCalls += 1;
              if (mode === "graph.self-retire" || mode === "thenable.revoked")
                retirement = f.graph.retire();
              if (mode === "context.self-retire") retirement = f.context.retire();
              if (mode === "thenable.revoked") return revokedThenable;
              if (
                mode === "graph.retire" ||
                mode === "context.retire" ||
                mode === "graph.self-retire" ||
                mode === "context.self-retire"
              )
                return nativeOriginal;
              return gate.promise;
            },
            settle: () => [],
          }),
        );
        const running = mutation.member({ id: "one", value: 2 }, { abortSignal: caller.signal });
        const outcome = running.then(
          () => undefined,
          (error: unknown) => error,
        );
        if (mode === "caller") caller.abort();
        if (mode === "graph") f.graph.cancelPending();
        if (mode === "context") f.context.dispose();
        if (mode === "reset") await f.graph.reset();
        if (mode === "dispose") f.graph.dispose();
        if (mode === "graph.retire") retirement = f.graph.retire();
        if (mode === "context.retire") retirement = f.context.retire();
        expect(await outcome).toMatchObject({ name: "AbortError" });
        const selfRetired = mode === "graph.self-retire" || mode === "context.self-retire";
        if (retirement !== undefined && !selfRetired) {
          void retirement.catch(() => undefined);
          await expectPending(retirement);
        }
        gate.resolve([]);
        expect(f.backend.calls).toHaveLength(0);
        if (mode === "caller" || mode === "graph") expect(visible(handle)).toBe(1);
        expect(prepareCalls).toBe(1);
        if (selfRetired) {
          if (retirement === undefined) throw new Error("Expected self-retirement promise");
          finalizer.resolve();
          await finalized.promise;
          await expectPending(retirement);
        } else if (retirement !== undefined) {
          if (mode === "thenable.revoked") {
            await new Promise<void>((resolve) => setTimeout(resolve, 0));
            expect(thenCalls).toBe(0);
            await expectPending(retirement);
          } else {
            await expectPending(retirement);
            finalizer.resolve();
            await finalized.promise;
            await retirement;
          }
        }
      } finally {
        gate.resolve([]);
        finalizer.resolve();
        f.graph.dispose();
      }
    },
  );

  it("enrolls before option and signal callouts and removes caller links after cancellation", async () => {
    const f = fixture();
    try {
      const mutation = f.binding();
      let linked = 0;
      let unlinked = 0;
      const controller = new AbortController();
      const caller = new Proxy(controller.signal, {
        get(target, key) {
          if (key === "addEventListener")
            return (...args: Parameters<AbortSignal["addEventListener"]>) => {
              linked += 1;
              target.addEventListener(...args);
              f.graph.cancelPending();
            };
          if (key === "removeEventListener")
            return (...args: Parameters<AbortSignal["removeEventListener"]>) => {
              unlinked += 1;
              target.removeEventListener(...args);
            };
          const value: unknown = Reflect.get(target, key, target);
          return typeof value === "function" ? value.bind(target) : value;
        },
      });
      await expect(
        mutation.member({ id: "one", value: 2 }, { abortSignal: caller }),
      ).rejects.toMatchObject({ name: "AbortError" });
      expect([linked, unlinked]).toEqual([1, 1]);
      const options = {
        get abortSignal() {
          f.graph.cancelPending();
          return controller.signal;
        },
      };
      await expect(mutation.member({ id: "one", value: 2 }, options)).rejects.toMatchObject({
        name: "AbortError",
      });
      expect(f.selections).toHaveLength(0);
      expect(f.backend.calls).toHaveLength(0);
    } finally {
      f.graph.dispose();
    }
  });

  it("discovers new settlement siblings and preserves selected versus current completed sources", async () => {
    const f = fixture();
    try {
      const numbers = f.native.member({ id: "one", revision: 1 }, { initial: 1 });
      const alternate = bindOperation(f.graph, f.query, {
        context: f.context,
        profile: {},
        fetch: async (input, call) => "alternate:" + (await f.query.fetch(input, call)),
      });
      const labels = alternate.member({ id: "one", revision: 99 }, { initial: "before" });
      const defaults = f.labels.member({ id: "one", revision: 2 }, { initial: "default" });
      const late = bindOperation(f.graph, f.query, {
        context: f.context,
        profile: {},
        fetch: async (input, call) => "late:" + (await f.query.fetch(input, call)),
      });
      const declared = (_current: string | undefined, input: Change) => "predicted:" + input.value;
      const effect = profileEffect(f.save, 0, f.query, {
        prepare: (phase) =>
          phase.existing(phase.selectedInput).map((target) => {
            const compatible =
              target.forBinding(f.labels) ??
              target.forBinding(alternate) ??
              target.forBinding(late);
            return compatible === undefined
              ? phase.prepare(target)
              : phase.predict(compatible, declared);
          }),
        settle: (phase) =>
          phase
            .existing(phase.selectedInput)
            .map((target) =>
              target.forBinding(f.native) === undefined
                ? phase.current(target)
                : phase.selected(target, phase.selectedInput),
            ),
      });
      const running = f.binding(effect).member({ id: "one", revision: 7, value: 8 });
      expect([visible(labels), visible(defaults)]).toEqual(["predicted:8", "predicted:8"]);
      const newSibling = late.member({ id: "one", revision: 100 }, { initial: "not predicted" });
      expect(visible(newSibling)).toBe("not predicted");
      f.completeSave();
      await running;
      const calls = f.backend.pending("read");
      expect(calls.map((call) => call.input)).toEqual([
        { id: "one", revision: 7 },
        { id: "one", revision: 2 },
        { id: "one", revision: 99 },
        { id: "one", revision: 100 },
      ]);
      expect(f.selections).toHaveLength(2);
      for (const call of calls) f.backend.resolve(call, 12);
      await Promise.all([numbers.ready, defaults.ready, labels.ready, newSibling.ready]);
      expect([visible(numbers), visible(defaults), visible(labels), visible(newSibling)]).toEqual([
        12,
        "label:12",
        "alternate:12",
        "late:12",
      ]);
    } finally {
      f.graph.dispose();
    }
  });

  it("omits targets collected during preparation without reviving them or suppressing dispatch", async () => {
    const f = fixture();
    const prepared = Promise.withResolvers<void>();
    const gate = Promise.withResolvers<void>();
    try {
      const handle = f.native.member({ id: "one" }, { initial: 1 });
      const effect = profileEffect(f.save, 0, f.query, {
        prepare: async (phase) => {
          const instructions = phase
            .existing(phase.selectedInput)
            .map((target) => phase.original(target.forBinding(f.native)!));
          prepared.resolve();
          await gate.promise;
          return instructions;
        },
        settle: () => [],
      });
      const started = Promise.withResolvers<void>();
      const binding = bindOperation(f.graph, f.save, {
        context: f.context,
        profile: {},
        effects: [effect],
        run: (input, call) => {
          started.resolve();
          return f.save.run(input, call);
        },
      });
      const running = binding.member({ id: "one", value: 2 });
      await prepared.promise;
      jest.advanceTimersByTime(101);
      expect(f.native.target(handle)).toBeUndefined();
      gate.resolve();
      await started.promise;
      expect(peekStore(handle)).toMatchObject({ kind: "stable", present: false });
      expect(f.backend.pending("read")).toHaveLength(0);
      f.completeSave();
      await running;
      const refreshing = handle.revalidate();
      expect(f.backend.pending("read")).toHaveLength(1);
      f.backend.resolve(f.backend.pending("read")[0]!, 9);
      await refreshing;
      expect(await handle.ready).toBe(9);
    } finally {
      gate.resolve();
      f.graph.dispose();
    }
  });

  it("retains old and fresh discovery provenance within one phase without applying the expired producer", async () => {
    const f = fixture();
    const discovered = Promise.withResolvers<void>();
    const gate = Promise.withResolvers<void>();
    const started = Promise.withResolvers<void>();
    let expiredProducerCalls = 0;
    let freshProducerCalls = 0;
    try {
      const handle = f.native.member({ id: "one" }, { initial: 1 });
      const oldTicket = f.native.target(handle)!;
      const effect = profileEffect(f.save, 0, f.query, {
        prepare: async (phase) => {
          const old = phase.existing(phase.selectedInput)[0]!;
          const oldTyped = old.forBinding(f.native)!;
          discovered.resolve();
          await gate.promise;
          const fresh = phase.existing(phase.selectedInput)[0]!;
          return [
            phase.prepare(old),
            phase.predict(oldTyped, () => {
              expiredProducerCalls += 1;
              return 99;
            }),
            phase.predict(fresh.forBinding(f.native)!, () => {
              freshProducerCalls += 1;
              return 22;
            }),
          ];
        },
        settle: () => [],
      });
      const binding = bindOperation(f.graph, f.save, {
        context: f.context,
        profile: {},
        effects: [effect],
        run: (input, call) => {
          started.resolve();
          return f.save.run(input, call);
        },
      });
      const running = binding.member({ id: "one", value: 2 });
      await discovered.promise;
      jest.advanceTimersByTime(101);
      expect(f.native.target(handle)).toBeUndefined();
      expect(peekStore(handle)).toMatchObject({ kind: "stable", present: false });
      expect(f.backend.calls).toHaveLength(0);
      const successor = f.native.member({ id: "one" });
      expect(successor.identity).toBe(handle.identity);
      expect(f.backend.pending("read")).toHaveLength(1);
      f.backend.resolve(f.backend.pending("read")[0]!, 7);
      expect(await successor.ready).toBe(7);
      expect(oldTicket.forBinding(f.native)).toBeUndefined();
      expect(f.graph.resetVersion.get()).toBe(0);
      expect(f.context.active.peek()).toBe(true);
      gate.resolve();
      await Promise.race([started.promise, running]);
      expect(f.backend.calls.filter((call) => call.name === "save")).toHaveLength(1);
      expect(expiredProducerCalls).toBe(0);
      expect(freshProducerCalls).toBeGreaterThan(0);
      expect([visible(handle), visible(successor)]).toEqual([22, 22]);
      expect(oldTicket.forBinding(f.native)).toBeUndefined();
      f.completeSave();
      await expect(running).resolves.toBe("saved");
    } finally {
      gate.resolve();
      f.graph.dispose();
    }
  });

  it("keeps old overrides stale while a retained reset handle constructs a fresh instruction", async () => {
    const f = fixture();
    let stop: (() => void) | undefined;
    try {
      const handle = f.native.member({ id: "one" }, { initial: 1 });
      stop = subscribe(handle, () => undefined);
      const identity = handle.identity;
      const old = predictionFor(f.save, f.native.target(handle)!, () => 99);
      const resetting = f.graph.reset();
      const fresh = predictionFor(f.save, f.native.target(handle)!, () => 22);
      expect(handle.identity).toBe(identity);
      expect(f.backend.pending("read")).toHaveLength(1);
      f.backend.resolve(f.backend.pending("read")[0]!, 2);
      await resetting;
      const first = f.binding().member({ id: "one", value: 3 }, { optimistic: [old] });
      expect(visible(handle)).toBe(2);
      f.completeSave();
      await first;
      f.backend.resolve(f.backend.pending("read")[0]!, 4);
      await handle.ready;
      const next = f.binding().member({ id: "one", value: 5 }, { optimistic: [fresh] });
      expect(visible(handle)).toBe(22);
      f.completeSave();
      await next;
    } finally {
      stop?.();
      f.graph.dispose();
    }
  });

  it.each(["throw", "read", "capacity"] as const)(
    "diagnoses and omits a %s prediction without fallback, issue mutation or dispatch failure",
    async (mode) => {
      const f = fixture("invalidate", mode === "capacity" ? 1 : 64);
      try {
        const handle = f.native.member({ id: "one" }, { initial: 1 });
        const label = f.labels.member({ id: "one" }, { initial: "one" });
        const lease = observeStore(f.graph, handle);
        const issue = Object.freeze({ message: "earlier observation issue" });
        lease.reject(issue);
        let fallbackCalls = 0;
        const effect = profileEffect(f.save, 0, f.query, {
          prepare: (phase) =>
            phase.existing(phase.selectedInput).map((target) => {
              const native = target.forBinding(f.native);
              if (native !== undefined)
                return phase.predict(native, () => {
                  fallbackCalls += 1;
                  return 77;
                });
              return phase.predict(target.forBinding(f.labels)!, () => "valid");
            }),
          settle: () => [],
        });
        const binding = f.binding(effect);
        let held: Promise<unknown> | undefined;
        if (mode === "capacity")
          held = f.binding().member(
            { id: "one", value: 6 },
            {
              optimistic: [predictionFor(f.save, f.native.target(handle)!, () => 6)],
            },
          );
        const readable = signal(10);
        const override = predictionFor(f.save, f.native.target(handle)!, () => {
          if (mode === "throw") throw new Error("predictor");
          if (mode === "read") return readable.get();
          return 99;
        });
        fallbackCalls = 0;
        const faults: Fault[] = [];
        const stopFaults = onFault(f.graph, (fault) => faults.push(fault));
        const running = binding.member({ id: "one", value: 2 }, { optimistic: [override] });
        expect(fallbackCalls).toBe(0);
        expect(lease.issue.peek()).toBe(issue);
        expect(faults.some((fault) => fault.kind === "prediction")).toBe(true);
        if (mode !== "capacity") expect([visible(handle), visible(label)]).toEqual([1, "valid"]);
        for (const call of f.backend.pending("save")) f.backend.resolve(call, "saved");
        await Promise.all([running, held]);
        expect(lease.issue.peek()).toBe(issue);
        stopFaults();
        lease.dispose();
      } finally {
        f.graph.dispose();
      }
    },
  );

  it.each(["caller", "graph", "context", "reset"] as const)(
    "distinguishes dispatched transport cancellation from %s settlement authority",
    async (mode) => {
      const f = fixture();
      try {
        const handle = f.native.member({ id: "one" }, { initial: 1 });
        const caller = new AbortController();
        const running = f.binding().member({ id: "one", value: 2 }, { abortSignal: caller.signal });
        const original = f.backend.pending("save")[0]!;
        if (mode === "caller") caller.abort();
        if (mode === "graph") f.graph.cancelPending();
        if (mode === "context") f.context.dispose();
        if (mode === "reset") await f.graph.reset();
        expect(original.aborted).toBe(true);
        f.backend.resolve(original, "caught abort and saved");
        await expect(running).resolves.toEqual({ receipt: "caught abort and saved" });
        expect(f.backend.pending("read")).toHaveLength(
          mode === "caller" || mode === "graph" ? 1 : 0,
        );
        if (mode === "caller" || mode === "graph") expect(handle.status.get()).toBe("revalidating");
      } finally {
        f.graph.dispose();
      }
    },
  );

  it("does not replay a backend write or settle effects when captured output completion rejects", async () => {
    const f = fixture();
    try {
      const handle = f.native.member({ id: "one" }, { initial: 1 });
      const cause = new Error("output rejected");
      const binding = bindOperation(f.graph, f.save, {
        context: f.context,
        profile: {},
        effects: [f.membership()],
        run: async (input, call) => {
          await f.save.run(input, call);
          throw cause;
        },
      });
      const override = predictionFor(f.save, f.native.target(handle)!, () => 2);
      const running = binding.member({ id: "one", value: 2 }, { optimistic: [override] });
      expect(visible(handle)).toBe(2);
      const outcome = running.then(
        () => undefined,
        (error: unknown) => error,
      );
      f.completeSave();
      expect(await outcome).toBe(cause);
      expect(visible(handle)).toBe(1);
      expect(f.selections).toHaveLength(1);
      expect(f.backend.calls.map((call) => call.name)).toEqual(["save"]);
    } finally {
      f.graph.dispose();
    }
  });

  it("keeps context retirement pending through bound mutation output adoption", async () => {
    const f = fixture();
    const finalization = Promise.withResolvers<void>();
    const listenerRemoved = Promise.withResolvers<void>();
    const started = Promise.withResolvers<void>();
    const controller = new AbortController();
    const nativeAdd = controller.signal.addEventListener.bind(controller.signal);
    const nativeRemove = controller.signal.removeEventListener.bind(controller.signal);
    let linkedListener: EventListenerOrEventListenerObject | null = null;
    let providerRead = false;
    let finalOutputAdoption = false;
    const signal = new Proxy(controller.signal, {
      get(target, property) {
        if (property === "addEventListener")
          return (
            type: string,
            listener: Parameters<typeof nativeAdd>[1],
            options?: Parameters<typeof nativeAdd>[2],
          ) => {
            nativeAdd(type, listener, options);
            if (type === "abort") linkedListener = listener;
          };
        if (property === "removeEventListener")
          return (
            type: string,
            listener: Parameters<typeof nativeRemove>[1],
            options?: Parameters<typeof nativeRemove>[2],
          ) => {
            nativeRemove(type, listener, options);
            if (type === "abort" && listener === linkedListener) {
              finalOutputAdoption = true;
              listenerRemoved.resolve();
            }
          };
        const value = Reflect.get(target, property, target);
        return typeof value === "function" ? value.bind(target) : value;
      },
    });
    const providerResult = Object.defineProperty({}, "then", {
      get() {
        if (!providerRead) {
          providerRead = true;
          return undefined;
        }
        if (!finalOutputAdoption) return undefined;
        return (resolve: (value: string) => void, reject: (reason: unknown) => void) => {
          finalization.promise.then(() => resolve("finalized"), reject);
        };
      },
    });
    let call: (typeof f.backend.calls)[number] | undefined;
    let providerResolved = false;
    try {
      const binding = bindOperation(f.graph, f.save, {
        context: f.context,
        profile: {},
        effects: [
          profileEffect(f.save, 0, f.query, {
            prepare: () => [],
            settle: () => [],
          }),
        ],
        run: (input, call) => {
          started.resolve();
          return f.save.run(input, call);
        },
      });
      const running = binding.member({ id: "one", value: 2 }, { abortSignal: signal });
      await started.promise;
      call = f.backend.pending("save")[0]!;
      f.backend.resolve(call, providerResult as unknown as string);
      providerResolved = true;
      expect(providerRead).toBe(true);
      await listenerRemoved.promise;
      expect(finalOutputAdoption).toBe(true);

      const retirement = f.context.retire();
      await expectPending(running);
      await expectPending(retirement);

      finalization.resolve();
      await expect(running).resolves.toBe("finalized");
      await retirement;
      expect(f.backend.calls.map((entry) => entry.name)).toEqual(["save"]);
    } finally {
      if (!providerResolved && call !== undefined)
        f.backend.resolve(call, providerResult as unknown as string);
      finalization.resolve();
      f.graph.dispose();
    }
  });

  it("retains a custom run original hidden behind asynchronous preparation", async () => {
    const f = fixture();
    const provider = Promise.withResolvers<unknown>();
    const legacyCarrier = Promise.withResolvers<unknown>();
    const started = Promise.withResolvers<void>();
    const callbackSettled = Promise.withResolvers<void>();
    const nativeThen = Promise.prototype.then;
    let prepareCalls = 0;
    let runCalls = 0;
    let observerCalls = 0;
    let callbackCalls = 0;
    let callbackInput: unknown;
    let callbackResult: unknown;
    let providerResolved = false;
    Object.defineProperty(provider.promise, "then", {
      configurable: true,
      value: function (
        this: Promise<unknown>,
        onFulfilled: ((value: unknown) => unknown) | null | undefined,
        onRejected: ((cause: unknown) => unknown) | null | undefined,
      ): Promise<unknown> {
        observerCalls += 1;
        if (typeof onFulfilled !== "function")
          throw new Error("Expected the public bound mutation continuation");
        const callbackCarrier = Reflect.apply(nativeThen, this, [
          (value: unknown) => {
            callbackCalls += 1;
            callbackInput = value;
            return onFulfilled(value);
          },
          onRejected,
        ]) as Promise<unknown>;
        void Reflect.apply(nativeThen, callbackCarrier, [
          (value: unknown) => {
            callbackResult = value;
            callbackSettled.resolve();
          },
          (cause: unknown) => {
            callbackResult = cause;
            callbackSettled.resolve();
          },
        ]);
        return legacyCarrier.promise;
      },
    });
    try {
      const binding = bindOperation(f.graph, f.save, {
        context: f.context,
        profile: {},
        effects: [
          profileEffect(f.save, 0, f.query, {
            prepare: async () => {
              prepareCalls += 1;
              return [];
            },
            settle: () => [],
          }),
        ],
        run: () => {
          runCalls += 1;
          started.resolve();
          return provider.promise;
        },
      });
      const running = binding.member({ id: "one", value: 2 });
      expect(runCalls).toBe(0);
      await started.promise;
      expect(prepareCalls).toBe(1);
      expect(runCalls).toBe(1);
      expect(observerCalls).toBe(1);

      const retirement = f.context.retire();
      await expectPending(retirement);
      await expectPending(running);
      legacyCarrier.resolve("independent legacy result");
      await expect(running).resolves.toBe("independent legacy result");
      expect(providerResolved).toBe(false);
      await expectPending(retirement);

      provider.resolve("original provider result");
      providerResolved = true;
      await callbackSettled.promise;
      expect(callbackInput).toBe("original provider result");
      expect(callbackResult).toBe("original provider result");
      expect(callbackCalls).toBe(1);
      await expectPending(retirement);
    } finally {
      legacyCarrier.resolve("independent legacy result");
      if (!providerResolved) provider.resolve("original provider result");
      f.graph.dispose();
    }
  });

  it("reports original post-success failures independently and finishes resolution before returning success", async () => {
    const f = fixture();
    try {
      const cause = new Error("settlement validation");
      const applied = new Error("target key");
      let failTarget = false;
      const query = createQuery({
        name: "effects",
        key: (input: Input) => {
          if (failTarget && input.revision === 88) throw applied;
          return input.id;
        },
        fetch: f.backend.respond<Input, number>("effects"),
      });
      const save = createMutation({
        name: "continuation",
        run: f.backend.perform<Change, string>("continuation"),
        affects: [
          affects(query, { select: (input: Change) => input }),
          affects(query, { select: (input: Change) => input, on: "revalidate" }),
        ],
      });
      const graph = createGraph({
        contract: defineContract({ namespace: "continuations", operations: { query, save } }),
        onUnobservedFault: (error) => f.faults.push(error),
      });
      try {
        const context = createBindingContext(graph);
        const binding = bindOperation(graph, query, { context, profile: {}, fetch: query.fetch });
        const handle = binding.member({ id: "one" }, { initial: 1 });
        const second = bindOperation(graph, query, { context, profile: {}, fetch: query.fetch });
        const independent = second.member({ id: "one" }, { initial: 2 });
        const entered = Promise.withResolvers<void>();
        const gate = Promise.withResolvers<void>();
        const mutation = bindOperation(graph, save, {
          context,
          profile: {},
          run: save.run,
          effects: [
            profileEffect(save, 0, query, {
              prepare: () => [],
              settle: () => {
                throw cause;
              },
            }),
            profileEffect(save, 1, query, {
              prepare: () => [],
              settle: async (phase) => {
                const targets = phase.existing(phase.selectedInput);
                entered.resolve();
                await gate.promise;
                return [
                  phase.selected(targets[0]!, { id: "one", revision: 88 }),
                  phase.current(targets[1]!),
                ];
              },
            }),
          ],
        });
        let returned = false;
        const running = mutation.member({ id: "one", value: 2 }).then((result) => {
          returned = true;
          return result;
        });
        f.backend.resolve(f.backend.pending("continuation")[0]!, "saved");
        await entered.promise;
        expect(returned).toBe(false);
        expect(f.faults).toEqual([cause]);
        failTarget = true;
        gate.resolve();
        await expect(running).resolves.toBe("saved");
        expect(f.faults).toEqual([cause, applied]);
        expect(f.backend.pending("effects")).toHaveLength(1);
        f.backend.resolve(f.backend.pending("effects")[0]!, 3);
        expect(await independent.ready).toBe(3);
        expect(visible(handle)).toBe(1);
      } finally {
        graph.dispose();
      }
    } finally {
      f.graph.dispose();
    }
  });

  it("rejects instructions reused from another preparation without publishing or running", async () => {
    const f = fixture();
    try {
      const handle = f.native.member({ id: "one" }, { initial: 1 });
      let previous: readonly PreparedEffect[] | undefined;
      const effect = profileEffect(f.save, 0, f.query, {
        prepare: (phase) =>
          (previous ??= phase.existing(phase.selectedInput).map((target) => phase.prepare(target))),
        settle: () => [],
      });
      const binding = f.binding(effect);
      const first = binding.member({ id: "one", value: 2 });
      f.completeSave();
      await first;
      await expect(binding.member({ id: "one", value: 3 })).rejects.toBeInstanceOf(TypeError);
      expect(f.backend.calls.filter((call) => call.name === "save")).toHaveLength(1);
      expect(visible(handle)).toBe(1);
    } finally {
      f.graph.dispose();
    }
  });

  it("keeps original raw pre-aborted callback behavior while refusing bound preparation", async () => {
    const f = fixture();
    try {
      const controller = new AbortController();
      controller.abort();
      const raw = f.graph.api.save({ id: "one", value: 1 }, { abortSignal: controller.signal });
      expect(f.backend.pending("save")[0]!.aborted).toBe(true);
      await expect(
        f.binding().member({ id: "one", value: 2 }, { abortSignal: controller.signal }),
      ).rejects.toMatchObject({ name: "AbortError" });
      expect(f.backend.calls).toHaveLength(1);
      f.completeSave();
      await raw;
    } finally {
      f.graph.dispose();
    }
  });

  it("retires only the bound context's mutation controllers before abort reentry", async () => {
    const f = fixture();
    try {
      const otherContext = createBindingContext(f.graph);
      const other = bindOperation(f.graph, f.save, {
        context: otherContext,
        profile: {},
        effects: [f.membership()],
        run: f.save.run,
      });
      let reentry: unknown;
      let activeDuringAbort: boolean | undefined;
      const own = bindOperation(f.graph, f.save, {
        context: f.context,
        profile: {},
        effects: [f.membership()],
        run: (input, call) => {
          call.abortSignal.addEventListener(
            "abort",
            () => {
              activeDuringAbort = f.context.active.peek();
              try {
                f.binding().member(input);
              } catch (error) {
                reentry = error;
              }
            },
            { once: true },
          );
          return f.save.run(input, call);
        },
      });
      const a = own.member({ id: "one", value: 1 });
      const b = other.member({ id: "one", value: 2 });
      const c = f.graph.api.save({ id: "one", value: 3 });
      const calls = [...f.backend.pending("save")];
      f.context.dispose();
      expect(reentry).toBeInstanceOf(Fault);
      expect(activeDuringAbort).toBe(false);
      expect(calls.map((call) => call.aborted)).toEqual([true, false, false]);
      expect(otherContext.active.peek()).toBe(true);
      for (const call of calls) f.backend.resolve(call, "saved");
      await Promise.all([a, b, c]);
    } finally {
      f.graph.dispose();
    }
  });

  it.each(["caller", "context", "reset"] as const)(
    "checks native authority across a settlement wait after %s cancellation",
    async (mode) => {
      const f = fixture();
      const gate = Promise.withResolvers<void>();
      const entered = Promise.withResolvers<void>();
      try {
        f.native.member({ id: "one" }, { initial: 1 });
        const caller = new AbortController();
        const effect = profileEffect(f.save, 0, f.query, {
          prepare: () => [],
          settle: async (phase) => {
            const targets = phase.existing(phase.selectedInput);
            entered.resolve();
            await gate.promise;
            return targets.map((target) => phase.current(target));
          },
        });
        const running = f
          .binding(effect)
          .member({ id: "one", value: 2 }, { abortSignal: caller.signal });
        f.completeSave();
        await entered.promise;
        if (mode === "caller") caller.abort();
        if (mode === "context") f.context.dispose();
        if (mode === "reset") await f.graph.reset();
        gate.resolve();
        await expect(running).resolves.toEqual({ receipt: "saved" });
        expect(f.backend.pending("read")).toHaveLength(mode === "caller" ? 1 : 0);
        expect(f.faults).toEqual([]);
      } finally {
        gate.resolve();
        f.graph.dispose();
      }
    },
  );

  it("does not dispatch an old effect when a predecessor abort resets and reacquires the retained handle", async () => {
    const f = fixture();
    let reenter = false;
    let resetting: Promise<void> | undefined;
    let stop: (() => void) | undefined;
    try {
      const binding = bindOperation(f.graph, f.query, {
        context: f.context,
        profile: {},
        fetch: (input, call) => {
          call.abortSignal.addEventListener(
            "abort",
            () => {
              if (!reenter) return;
              reenter = false;
              resetting = f.graph.reset();
            },
            { once: true },
          );
          return f.query.fetch(input, call);
        },
      });
      const handle = binding.member({ id: "one", revision: 1 });
      stop = subscribe(handle, () => undefined);
      const original = f.backend.pending("read")[0]!;
      const mutation = f.binding();
      const running = mutation.member({ id: "one", revision: 7, value: 2 });
      reenter = true;
      f.completeSave();
      await running;
      expect(original.aborted).toBe(true);
      expect(resetting).toBeDefined();
      const requests = f.backend.calls.filter((call) => call.name === "read");
      expect(requests).toHaveLength(2);
      expect(requests[1]!.input).toEqual({ id: "one", revision: 1 });
      f.backend.resolve(requests[1]!, 3);
      await resetting;
      expect(await handle.ready).toBe(3);
      expect(binding.target(handle)).toBeDefined();
    } finally {
      stop?.();
      f.graph.dispose();
    }
  });
});
