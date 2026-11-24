import { describe, expect, it } from "bun:test";
import { createGraph, createStore } from "../../src/index.ts";
import { Fault } from "../../src/fault.ts";
import {
  Signal,
  computed,
  signal,
  subscribe,
  watch,
  type Readable,
  type WritableSignal,
} from "../../src/reactive.ts";

describe("Aeolia value subscriptions", () => {
  it("delivers the current value immediately, including undefined", () => {
    const source = signal<number | undefined>(undefined);
    const values: Array<number | undefined> = [];
    const stop = subscribe(source, (value) => values.push(value));

    expect(values).toStrictEqual([undefined]);
    source.set(1);
    source.set(undefined);
    expect(values).toStrictEqual([undefined, 1, undefined]);
    stop();
  });

  it("suppresses equal computed values and coalesces writes before delivery", () => {
    const trigger = signal(0);
    const source = signal(0);
    const parity = computed(() => source.get() % 2);
    const values: number[] = [];
    const stop = subscribe(parity, (value) => values.push(value));
    const stopTrigger = watch(trigger, () => {
      source.set(1);
      source.set(2);
    });

    trigger.set(1);
    expect(values).toStrictEqual([0]);
    source.set(3);
    expect(values).toStrictEqual([0, 1]);
    stopTrigger();
    stop();
  });

  it("rechecks an earlier cached dependency after refreshing a later one", () => {
    const first = signal(1);
    const trigger = signal(false);
    const later = computed(() => {
      if (trigger.get()) first.set(3);
      return 0;
    });
    const view = computed(() => {
      const value = first.get();
      later.get();
      return value;
    });
    expect(view.get()).toBe(1);
    trigger.set(true);
    expect(view.get()).toBe(3);
    expect(first.get()).toBe(3);
  });

  it.each([
    { earlierSide: false, hookReadsSide: true, explicitTail: false },
    { earlierSide: true, hookReadsSide: true, explicitTail: false },
    { earlierSide: false, hookReadsSide: false, explicitTail: true },
    { earlierSide: false, hookReadsSide: true, explicitTail: true },
  ])("introspects distinct sources in first-read order during a watched hook: %j", (options) => {
    const branch = signal(false);
    const side = signal(100);
    const left = signal(1);
    let activeOuter: Readable<number> | null = null;
    const trace: string[] = [];
    const right = signal(2, {
      [Signal.subtle.watched]() {
        if (options.hookReadsSide) {
          expect(Signal.subtle.currentComputed()).toBe(activeOuter);
          trace.push("hook");
          side.get();
        }
      },
    });
    const child = computed(() => (branch.get() ? right.get() : left.get()));
    const stop = watch(child, () => {});
    try {
      branch.set(true);
      const outer = computed(() => {
        if (options.earlierSide) side.get();
        trace.push("enter child");
        const result = child.get();
        trace.push("return from child");
        if (options.explicitTail) side.get();
        return result;
      });
      activeOuter = outer;
      expect(outer.get()).toBe(2);
      expect(trace).toEqual(
        options.hookReadsSide
          ? ["enter child", "hook", "return from child"]
          : ["enter child", "return from child"],
      );
      const sources = Signal.subtle.introspectSources(outer);
      expect(sources).toHaveLength(2);
      expect(sources[0]).toBe(options.earlierSide ? side : child);
      expect(sources[1]).toBe(options.earlierSide ? child : side);
    } finally {
      stop();
    }
  });

  it("orders a source at its first admitted read when an earlier attempt was rejected", async () => {
    const entry = createStore({ name: "entry", initial: 1 });
    const local = createGraph({ contract: { namespace: "ordered-local", operations: { entry } } });
    const foreign = createGraph({
      contract: { namespace: "ordered-foreign", operations: { entry } },
    });
    const localStore = local.api.entry();
    const foreignStore = foreign.api.entry();
    const side = signal(100);
    const switchGraph = signal(false);
    const child = computed(() => (switchGraph.get() ? localStore : foreignStore).value.get());
    const outer = computed(() => {
      localStore.value.get();
      try {
        child.get();
      } catch (error) {
        if (!(error instanceof Fault) || error.kind !== "cross-graph") throw error;
        side.get();
        switchGraph.set(true);
      }
      return child.get();
    });
    try {
      expect(outer.get()).toBe(1);
      const sources = Signal.subtle.introspectSources(outer);
      expect(sources).toHaveLength(3);
      expect(sources[0]).toBe(localStore.value);
      expect(sources[1]).toBe(side);
      expect(sources[2]).toBe(child);
    } finally {
      await Promise.all([local.retire(), foreign.retire()]);
    }
  });

  it("publishes a computed fallback after a caught self-read", () => {
    let runs = 0;
    const view: Readable<number> = computed(() => {
      runs += 1;
      try {
        return view.get();
      } catch (error) {
        if (!(error instanceof Fault) || error.kind !== "cycle") throw error;
        return 7;
      }
    });
    expect(view.get()).toBe(7);
    expect(view.get()).toBe(7);
    expect(runs).toBe(1);
    const unhandled: Readable<number> = computed(() => unhandled.get());
    expect(() => unhandled.get()).toThrow(expect.objectContaining({ kind: "cycle" }));
  });

  it("keeps local dependencies when a computed catches a rejected foreign Store read", async () => {
    const entry = createStore({ name: "entry", initial: 1 });
    const local = createGraph({ contract: { namespace: "local", operations: { entry } } });
    const foreign = createGraph({ contract: { namespace: "foreign", operations: { entry } } });
    const localStore = local.api.entry();
    const foreignStore = foreign.api.entry();
    let runs = 0;
    const view = computed(() => {
      runs += 1;
      const own = localStore.value.get();
      try {
        return (own ?? 0) + (foreignStore.value.get() ?? 0);
      } catch (error) {
        if (!(error instanceof Fault) || error.kind !== "cross-graph") throw error;
        return own;
      }
    });
    const unhandled = computed(() => {
      localStore.value.get();
      return foreignStore.value.get();
    });
    let stop: (() => void) | undefined;
    try {
      expect(() => unhandled.get()).toThrow(expect.objectContaining({ kind: "cross-graph" }));
      const values: Array<number | undefined> = [];
      stop = subscribe(view, (value) => values.push(value));
      foreignStore.set(23);
      expect(runs).toBe(1);
      localStore.set(2);
      expect(values).toEqual([1, 2]);
      expect(runs).toBe(2);
    } finally {
      stop?.();
      await Promise.all([local.retire(), foreign.retire()]);
    }
  });

  it("publishes a changed value after equality catches a self-read", () => {
    const source = signal(1);
    let caught = 0;
    const view = computed(() => source.get(), {
      equals(previous, next) {
        try {
          this.get();
        } catch (error) {
          if (!(error instanceof Fault) || error.kind !== "cycle") throw error;
          caught += 1;
        }
        return Object.is(previous, next);
      },
    });
    const values: number[] = [];
    const stop = subscribe(view, (value) => values.push(value));
    try {
      source.set(2);
      expect(values).toEqual([1, 2]);
      expect(caught).toBe(1);
    } finally {
      stop();
    }
  });

  it.each(["ordinary error", "cached child cycle"] as const)(
    "retains an observed child dependency after catching %s",
    (failure) => {
      const enabled = signal(false);
      const child: Readable<number> = computed(() => {
        if (enabled.get()) return 9;
        if (failure === "cached child cycle") return child.get();
        throw new Error("not ready");
      });
      const view = computed(() => {
        try {
          return child.get();
        } catch {
          return 7;
        }
      });
      const values: number[] = [];
      const stop = subscribe(view, (value) => values.push(value));
      try {
        enabled.set(true);
        expect(values).toEqual([7, 9]);
        expect(view.get()).toBe(9);
      } finally {
        stop();
      }
    },
  );

  it("allows safe callback reads without tracking an enclosing computed", () => {
    const source = signal(1);
    const stable = signal(10);
    let runs = 0;
    let stopInner = (): void => undefined;
    const outer = computed(() => {
      runs += 1;
      stopInner = subscribe(source, (value) => {
        expect(source.peek()).toBe(value);
        expect(source.get()).toBe(value);
      });
      return stable.get();
    });

    expect(outer.get()).toBe(10);
    expect(Signal.subtle.introspectSources(outer)).toEqual([stable]);
    source.set(2);
    expect(runs).toBe(1);
    stopInner();
  });

  it("queues writes from callbacks and refreshes warmed derived reads", () => {
    const source = signal(0);
    const doubled = computed(() => source.get() * 2);
    expect(doubled.get()).toBe(0);
    const values: Array<readonly [number, number]> = [];
    const stopDerivedWatch = watch(doubled, () => undefined);
    let callbackDepth = 0;
    let maximumDepth = 0;
    const stop = subscribe(source, (value) => {
      callbackDepth += 1;
      maximumDepth = Math.max(maximumDepth, callbackDepth);
      try {
        if (value === 1) source.set(2);
        values.push([value, doubled.get()]);
      } finally {
        callbackDepth -= 1;
      }
    });

    source.set(1);
    expect(values).toStrictEqual([
      [0, 0],
      [1, 4],
      [2, 4],
    ]);
    expect(maximumDepth).toBe(1);
    stop();
    stopDerivedWatch();
  });

  it("does not deliver after unsubscribe and bounds subscription feedback", () => {
    const source = signal(0, { label: "subscription-loop" });
    let calls = 0;
    let stop = (): void => undefined;
    stop = subscribe(source, (value) => {
      calls += 1;
      if (value === 1) {
        stop();
        source.set(2);
      }
    });
    source.set(1);
    expect(calls).toBe(2);
    source.set(3);
    expect(calls).toBe(2);

    const loop = signal(0);
    let loopCalls = 0;
    expect(() =>
      subscribe(loop, (value) => {
        loopCalls += 1;
        loop.set(value + 1);
      }),
    ).toThrow(Fault);
    expect(loopCalls).toBe(9);
    expect(loop.peek()).toBe(9);
  });

  it("runs every subscriber and aggregates later callback failures", () => {
    const source = signal(0);
    const first = new Error("first subscription");
    const second = new Error("second subscription");
    const order: number[] = [];
    const stopFirst = subscribe(source, () => {
      if (source.peek() !== 0) {
        order.push(1);
        throw first;
      }
    });
    const stopSecond = subscribe(source, () => {
      if (source.peek() !== 0) {
        order.push(2);
        throw second;
      }
    });

    expect(() => source.set(1)).toThrow(AggregateError);
    expect(new Set(order)).toEqual(new Set([1, 2]));
    stopFirst();
    stopSecond();
  });

  it("rolls back a failed initial setup and maintains liveness counts", () => {
    const lifecycle: string[] = [];
    let source!: WritableSignal<number>;
    source = signal(0, {
      [Signal.subtle.watched]() {
        lifecycle.push("watched");
        source.set(1);
      },
      [Signal.subtle.unwatched]() {
        lifecycle.push("unwatched");
      },
    });
    const failure = new Error("initial observer");
    expect(() =>
      subscribe(source, () => {
        throw failure;
      }),
    ).toThrow(failure);
    expect(lifecycle).toStrictEqual(["watched", "unwatched"]);

    const values: number[] = [];
    const stop = subscribe(source, (value) => values.push(value));
    expect(lifecycle).toStrictEqual(["watched", "unwatched", "watched"]);
    expect(values).toStrictEqual([1]);
    stop();
    expect(lifecycle).toStrictEqual(["watched", "unwatched", "watched", "unwatched"]);
  });

  it("drains writes from a failed initial callback to existing observers", () => {
    const source = signal(0);
    const seen: number[] = [];
    const stop = watch(source, () => seen.push(1));
    const failure = new Error("initial write");

    expect(() =>
      subscribe(source, () => {
        source.set(1);
        throw failure;
      }),
    ).toThrow(failure);
    expect(seen).toStrictEqual([1]);
    stop();
  });

  it("honors source equality tokens for same-reference writes", () => {
    const value = { count: 0 };
    const source = signal(value, { equals: () => false });
    const values: object[] = [];
    const stop = subscribe(source, (next) => values.push(next));

    value.count = 1;
    source.set(value);
    expect(values).toStrictEqual([value, value]);
    stop();
  });

  it("rejects a non-callable observer before installing a live edge", () => {
    const lifecycle: string[] = [];
    const source = signal(0, {
      [Signal.subtle.watched]() {
        lifecycle.push("watched");
      },
    });
    expect(() => subscribe(source, null as never)).toThrow(TypeError);
    expect(lifecycle).toStrictEqual([]);
  });
});
