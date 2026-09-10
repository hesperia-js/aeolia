import { describe, expect, it } from "bun:test";
import { Fault } from "../../src/fault.ts";
import {
  Signal,
  computed,
  signal,
  subscribe,
  watch,
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
