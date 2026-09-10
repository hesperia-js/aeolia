import { describe, expect, it } from "bun:test";
import { Fault, Signal, computed, signal, watch } from "../../src/index.ts";

function thrownBy(run: () => unknown): unknown {
  try {
    run();
  } catch (error) {
    return error;
  }
  return undefined;
}

function faultFrom(run: () => unknown, kind: Fault["kind"]): Fault {
  const error = thrownBy(run);
  expect(error).toBeInstanceOf(Fault);
  const fault = error as Fault;
  expect(fault.kind).toBe(kind);
  return fault;
}

describe("Aeolia reactive public API", () => {
  it("keeps a diamond lazy, memoized, and fresh after its source changes", () => {
    const source = signal(1);
    let leftRuns = 0;
    let rightRuns = 0;
    let sinkRuns = 0;
    const left = computed(() => {
      leftRuns += 1;
      return source.get() * 2;
    });
    const right = computed(() => {
      rightRuns += 1;
      return source.get() + 3;
    });
    const sink = computed(() => {
      sinkRuns += 1;
      return left.get() + right.get();
    });

    expect([leftRuns, rightRuns, sinkRuns]).toEqual([0, 0, 0]);
    expect(sink.get()).toBe(6);
    expect(sink.get()).toBe(6);
    expect([leftRuns, rightRuns, sinkRuns]).toEqual([1, 1, 1]);

    source.set(2);
    expect([leftRuns, rightRuns, sinkRuns]).toEqual([1, 1, 1]);
    expect(sink.get()).toBe(9);
    expect(sink.get()).toBe(9);
    expect([leftRuns, rightRuns, sinkRuns]).toEqual([2, 2, 2]);
  });

  it("tracks only taken get branches and never tracks peek", () => {
    const takeLeft = signal(true);
    const left = signal(10);
    const right = signal(20);
    let branchRuns = 0;
    const branch = computed(() => {
      branchRuns += 1;
      return takeLeft.get() ? left.get() : right.get();
    });

    expect(branch.get()).toBe(10);
    right.set(21);
    expect(branch.get()).toBe(10);
    expect(branchRuns).toBe(1);

    takeLeft.set(false);
    expect(branch.get()).toBe(21);
    expect(branchRuns).toBe(2);

    const peeked = signal(4);
    let peekRuns = 0;
    const peekOnly = computed(() => {
      peekRuns += 1;
      return peeked.peek() + 1;
    });
    expect(peekOnly.get()).toBe(5);
    peeked.set(8);
    expect(peekOnly.get()).toBe(5);
    expect(peekRuns).toBe(1);
  });

  it("suppresses equal writes", () => {
    const value = signal(0);
    let notifications = 0;
    const unsubscribe = watch(value, () => {
      notifications += 1;
    });

    value.set(0);
    value.update((current) => current);
    expect(notifications).toBe(0);

    value.update((current) => current + 1);
    expect(value.get()).toBe(1);
    expect(notifications).toBe(1);
    value.set(1);
    expect(notifications).toBe(1);
    unsubscribe();
  });

  it("caches a failed computation until a dependency changes, then recovers", () => {
    const input = signal(0);
    const failure = new Error("not ready");
    let runs = 0;
    const value = computed(() => {
      runs += 1;
      if (input.get() === 0) throw failure;
      return input.get() * 2;
    });

    expect(thrownBy(() => value.get())).toBe(failure);
    expect(thrownBy(() => value.get())).toBe(failure);
    expect(runs).toBe(1);

    input.set(2);
    expect(value.get()).toBe(4);
    expect(runs).toBe(2);
  });

  it("does not recompute a dependent forever when its inner failure is cached", () => {
    const input = signal(0);
    const failure = new Error("nested failure");
    let innerRuns = 0;
    let outerRuns = 0;
    const inner = computed(() => {
      innerRuns += 1;
      input.get();
      throw failure;
    });
    const outer = computed(() => {
      outerRuns += 1;
      return inner.get();
    });

    expect(thrownBy(() => outer.get())).toBe(failure);
    expect(thrownBy(() => outer.get())).toBe(failure);
    expect([innerRuns, outerRuns]).toEqual([1, 1]);

    input.set(1);
    expect(thrownBy(() => outer.get())).toBe(failure);
    expect([innerRuns, outerRuns]).toEqual([2, 2]);
  });

  it("rejects a thenable after reading then once without invoking it", () => {
    let reads = 0;
    let calls = 0;
    const thenable = {
      // oxlint-disable-next-line unicorn/no-thenable
      get then(): () => void {
        reads += 1;
        return () => {
          calls += 1;
        };
      },
    };
    const value = computed(() => thenable);

    faultFrom(() => value.get(), "async-compute");
    expect(reads).toBe(1);
    expect(calls).toBe(0);
  });

  it("reports every readable in a cycle", () => {
    let first!: { get(): number };
    let second!: { get(): number };
    first = computed(() => second.get(), { label: "first" });
    second = computed(() => first.get(), { label: "second" });

    const error = faultFrom(() => first.get(), "cycle");
    expect(error.involved).toEqual(["first", "second"]);
    expect(error.message).toContain("first");
    expect(error.message).toContain("second");
  });

  it("evaluates a watched computed at registration and invalidates it lazily", () => {
    const input = signal(1);
    let runs = 0;
    let notifications = 0;
    const value = computed(() => {
      runs += 1;
      return input.get() * 2;
    });

    const unsubscribe = watch(value, () => {
      notifications += 1;
    });
    expect(runs).toBe(1);

    input.set(2);
    expect(notifications).toBe(1);
    expect(runs).toBe(1);
    expect(value.get()).toBe(4);
    expect(runs).toBe(2);
    unsubscribe();
  });

  it("does not leak a computed watched during evaluation into its dependencies", () => {
    const watchedInput = signal(0);
    const watched = computed(() => watchedInput.get());
    const stable = signal(1);
    let outerRuns = 0;
    let outerNotifications = 0;
    let registered = false;
    let stopInner = () => {};
    const outer = computed(() => {
      outerRuns += 1;
      if (!registered) {
        registered = true;
        stopInner = watch(watched, () => undefined);
      }
      return stable.get();
    });
    const unsubscribe = watch(outer, () => {
      outerNotifications += 1;
    });

    expect(outer.get()).toBe(1);
    expect(outerRuns).toBe(1);
    expect(Signal.subtle.introspectSources(outer)).toEqual([stable]);

    watchedInput.set(1);
    expect(outer.get()).toBe(1);
    expect(outerRuns).toBe(1);
    expect(outerNotifications).toBe(0);

    stable.set(2);
    expect(outer.get()).toBe(2);
    expect(outerRuns).toBe(2);
    expect(outerNotifications).toBe(1);
    unsubscribe();
    stopInner();
  });

  it("propagates through a wide set of live computed dependents", () => {
    const source = signal(0);
    const width = 4_096;
    const notifications = new Uint8Array(width);
    const unsubscribes = Array.from({ length: width }, (_, index) => {
      const branch = computed(() => source.get() + index);
      return watch(branch, () => {
        notifications[index] = notifications[index]! + 1;
      });
    });

    source.set(1);

    expect([...notifications].every((count) => count === 1)).toBe(true);
    unsubscribes.forEach((unsubscribe) => unsubscribe());
  });

  it("cuts off a watched downstream recomputation when its upstream stays equal", () => {
    const source = signal(0);
    let upstreamRuns = 0;
    let downstreamRuns = 0;
    const upstream = computed(() => {
      upstreamRuns += 1;
      source.get();
      return 1;
    });
    const downstream = computed(() => {
      downstreamRuns += 1;
      return upstream.get() + 1;
    });
    const unsubscribe = watch(downstream, () => undefined);

    expect(downstream.get()).toBe(2);
    source.set(1);
    expect(downstream.get()).toBe(2);
    expect(upstreamRuns).toBe(2);
    expect(downstreamRuns).toBe(1);
    unsubscribe();
  });

  it("leaves no watcher after a throwing computed registration and can recover", () => {
    const input = signal(0);
    const failure = new Error("initial failure");
    let callbackCalls = 0;
    const value = computed(() => {
      if (input.get() === 0) throw failure;
      return input.get();
    });

    expect(() =>
      watch(value, () => {
        callbackCalls += 1;
        throw new Error("orphaned watcher");
      }),
    ).toThrow(failure);

    expect(() => input.set(1)).not.toThrow();
    expect(callbackCalls).toBe(0);
    expect(value.get()).toBe(1);
  });

  it("routes readable reads during notification to a watcher-read fault", () => {
    const source = signal(0, { label: "source" });
    const unsubscribe = watch(source, () => {
      source.get();
    });

    const error = faultFrom(() => source.set(1), "watcher-read");
    expect(error.involved).toEqual(["source"]);
    unsubscribe();
  });

  it("applies writes during notification immediately and notifies them on the next pass", () => {
    const source = signal(0);
    const other = signal(0);
    const events: string[] = [];
    let otherNotifications = 0;

    watch(other, () => {
      events.push("other");
      otherNotifications += 1;
    });
    watch(source, () => {
      events.push("source");
      other.set(1);
    });

    source.set(1);
    expect(other.get()).toBe(1);
    expect(otherNotifications).toBe(1);
    expect(events).toEqual(["source", "other"]);
  });

  it("delivers every watcher and aggregates watcher failures without an order contract", () => {
    const source = signal(0);
    const order: number[] = [];
    const firstFailure = new Error("first watcher");
    const secondFailure = new Error("second watcher");
    watch(source, () => {
      order.push(1);
      throw firstFailure;
    });
    watch(source, () => {
      order.push(2);
      throw secondFailure;
    });

    const error = thrownBy(() => source.set(1));
    expect(order).toHaveLength(2);
    expect(new Set(order)).toEqual(new Set([1, 2]));
    expect(error).toBeInstanceOf(AggregateError);
    expect((error as AggregateError).errors).toHaveLength(2);
    expect((error as AggregateError).errors).toEqual(
      expect.arrayContaining([firstFailure, secondFailure]),
    );
  });

  it("prevents notification after unsubscribe and makes it idempotent", () => {
    const source = signal(0);
    let notifications = 0;
    const stop = watch(source, () => {
      notifications += 1;
    });

    stop();
    stop();
    source.set(1);
    expect(notifications).toBe(0);
  });

  it("starts a listener added during notification with the next write", () => {
    const source = signal(0);
    let addedNotifications = 0;
    let added = false;
    watch(source, () => {
      if (added) return;
      added = true;
      watch(source, () => {
        addedNotifications += 1;
      });
    });

    source.set(1);
    expect(addedNotifications).toBe(0);
    source.set(2);
    expect(addedNotifications).toBe(1);
  });

  it("bounds a propagation loop and restores the next write", () => {
    const source = signal(0, { label: "loop-source" });
    let next = 2;
    const stop = watch(source, () => {
      source.set(next++);
    });

    const error = faultFrom(() => source.set(1), "propagation");
    expect(error.involved).toEqual(["loop-source"]);
    expect(source.peek()).toBe(9);
    expect(next).toBe(10);

    stop();
    let recoveredNotifications = 0;
    const unsubscribe = watch(source, () => {
      recoveredNotifications += 1;
    });
    source.set(10);
    expect(source.get()).toBe(10);
    expect(recoveredNotifications).toBe(1);
    unsubscribe();
  });
});
