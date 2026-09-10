import { describe, expect, it } from "bun:test";

import { Signal } from "../../src/index.ts";

describe("canonical Signals graph behavior", () => {
  it("settles an uneven reconverging graph exactly once per pull", () => {
    const source = new Signal.State(1);
    const left = new Signal.Computed(() => source.get() + 1);
    const rightBase = new Signal.Computed(() => source.get() * 2);
    const right = new Signal.Computed(() => rightBase.get() - 1);
    let mergeRuns = 0;
    const merge = new Signal.Computed(() => {
      mergeRuns += 1;
      return left.get() + right.get();
    });
    let firstTailRuns = 0;
    let secondTailRuns = 0;
    const firstTail = new Signal.Computed(() => {
      firstTailRuns += 1;
      return merge.get() * 2;
    });
    const secondTail = new Signal.Computed(() => {
      secondTailRuns += 1;
      return merge.get() + 1;
    });

    expect([firstTail.get(), secondTail.get()]).toEqual([6, 4]);
    source.set(3);
    expect([secondTail.get(), firstTail.get()]).toEqual([10, 18]);
    expect([mergeRuns, firstTailRuns, secondTailRuns]).toEqual([2, 2, 2]);
  });

  it("handles one State reached through both direct and derived paths", () => {
    const source = new Signal.State(2);
    const offset = new Signal.Computed(() => source.get() + 10);
    let mergeRuns = 0;
    const merge = new Signal.Computed(() => {
      mergeRuns += 1;
      return source.get() + offset.get();
    });
    const tail = new Signal.Computed(() => `value:${merge.get()}`);

    expect(tail.get()).toBe("value:14");
    source.set(5);
    expect(tail.get()).toBe("value:20");
    expect(mergeRuns).toBe(2);
  });

  it("prunes equal branches without hiding a changed direct dependency", () => {
    const source = new Signal.State(0);
    let stableLeftRuns = 0;
    let stableRightRuns = 0;
    const stableLeft = new Signal.Computed(() => {
      stableLeftRuns += 1;
      source.get();
      return "left";
    });
    const stableRight = new Signal.Computed(() => {
      stableRightRuns += 1;
      source.get();
      return "right";
    });
    let prunedRuns = 0;
    const pruned = new Signal.Computed(() => {
      prunedRuns += 1;
      return `${stableLeft.get()}:${stableRight.get()}`;
    });
    let mixedRuns = 0;
    const mixed = new Signal.Computed(() => {
      mixedRuns += 1;
      return `${stableLeft.get()}:${stableRight.get()}:${source.get()}`;
    });

    expect(pruned.get()).toBe("left:right");
    expect(mixed.get()).toBe("left:right:0");
    source.set(1);
    expect(pruned.get()).toBe("left:right");
    expect(mixed.get()).toBe("left:right:1");
    expect([stableLeftRuns, stableRightRuns, prunedRuns, mixedRuns]).toEqual([2, 2, 1, 2]);
  });

  it("pulls dirty dependencies before their dependent in source order", () => {
    const source = new Signal.State(0);
    const trace: string[] = [];
    const first = new Signal.Computed(
      () => {
        source.get();
        trace.push("first");
        return undefined;
      },
      { equals: () => false },
    );
    const second = new Signal.Computed(
      () => {
        source.get();
        trace.push("second");
        return undefined;
      },
      { equals: () => false },
    );
    const joined = new Signal.Computed(
      () => {
        first.get();
        second.get();
        trace.push("joined");
        return undefined;
      },
      { equals: () => false },
    );

    joined.get();
    trace.length = 0;
    source.set(1);
    joined.get();
    expect(trace).toEqual(["first", "second", "joined"]);
  });

  it("refreshes an established dependee before rerunning its consumer", () => {
    const source = new Signal.State(0);
    const trace: string[] = [];
    const direct = new Signal.Computed(() => {
      trace.push("direct");
      return source.get();
    });
    const condition = new Signal.Computed(() => {
      trace.push("condition");
      return source.get() === 0;
    });
    const consumer = new Signal.Computed(() => {
      trace.push("consumer");
      return condition.get();
    });

    direct.get();
    consumer.get();
    expect(trace).toEqual(["direct", "consumer", "condition"]);

    trace.length = 0;
    source.set(1);
    direct.get();
    consumer.get();
    expect(trace).toEqual(["direct", "condition", "consumer"]);
  });

  it("recomputes a high-fanout convergence only once", () => {
    const source = new Signal.State(1);
    const firstLayer = Array.from(
      { length: 7 },
      (_, index) => new Signal.Computed(() => source.get() + index),
    );
    const secondLayer = Array.from(
      { length: 5 },
      (_, index) =>
        new Signal.Computed(() => firstLayer.reduce((sum, item) => sum + item.get(), index)),
    );
    let rootRuns = 0;
    const root = new Signal.Computed(() => {
      rootRuns += 1;
      return secondLayer.reduce((sum, item) => sum + item.get(), 0);
    });

    expect(root.get()).toBe(150);
    source.set(2);
    expect(root.get()).toBe(185);
    expect(rootRuns).toBe(2);
  });

  it("replaces live branch dependencies without retaining the inactive source", () => {
    const chooseLeft = new Signal.State(true);
    const left = new Signal.State(10);
    const right = new Signal.State(20);
    let runs = 0;
    const selected = new Signal.Computed(() => {
      runs += 1;
      return chooseLeft.get() ? left.get() : right.get();
    });
    let notifications = 0;
    const watcher = new Signal.subtle.Watcher(() => {
      notifications += 1;
    });

    expect(selected.get()).toBe(10);
    watcher.watch(selected);
    right.set(21);
    expect(notifications).toBe(0);
    expect(selected.get()).toBe(10);
    expect(runs).toBe(1);

    chooseLeft.set(false);
    expect(notifications).toBe(1);
    expect(selected.get()).toBe(21);
    watcher.watch();

    left.set(11);
    expect(notifications).toBe(1);
    right.set(22);
    expect(notifications).toBe(2);
    expect(selected.get()).toBe(22);
    expect(runs).toBe(3);
  });

  it("does not rerun a downstream Computed when every dirty branch settles equal", () => {
    const source = new Signal.State(0);
    const parity = new Signal.Computed(() => source.get() % 2);
    const bucket = new Signal.Computed(() => Math.floor(source.get() / 10));
    let downstreamRuns = 0;
    const downstream = new Signal.Computed(() => {
      downstreamRuns += 1;
      return `${parity.get()}:${bucket.get()}`;
    });

    expect(downstream.get()).toBe("0:0");
    source.set(2);
    expect(downstream.get()).toBe("0:0");
    expect(downstreamRuns).toBe(1);

    source.set(11);
    expect(downstream.get()).toBe("1:1");
    expect(downstreamRuns).toBe(2);
  });

  it("keeps every pending branch consistent after a sibling was pulled", () => {
    const quantity = new Signal.State(1);
    const enabled = new Signal.State(false);
    const positive = new Signal.Computed(() => quantity.get() > 0);
    let siblingRuns = 0;
    const alsoPositive = new Signal.Computed(() => {
      siblingRuns += 1;
      return quantity.get() > 0;
    });
    const exact = new Signal.Computed(() => quantity.get());
    const gated = new Signal.Computed(() => quantity.get() > 0 && enabled.get());
    let rootRuns = 0;
    const root = new Signal.Computed(() => {
      positive.get();
      alsoPositive.get();
      exact.get();
      gated.get();
      rootRuns += 1;
      return exact.get();
    });

    expect(root.get()).toBe(1);
    expect(siblingRuns).toBe(1);
    enabled.set(true);
    expect(root.get()).toBe(1);
    expect(siblingRuns).toBe(1);
    quantity.set(2);
    expect(alsoPositive.get()).toBe(true);
    expect(siblingRuns).toBe(2);
    expect(root.get()).toBe(2);
    expect(rootRuns).toBe(3);
  });

  it("refreshes all stale sources before publishing their dependent", () => {
    const source = new Signal.State(1, { equals: () => false });
    const trace: string[] = [];
    const low = new Signal.Computed(() => {
      trace.push("low");
      return source.get() < 5;
    });
    const high = new Signal.Computed(() => {
      trace.push("high");
      return source.get() > 5;
    });
    const exact = new Signal.Computed(
      () => {
        trace.push("exact");
        return source.get();
      },
      { equals: () => false },
    );
    const root = new Signal.Computed(() => {
      low.get();
      high.get();
      exact.get();
      trace.push("root");
      return source.get();
    });

    root.get();
    trace.length = 0;
    source.set(1);
    expect(root.get()).toBe(1);
    expect(trace).toEqual(["low", "high", "exact", "root"]);

    trace.length = 0;
    source.set(8);
    expect(root.get()).toBe(8);
    expect(trace).toEqual(["low", "high", "exact", "root"]);
  });

  it("pulls a newly selected dirty Computed before using its value", () => {
    const route = new Signal.State<"primary" | "fallback">("fallback");
    const source = new Signal.State(2);
    let primaryRuns = 0;
    const primary = new Signal.Computed(() => {
      primaryRuns += 1;
      return source.get() * 10;
    });
    const selected = new Signal.Computed(() =>
      route.get() === "primary" ? primary.get() : source.get(),
    );

    expect(primary.get()).toBe(20);
    expect(selected.get()).toBe(2);
    source.set(3);
    route.set("primary");
    expect(selected.get()).toBe(30);
    expect(primaryRuns).toBe(2);
  });
});
