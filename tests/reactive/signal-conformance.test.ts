import { describe, expect, it } from "bun:test";
import { Fault, Signal } from "../../src/index.ts";

function thrownBy(run: () => unknown): unknown {
  try {
    run();
  } catch (error) {
    return error;
  }
  return undefined;
}

describe("canonical Signals surface", () => {
  describe("classes, receivers, and subclassing", () => {
    it("exposes State, Computed, and Watcher as constructible classes", () => {
      const state = new Signal.State(1);
      const computed = new Signal.Computed(() => state.get() * 2);
      const watcher = new Signal.subtle.Watcher(() => undefined);

      expect(state).toBeInstanceOf(Signal.State);
      expect(computed).toBeInstanceOf(Signal.Computed);
      expect(watcher).toBeInstanceOf(Signal.subtle.Watcher);
      expect(Object.getPrototypeOf(state)).toBe(Signal.State.prototype);
      expect(Object.getPrototypeOf(computed)).toBe(Signal.Computed.prototype);
      expect(Object.getPrototypeOf(watcher)).toBe(Signal.subtle.Watcher.prototype);
      expect(state.get()).toBe(1);
      expect(computed.get()).toBe(2);
    });

    it("supports subclassing without losing graph behavior", () => {
      class ChildState extends Signal.State<number> {
        increment(): void {
          this.set(this.get() + 1);
        }
      }

      class ChildComputed extends Signal.Computed<number> {
        readonly kind = "child";
      }

      const state = new ChildState(1);
      const computed = new ChildComputed(() => state.get() * 2);
      state.increment();

      expect(state).toBeInstanceOf(ChildState);
      expect(computed).toBeInstanceOf(ChildComputed);
      expect(state.get()).toBe(2);
      expect(computed.get()).toBe(4);
      expect(computed.kind).toBe("child");
    });

    it("identifies canonical instances without accepting lookalikes", () => {
      const state = new Signal.State(1);
      const computed = new Signal.Computed(() => state.get());
      const watcher = new Signal.subtle.Watcher(() => undefined);

      expect(Signal.isState(state)).toBe(true);
      expect(Signal.isState(computed)).toBe(false);
      expect(Signal.isComputed(computed)).toBe(true);
      expect(Signal.isComputed(state)).toBe(false);
      expect(Signal.isWatcher(watcher)).toBe(true);
      expect(Signal.isWatcher({ watch() {}, unwatch() {}, getPending() {} })).toBe(false);
      expect(Signal.isState({ get: () => 1, set() {} })).toBe(false);
      expect(Signal.isComputed(null)).toBe(false);
    });

    it("rejects methods called with foreign receivers", () => {
      const state = new Signal.State(1);
      const computed = new Signal.Computed(() => 1);
      const watcher = new Signal.subtle.Watcher(() => undefined);

      expect(() => Signal.State.prototype.get.call({})).toThrow(TypeError);
      expect(() => Signal.State.prototype.set.call({}, 2)).toThrow(TypeError);
      expect(() => Signal.Computed.prototype.get.call({})).toThrow(TypeError);
      expect(() => Signal.subtle.Watcher.prototype.watch.call({}, state)).toThrow(TypeError);
      expect(() => Signal.subtle.Watcher.prototype.unwatch.call({}, state)).toThrow(TypeError);
      expect(() => Signal.subtle.Watcher.prototype.getPending.call({})).toThrow(TypeError);

      watcher.watch(computed);
      watcher.unwatch(computed);
    });
  });

  describe("State", () => {
    it("uses Object.is by default", () => {
      const state = new Signal.State(Number.NaN);
      let notifications = 0;
      const watcher = new Signal.subtle.Watcher(() => {
        notifications += 1;
      });

      watcher.watch(state);
      state.set(Number.NaN);
      expect(notifications).toBe(0);

      state.set(-0);
      expect(notifications).toBe(1);
      expect(Object.is(state.get(), -0)).toBe(true);

      watcher.watch();
      state.set(+0);
      expect(notifications).toBe(2);
      expect(Object.is(state.get(), +0)).toBe(true);
    });

    it("uses a custom equality function and binds it to the State", () => {
      const receiver = { value: undefined as unknown };
      let calls = 0;
      const state = new Signal.State(1, {
        equals: function (this: unknown, a: number, b: number): boolean {
          receiver.value = this;
          calls += 1;
          return a === b;
        },
      });

      state.set(1);
      state.set(2);

      expect(calls).toBe(2);
      expect(receiver.value).toBe(state);
      expect(state.get()).toBe(2);
    });

    it("aborts a State write when equality throws", () => {
      const failure = new Error("state equality failed");
      const state = new Signal.State(1, {
        equals: () => {
          throw failure;
        },
      });
      let notifications = 0;
      const watcher = new Signal.subtle.Watcher(() => {
        notifications += 1;
      });
      watcher.watch(state);

      expect(() => state.set(2)).toThrow(failure);
      expect(state.get()).toBe(1);
      expect(notifications).toBe(0);
    });
  });

  describe("Computed", () => {
    it("is lazy and memoized, then recomputes after a dependency changes", () => {
      const source = new Signal.State(1);
      let runs = 0;
      const value = new Signal.Computed(() => {
        runs += 1;
        return source.get() * 2;
      });

      expect(runs).toBe(0);
      expect(value.get()).toBe(2);
      expect(value.get()).toBe(2);
      expect(runs).toBe(1);

      source.set(2);
      expect(runs).toBe(1);
      expect(value.get()).toBe(4);
      expect(runs).toBe(2);
    });

    it("keeps a live computation failure cached until a dependency invalidates it", () => {
      const source = new Signal.State(0);
      const failure = new Error("not ready");
      let runs = 0;
      const value = new Signal.Computed(() => {
        runs += 1;
        if (source.get() === 0) throw failure;
        return source.get();
      });
      const watcher = new Signal.subtle.Watcher(() => undefined);

      expect(() => value.get()).toThrow(failure);
      watcher.watch(value);
      expect(() => value.get()).toThrow(failure);
      expect(runs).toBe(1);

      source.set(2);
      expect(watcher.getPending()).toEqual([value]);
      expect(value.get()).toBe(2);
      expect(runs).toBe(2);
    });

    it("tracks dynamic dependencies and drops branches no longer read", () => {
      const chooseLeft = new Signal.State(true);
      const left = new Signal.State("left");
      const right = new Signal.State("right");
      let runs = 0;
      const value = new Signal.Computed(() => {
        runs += 1;
        return chooseLeft.get() ? left.get() : right.get();
      });

      expect(value.get()).toBe("left");
      right.set("right-2");
      expect(value.get()).toBe("left");
      expect(runs).toBe(1);

      chooseLeft.set(false);
      expect(value.get()).toBe("right-2");
      left.set("left-2");
      expect(value.get()).toBe("right-2");
      right.set("right-3");
      expect(value.get()).toBe("right-3");
      expect(runs).toBe(3);
    });

    it("binds the callback and custom equality to the Computed instance", () => {
      const source = new Signal.State(1);
      const callbackReceiver = { value: undefined as unknown };
      const equalityReceiver = { value: undefined as unknown };
      const value = new Signal.Computed(
        function (this: unknown): number {
          callbackReceiver.value = this;
          return source.get();
        },
        {
          equals: function (this: unknown, a: number, b: number): boolean {
            equalityReceiver.value = this;
            return a === b;
          },
        },
      );

      expect(value.get()).toBe(1);
      source.set(2);
      expect(value.get()).toBe(2);
      expect(callbackReceiver.value).toBe(value);
      expect(equalityReceiver.value).toBe(value);
    });

    it("tracks equality reads on the inner Computed without leaking them to its caller", () => {
      const exact = new Signal.State(1);
      const tolerance = new Signal.State(0.1);
      const outerTrigger = new Signal.State(0);
      let innerRuns = 0;
      let outerRuns = 0;
      let equalityRuns = 0;
      const inner = new Signal.Computed(
        () => {
          innerRuns += 1;
          return exact.get();
        },
        {
          equals: (a, b) => {
            equalityRuns += 1;
            tolerance.get();
            return a === b;
          },
        },
      );
      const outer = new Signal.Computed(() => {
        outerRuns += 1;
        outerTrigger.get();
        return inner.get();
      });

      expect(outer.get()).toBe(1);
      exact.set(2);
      outerTrigger.set(1);
      expect(outer.get()).toBe(2);
      expect([innerRuns, outerRuns, equalityRuns]).toEqual([2, 2, 1]);

      tolerance.set(0.2);
      expect(outer.get()).toBe(2);
      expect([innerRuns, outerRuns, equalityRuns]).toEqual([3, 2, 2]);
    });

    it("caches a computed equality failure until a dependency changes", () => {
      const source = new Signal.State(0);
      const failure = new Error("computed equality failed");
      let runs = 0;
      let equalityRuns = 0;
      const value = new Signal.Computed(
        () => {
          runs += 1;
          return source.get();
        },
        {
          equals: () => {
            equalityRuns += 1;
            throw failure;
          },
        },
      );

      expect(value.get()).toBe(0);
      source.set(1);
      expect(() => value.get()).toThrow(failure);
      expect(() => value.get()).toThrow(failure);
      expect([runs, equalityRuns]).toEqual([2, 1]);

      source.set(2);
      expect(() => value.get()).toThrow(failure);
      expect([runs, equalityRuns]).toEqual([3, 2]);
    });

    it("does not propagate a computed equality match to its downstream readers", () => {
      const source = new Signal.State(0);
      let innerRuns = 0;
      let outerRuns = 0;
      const inner = new Signal.Computed(() => {
        innerRuns += 1;
        return Math.floor(source.get() / 2);
      });
      const outer = new Signal.Computed(() => {
        outerRuns += 1;
        return inner.get() + 1;
      });

      expect(outer.get()).toBe(1);
      source.set(1);
      expect(outer.get()).toBe(1);
      expect([innerRuns, outerRuns]).toEqual([2, 1]);

      source.set(2);
      expect(outer.get()).toBe(2);
      expect([innerRuns, outerRuns]).toEqual([3, 2]);
    });

    it("caches a computation failure and recovers after its dependency changes", () => {
      const source = new Signal.State(0);
      const failure = new Error("not ready");
      let runs = 0;
      const value = new Signal.Computed(() => {
        runs += 1;
        if (source.get() === 0) throw failure;
        return source.get() * 2;
      });

      expect(() => value.get()).toThrow(failure);
      expect(() => value.get()).toThrow(failure);
      expect(runs).toBe(1);
      source.set(2);
      expect(value.get()).toBe(4);
      expect(runs).toBe(2);
    });

    it("allows a computed callback to write State without making the write asynchronous", () => {
      const source = new Signal.State(0);
      let runs = 0;
      const value = new Signal.Computed(() => {
        runs += 1;
        const current = source.get();
        if (current === 0) source.set(1);
        return source.get();
      });

      expect(value.get()).toBe(1);
      expect(source.get()).toBe(1);
      expect(runs).toBe(1);
      expect(value.get()).toBe(1);
      expect(runs).toBe(1);
    });

    it("rejects thenable results as the Aeolia synchronous-computation extension", () => {
      let thenReads = 0;
      let thenCalls = 0;
      const thenable = {
        // oxlint-disable-next-line unicorn/no-thenable
        get then(): () => void {
          thenReads += 1;
          return () => {
            thenCalls += 1;
          };
        },
      };
      const value = new Signal.Computed(() => thenable);

      const error = thrownBy(() => value.get());
      expect(error).toBeInstanceOf(Fault);
      expect((error as Fault).kind).toBe("async-compute");
      expect(thenReads).toBe(1);
      expect(thenCalls).toBe(0);
    });
  });

  describe("tracking and graph introspection", () => {
    it("supports untrack and restores the current computation after a throw", () => {
      const tracked = new Signal.State(1);
      const untracked = new Signal.State(10);
      const failure = new Error("untrack callback failed");
      let value!: Signal.Computed<number>;
      let seenCurrent: unknown;
      let currentAfterThrow: unknown;

      value = new Signal.Computed(() => {
        seenCurrent = Signal.subtle.currentComputed();
        expect(() =>
          Signal.subtle.untrack(() => {
            untracked.get();
            throw failure;
          }),
        ).toThrow(failure);
        currentAfterThrow = Signal.subtle.currentComputed();
        return tracked.get() + Signal.subtle.untrack(() => untracked.get());
      });

      expect(Signal.subtle.currentComputed()).toBeNull();
      expect(value.get()).toBe(11);
      expect(seenCurrent).toBe(value);
      expect(currentAfterThrow).toBe(value);
      untracked.set(20);
      expect(value.get()).toBe(11);
      tracked.set(2);
      expect(value.get()).toBe(22);
    });

    it("reports ordered immediate sources and live sinks", () => {
      const first = new Signal.State(1);
      const second = new Signal.State(2);
      const value = new Signal.Computed(() => {
        second.get();
        first.get();
        second.get();
        return first.get() + second.get();
      });

      expect(Signal.subtle.hasSources(value)).toBe(false);
      expect(Signal.subtle.introspectSources(value)).toEqual([]);
      expect(Signal.subtle.hasSinks(first)).toBe(false);

      expect(value.get()).toBe(3);
      expect(Signal.subtle.hasSources(value)).toBe(true);
      expect(Signal.subtle.introspectSources(value)).toEqual([second, first]);
      expect(Signal.subtle.introspectSources(value)).not.toBe(
        Signal.subtle.introspectSources(value),
      );

      const watcher = new Signal.subtle.Watcher(() => undefined);
      watcher.watch(value);
      expect(Signal.subtle.hasSinks(value)).toBe(true);
      expect(Signal.subtle.hasSinks(first)).toBe(true);
      expect(Signal.subtle.hasSinks(second)).toBe(true);
      expect(Signal.subtle.introspectSinks(first)).toEqual([value]);
      expect(Signal.subtle.introspectSinks(value)).toEqual([watcher]);
      expect(Signal.subtle.introspectSources(watcher)).toEqual([value]);

      watcher.unwatch(value);
      expect(Signal.subtle.hasSinks(first)).toBe(false);
      expect(Signal.subtle.hasSinks(second)).toBe(false);
    });

    it("rejects values outside each subtle introspection domain", () => {
      const state = new Signal.State(1);
      const watcher = new Signal.subtle.Watcher(() => undefined);

      expect(() => Signal.subtle.introspectSources(state as never)).toThrow(TypeError);
      expect(() => Signal.subtle.hasSources(state as never)).toThrow(TypeError);
      expect(() => Signal.subtle.introspectSinks(watcher as never)).toThrow(TypeError);
      expect(() => Signal.subtle.hasSinks(watcher as never)).toThrow(TypeError);
    });

    it("does not evaluate an uninitialized computed merely because it is watched", () => {
      const source = new Signal.State(1);
      let runs = 0;
      const value = new Signal.Computed(() => {
        runs += 1;
        return source.get() * 2;
      });
      let notifications = 0;
      const watcher = new Signal.subtle.Watcher(() => {
        notifications += 1;
      });

      watcher.watch(value);
      expect(runs).toBe(0);
      expect(Signal.subtle.introspectSources(value)).toEqual([]);
      expect(notifications).toBe(0);

      source.set(2);
      expect(notifications).toBe(0);
      expect(value.get()).toBe(4);
      expect(runs).toBe(1);
      watcher.watch();
      source.set(3);
      expect(notifications).toBe(1);
      expect(value.get()).toBe(6);
      expect(runs).toBe(2);
    });
  });

  describe("liveness hooks", () => {
    it("fires watched and unwatched only on first and last live descendant", () => {
      const events: string[] = [];
      const watchedReceiver = { value: undefined as unknown };
      const unwatchedReceiver = { value: undefined as unknown };
      const state = new Signal.State(1, {
        [Signal.subtle.watched]: function (this: unknown): void {
          watchedReceiver.value = this;
          events.push("watched");
        },
        [Signal.subtle.unwatched]: function (this: unknown): void {
          unwatchedReceiver.value = this;
          events.push("unwatched");
        },
      });
      const derived = new Signal.Computed(() => state.get());
      const first = new Signal.subtle.Watcher(() => undefined);
      const second = new Signal.subtle.Watcher(() => undefined);

      derived.get();
      expect(events).toEqual([]);
      first.watch(derived);
      expect(events).toEqual(["watched"]);
      second.watch(derived);
      expect(events).toEqual(["watched"]);
      second.unwatch(derived);
      expect(events).toEqual(["watched"]);
      first.unwatch(derived);
      expect(events).toEqual(["watched", "unwatched"]);
      expect(watchedReceiver.value).toBe(state);
      expect(unwatchedReceiver.value).toBe(state);
    });

    it("fires lifecycle hooks for a Computed and binds them to that instance", () => {
      const source = new Signal.State(1);
      const events: string[] = [];
      const watchedReceiver = { value: undefined as unknown };
      const unwatchedReceiver = { value: undefined as unknown };
      const value = new Signal.Computed(() => source.get(), {
        [Signal.subtle.watched]: function (this: unknown): void {
          watchedReceiver.value = this;
          events.push("watched");
        },
        [Signal.subtle.unwatched]: function (this: unknown): void {
          unwatchedReceiver.value = this;
          events.push("unwatched");
        },
      });
      const watcher = new Signal.subtle.Watcher(() => undefined);

      value.get();
      watcher.watch(value);
      watcher.unwatch(value);

      expect(events).toEqual(["watched", "unwatched"]);
      expect(watchedReceiver.value).toBe(value);
      expect(unwatchedReceiver.value).toBe(value);
    });

    it("throws a watched-hook failure without rolling back the live edge", () => {
      const failure = new Error("watched hook failed");
      const state = new Signal.State(1, {
        [Signal.subtle.watched]: () => {
          throw failure;
        },
      });
      const watcher = new Signal.subtle.Watcher(() => undefined);

      expect(() => watcher.watch(state)).toThrow(failure);
      expect(Signal.subtle.hasSinks(state)).toBe(true);
      expect(Signal.subtle.introspectSinks(state)).toEqual([watcher]);

      expect(() => watcher.unwatch(state)).not.toThrow();
      expect(Signal.subtle.hasSinks(state)).toBe(false);
    });

    it("throws an unwatched-hook failure after removing the live edge", () => {
      const failure = new Error("unwatched hook failed");
      const state = new Signal.State(1, {
        [Signal.subtle.unwatched]: () => {
          throw failure;
        },
      });
      const watcher = new Signal.subtle.Watcher(() => undefined);
      watcher.watch(state);

      expect(() => watcher.unwatch(state)).toThrow(failure);
      expect(Signal.subtle.hasSinks(state)).toBe(false);
      expect(Signal.subtle.introspectSinks(state)).toEqual([]);
      expect(Signal.subtle.introspectSources(watcher)).toEqual([]);
      expect(() => watcher.unwatch(state)).not.toThrow();
    });

    it("runs cascading Computed and State lifecycle hooks before aggregating failures", () => {
      const computedFailure = new Error("computed watched hook failed");
      const stateFailure = new Error("state watched hook failed");
      let computedCalls = 0;
      let stateCalls = 0;
      const state = new Signal.State(1, {
        [Signal.subtle.watched]: () => {
          stateCalls += 1;
          throw stateFailure;
        },
      });
      const value = new Signal.Computed(() => state.get(), {
        [Signal.subtle.watched]: () => {
          computedCalls += 1;
          throw computedFailure;
        },
      });
      const watcher = new Signal.subtle.Watcher(() => undefined);

      expect(value.get()).toBe(1);
      const error = thrownBy(() => watcher.watch(value));
      expect(error).toBeInstanceOf(AggregateError);
      expect((error as AggregateError).errors).toEqual(
        expect.arrayContaining([computedFailure, stateFailure]),
      );
      expect(computedCalls).toBe(1);
      expect(stateCalls).toBe(1);
      expect(Signal.subtle.hasSinks(value)).toBe(true);
      expect(Signal.subtle.hasSinks(state)).toBe(true);

      expect(() => watcher.unwatch(value)).not.toThrow();
      expect(Signal.subtle.hasSinks(value)).toBe(false);
      expect(Signal.subtle.hasSinks(state)).toBe(false);
    });
  });

  describe("Watcher", () => {
    it("notifies once, exposes pending computed signals, and rearms with watch()", () => {
      const source = new Signal.State(1);
      const value = new Signal.Computed(() => source.get() * 2);
      const pending: Array<readonly unknown[]> = [];
      const receiver = { value: undefined as unknown };
      const watcher = new Signal.subtle.Watcher(function (this: unknown): void {
        receiver.value = this;
        pending.push(watcher.getPending());
      });

      expect(value.get()).toBe(2);
      watcher.watch(value);
      source.set(2);
      source.set(3);
      expect(pending).toHaveLength(1);
      expect(pending[0]).toEqual([value]);
      expect(receiver.value).toBe(watcher);
      expect(value.get()).toBe(6);

      watcher.watch();
      source.set(4);
      expect(pending).toHaveLength(2);
      expect(pending[1]).toEqual([value]);
    });

    it("can rearm with no arguments from its own notification", () => {
      const state = new Signal.State(0);
      let notifications = 0;
      const watcher = new Signal.subtle.Watcher(() => {
        notifications += 1;
        watcher.watch();
      });

      watcher.watch(state);
      state.set(1);
      state.set(2);
      expect(notifications).toBe(2);
    });

    it("unwatches a signal and treats repeated valid unwatch calls as no-ops", () => {
      const state = new Signal.State(0);
      let notifications = 0;
      const watcher = new Signal.subtle.Watcher(() => {
        notifications += 1;
      });

      watcher.watch(state);
      watcher.unwatch(state);
      expect(() => watcher.unwatch(state)).not.toThrow();
      state.set(1);
      expect(notifications).toBe(0);
    });

    it("validates every watch argument before attaching any source", () => {
      const state = new Signal.State(0);
      const watcher = new Signal.subtle.Watcher(() => undefined);

      expect(() => watcher.watch(state, {} as never)).toThrow(TypeError);
      expect(Signal.subtle.introspectSources(watcher)).toEqual([]);
      expect(Signal.subtle.hasSinks(state)).toBe(false);

      watcher.watch(state);
      expect(() => watcher.unwatch(state, {} as never)).toThrow(TypeError);
      expect(Signal.subtle.introspectSources(watcher)).toEqual([state]);
      expect(Signal.subtle.hasSinks(state)).toBe(true);
    });

    it("keeps notifying all watchers and aggregates notify failures", () => {
      const state = new Signal.State(0);
      const firstFailure = new Error("first notify");
      const secondFailure = new Error("second notify");
      let firstCalls = 0;
      let secondCalls = 0;
      const first = new Signal.subtle.Watcher(() => {
        firstCalls += 1;
        throw firstFailure;
      });
      const second = new Signal.subtle.Watcher(() => {
        secondCalls += 1;
        throw secondFailure;
      });
      first.watch(state);
      second.watch(state);

      const error = thrownBy(() => state.set(1));
      expect(firstCalls).toBe(1);
      expect(secondCalls).toBe(1);
      expect(error).toBeInstanceOf(AggregateError);
      expect((error as AggregateError).errors).toEqual(
        expect.arrayContaining([firstFailure, secondFailure]),
      );
      expect(state.get()).toBe(1);
    });

    it("forbids reads and writes while notify is frozen", () => {
      const state = new Signal.State(0);
      const readWatcher = new Signal.subtle.Watcher(() => {
        state.get();
      });
      readWatcher.watch(state);
      expect(() => state.set(1)).toThrow();
      readWatcher.unwatch(state);

      const writeWatcher = new Signal.subtle.Watcher(() => {
        state.set(2);
      });
      writeWatcher.watch(state);
      expect(() => state.set(3)).toThrow();
      writeWatcher.unwatch(state);
      expect(() => state.set(4)).not.toThrow();
      expect(state.get()).toBe(4);
    });

    it("does not let untrack bypass the frozen notification context", () => {
      const state = new Signal.State(0);
      const readWatcher = new Signal.subtle.Watcher(() => {
        Signal.subtle.untrack(() => state.get());
      });
      readWatcher.watch(state);
      expect(() => state.set(1)).toThrow();
      readWatcher.unwatch(state);

      const writeWatcher = new Signal.subtle.Watcher(() => {
        Signal.subtle.untrack(() => state.set(2));
      });
      writeWatcher.watch(state);
      expect(() => state.set(3)).toThrow();
      writeWatcher.unwatch(state);
    });

    it("does not allow graph mutations while notify is frozen", () => {
      const state = new Signal.State(0);
      let watchFailure: unknown;
      let unwatchFailure: unknown;
      const watcher = new Signal.subtle.Watcher(() => {
        watchFailure = thrownBy(() => watcher.watch(state));
        unwatchFailure = thrownBy(() => watcher.unwatch(state));
      });

      watcher.watch(state);
      expect(() => state.set(1)).not.toThrow();
      expect(watchFailure).toBeInstanceOf(Error);
      expect(unwatchFailure).toBeInstanceOf(Error);
      watcher.unwatch(state);
    });
  });

  describe("cycles", () => {
    it("detects a cycle without hanging and leaves the graph usable", () => {
      let self!: Signal.Computed<number>;
      self = new Signal.Computed(() => self.get());
      expect(() => self.get()).toThrow();

      let first!: Signal.Computed<number>;
      let second!: Signal.Computed<number>;
      first = new Signal.Computed(() => second.get());
      second = new Signal.Computed(() => first.get());

      expect(() => first.get()).toThrow();

      const state = new Signal.State(1);
      const recovered = new Signal.Computed(() => state.get() * 2);
      expect(recovered.get()).toBe(2);
      state.set(2);
      expect(recovered.get()).toBe(4);
    });
  });
});
