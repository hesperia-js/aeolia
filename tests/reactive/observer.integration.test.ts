import { describe, expect, it } from "bun:test";
import {
  Fault,
  afterPropagation,
  computed,
  createObserver,
  signal,
  withoutWrites,
} from "../../src/index.ts";

describe("framework-controlled observers", () => {
  it("tracks dynamic dependencies and checks them after notification", () => {
    const branch = signal(true);
    const first = signal(0);
    const second = signal(0);
    let notifications = 0;
    let executions = 0;
    const observer = createObserver({
      notify() {
        notifications += 1;
        afterPropagation(() => {
          if (observer.check()) {
            observer.track(() => {
              executions += 1;
              return branch.get() ? first.get() : second.get();
            });
          }
        });
      },
    });

    observer.track(() => {
      executions += 1;
      return branch.get() ? first.get() : second.get();
    });
    expect(executions).toBe(1);

    first.set(1);
    expect(notifications).toBe(1);
    expect(executions).toBe(2);

    branch.set(false);
    expect(notifications).toBe(2);
    expect(executions).toBe(3);
    first.set(2);
    expect(notifications).toBe(2);
    second.set(1);
    expect(notifications).toBe(3);
    expect(executions).toBe(4);
    observer.dispose();
  });

  it("preserves an invalidation caused while the initial tracking callback writes", () => {
    const source = signal(0);
    let notifications = 0;
    let executions = 0;
    const observer = createObserver({
      notify() {
        notifications += 1;
        afterPropagation(() => {
          if (observer.check()) {
            observer.track(() => {
              executions += 1;
              return source.get();
            });
          }
        });
      },
    });

    observer.track(() => {
      executions += 1;
      const value = source.get();
      if (value === 0) source.set(1);
      return value;
    });

    expect(source.get()).toBe(1);
    expect(notifications).toBe(1);
    expect(executions).toBe(2);
    observer.dispose();
  });

  it("uses computed change tokens so equal derived values do not rerun work", () => {
    const source = signal({ value: 0 });
    const derived = computed(() => source.get().value, { equals: Object.is });
    let notifications = 0;
    let executions = 0;
    const observer = createObserver({
      notify() {
        notifications += 1;
        afterPropagation(() => {
          if (observer.check()) {
            observer.track(() => {
              executions += 1;
              return derived.get();
            });
          }
        });
      },
    });
    observer.track(() => {
      executions += 1;
      return derived.get();
    });

    source.set({ value: 0 });
    expect(notifications).toBe(1);
    expect(executions).toBe(1);
    source.set({ value: 1 });
    expect(notifications).toBe(2);
    expect(executions).toBe(2);
    observer.dispose();
  });

  it("freezes notification callbacks and makes disposal terminal", () => {
    const source = signal(0);
    let observer!: ReturnType<typeof createObserver>;
    observer = createObserver({
      notify() {
        expect(() => source.get()).toThrow(Fault);
        expect(() => source.set(2)).toThrow(Fault);
      },
    });
    observer.track(() => source.get());
    source.set(1);
    observer.dispose();
    observer.dispose();
    expect(observer.check()).toBe(false);
    expect(() => observer.track(() => source.get())).toThrow("disposed");
    source.set(3);
  });

  it("forbids raw writes, including equal writes, only within nested synchronous scopes", () => {
    const source = signal(1);
    expect(() =>
      withoutWrites(() => {
        expect(source.get()).toBe(1);
        expect(() => source.set(1)).toThrow(Fault);
        withoutWrites(() => {
          expect(() => source.update((value) => value)).toThrow(Fault);
        });
      }),
    ).not.toThrow();
    expect(() =>
      withoutWrites(() => {
        throw new Error("cleanup failed");
      }),
    ).toThrow("cleanup failed");
    expect(() => source.set(2)).not.toThrow();
    expect(source.get()).toBe(2);
  });
});
