import { readableBrand } from "./symbols.ts";
import {
  initializeComputedNode,
  initializeSignalNode,
  initializeWatcher,
  nodeFor,
  readNode,
  writeNode,
  watcherGetPending,
  watcherUnwatch,
  watcherWatch,
  withLifecycleErrors,
} from "./engine.ts";
import type {
  Computed as ComputedShape,
  ComputedOptions,
  Signal,
  SignalOptions,
  WritableSignal,
} from "./types.ts";
/**
 * Writable state signal with synchronous propagation.
 *
 * `State` is the canonical Signals-shaped class. It uses `Object.is` unless
 * an equality function is supplied, and its value is available immediately
 * after construction. It can be subclassed without losing signal identity.
 */
export class State<T> implements WritableSignal<T> {
  /** Nominal marker identifying this object as an Aeolia readable. */
  readonly [readableBrand] = true as const;

  /**
   * Create a writable state signal.
   *
   * Equality and lifecycle callbacks receive this signal as their `this`
   * value. Construction does not notify observers or evaluate any computed.
   *
   * @param initial - Value returned by reads before the first write.
   * @param options - Equality, label, and liveness options.
   */
  constructor(initial: T, options: SignalOptions<T> = {}) {
    initializeSignalNode(this, initial, options);
  }

  /**
   * Read the state and record it as a dependency of the active computed.
   *
   * The read is synchronous. A foreign receiver throws `TypeError`, and a
   * read attempted from a watcher notification throws a `watcher-read`
   * {@link Fault}.
   */
  get(): T {
    return withLifecycleErrors(() => {
      const node = nodeFor(this);
      if (node.kind !== "signal") throw new TypeError("Expected a Signal.State");
      return readNode(node, true);
    });
  }

  /**
   * Read the state without recording a dependency.
   *
   * This is Aeolia's explicit extension to the Signals-shaped surface. It
   * still performs graph validation and cannot bypass notification read
   * restrictions.
   */
  peek(): T {
    return withLifecycleErrors(() => {
      const node = nodeFor(this);
      if (node.kind !== "signal") throw new TypeError("Expected a Signal.State");
      return readNode(node, false);
    });
  }

  /**
   * Set the state synchronously.
   *
   * An equality match suppresses propagation. Otherwise dependent
   * computeds are invalidated and watchers run before `set()` returns. A
   * write made during canonical watcher notification is rejected; a write
   * made by Aeolia's {@link watch} helper is applied immediately and
   * delivered on the next propagation pass.
   */
  set(value: T): void {
    withLifecycleErrors(() => {
      const node = nodeFor(this);
      if (node.kind !== "signal") throw new TypeError("Expected a Signal.State");
      writeNode(node, value);
    });
  }

  /**
   * Compute a next state from the current state and set it synchronously.
   *
   * The updater runs before equality is checked. If the updater or equality
   * callback throws, this state write is aborted.
   *
   * @param next - Function that derives the candidate value from the current
   * state.
   */
  update(next: (current: T) => T): void {
    withLifecycleErrors(() => {
      const node = nodeFor(this);
      if (node.kind !== "signal") throw new TypeError("Expected a Signal.State");
      writeNode(node, next(node.value));
    });
  }
}

/**
 * Lazy, memoized derived signal with dynamic dependency tracking.
 *
 * The computation runs on the first read and again only after a tracked
 * dependency changes. Dependencies are the readables reached through
 * `get()` during the latest run; `peek()` does not add one. Computation and
 * equality failures are cached until a dependency changes, while thenable
 * results are rejected because computations must be synchronous.
 */
export class Computed<T = unknown> implements ComputedShape<T> {
  /** Nominal marker identifying this object as an Aeolia readable. */
  readonly [readableBrand] = true as const;

  /**
   * Create a lazy computed signal.
   *
   * The callback and equality function are invoked with this computed as
   * `this`. Construction does not run the callback. A callback that returns
   * a thenable produces an `async-compute` {@link Fault} when read.
   *
   * @param compute - Synchronous function that derives the value. Its `this`
   * value is this computed instance.
   * @param options - Equality, label, and liveness options.
   */
  constructor(compute: (this: ComputedShape<T>) => T, options: ComputedOptions<T> = {}) {
    initializeComputedNode(this, compute, options);
  }

  /**
   * Read the computed value, evaluating or refreshing it when necessary.
   *
   * The read records this computed as a dependency of its caller. A thrown
   * computation failure is replayed until a dependency invalidates it; a
   * cycle is reported as a `cycle` {@link Fault}.
   */
  get(): T {
    return withLifecycleErrors(() => {
      const node = nodeFor(this);
      if (node.kind !== "computed") throw new TypeError("Expected a Signal.Computed");
      return readNode(node, true);
    });
  }

  /**
   * Read the computed value without recording it as a dependency of the
   * caller.
   *
   * The computed is still evaluated or refreshed when necessary. This method
   * does not bypass graph validation or notification read restrictions.
   */
  peek(): T {
    return withLifecycleErrors(() => {
      const node = nodeFor(this);
      if (node.kind !== "computed") throw new TypeError("Expected a Signal.Computed");
      return readNode(node, false);
    });
  }
}
/**
 * Canonical Signals watcher for one-shot dependency notifications.
 *
 * Attach sources with {@link Signal.subtle.Watcher.watch}. A notification
 * callback runs with this watcher as `this`, is delivered at most once while
 * the watcher is armed, and must be rearmed with `watch()` before the next
 * notification. During notification, reads, writes, and source attachment
 * changes are frozen; the no-argument `watch()` rearm operation is the
 * exception. Use {@link Signal.subtle.Watcher.getPending} from the callback to
 * inspect invalidated computed sources before pulling them.
 */
export class Watcher {
  /**
   * Create a watcher with no sources attached and a notification callback.
   *
   * The callback remains attached until its sources are unwatched. It
   * receives no arguments and is called synchronously by propagation;
   * callback failures are reported back through the write that triggered
   * notification.
   *
   * @param notify - Callback invoked when an attached source invalidates
   * this watcher.
   */
  constructor(notify: (this: Watcher) => void) {
    initializeWatcher(this, notify);
  }

  /**
   * Attach signals and arm this watcher.
   *
   * Existing attachments are retained. Calling `watch()` with no arguments
   * only rearms the existing attachments, which makes it safe to call from
   * the notification callback. Every argument is validated before any new
   * source is attached. A computed is invalidated lazily; merely attaching
   * it does not evaluate it.
   *
   * @throws {TypeError} If any argument is not an Aeolia signal or if the
   * receiver is not a canonical watcher.
   * @throws {Fault} If called with source arguments from a notification
   * callback, or if a lifecycle callback fails while liveness changes.
   *
   * @param signals - Signals to attach. With no arguments, rearm all current
   * attachments.
   */
  watch(...signals: Signal<unknown>[]): void {
    watcherWatch(this, signals);
  }

  /**
   * Detach the supplied signals from this watcher.
   *
   * Repeating a valid unwatch is a no-op. All arguments are validated
   * before any attachment is removed. Calling this method during canonical
   * notification is forbidden, including with no arguments.
   *
   * @throws {TypeError} If any argument is not an Aeolia signal or if the
   * receiver is not a canonical watcher.
   * @throws {Fault} If called during canonical notification, or if a
   * lifecycle callback fails while liveness changes.
   *
   * @param signals - Signals to detach. With no arguments, nothing is
   * detached.
   */
  unwatch(...signals: Signal<unknown>[]): void {
    watcherUnwatch(this, signals);
  }

  /**
   * Return computed sources that are currently marked dirty.
   *
   * The returned array is a snapshot. State signals are never included,
   * and computed sources disappear after they are pulled and become clean.
   * Calling this method does not rearm the watcher or evaluate a computed.
   */
  getPending(): Signal<unknown>[] {
    return watcherGetPending(this);
  }
}
