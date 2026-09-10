import type { Unsubscribe } from "../fault.ts";
import {
  Computed as ComputedClass,
  State as StateClass,
  Watcher as WatcherClass,
} from "./classes.ts";
import {
  currentComputed as engineCurrentComputed,
  introspectComputedSources,
  introspectSinks as engineIntrospectSinks,
  introspectWatcherSources,
  isComputed as engineIsComputed,
  isState as engineIsState,
  isWatcher as engineIsWatcher,
  subscribeReadable,
  watchReadable,
  untrack as engineUntrack,
} from "./engine.ts";
import { unwatched, watched } from "./symbols.ts";
import type {
  Computed,
  ComputedOptions,
  LifecycleCallback,
  Readable,
  Signal as SignalShape,
  SignalOptions,
  WritableSignal,
} from "./types.ts";

const watchedSymbol: typeof watched = watched;
const unwatchedSymbol: typeof unwatched = unwatched;

export { readableBrand } from "./symbols.ts";
export type {
  Computed,
  ComputedOptions,
  LifecycleCallback,
  Readable,
  SignalOptions,
  WritableSignal,
} from "./types.ts";

/**
 * Readable signal surface shared by {@link Signal.State} and
 * {@link Signal.Computed}.
 *
 * The classes are constructible and nominally validated at runtime; a
 * lookalike object with `get()` and `peek()` methods is not treated as an
 * Aeolia signal.
 */
export interface Signal<T> extends SignalShape<T> {}

/** Canonical signal classes and the low-level Signals-compatible utilities. */
export namespace Signal {
  /**
   * Options shared by canonical state and computed signals.
   *
   * Equality is called with the signal instance as `this` and defaults to
   * `Object.is`. Lifecycle callbacks run when the signal becomes live or
   * stops being live.
   */
  export interface SignalOptions<T> extends SignalOptionsShape<T> {
    /** Compare the previous and candidate values; return `true` to suppress a change. */
    readonly equals?: (this: Signal<T>, a: NoInfer<T>, b: NoInfer<T>) => boolean;

    /** Optional diagnostic name used in faults involving this signal. */
    readonly label?: string;

    /** Run when this signal gains its first live descendant. */
    readonly [watchedSymbol]?: LifecycleCallback<T>;

    /** Run after this signal loses its last live descendant. */
    readonly [unwatchedSymbol]?: LifecycleCallback<T>;
  }

  /**
   * Writable state signal with synchronous propagation.
   *
   * `State` is the canonical Signals-shaped class. It uses `Object.is` unless
   * an equality function is supplied, and its value is available immediately
   * after construction. It can be subclassed without losing signal identity.
   */
  export const State = StateClass;
  export type State<T> = StateClass<T>;

  /**
   * Lazy, memoized derived signal with dynamic dependency tracking.
   *
   * The computation runs on the first read and again only after a tracked
   * dependency changes. Dependencies are the readables reached through
   * `get()` during the latest run; `peek()` does not add one. Computation and
   * equality failures are cached until a dependency changes, while thenable
   * results are rejected because computations must be synchronous.
   */
  export const Computed = ComputedClass;
  export type Computed<T = unknown> = ComputedClass<T>;

  /**
   * Low-level Signals-compatible controls and lifecycle keys.
   *
   * The namespace includes the canonical one-shot
   * {@link Signal.subtle.Watcher}; it is separate from Aeolia's eager
   * {@link watch} helper.
   */
  export namespace subtle {
    /**
     * Option key for a callback that runs when a signal gains its first live
     * descendant.
     *
     * The callback is invoked with the signal instance as `this`. A live
     * descendant can be a computed dependent, a canonical
     * {@link Signal.subtle.Watcher}, or Aeolia's {@link watch} and
     * {@link subscribe} helpers.
     */
    export const watched: typeof watchedSymbol = watchedSymbol;

    /**
     * Option key for a callback that runs after a signal loses its last live
     * descendant.
     *
     * The callback is invoked with the signal instance as `this`. The live
     * edge has already been removed when this callback runs.
     */
    export const unwatched: typeof unwatchedSymbol = unwatchedSymbol;

    /**
     * Run a callback without collecting reactive dependencies.
     *
     * The suppression is scoped to the callback and is restored even when it
     * throws. `untrack` does not bypass the canonical watcher notification
     * freeze: reads and graph writes remain forbidden there.
     */
    export function untrack<T>(run: () => T): T {
      return engineUntrack(run);
    }

    /**
     * Return the computed currently evaluating on this synchronous call stack.
     *
     * Returns `null` outside a computation and while dependency tracking is
     * suppressed by {@link untrack}. The result is a live object reference,
     * not a snapshot or a newly created wrapper.
     */
    export function currentComputed(): Signal.Computed<unknown> | null {
      return engineCurrentComputed() as Signal.Computed<unknown> | null;
    }

    /**
     * Return a new array containing a readable's immediate sources.
     *
     * For a computed, sources are the distinct readables reached through
     * `get()` during its most recent evaluation, in first-read order. For a
     * watcher, sources are the signals currently attached with `watch()`; no
     * ordering contract is made for that set. This function never evaluates
     * an uninitialized computed.
     *
     * @throws {TypeError} If `source` is neither a computed nor a canonical
     * watcher.
     */
    export function introspectSources(
      source: Signal.Computed<unknown> | Signal.subtle.Watcher,
    ): Signal<unknown>[] {
      // Preserve the original Signals branch: only a Watcher instance enters
      // the watcher table; all other values go through readable validation.
      if (source instanceof WatcherClass) return introspectWatcherSources(source);
      return introspectComputedSources(source);
    }

    /**
     * Return a new array containing a signal's immediate sinks.
     *
     * The result includes dependent computeds and canonical watchers, with
     * no ordering guarantee. It does not include Aeolia's separate
     * {@link watch} and {@link subscribe} helper subscriptions. The array is
     * independent of the graph and can be safely modified by the caller.
     *
     * @throws {TypeError} If `source` is not an Aeolia state or computed
     * signal.
     */
    export function introspectSinks(
      source: Signal.State<unknown> | Signal.Computed<unknown>,
    ): (Signal<unknown> | Signal.subtle.Watcher)[] {
      return engineIntrospectSinks(source);
    }

    /** Return whether a computed or canonical watcher currently has a source. */
    export function hasSources(source: Signal.Computed<unknown> | Signal.subtle.Watcher): boolean {
      return introspectSources(source).length !== 0;
    }

    /** Return whether a state or computed signal currently has a sink. */
    export function hasSinks(source: Signal.State<unknown> | Signal.Computed<unknown>): boolean {
      return introspectSinks(source).length !== 0;
    }

    /**
     * Canonical Signals watcher for one-shot dependency notifications.
     *
     * Attach sources with {@link Signal.subtle.Watcher.watch}. A notification
     * callback runs with this watcher as `this`, is delivered at most once
     * while the watcher is armed, and must be rearmed with `watch()` before the
     * next notification. During notification, reads, writes, and source
     * attachment changes are frozen; the no-argument `watch()` rearm operation
     * is the exception. Use {@link Signal.subtle.Watcher.getPending} from the
     * callback to inspect invalidated computed sources before pulling them.
     */
    export const Watcher = WatcherClass;
    export type Watcher = WatcherClass;
  }

  /**
   * Test whether a value is an Aeolia {@link Signal.State} instance.
   *
   * The check uses Aeolia's runtime identity and rejects structural lookalikes
   * and computed signals.
   */
  export function isState(value: unknown): value is StateClass<unknown> {
    return engineIsState(value);
  }

  /**
   * Test whether a value is an Aeolia {@link Signal.Computed} instance.
   *
   * The check uses Aeolia's runtime identity and rejects structural lookalikes
   * and state signals.
   */
  export function isComputed(value: unknown): value is ComputedClass<unknown> {
    return engineIsComputed(value);
  }

  /**
   * Test whether a value is an Aeolia {@link Signal.subtle.Watcher} instance.
   *
   * Structural objects with matching methods are not accepted.
   */
  export function isWatcher(value: unknown): value is Signal.subtle.Watcher {
    return engineIsWatcher(value);
  }
}

type SignalOptionsShape<T> = SignalOptions<T>;

/**
 * Create a writable state signal.
 *
 * This convenience function has the same semantics as `new Signal.State`:
 * writes and watcher propagation are synchronous, equality defaults to
 * `Object.is`, and lifecycle options are honored. Use `Signal.State` when a
 * constructible class or subclass is required.
 *
 * @param initial - Initial value returned by the signal.
 * @param options - Equality, label, and liveness options.
 * @returns A writable signal with the supplied initial value.
 *
 * @example
 * ```ts
 * import { computed, signal } from "aeolia";
 *
 * const count = signal(0);
 * const doubled = computed(() => count.get() * 2);
 * count.set(2);
 * doubled.get(); // 4
 * ```
 */
export function signal<T>(initial: T, options: SignalOptions<T> = {}): WritableSignal<T> {
  return new StateClass(initial, options);
}

/**
 * Create a lazy, memoized computed signal.
 *
 * The callback runs on the first read and tracks only `get()` calls made during
 * that run. It must return synchronously; returning a thenable raises an
 * `async-compute` {@link Fault} when the computed is read.
 *
 * @param compute - Synchronous derivation callback. `get()` calls inside it
 * become dependencies; `peek()` calls do not.
 * @param options - Equality, label, and liveness options.
 * @returns A lazy computed signal.
 */
export function computed<T>(
  compute: (this: Signal.Computed<T>) => T,
  options: ComputedOptions<T> = {},
): Computed<T> {
  return new ComputedClass(compute, options);
}

/**
 * Subscribe an observer to a readable using Aeolia's eager convenience watcher.
 *
 * A computed source is evaluated immediately to establish its initial
 * dependencies. On later synchronous propagation the observer receives no
 * arguments and runs once for each relevant pass. Unlike the canonical
 * {@link Signal.subtle.Watcher}, this helper permits writes; writes made by the
 * observer are applied immediately and notified on the next propagation pass.
 * Registering a watch while a computed is evaluating does not make that
 * computed depend on the watched source; the registration only establishes the
 * watcher's own subscription.
 * Reads from the observer are forbidden and produce a `watcher-read` fault.
 * Sibling observer order is deliberately unspecified.
 *
 * The returned unsubscribe function is idempotent. If initial computed
 * evaluation fails, no subscription is installed. If the observer throws,
 * other observers still run and the triggering write rethrows the failure,
 * aggregating multiple failures when necessary.
 *
 * @param source - Readable to observe.
 * @param observer - Synchronous callback invoked after the source changes.
 * @returns A handle that removes this observer when called.
 *
 * @example
 * ```ts
 * import { signal, watch } from "aeolia";
 *
 * const count = signal(0);
 * const stop = watch(count, () => console.log("count changed"));
 * count.set(1);
 * stop();
 * ```
 *
 * @throws {TypeError} If `source` is not an Aeolia readable. A non-callable
 * observer fails with `TypeError` when propagation first invokes it.
 * @throws The original error if attaching the observer requires an initial
 * computed evaluation or liveness transition that fails. Failures from later
 * observer delivery are thrown by the write that triggered that delivery;
 * Aeolia-specific read, write, and lifecycle violations use {@link Fault}.
 */
export function watch(source: Readable<unknown>, observer: () => void): Unsubscribe {
  return watchReadable(source, observer);
}

/**
 * Subscribe to a readable and receive its current value immediately.
 *
 * The initial callback and every later callback run synchronously. Later
 * callbacks run after the propagation notification phase has finished and
 * before the write that caused the change returns. A computed source is
 * pulled before delivery; if its own equality suppresses the change, no
 * callback is made. With the default `Object.is` equality this also suppresses
 * equal computed values. A callback may read readables safely and may write;
 * writes are queued as later propagation passes, so a callback never recurses
 * through its own write.
 *
 * Registration and the initial callback do not add dependencies to an
 * enclosing computed. If initial evaluation or delivery throws, no live
 * subscription is retained. The returned handle is idempotent, and calling it
 * before a queued delivery suppresses that delivery. Observer failures are
 * rethrown by the triggering write after all reached observers have had a
 * chance to run; multiple failures are reported as an `AggregateError`.
 *
 * @param source - Aeolia readable whose settled values should be delivered.
 * @param observer - Synchronous callback receiving the current or changed value.
 * @returns An idempotent handle that removes this subscription.
 * @throws {TypeError} If `source` is not an Aeolia readable or `observer` is
 * not callable.
 * @throws The initial evaluation or callback failure synchronously. Later
 * callback failures are thrown by the write that triggered them.
 *
 * @example
 * ```ts
 * import { signal, subscribe } from "aeolia";
 *
 * const count = signal(0);
 * const stop = subscribe(count, (value) => console.log(value)); // 0
 * count.set(1); // 1
 * stop();
 * ```
 */
export function subscribe<T>(source: Readable<T>, observer: (value: T) => void): Unsubscribe {
  return subscribeReadable(source, observer);
}
