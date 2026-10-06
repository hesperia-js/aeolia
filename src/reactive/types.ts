import { readableBrand, unwatched, watched } from "./symbols.ts";
import { isObject } from "../utils.ts";

/**
 * Minimal read-only surface shared by Aeolia state and computed signals.
 *
 * Both read methods are synchronous. `get()` participates in dependency
 * tracking; `peek()` deliberately does not.
 */
export interface Readable<T> {
  /** Nominal marker carried by Aeolia-created readable objects. */
  readonly [readableBrand]: true;

  /**
   * Read the current value and record this readable as a dependency of the
   * currently evaluating computed, if there is one.
   *
   * Reads are synchronous. Reading a computed may evaluate it lazily and may
   * rethrow a previously cached computation failure. Reads made from a
   * watcher notification are rejected with a `watcher-read` fault.
   */
  get(): T;

  /**
   * Read the current value without recording a reactive dependency.
   *
   * `peek()` still participates in graph validation and retention bookkeeping,
   * and it does not bypass the read restriction during watcher notification.
   */
  peek(): T;
}

/** A lifecycle callback invoked with its signal as `this`. */
export type LifecycleCallback<T> = (this: Signal<T>) => void;

/**
 * Options shared by state and computed signals.
 *
 * Equality controls whether a new value is considered a change. It is called
 * with the signal instance as `this` and defaults to {@link Object.is}.
 * Lifecycle callbacks run when the signal becomes live or stops being live;
 * their symbol keys are {@link Signal.subtle.watched} and
 * {@link Signal.subtle.unwatched}.
 */
export interface SignalOptions<T> {
  /** Compare the previous and candidate values; return `true` to suppress a change. */
  readonly equals?: (this: Signal<T>, a: NoInfer<T>, b: NoInfer<T>) => boolean;

  /** Optional diagnostic name used in faults involving this signal. */
  readonly label?: string;

  /** Run when this signal gains its first live descendant. */
  readonly [watched]?: LifecycleCallback<T>;

  /** Run after this signal loses its last live descendant. */
  readonly [unwatched]?: LifecycleCallback<T>;
}

/**
 * Check whether a value contains valid signal options.
 *
 * The check validates the declared option fields. It cannot validate the
 * types of values passed through `equals` or the behavior of callbacks.
 *
 * @param value - Value to check.
 * @returns `true` when the value is a non-array object whose declared fields
 * are absent or have their required runtime types.
 */
export function isSignalOptions(value: unknown): value is SignalOptions<unknown> {
  if (!isObject(value) || typeof value === "function" || Array.isArray(value)) return false;

  const options = value as Record<PropertyKey, unknown>;
  return (
    (options.equals === undefined || typeof options.equals === "function") &&
    (options.label === undefined || typeof options.label === "string") &&
    (options[watched] === undefined || typeof options[watched] === "function") &&
    (options[unwatched] === undefined || typeof options[unwatched] === "function")
  );
}

/**
 * Readable signal surface shared by {@link Signal.State} and
 * {@link Signal.Computed}.
 *
 * The classes are constructible and nominally validated at runtime; a
 * lookalike object with `get()` and `peek()` methods is not treated as an
 * Aeolia signal.
 */
export interface Signal<T> extends Readable<T> {}

/** A writable signal whose value changes synchronously when written. */
export interface WritableSignal<T> extends Signal<T> {
  /** Set a new value, propagating synchronously when it is not equal. */
  set(value: T): void;

  /** Derive and set a value from the signal's current value. */
  update(next: (current: T) => T): void;
}

/** The readable shape implemented by {@link Signal.Computed}. */
export interface Computed<T = unknown> extends Signal<T> {}

/** Options accepted by {@link computed} and {@link Signal.Computed}. */
export interface ComputedOptions<T> extends SignalOptions<T> {}

/**
 * Notification options for a framework-controlled observer.
 *
 * The notification is an invalidation signal only. It runs during Aeolia's
 * propagation phase, where reactive reads and writes are forbidden. Use
 * {@link Observer.check} and {@link Observer.track} from an
 * {@link afterPropagation} callback to perform framework work.
 */
export interface ObserverOptions {
  /** Notify the framework that this observer has pending invalidation. */
  readonly notify: (this: Observer) => void;
}

/**
 * A manually controlled dependency observer.
 *
 * `track` evaluates framework work and replaces its dynamic dependencies.
 * `check` refreshes dependency freshness without evaluating that work and
 * returns whether it was invalidated. `dispose` removes all dependencies and
 * makes the observer inert.
 */
export interface Observer {
  /** Evaluate work synchronously and replace the observer's dependencies. */
  track<T>(run: () => T): T;

  /** Check dependency freshness without evaluating the tracked work. */
  check(): boolean;

  /** Dispose the observer; repeated calls are harmless. */
  dispose(): void;
}
