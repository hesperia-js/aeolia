import type { Store } from "./contract/types.ts";
import type { Unsubscribe } from "./fault.ts";
import { subscribe as subscribeReadable } from "./reactive.ts";
import type { Readable } from "./reactive.ts";
import type { Projection } from "./contract/projection.ts";
import { isObject } from "./utils.ts";

/** @internal Private dispatch key installed on graph stores. */
export const storeSubscribe = Symbol("aeolia.store-subscribe");

function storeSubscribeOf(
  source: unknown,
): ((observer: (value: unknown) => void) => Unsubscribe) | undefined {
  if (!isObject(source)) return undefined;
  const candidate = (source as Record<typeof storeSubscribe, unknown>)[storeSubscribe];
  return typeof candidate === "function"
    ? (candidate as (observer: (value: unknown) => void) => Unsubscribe)
    : undefined;
}

/**
 * Subscribes to a graph store, projection, or reactive readable.
 *
 * Store subscriptions deliver only present store values. A store without a
 * committed value, including one whose raw value is `undefined`, is absent
 * until an explicit initial value, prediction, direct write, query landing,
 * stream emission, or snapshot adoption establishes presence. A real
 * `undefined` landing is delivered once even when the committed signal's
 * equality considers its raw value unchanged. Projection subscriptions
 * observe their `value` readable directly, so their
 * initial seed and later stream-emission behavior are unchanged. Readable
 * subscriptions retain the existing eager `subscribe` behavior.
 *
 * @param source - Graph store, projection, or reactive readable to observe.
 * @param observer - Synchronous callback receiving each present store value or
 * every readable value according to the source kind.
 * @returns An idempotent handle that removes the subscription.
 * @throws {TypeError} If `source` is neither an Aeolia store nor a readable,
 * or if `observer` is not callable.
 * @throws Store lifecycle and callback failures synchronously, following the
 * underlying subscription behavior.
 *
 * @example
 * ```ts
 * import { createGraph, subscribe } from "aeolia";
 *
 * declare const graph: ReturnType<typeof createGraph>;
 * const store = graph.at("count");
 * const stop = subscribe(store, (count) => console.log(count));
 * store.set(1);
 * stop();
 * ```
 */
export function subscribe<T>(source: Store<T>, observer: (value: T) => void): Unsubscribe;
export function subscribe<V>(source: Projection<V>, observer: (value: V) => void): Unsubscribe;
export function subscribe<T>(source: Readable<T>, observer: (value: T) => void): Unsubscribe;
export function subscribe<T>(
  source: Store<T> | Projection<T> | Readable<T>,
  observer: (value: T) => void,
): Unsubscribe {
  if (typeof observer !== "function")
    throw new TypeError("Subscription observer must be a function");
  const storeSubscription = storeSubscribeOf(source);
  if (storeSubscription !== undefined) {
    return storeSubscription(observer as (value: unknown) => void);
  }
  if (source !== null && typeof source === "object" && "value" in source) {
    return subscribeReadable((source as Projection<T>).value, observer);
  }
  return subscribeReadable(source as Readable<T>, observer);
}
