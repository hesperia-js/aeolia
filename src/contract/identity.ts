/**
 * The exact runtime key of a store.
 *
 * Keys are graph-local identities. Query and stream definitions derive them
 * from inputs; declared stores use their `name` as their key. The brand keeps
 * an arbitrary string from being passed where a previously validated key is
 * required.
 */
export type StoreKey = string & { readonly __storeKey: unique symbol };

/**
 * The opaque identifier of the graph that owns a callback invocation.
 *
 * A graph ID is useful for correlating backend work with its graph, but it is
 * not a handle for accessing another graph.
 */
export type GraphId = string & { readonly __graphId: unique symbol };

/**
 * Monotonically increasing ordering token for requests, writes, emissions,
 * and adoption.
 *
 * Starting a request advances the token before that request lands. The graph
 * uses generations to discard a late result after newer work has started or
 * state has been written, emitted, or adopted.
 */
export type Generation = number & { readonly __generation: unique symbol };

/**
 * Brands a non-empty string as a store key.
 *
 * @param value - The exact key to use for a graph store.
 * @returns The branded form of `value`; the string is not normalized.
 * @throws A {@link TypeError} If `value` is empty.
 */
export function storeKey(value: string): StoreKey {
  if (value.length === 0) {
    throw new TypeError("StoreKey cannot be empty");
  }
  return value as StoreKey;
}

/**
 * Lifecycle of one query caller.
 *
 * The value, error, active request, failure state, and invalidation state are
 * shared by callers that resolve to the same key. Each caller has its own
 * freshness window and status cell, so window-based transitions can differ.
 * `empty` has no committed value, `fetching` has a request without a
 * committed value, `ready` has committed state published as fresh (including
 * the landing turn before its freshness timer runs), `stale` needs a request
 * but has none active, `revalidating` has a committed value with a request
 * active, and `failed` records the latest failed request.
 */
export type StoreStatus = "empty" | "fetching" | "ready" | "stale" | "revalidating" | "failed";

/**
 * Lifecycle of the shared source behind one keyed stream store.
 *
 * `empty` means no source/value is active, `opening` is waiting for the first
 * source emission, `live` has received an emission, `stale` follows a reported gap,
 * `closed` records normal or cancellation completion of the current session;
 * a later member call can open a new session. `failed` records a source or
 * iterator error and likewise permits a later member call to reopen.
 */
export type StreamStatus = "empty" | "opening" | "live" | "stale" | "closed" | "failed";
