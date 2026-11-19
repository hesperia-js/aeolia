/**
 * The exact authored logical key of a store.
 *
 * Raw entries use graph-local keys. Query and stream definitions derive them
 * from inputs; declared stores use their `name` as their key. The brand keeps
 * an arbitrary string from being passed where a previously validated key is
 * required. A bound query additionally distinguishes context, declaration and
 * profile; its physical Store.identity is separate from this logical key.
 */
export type StoreKey = string & { readonly __storeKey: unique symbol };

/**
 * Opaque identity of one physical graph store, shared by its caller wrappers.
 * It survives reset and idle collection, and is not a portable snapshot address
 * or authority to access a current store incarnation.
 */
export interface StoreIdentity {
  /** @internal Nominal identity brand; there is no public constructor. */
  readonly __storeIdentity: unique symbol;
}

/** Store-local visible-value ordering, independent of request generations. */
export type ValueRevision = number & { readonly __valueRevision: unique symbol };

/** @internal Reserve the next exact revision before changing native state. */
export function nextValueRevision(current: ValueRevision): ValueRevision {
  if (!Number.isSafeInteger(current) || current < 0 || current >= Number.MAX_SAFE_INTEGER)
    throw new RangeError("Store value revision exhausted");
  return (current + 1) as ValueRevision;
}

/** Checks the non-empty string identity without normalizing it. */
export function isStoreKey(value: unknown): value is StoreKey {
  return typeof value === "string" && value.length > 0;
}

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
 * @throws A {@link TypeError} If `value` is not a non-empty string.
 */
export function storeKey(value: string): StoreKey {
  if (!isStoreKey(value)) {
    throw new TypeError("StoreKey must be a non-empty string");
  }
  return value;
}

/**
 * Lifecycle of one query caller.
 *
 * The value, error, active request, failure state, and invalidation state are
 * shared by callers that resolve to the same key. Each caller has its own
 * freshness window and status cell, so window-based transitions can differ.
 * `empty` has no accepted landing, `fetching` has a request without an
 * accepted landing, `ready` has authoritative state published as fresh (including
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
