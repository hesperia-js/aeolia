import type { Readable } from "../reactive.ts";
import type { GraphId, StoreKey, StoreStatus, StreamStatus } from "./identity.ts";
import type { affectedBrand } from "./symbols.ts";
export type { Readable } from "../reactive.ts";
export type { Generation, GraphId, StoreKey, StoreStatus, StreamStatus } from "./identity.ts";

/**
 * The graph-owned value for one store key.
 *
 * `value` exposes the committed value with active optimistic predictions
 * folded over it. Reading it never starts I/O, but reads and live observers
 * participate in the graph's retention policy. The public shell keeps its
 * identity if an idle keyed store later releases its contents.
 */
export interface Store<T> {
  /** The graph-local key shared by all views of this store. */
  readonly key: StoreKey;

  /** The graph that owns this store. */
  readonly graph: GraphId;

  /**
   * The visible value, including active predictions over the committed base.
   *
   * The value is `undefined` before a committed value or explicit `initial`
   * configuration, or after collection of an unobserved keyed store. Reading
   * does not initiate a backend call.
   */
  readonly value: Readable<T | undefined>;

  /**
   * Commits a local value and advances the store generation.
   *
   * An in-flight request is allowed to settle, but its older result cannot
   * overwrite this newer generation. Active predictions remain layered over
   * the new committed value.
   *
   * @param value - The value to commit beneath active predictions.
   * @throws A {@link Fault} If the graph or store has been disposed.
   * Synchronous observer, equality, or propagation failures may also be
   * rethrown to the caller.
   */
  set(value: T): void;

  /**
   * Computes and commits a value from the committed base.
   *
   * `next` receives the committed value beneath optimistic predictions, not
   * the currently folded value visible through `value`.
   *
   * @param next - Pure update function used to produce the next committed
   * value.
   * @throws A {@link Fault} If the graph or store has been disposed.
   * Synchronous updater, observer, equality, or propagation failures may also
   * be rethrown to the caller.
   */
  update(next: (current: T | undefined) => T): void;
}

/**
 * A query-specific view of a keyed {@link Store}.
 *
 * Multiple calls for one key share the value, error, and active request, but
 * each returned view has its own status and freshness window. It remains a
 * writable store, so `set` and `update` can establish local authoritative
 * state while a request is in flight.
 */
export interface QueryStore<T> extends Store<T> {
  /**
   * Resolves with the committed query value when this caller is ready.
   *
   * The getter is lazy and does not fetch or revalidate. It waits until this
   * caller's `pending` is `false` and `status` is `"ready"`, so a stale value
   * and a value during revalidation do not resolve early. A committed
   * `undefined` is a valid result; active optimistic predictions are not
   * included. A `"failed"` status rejects with the same reason exposed by
   * `error`, including when an older committed value remains visible.
   * An active wait keeps this caller's status and pending readables live, so
   * collection cannot discard it. If a handle is used after an earlier
   * collection, the getter reactivates that caller without fetching; a later
   * explicit member call, local write, or adoption can then update its status.
   * Accessing this property on a disposed store throws its `disposed` fault
   * synchronously, like the other store readables; asynchronous readiness
   * failures reject the returned promise.
   *
   * @returns A promise for the committed query value.
   */
  readonly ready: Promise<T>;

  /** Whether this caller currently has a fetching or revalidating request. */
  readonly pending: Readable<boolean>;

  /** The latest shared query failure, or `undefined` when none is recorded. */
  readonly error: Readable<unknown>;

  /** The lifecycle status for this query caller. */
  readonly status: Readable<StoreStatus>;

  /**
   * Starts a fresh request for this caller's input and key.
   *
   * Revalidation is explicit: reading or watching a value does not start a
   * request. A call may reactivate the stable shell of a store whose contents
   * were collected. When `options.abortSignal` is omitted, the signal from the
   * original member call is reused. The returned promise resolves after the
   * shared request is processed; backend rejection is exposed through `error`
   * and `status`, not as a rejection of this promise. A late or superseded
   * result may be ignored by the graph.
   *
   * @param options - Optional caller-owned cancellation signal.
   * @returns A promise that resolves after request settlement processing.
   * @throws A {@link Fault} If the graph or store has been disposed.
   * Synchronous status or propagation failures may also be rethrown while the
   * request is being started.
   */
  revalidate(options?: { readonly abortSignal?: AbortSignal }): Promise<void>;
}

/**
 * A view of the latest value produced by a keyed stream.
 *
 * The source is shared by callers resolving to the same key. The first caller
 * opens the source and owns the source-level cancellation signal; later
 * callers join that session. Stream stores are writable through the inherited
 * `set` and `update` methods, but those writes do not emit into the source.
 */
export interface StreamStore<T> extends Store<T> {
  /**
   * Whether the current shared session is waiting for its first emission.
   *
   * An initial, adopted, or previously emitted value does not end this wait.
   * Gaps before the session's first emission are ignored. Closing or failing
   * the session ends the wait even if it delivered no value.
   */
  readonly pending: Readable<boolean>;

  /** The latest shared stream failure, or `undefined` when none is recorded. */
  readonly error: Readable<unknown>;

  /** The lifecycle status for the shared keyed stream source. */
  readonly status: Readable<StreamStatus>;
}

/**
 * Input used to declare a writable store in a contract.
 *
 * The object is consumed by {@link createStore}; definitions are inert until
 * a graph uses the resulting contract.
 */
export interface CreateStoreInput<T, N extends string> {
  /** The non-empty store name and graph-local key. */
  readonly name: N;

  /** The value used when the declared store is first materialized. */
  readonly initial: T;

  /**
   * Equality used to suppress equivalent committed and visible updates.
   *
   * @defaultValue `Object.is`
   */
  readonly equals?: (a: NoInfer<T>, b: NoInfer<T>) => boolean;

  /**
   * Whether committed values are included in realm snapshots.
   *
   * @defaultValue `false`
   */
  readonly snapshot?: boolean;
}

/**
 * An immutable declaration of a writable store.
 *
 * `createStore` shallow-freezes the returned definition. The declaration does
 * not create graph state until a graph materializes it.
 */
export interface StoreDefinition<T, N extends string = string> extends CreateStoreInput<T, N> {
  /** Discriminator identifying this declaration as a store. */
  readonly kind: "store";
}

/**
 * Value identity, snapshot, and cancellation options for one query or stream
 * member call.
 *
 * The first explicit value configuration for a key becomes that store's
 * configuration. Later calls for the same key must not contradict it.
 */
export interface ValueOptions<T> {
  /**
   * Caller-owned cancellation signal.
   *
   * For a query, aborting this signal aborts that request when it is the
   * request's signal. For a stream, the first opener's signal owns the shared
   * session; a later joiner cannot replace it.
   */
  readonly abortSignal?: AbortSignal;

  /**
   * Optional initial committed value for a keyed query or stream store.
   *
   * The first explicit configuration establishes the key's value identity;
   * later incompatible configuration for the same key is a contract fault.
   */
  readonly initial?: T;

  /**
   * Equality used when the keyed store compares committed or visible values.
   *
   * @defaultValue `Object.is`
   */
  readonly equals?: (a: NoInfer<T>, b: NoInfer<T>) => boolean;

  /**
   * Whether committed values for this keyed store are included in snapshots.
   * Defaults to `true` for query and stream stores when no definition or
   * caller supplies a value.
   *
   * @defaultValue `true` for query and stream stores when omitted everywhere.
   */
  readonly snapshot?: boolean;
}

/** Options for one query caller, extending keyed value configuration. */
export interface StoreOptions<T> extends ValueOptions<T> {
  /**
   * Freshness window for this caller, in milliseconds.
   *
   * The definition's window is used when this is omitted; when both are
   * omitted the window is infinite. A finite, non-negative number is required.
   * An invalid effective window throws a `contract` {@link Fault} synchronously
   * from the query member call, before its backend callback runs.
   *
   * @defaultValue The query definition's window, or an infinite window when both are omitted.
   */
  readonly revalidateAfterMs?: number;
}

/** Context supplied to a query, mutation, or stream callback. */
export interface CallContext {
  /**
   * Signal controlled by the graph for this operation.
   *
   * It is aborted when the caller/graph cancels the operation or the graph is
   * disposed. Callbacks should pass it to their underlying I/O.
   */
  readonly abortSignal: AbortSignal;

  /** The graph that issued this callback invocation. */
  readonly graph: GraphId;
}

/** Context supplied to a query fetch callback. */
export interface FetchOptions extends CallContext {
  /** The keyed store this request may fill. */
  readonly key: StoreKey;
}

/** Input used to declare a query operation in a contract. */
export interface CreateQueryInput<I, T, K extends string> {
  /** Human-readable operation name used in diagnostics and test records. */
  readonly name: string;

  /**
   * Backend callback that resolves the authoritative value.
   *
   * Aeolia invokes this callback when a member call needs data or when a
   * caller explicitly revalidates. The callback may reject; the graph records
   * that failure on the keyed store and caller status.
   */
  readonly fetch: (input: I, options: FetchOptions) => Promise<T>;

  /**
   * Derives the shared store key for an input.
   *
   * Inputs producing the same key share committed value, errors, requests,
   * predictions, and freshness source. The key must be non-empty.
   */
  readonly key: (input: I) => K;

  /**
   * Default freshness window for query callers, in milliseconds.
   *
   * Must be finite and non-negative when supplied. The effective window is
   * validated when a query member is called; invalid values throw a `contract`
   * {@link Fault} before invoking the backend callback.
   *
   * @defaultValue An infinite window when omitted.
   */
  readonly revalidateAfterMs?: number;

  /**
   * Whether this query's committed values are eligible for snapshots.
   *
   * @defaultValue `true` when omitted.
   */
  readonly snapshot?: boolean;

  /**
   * Equality used by stores materialized from this query.
   *
   * @defaultValue `Object.is`
   */
  readonly equals?: (a: NoInfer<T>, b: NoInfer<T>) => boolean;
}

/**
 * An immutable declaration of a query operation.
 *
 * The definition only describes callbacks and keying. It does not invoke the
 * backend or create a store until a graph member call uses it.
 */
export interface QueryDefinition<I, T, K extends string = string> extends CreateQueryInput<
  I,
  T,
  K
> {
  /** Discriminator identifying this declaration as a query. */
  readonly kind: "query";
}

/** Context supplied to a stream-opening callback. */
export interface OpenOptions extends FetchOptions {
  /**
   * Marks the shared keyed stream as stale after an ordering or transport gap.
   *
   * The next emission can establish a fresh committed value. Calling this
   * callback before the first committed value has no effect. If an explicit
   * initial value is already committed, a gap reported before the first source
   * emission marks the stream stale.
   */
  readonly reportGap: () => void;
}

/** Input used to declare a stream operation in a contract. */
export interface CreateStreamInput<I, T, K extends string> {
  /** Human-readable operation name used in diagnostics and test records. */
  readonly name: string;

  /**
   * Opens an async source of values.
   *
   * The graph supplies a cancellation signal, key, graph ID, and gap marker.
   * The first caller for a key owns the source session; later callers share
   * the already-open session.
   */
  readonly open: (input: I, options: OpenOptions) => AsyncIterable<T>;

  /** Derives the shared store key for an input; the result must be non-empty. */
  readonly key: (input: I) => K;

  /**
   * Whether committed emissions for this stream are eligible for snapshots.
   *
   * @defaultValue `true` when omitted.
   */
  readonly snapshot?: boolean;

  /**
   * Equality used by stores materialized from this stream.
   *
   * @defaultValue `Object.is`
   */
  readonly equals?: (a: NoInfer<T>, b: NoInfer<T>) => boolean;
}

/**
 * An immutable declaration of a stream operation.
 *
 * A stream stores the latest committed emission for each key. It has no
 * freshness window or automatic polling; source-level lifecycle is governed
 * by the async iterable and cancellation.
 */
export interface StreamDefinition<I, T, K extends string = string> extends CreateStreamInput<
  I,
  T,
  K
> {
  /** Discriminator identifying this declaration as a stream. */
  readonly kind: "stream";
}

/**
 * A mutation effect bound to one query definition.
 *
 * Effects resolve their query key only when the mutation is invoked. They
 * operate on stores that already exist and have not been collected; an effect
 * does not materialize a missing or dropped store. Its effect callbacks inherit
 * the erased {@link AffectSpec} shape because the query value type is not
 * needed when the mutation is later resolved.
 */
export interface Affected<I> extends AffectSpec<I, unknown, unknown> {
  /** Runtime marker identifying this value as a mutation effect. */
  readonly [affectedBrand]: true;

  /** Query whose keyed store is invalidated, revalidated, or predicted. */
  readonly query: QueryDefinition<any, any, any>;
}

/** Type-safe specification for an effect created by {@link affects}. */
export interface AffectSpec<MI, QI, T> {
  /** Maps mutation input to the affected query input. */
  readonly select: (input: MI) => QI;

  /**
   * Settled effect to apply after a successful mutation.
   *
   * `invalidate` marks existing state stale without starting I/O;
   * `revalidate` starts a fresh query for the selected key.
   */
  readonly on: "invalidate" | "revalidate";

  /**
   * Optional optimistic value producer applied until the mutation fails or a
   * later accepted query landing reconciles its successful result.
   */
  readonly optimistic?: (current: T | undefined, input: MI) => T;
}

/** Input used to declare an authoritative mutation and its query effects. */
export interface CreateMutationInput<I, R> {
  /** Human-readable operation name used in diagnostics and test records. */
  readonly name: string;

  /**
   * Backend callback that performs the authoritative mutation.
   *
   * The graph applies optimistic effects before invoking this callback. On
   * success it marks those predictions as belonging to a succeeded mutation
   * and applies settled effects. A prediction from a succeeded mutation
   * remains visible until an accepted query landing that was issued after the
   * mutation succeeds reconciles it with authoritative state. Mutation
   * success does not validate the predicted value. On failure the graph
   * removes that mutation's predictions and leaves settled effects unapplied.
   */
  readonly run: (input: I, options: CallContext) => Promise<R>;

  /** Query effects to prepare before the call and settle after success. */
  readonly affects: readonly Affected<I>[];
}

/** An immutable declaration of a mutation operation. */
export interface MutationDefinition<I, R> extends CreateMutationInput<I, R> {
  /** Discriminator identifying this declaration as a mutation. */
  readonly kind: "mutation";
}

/** Options for one mutation invocation. */
export interface MutationOptions {
  /** Caller-owned signal that cancels the mutation callback. */
  readonly abortSignal?: AbortSignal;

  /**
   * Per-key optimistic producers.
   *
   * A producer for a selected key overrides the matching declared producer;
   * it may also provide an optimistic update where the declaration has none.
   * Producers run without reactive reads. Predictions from failed mutations
   * are removed when their mutation rejects. Predictions from succeeded
   * mutations remain layered over the committed value until an accepted query
   * landing issued after that mutation succeeds retires them; an `invalidate`
   * effect therefore retains its prediction until a later explicit refresh.
   * Mutation success does not validate the predicted value. Effects still
   * apply only to existing, non-collected stores.
   */
  readonly optimistic?: ReadonlyMap<StoreKey, (current: unknown, input: unknown) => unknown>;
}

/** Any executable query, mutation, or stream declaration. */
export type OperationDefinition =
  | QueryDefinition<any, any, any>
  | MutationDefinition<any, any>
  | StreamDefinition<any, any, any>;

/**
 * A nested, named operation and store declaration tree.
 *
 * Keys become properties of the generated graph API. Values are inert
 * definitions or further operation trees. A contract must not contain cyclic
 * or aliased declaration objects, and operation/store names must be unique
 * within their respective categories.
 */
export type OperationTree = {
  readonly [name: string]: OperationDefinition | StoreDefinition<any> | OperationTree;
};

/** Input accepted by {@link defineContract}. */
export interface DefineContractInput<T extends OperationTree> {
  /** Stable namespace used to identify compatible realm snapshots. */
  readonly namespace: string;

  /** Nested operation and store declarations exposed by the contract. */
  readonly operations: T;
}

/**
 * Immutable contract metadata and its operation declaration tree.
 *
 * A contract is shared as a definition between graphs; it owns no graph state
 * and does not invoke callbacks. The namespace must remain stable for
 * snapshots intended for another graph.
 */
export interface Contract<T extends OperationTree = OperationTree> extends DefineContractInput<T> {}

/**
 * Maps one declaration or nested declaration tree to its graph API member.
 *
 * Queries and streams become keyed store factories, mutations become promise
 * factories, and declared stores become zero-argument store factories.
 */
export type MemberOf<D> =
  D extends QueryDefinition<infer I, infer T, any>
    ? (input: I, options?: StoreOptions<T>) => QueryStore<T>
    : D extends MutationDefinition<infer I, infer R>
      ? (input: I, options?: MutationOptions) => Promise<R>
      : D extends StreamDefinition<infer I, infer T, any>
        ? (input: I, options?: ValueOptions<T>) => StreamStore<T>
        : D extends StoreDefinition<infer T, any>
          ? () => Store<T>
          : D extends OperationTree
            ? { readonly [K in keyof D]: MemberOf<D[K]> }
            : never;

/** The recursively generated API shape exposed by {@link Graph.api}. */
export type ContractApi<C extends Contract> =
  C extends Contract<infer T> ? { readonly [K in keyof T]: MemberOf<T[K]> } : never;

/** Extracts query and stream key patterns from a declaration tree. */
export type StorePatterns<D> =
  D extends QueryDefinition<any, any, infer K>
    ? K
    : D extends StreamDefinition<any, any, infer K>
      ? K
      : D extends StoreDefinition<any, any> | MutationDefinition<any, any>
        ? never
        : D extends object
          ? { readonly [P in keyof D]: StorePatterns<D[P]> }[keyof D]
          : never;

/** Extracts declared store names from a declaration tree. */
export type StoreNames<D> =
  D extends StoreDefinition<any, infer N>
    ? N
    : D extends OperationDefinition
      ? never
      : D extends object
        ? { readonly [P in keyof D]: StoreNames<D[P]> }[keyof D]
        : never;

/** Extracts declared store definitions from a declaration tree. */
export type DeclaredStores<D> =
  D extends StoreDefinition<infer T, infer N>
    ? StoreDefinition<T, N>
    : D extends OperationDefinition
      ? never
      : D extends object
        ? { readonly [P in keyof D]: DeclaredStores<D[P]> }[keyof D]
        : never;

/**
 * The declared store names that collide with a query or stream key pattern.
 *
 * This helper is used by {@link NoCollision} and is generally useful only in
 * type-level contract diagnostics.
 */
export type Colliding<D> = Extract<StoreNames<D>, StorePatterns<D>>;

/**
 * A compile-time guard against declared store/key-pattern collisions.
 *
 * It is `unknown` for a valid declaration tree. When a collision exists it
 * adds an unsatisfiable property whose name describes the conflicting store,
 * causing {@link defineContract} to be rejected by TypeScript.
 */
export type NoCollision<T> = [Colliding<T>] extends [never]
  ? unknown
  : {
      readonly [
        K in `AEOLIA: declared store name collides with a query key pattern: ${Colliding<T> &
          string}`
      ]: never;
    };

type KeyedStoreEntry<P extends string, V> = {
  readonly pattern: P;
  readonly value: V;
};

/** Flattens a declaration tree into key-pattern/value pairs; mutations yield no pair. */
export type KeyedStores<D> =
  D extends QueryDefinition<any, infer T, infer K>
    ? KeyedStoreEntry<K, T>
    : D extends StreamDefinition<any, infer T, infer K>
      ? KeyedStoreEntry<K, T>
      : D extends StoreDefinition<infer T, infer N>
        ? KeyedStoreEntry<N, T>
        : D extends MutationDefinition<any, any>
          ? never
          : D extends object
            ? { readonly [P in keyof D]: KeyedStores<D[P]> }[keyof D]
            : never;

/**
 * Resolves a known contract key to the value type of its matching store.
 *
 * Wide (`string`) patterns intentionally do not claim arbitrary keys, so a
 * key can still fall back to `Store<unknown>` through {@link StoreAt}.
 */
export type Matching<C extends Contract, K extends string> =
  KeyedStores<C["operations"]> extends infer E
    ? E extends { readonly pattern: infer P; readonly value: infer V }
      ? string extends P
        ? never
        : K extends P
          ? Store<V>
          : never
      : never
    : never;

/**
 * The statically known store type at a key.
 *
 * A key matching one or more narrow query, stream, or declared-store patterns
 * receives the corresponding `Store<T>` union. Unknown keys and wide patterns
 * fall back to `Store<unknown>`.
 */
export type StoreAt<C extends Contract, K extends string> = [Matching<C, K>] extends [never]
  ? Store<unknown>
  : Matching<C, K>;

/** Options used to create one isolated graph. */
export interface GraphOptions<C extends Contract = Contract> {
  /** Contract whose declaration tree becomes the graph's typed API. */
  readonly contract: C;

  /**
   * Signal whose abort disposes the graph and all graph-owned work.
   *
   * An already-aborted signal produces an already-disposed graph.
   */
  readonly abortSignal?: AbortSignal;

  /**
   * Idle retention period for unobserved keyed store contents, in
   * milliseconds.
   *
   * Must be finite and non-negative. Declared stores are retained regardless
   * of this value.
   *
   * @defaultValue `300_000` (five minutes)
   */
  readonly idleMs?: number;

  /**
   * Maximum number of simultaneous optimistic predictions per store.
   *
   * Must be a finite positive integer.
   *
   * @defaultValue `64`
   */
  readonly maxPredictions?: number;

  /**
   * Receives non-`Fault` errors from asynchronous continuation work. A
   * structured {@link Fault} is delivered to graph fault observers when any
   * are registered and reaches this callback only when none are present.
   * Errors thrown by graph fault observers are also reported here. Exceptions
   * thrown by this diagnostic callback are swallowed.
   */
  readonly onUnobservedFault?: (error: unknown) => void;
}

/**
 * An isolated runtime graph created from a {@link Contract}.
 *
 * Graphs do not share stores, requests, timers, predictions, streams, or fault
 * observers, even when they use the same contract. Disposal is terminal and
 * aborts graph-owned work; operations and store reads after disposal throw a
 * `disposed` {@link Fault}.
 */
export interface Graph<C extends Contract = Contract> {
  /** Opaque unique identity for this graph and its callback context. */
  readonly id: GraphId;

  /** The frozen API generated recursively from the graph's contract. */
  readonly api: ContractApi<C>;

  /**
   * Materializes the declared store identified by `definition.name`.
   *
   * Repeated calls for one key return the same stable store shell. The type
   * accepts only a store definition declared by this graph's contract. When
   * the key is unconfigured, first materialization commits the declaration's
   * `initial` value. Declared stores are not collected as idle keyed stores.
   *
   * @param definition - A store definition from this graph's contract.
   * @returns The graph-owned store for the declaration's key.
   * @throws A {@link Fault} If the graph has been disposed.
   * @throws A {@link TypeError} If `definition.name` is empty.
   */
  store<T, N extends string>(
    definition: StoreDefinition<T, N> & DeclaredStores<C["operations"]>,
  ): Store<T>;

  /**
   * Gets or creates the store at an exact runtime key.
   *
   * Declared stores are configured when their declared name is reached;
   * otherwise the returned store starts empty. The type narrows the value only
   * for a matching narrow contract pattern; unknown keys return
   * `Store<unknown>`. A created keyed store keeps its shell identity after idle
   * collection, although its contents and callers are released.
   *
   * @param key - Non-empty exact store key.
   * @returns The graph-owned store at `key`.
   * @throws A {@link TypeError} If `key` is empty.
   * @throws A {@link Fault} If the graph has been disposed.
   */
  at<K extends string>(key: K): StoreAt<C, K>;

  /**
   * Permanently disposes the graph.
   *
   * Disposal aborts active requests, mutation callbacks, stream sessions, and
   * projections, clears timers and fault observers, and marks stores disposed.
   * Calling `dispose` again is harmless. Subsequent graph/store operations and
   * reads through held store readables fail with a `disposed` {@link Fault}.
   */
  dispose(): void;
}
