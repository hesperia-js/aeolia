import type { Readable } from "../reactive.ts";
import { isAbortSignal, isObject } from "../utils.ts";
import type { GraphId, StoreKey, StoreStatus, StreamStatus } from "./identity.ts";
import { isStoreKey } from "./identity.ts";
import { affectedBrand } from "./symbols.ts";
import type { queryOptionsBrand } from "./symbols.ts";
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
   * This is an explicit retry: it re-arms automatic recovery after a previous
   * recovery exhausted its allowance. Starting the request clears the old error.
   * The returned promise does not guarantee that the backend succeeded.
   *
   * Reads alone do not start requests. A value observer can schedule automatic
   * refresh for invalidated data or an elapsed freshness window; this method
   * requests a refresh regardless. A call may reactivate a store whose contents
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
   * @throws TypeError if the cancellation options are malformed.
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

/** Checks declaration fields without evaluating equality or initial data. */
export function isCreateStoreInput(value: unknown): value is CreateStoreInput<unknown, string> {
  return (
    isNamedDeclaration(value) &&
    isStoreKey(value.name) &&
    "initial" in value &&
    matchesOptions(value, valueSemanticsChecks)
  );
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

export function isStoreDefinition(value: unknown): value is StoreDefinition<unknown> {
  return isCreateStoreInput(value) && "kind" in value && value.kind === "store";
}

/**
 * Value identity, snapshot, and cancellation options for one query or stream
 * member call.
 *
 * The first explicit value configuration for a key becomes that store's
 * configuration. Later calls for the same key must not contradict it.
 * Malformed option types throw TypeError synchronously before resolving the
 * key or creating a store. Initial data is not checked against the erased T.
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

type OptionChecks = readonly (readonly [string, (value: unknown) => boolean])[];

function matchesOptions(value: unknown, checks: OptionChecks): boolean {
  if (!isObject(value) || typeof value === "function" || Array.isArray(value)) return false;
  return checks.every(([field, check]) => {
    const option: unknown = Reflect.get(value, field);
    return option === undefined || check(option);
  });
}

function hasOptionFields(value: unknown, checks: OptionChecks): boolean {
  return (
    isObject(value) &&
    typeof value !== "function" &&
    !Array.isArray(value) &&
    checks.some(([field]) => Object.hasOwn(value, field))
  );
}

const valueOptionValidators = {
  abortSignal: isAbortSignal,
  initial: (_value: unknown) => true,
  equals: (value: unknown) => typeof value === "function",
  snapshot: (value: unknown) => typeof value === "boolean",
} satisfies Record<keyof ValueOptions<unknown>, (value: unknown) => boolean>;
const valueOptionChecks = Object.entries(valueOptionValidators);
const valueSemanticsChecks = Object.entries({
  equals: valueOptionValidators.equals,
  snapshot: valueOptionValidators.snapshot,
} satisfies Record<
  keyof Pick<ValueOptions<unknown>, "equals" | "snapshot">,
  (value: unknown) => boolean
>);

function isNamedDeclaration(value: unknown): value is { readonly name: string } {
  return (
    isObject(value) &&
    typeof value !== "function" &&
    !Array.isArray(value) &&
    "name" in value &&
    typeof value.name === "string"
  );
}

/** Validates value options; the caller's arbitrary initial data remains unknown. */
export function isValueOptions(value: unknown): value is ValueOptions<unknown> {
  return matchesOptions(value, valueOptionChecks);
}

/** Validates the cancellation-only options accepted by explicit revalidation. */
export function isCancellationOptions(
  value: unknown,
): value is Pick<ValueOptions<unknown>, "abortSignal"> {
  return (
    isObject(value) &&
    typeof value !== "function" &&
    !Array.isArray(value) &&
    (!("abortSignal" in value) ||
      value.abortSignal === undefined ||
      isAbortSignal(value.abortSignal))
  );
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

const storeOptionChecks = Object.entries({
  ...valueOptionValidators,
  revalidateAfterMs: (value: unknown) => typeof value === "number",
} satisfies Record<keyof StoreOptions<unknown>, (value: unknown) => boolean>);

/** Validates query option types; effective freshness bounds are checked when used. */
export function isStoreOptions(value: unknown): value is StoreOptions<unknown> {
  return matchesOptions(value, storeOptionChecks);
}

/** Recognizes the options-only call form, including malformed option values. */
export function hasStoreOptionFields(value: unknown): boolean {
  return hasOptionFields(value, storeOptionChecks);
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

export function isCreateQueryInput(
  value: unknown,
): value is CreateQueryInput<unknown, unknown, string> {
  return (
    isNamedDeclaration(value) &&
    "key" in value &&
    typeof value.key === "function" &&
    "fetch" in value &&
    typeof value.fetch === "function" &&
    (!("revalidateAfterMs" in value) ||
      value.revalidateAfterMs === undefined ||
      typeof value.revalidateAfterMs === "number") &&
    matchesOptions(value, valueSemanticsChecks)
  );
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

export function isQueryDefinition(value: unknown): value is QueryDefinition<unknown, unknown> {
  return isCreateQueryInput(value) && "kind" in value && value.kind === "query";
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

export function isCreateStreamInput(
  value: unknown,
): value is CreateStreamInput<unknown, unknown, string> {
  return (
    isNamedDeclaration(value) &&
    "key" in value &&
    typeof value.key === "function" &&
    "open" in value &&
    typeof value.open === "function" &&
    matchesOptions(value, valueSemanticsChecks)
  );
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

export function isStreamDefinition(value: unknown): value is StreamDefinition<unknown, unknown> {
  return isCreateStreamInput(value) && "kind" in value && value.kind === "stream";
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
export interface Affected<I> {
  /** Runtime marker identifying this value as a mutation effect. */
  readonly [affectedBrand]: true;

  /** Query whose keyed store is invalidated, revalidated, or predicted. */
  readonly query: QueryDefinition<any, any, any>;

  /** Maps mutation input to the query input when the affected query needs one. */
  readonly select: (input: I) => unknown;

  /** Settled effect; omitted input specs are normalized to `"invalidate"`. */
  readonly on: "invalidate" | "revalidate";

  /** Optional optimistic value producer. */
  readonly optimistic?: (current: unknown, input: I) => unknown;
}

export function isAffected(value: unknown): value is Affected<unknown> {
  return (
    isObject(value) &&
    affectedBrand in value &&
    value[affectedBrand] === true &&
    "query" in value &&
    isQueryDefinition(value.query) &&
    "select" in value &&
    typeof value.select === "function" &&
    "on" in value &&
    (value.on === "invalidate" || value.on === "revalidate") &&
    (!("optimistic" in value) ||
      value.optimistic === undefined ||
      typeof value.optimistic === "function")
  );
}

/** Type-safe specification for an effect created by {@link affects}. */
export type AffectSpec<MI, QI, T> = {
  /**
   * Settled effect to apply after a successful mutation.
   *
   * `invalidate` marks an existing store stale and starts a fresh query if its
   * value has a live observer. Observing only status, pending, or error does not
   * trigger that refresh. `revalidate` starts a fresh query regardless of
   * observers. Neither effect creates a missing store.
   * If an invalidated value becomes observed later, it schedules a refresh
   * without requiring `revalidateAfterMs`. The observer must still be present
   * when that refresh dispatches. A fresh value or failed query is not retried
   * merely because observers return; existing recovery limits still apply.
   *
   * Either refresh can run after repeated failures have stopped automatic
   * refresh. A successful landing restores its timer; another failure leaves
   * an exhausted query stopped.
   * Superseding that recovery with a newer mutation does not grant extra
   * automatic retries.
   *
   * @defaultValue `"invalidate"`
   */
  readonly on?: "invalidate" | "revalidate";

  /**
   * Optional optimistic value producer applied until the mutation fails or a
   * later accepted query landing reconciles its successful result.
   */
  readonly optimistic?: (current: T | undefined, input: MI) => T;
} & ([QI] extends [void]
  ? {
      /** Optional selector for a query with no input value. */
      readonly select?: (input: MI) => QI;
    }
  : {
      /** Maps mutation input to the affected query input. */
      readonly select: (input: MI) => QI;
    });

/** Checks the optional selector form; its input type is only known to TypeScript. */
export function isAffectSpec(value: unknown): value is AffectSpec<unknown, void, unknown> {
  return (
    isObject(value) &&
    typeof value !== "function" &&
    !Array.isArray(value) &&
    (!("on" in value) ||
      value.on === undefined ||
      value.on === "invalidate" ||
      value.on === "revalidate") &&
    (!("select" in value) || value.select === undefined || typeof value.select === "function") &&
    (!("optimistic" in value) ||
      value.optimistic === undefined ||
      typeof value.optimistic === "function")
  );
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

export function isCreateMutationInput(
  value: unknown,
): value is CreateMutationInput<unknown, unknown> {
  if (
    !isNamedDeclaration(value) ||
    !("run" in value) ||
    typeof value.run !== "function" ||
    !("affects" in value) ||
    !Array.isArray(value.affects)
  )
    return false;
  for (const effect of value.affects) if (!isAffected(effect)) return false;
  return true;
}

/** An immutable declaration of a mutation operation. */
export interface MutationDefinition<I, R> extends CreateMutationInput<I, R> {
  /** Discriminator identifying this declaration as a mutation. */
  readonly kind: "mutation";
}

export function isMutationDefinition(
  value: unknown,
): value is MutationDefinition<unknown, unknown> {
  return isCreateMutationInput(value) && "kind" in value && value.kind === "mutation";
}

/**
 * Options for one mutation invocation.
 * Malformed signals, maps, keys, or producer types throw TypeError before
 * resolving effects or starting the mutation callback.
 */
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

function isOptimisticMap(value: unknown): value is NonNullable<MutationOptions["optimistic"]> {
  if (!isObject(value)) return false;
  const map = value as Partial<ReadonlyMap<unknown, unknown>>;
  if (
    typeof map.size !== "number" ||
    !Number.isInteger(map.size) ||
    map.size < 0 ||
    typeof map.get !== "function" ||
    typeof map.has !== "function" ||
    typeof map.forEach !== "function" ||
    typeof map.entries !== "function" ||
    typeof map.keys !== "function" ||
    typeof map.values !== "function" ||
    typeof map[Symbol.iterator] !== "function"
  )
    return false;
  for (const entry of value as Iterable<unknown>) {
    if (
      !Array.isArray(entry) ||
      entry.length !== 2 ||
      !isStoreKey(entry[0]) ||
      typeof entry[1] !== "function"
    )
      return false;
  }
  return true;
}

const mutationOptionChecks = Object.entries({
  abortSignal: isAbortSignal,
  optimistic: isOptimisticMap,
} satisfies Record<keyof MutationOptions, (value: unknown) => boolean>);

/** Validates mutation options and every supplied optimistic producer. */
export function isMutationOptions(value: unknown): value is MutationOptions {
  return matchesOptions(value, mutationOptionChecks);
}

/** Recognizes the options-only call form, including malformed option values. */
export function hasMutationOptionFields(value: unknown): boolean {
  return hasOptionFields(value, mutationOptionChecks);
}

/** Any executable query, mutation, or stream declaration. */
export type OperationDefinition =
  | QueryDefinition<any, any, any>
  | MutationDefinition<any, any>
  | StreamDefinition<any, any, any>;

export function isOperationDefinition(value: unknown): value is OperationDefinition {
  if (!isObject(value) || !("kind" in value)) return false;
  switch (value.kind) {
    case "query":
      return isQueryDefinition(value);
    case "mutation":
      return isMutationDefinition(value);
    case "stream":
      return isStreamDefinition(value);
    default:
      return false;
  }
}

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

type BrandedOperationOptions<Options extends object> = Options & {
  readonly [queryOptionsBrand]: true;
};

/**
 * Maps one declaration or nested declaration tree to its graph API member.
 *
 * Queries and streams become keyed store factories, mutations become promise
 * factories, and declared stores become zero-argument store factories. For an
 * inputless query or mutation, a branded options object may be passed first;
 * an optional second options object overrides matching branded fields.
 */
export type MemberOf<D> =
  D extends QueryDefinition<infer I, infer T, any>
    ? ((input: I, options?: StoreOptions<T>) => QueryStore<T>) &
        ([I] extends [void | undefined]
          ? (() => QueryStore<T>) &
              ((
                options: StoreOptions<T> | BrandedOperationOptions<StoreOptions<T>>,
              ) => QueryStore<T>)
          : {}) &
        ([I] extends [void | undefined]
          ? (
              options: BrandedOperationOptions<StoreOptions<T>>,
              overrides?: StoreOptions<T>,
            ) => QueryStore<T>
          : {})
    : D extends MutationDefinition<infer I, infer R>
      ? ((input: I, options?: MutationOptions) => Promise<R>) &
          ([I] extends [void | undefined]
            ? (() => Promise<R>) &
                ((
                  options: MutationOptions | BrandedOperationOptions<MutationOptions>,
                ) => Promise<R>)
            : {}) &
          ([I] extends [void | undefined]
            ? (
                options: BrandedOperationOptions<MutationOptions>,
                overrides?: MutationOptions,
              ) => Promise<R>
            : {})
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
   * An open stream measures this period from its final listener's departure,
   * or from opening if it never had a listener. Emissions and untracked reads
   * do not extend that deadline. A returning listener ends the unobserved
   * period; its later departure starts a new one. Collection closes the
   * source and ignores subsequent emissions even if cancellation is ignored.
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

  /**
   * Monotonic identity for the graph's current state epoch.
   *
   * The value starts at `0` and changes synchronously whenever {@link reset}
   * begins. Observe it to discard owner-bound work from an earlier graph
   * identity. The readable object itself cannot be written through this API.
   */
  readonly resetVersion: Readable<number>;

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
   * @throws A {@link Fault} If the graph has been disposed or writes are
   * restricted by the current reactive scope.
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
   * Resets graph-owned state while preserving the graph, API, and store shells.
   *
   * Declared stores return to their configured initial values.
   * Keyed query and stream contents, query initial configuration, errors, predictions,
   * freshness timestamps, and timers are cleared.
   *
   * Open projections reset their accumulated value and restart. Completed, failed,
   * or closed projections keep their terminal status and do not restart. Their
   * value returns to an empty accumulation or the reduction's initial value,
   * and their error is cleared.
   *
   * Streams with live readers restart with the opener's abort signal unless that signal
   * has already been aborted.
   *
   * Queries with any live readable are refreshed and this promise waits for every
   * required refresh outcome.
   *
   * An in-flight mutation receives an abort signal, but reset does not wait for its
   * callback; late results remain available from the returned promise while old-epoch
   * effects are ignored.
   *
   * A failed query refresh rejects with its original reason, or an `AggregateError`
   * when several refreshes fail. Starting another reset supersedes this one
   * and rejects its promise with an `Error` subclass named `"AbortError"`.
   *
   * @returns A promise that settles after all required fresh query outcomes.
   * @throws A {@link Fault} If the graph has been disposed or writes are
   * restricted by the current reactive scope.
   */
  reset(): Promise<void>;

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
