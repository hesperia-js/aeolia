import type {
  Affected,
  AffectSpec,
  CallContext,
  Contract,
  CreateMutationInput,
  CreateQueryInput,
  CreateStoreInput,
  CreateStreamInput,
  DefineContractInput,
  FetchOptions,
  MutationDefinition,
  NoCollision,
  OperationTree,
  QueryDefinition,
  StoreDefinition,
  StreamDefinition,
} from "./types.ts";
import { affectedBrand } from "./symbols.ts";
import {
  isCreateStoreInput,
  isCreateQueryInput,
  isCreateStreamInput,
  isCreateMutationInput,
  isAffectSpec,
  isQueryDefinition,
} from "./types.ts";
import { validateContract } from "./validation.ts";
export { createGraph } from "./engine.ts";
export { storeKey } from "./identity.ts";

type QueryInputFromCallbacks<
  I,
  Key extends (...args: any[]) => string,
  Fetch extends (...args: any[]) => Promise<unknown>,
> = Parameters<Key> extends [] ? (Parameters<Fetch> extends [] ? void : I) : I;

type MutationInputFromCallbacks<
  I,
  Run extends (...args: any[]) => Promise<unknown>,
  Effects extends readonly unknown[],
> = Parameters<Run> extends [] ? (Effects extends readonly [] ? void : I) : I;

/**
 * Declares a writable store for use in a contract.
 *
 * @param input - Store name, initial value, and optional value semantics.
 * @returns A shallow-frozen inert store definition.
 * @throws TypeError if the declaration name, equality, or snapshot fields are malformed.
 * @example
 * ```ts
 * import { createStore } from "aeolia";
 *
 * const cart = createStore({ name: "cart", initial: [] as string[] });
 * ```
 */
export function createStore<T, N extends string>(
  input: CreateStoreInput<T, N>,
): StoreDefinition<T, N> {
  if (!isCreateStoreInput(input)) throw new TypeError("Invalid store declaration.");
  return Object.freeze({ ...input, kind: "store" as const });
}

/**
 * Declares a query operation for use in a contract.
 *
 * @param input - Query name, key function, backend callback, and value
 * configuration.
 * Zero-parameter key and fetch callbacks infer a no-input query. Callbacks
 * with optional parameters remain input-bearing; a declared input in either
 * callback also keeps the query input-bearing.
 * @returns A shallow-frozen inert query definition.
 * @throws TypeError if declaration fields or callback types are malformed.
 * @example
 * ```ts
 * import { createQuery } from "aeolia";
 *
 * declare function loadUser(
 *   id: string,
 *   signal: AbortSignal,
 * ): Promise<{ id: string; name: string }>;
 *
 * const users = createQuery({
 *   name: "users.get",
 *   key: ({ id }: { id: string }) => `users/${id}`,
 *   fetch: async ({ id }, { abortSignal }) => loadUser(id, abortSignal),
 * });
 * ```
 */
export function createQuery<
  I,
  T,
  const K extends string,
  Key extends (input: I) => K,
  Fetch extends (input: I, options: FetchOptions) => Promise<T>,
>(
  input: CreateQueryInput<I, T, K> & { readonly key: Key; readonly fetch: Fetch },
): QueryDefinition<QueryInputFromCallbacks<I, Key, Fetch>, T, K>;
export function createQuery<I, T, K extends string>(
  input: CreateQueryInput<I, T, K>,
): QueryDefinition<I, T, K>;
export function createQuery<I, T, K extends string>(
  input: CreateQueryInput<I, T, K>,
): QueryDefinition<I, T, K> {
  if (!isCreateQueryInput(input)) throw new TypeError("Invalid query declaration.");
  return Object.freeze({ ...input, kind: "query" as const });
}

/**
 * Declares a stream operation for use in a contract.
 *
 * @param input - Stream name, key function, source opener, and value
 * configuration.
 * @returns A shallow-frozen inert stream definition.
 * @throws TypeError if declaration fields or callback types are malformed.
 * @example
 * ```ts
 * import { createStream } from "aeolia";
 *
 * declare function readUserEvents(
 *   id: string,
 *   signal: AbortSignal,
 * ): AsyncIterable<{ userId: string; type: string }>;
 *
 * const events = createStream({
 *   name: "users.events",
 *   key: ({ id }: { id: string }) => `users/${id}/events`,
 *   open: (input, { abortSignal }) => readUserEvents(input.id, abortSignal),
 * });
 * ```
 */
export function createStream<I, T, K extends string>(
  input: CreateStreamInput<I, T, K>,
): StreamDefinition<I, T, K> {
  if (!isCreateStreamInput(input)) throw new TypeError("Invalid stream declaration.");
  return Object.freeze({ ...input, kind: "stream" as const });
}

/**
 * Binds a mutation effect to a query definition.
 *
 * @param query - Query whose keyed store the effect targets.
 * @param spec - Input selector, settled behavior, and optional prediction.
 * @returns A shallow-frozen effect declaration.
 * @throws TypeError if the target or effect fields are malformed.
 * @example
 * ```ts
 * import { affects, type QueryDefinition } from "aeolia";
 *
 * declare const user: QueryDefinition<
 *   { id: string },
 *   { id: string; name: string }
 * >;
 *
 * const renameEffect = affects(user, {
 *   select: ({ id }: { id: string; name: string }) => ({ id }),
 *   on: "revalidate",
 *   optimistic: (_current, input) => ({ id: input.id, name: input.name }),
 * });
 * ```
 */
export function affects<MI, QI, T>(
  query: QueryDefinition<QI, T>,
  spec: AffectSpec<MI, NoInfer<QI>, T>,
): Affected<MI>;
export function affects<MI, QI, T>(
  query: QueryDefinition<QI, T>,
  spec: AffectSpec<MI, QI, T>,
): Affected<MI> {
  if (!isQueryDefinition(query)) throw new TypeError("An effect must target a query declaration.");
  if (!isAffectSpec(spec)) throw new TypeError("Invalid mutation effect specification.");
  const result: Affected<MI> = {
    [affectedBrand]: true,
    query,
    select: spec.select ?? (() => undefined),
    on: spec.on ?? "invalidate",
    ...(spec.optimistic === undefined
      ? {}
      : { optimistic: spec.optimistic as (current: unknown, input: MI) => unknown }),
  };
  return Object.freeze(result);
}

/**
 * Declares a mutation operation for use in a contract.
 *
 * @param input - Mutation name, backend callback, and affected query effects.
 * A zero-parameter run infers no input only when the mutation has no effects
 * requiring input. Optional run parameters and effect selectors remain
 * input-bearing.
 * @returns A shallow-frozen inert mutation definition.
 * @throws TypeError if declaration fields, callback types, or effects are malformed.
 * @example
 * ```ts
 * import { affects, createMutation, type QueryDefinition } from "aeolia";
 *
 * interface RenameInput {
 *   id: string;
 *   name: string;
 * }
 *
 * declare const user: QueryDefinition<
 *   { id: string },
 *   { id: string; name: string }
 * >;
 * declare function persistUser(input: RenameInput, signal: AbortSignal): Promise<void>;
 *
 * const saveUser = createMutation({
 *   name: "users.save",
 *   affects: [
 *     affects(user, {
 *       select: (input: RenameInput) => ({ id: input.id }),
 *       on: "revalidate",
 *     }),
 *   ],
 *   run: (input: RenameInput, { abortSignal }) => persistUser(input, abortSignal),
 * });
 * ```
 */
export function createMutation<
  I,
  R,
  Run extends (input: I, options: CallContext) => Promise<R>,
  const Effects extends readonly Affected<I>[],
>(
  input: CreateMutationInput<I, R> & { readonly run: Run; readonly affects: Effects },
): MutationDefinition<MutationInputFromCallbacks<I, Run, Effects>, R>;
export function createMutation<I, R>(input: CreateMutationInput<I, R>): MutationDefinition<I, R>;
export function createMutation<I, R>(input: CreateMutationInput<I, R>): MutationDefinition<I, R> {
  if (!isCreateMutationInput(input)) throw new TypeError("Invalid mutation declaration.");
  return Object.freeze({ ...input, kind: "mutation" as const });
}

/**
 * Validates and freezes a contract declaration tree.
 *
 * Validation rejects cycles, aliases, duplicate operation names, duplicate
 * declared store names, and mutation effects that reference a query outside
 * the tree. The type also rejects declared store names assignable to a narrow
 * query or stream key pattern.
 *
 * @param input - Namespace and nested operation declarations.
 * @returns A shallow-frozen contract; the supplied declaration tree is kept
 * by reference and is not deep-cloned.
 * @throws A {@link Fault} If the tree or its mutation effects violate contract
 * invariants.
 * @throws TypeError if the namespace, tree entries, or declaration fields are malformed.
 * @example
 * ```ts
 * import { defineContract, createStore } from "aeolia";
 *
 * const count = createStore({ name: "count", initial: 0 });
 * const contract = defineContract({
 *   namespace: "example",
 *   operations: { count },
 * });
 * ```
 */
export function defineContract<const T extends OperationTree>(
  input: DefineContractInput<T> & NoCollision<T>,
): Contract<T> {
  validateContract(input);
  return Object.freeze({ namespace: input.namespace, operations: input.operations });
}
