import type {
  Affected,
  AffectSpec,
  Contract,
  CreateMutationInput,
  CreateQueryInput,
  CreateStoreInput,
  CreateStreamInput,
  DefineContractInput,
  MutationDefinition,
  NoCollision,
  OperationTree,
  QueryDefinition,
  StoreDefinition,
  StreamDefinition,
} from "./types.ts";
import { affectedBrand } from "./symbols.ts";
import { validateContractTree, validateAffectedQueries } from "./validation.ts";
export { createGraph } from "./engine.ts";
export { storeKey } from "./identity.ts";

/**
 * Declares a writable store for use in a contract.
 *
 * @param input - Store name, initial value, and optional value semantics.
 * @returns A shallow-frozen inert store definition.
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
  return Object.freeze({ ...input, kind: "store" as const });
}

/**
 * Declares a query operation for use in a contract.
 *
 * @param input - Query name, key function, backend callback, and value
 * configuration.
 * @returns A shallow-frozen inert query definition.
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
export function createQuery<I, T, K extends string>(
  input: CreateQueryInput<I, T, K>,
): QueryDefinition<I, T, K> {
  return Object.freeze({ ...input, kind: "query" as const });
}

/**
 * Declares a stream operation for use in a contract.
 *
 * @param input - Stream name, key function, source opener, and value
 * configuration.
 * @returns A shallow-frozen inert stream definition.
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
  return Object.freeze({ ...input, kind: "stream" as const });
}

/**
 * Binds a mutation effect to a query definition.
 *
 * @param query - Query whose keyed store the effect targets.
 * @param spec - Input selector, settled behavior, and optional prediction.
 * @returns A shallow-frozen effect declaration.
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
  spec: AffectSpec<MI, QI, T>,
): Affected<MI> {
  const result: Affected<MI> = {
    [affectedBrand]: true,
    query,
    select: spec.select,
    on: spec.on,
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
 * @returns A shallow-frozen inert mutation definition.
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
export function createMutation<I, R>(input: CreateMutationInput<I, R>): MutationDefinition<I, R> {
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
  const walk = validateContractTree(input.operations);
  validateAffectedQueries(walk);
  return Object.freeze({ namespace: input.namespace, operations: input.operations });
}
