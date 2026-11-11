import type {
  FetchOptions,
  Graph,
  GraphId,
  MemberOf,
  QueryDefinition,
  QueryStore,
  Readable,
  StoreIdentity,
  StoreKey,
  StoreOptions,
  ValueOptions,
} from "./types.ts";
import { isStoreOptions, isValueOptions } from "./types.ts";
import { computed, signal } from "../reactive.ts";
import { Fault } from "../fault.ts";
import { assertGraphOpen } from "./faults.ts";
import {
  assertBindingContextOpen as assertContextOpen,
  bindingContextByHandle as contexts,
  captureQueryTarget,
  graphRuntimeByGraph,
  queryBindingByHandle as bindings,
  queryRuntimeByStore,
} from "./registry.ts";
import { bindMutationOperation } from "./mutation-binding.ts";
import type { MutationBinding, MutationBindingConfiguration } from "./mutation-binding.ts";
import type { MutationDefinition } from "./types.ts";
import {
  disposeBindingContextRuntime,
  retireBindingContextRuntime,
  normalizeQueryCall,
} from "./engine.ts";
import { beginPhysicalWork, createRetirementOwner, finishPhysicalWork } from "./retirement.ts";
import { makeQueryStore } from "./query.ts";
import type { BindingContextRuntime, QueryBindingRuntime } from "./runtime.ts";

type AnyQuery = QueryDefinition<any, any, any>;
type QueryInput<Q extends AnyQuery> = Q extends QueryDefinition<infer I, any, any> ? I : never;
type QueryKey<Q extends AnyQuery> = Q extends QueryDefinition<any, any, infer K> ? K : never;
declare const queryBindingHandleBrand: unique symbol;
declare const targetBrand: unique symbol;

function copyOptions<T extends object>(source: T, assertCurrent: () => void): T {
  const copy: Record<PropertyKey, unknown> = {};
  const keys = Reflect.ownKeys(source);
  assertCurrent();
  for (const key of keys) {
    const descriptor = Reflect.getOwnPropertyDescriptor(source, key);
    assertCurrent();
    if (!descriptor?.enumerable) continue;
    const value: unknown = Reflect.get(source, key);
    assertCurrent();
    Object.defineProperty(copy, key, {
      value,
      enumerable: true,
      writable: true,
      configurable: true,
    });
  }
  return copy as T;
}

/** Graph-owned lifetime for one fixed application or security environment. */
export interface BindingContext {
  /** Actual graph owning every binding admitted by this context. */
  readonly graph: GraphId;
  /** Nonretaining lifecycle metadata; observing it starts no query work. */
  readonly active: Readable<boolean>;
  /**
   * Revokes this context and its entries/requests without affecting raw entries
   * or another context. Repeated disposal does nothing.
   * @throws Fault if writes are restricted by the current reactive scope.
   */
  dispose(): void;
  /**
   * Permanently disposes this context and waits for its original bound work and cleanup.
   * Other contexts and raw graph operations remain independent. Earlier logical
   * disposal does not discard this completion. Repeated and reentrant calls
   * return the same promise. Original work that never finishes keeps it pending.
   * Semantic operation failures do not reject retirement; cleanup failures retain
   * their original cause, or an AggregateError when several cleanups fail.
   * Disposal's reactive-write precondition still applies; its failure rejects
   * the promise before admission changes.
   * @returns This context's shared physical retirement promise.
   */
  retire(): Promise<void>;
}

/** Query call forms retain the original input/key and use the completed value. */
export type BoundQueryMember<Q extends AnyQuery, V> = MemberOf<
  QueryDefinition<QueryInput<Q>, V, QueryKey<Q>>
>;

/** Opaque query binding metadata; its fields alone grant no runtime authority. */
export interface QueryBindingHandle {
  /** Owning graph identity. */
  readonly graph: GraphId;
  /** Fixed context that admitted the binding. */
  readonly context: BindingContext;
  /** Exact opaque profile token supplied at registration. */
  readonly profile: object;
  readonly [queryBindingHandleBrand]: true;
}

/** Existing physical entry and the binding needed to recover its value type. */
export interface ExistingTarget<Q extends AnyQuery> {
  /** Authored logical key, which may equal another profile's key. */
  readonly key: StoreKey;
  /** Canonical physical store identity. */
  readonly identity: StoreIdentity;
  /** Exact registered profile token. */
  readonly profile: object;
  /**
   * Checks the genuine binding association without retaining or acquiring data.
   * @param binding - Binding whose exact value association is required.
   * @returns The same ticket, or undefined for another association or an expired incarnation.
   * @throws TypeError for a fabricated binding; a disposed binding throws its native Fault.
   */
  forBinding<V>(binding: QueryBinding<Q, V>): TypedTarget<Q, V> | undefined;
}

/** Invariant query/value ticket for one existing native incarnation. */
export interface TypedTarget<Q extends AnyQuery, V> extends ExistingTarget<Q> {
  readonly [targetBrand]: (query: Q, value: V) => readonly [Q, V];
}

/** Registered query execution using the original declaration's key and input. */
export interface QueryBinding<Q extends AnyQuery, V> extends QueryBindingHandle {
  /** Exact original declaration included in the graph's contract. */
  readonly definition: Q;
  /** Executes through the native query, request and store owners. */
  readonly member: BoundQueryMember<Q, V>;
  /**
   * Captures a fresh ticket on an already-acquired genuine query handle.
   * @param store - Handle acquired through this exact binding.
   * @returns A current ticket, or undefined for an inactive/collected entry.
   * Reset permits a new ticket on the retained handle; older tickets stay stale.
   * @throws TypeError for a foreign/fabricated handle; disposed lifetimes throw Fault.
   */
  target(store: QueryStore<V>): TypedTarget<Q, V> | undefined;
}

/** Captured callback and value policy for one context/query/profile association. */
export interface QueryBindingConfiguration<Q extends AnyQuery, V> {
  /** Active fixed context on the exact graph. */
  readonly context: BindingContext;
  /** Stable opaque token; duplicate registration is rejected. */
  readonly profile: object;
  /**
   * Produces the completed native store value before native request success.
   * @param input - Original native query input, already prepared by the caller.
   * @param context - Native request cancellation, graph and logical key.
   * @returns A completed value or thenable; rejection uses normal query failure.
   */
  readonly fetch: (input: QueryInput<Q>, context: FetchOptions) => V | PromiseLike<V>;
  /** Value-specific defaults; raw equality is never inherited through a cast. */
  readonly value?: Pick<ValueOptions<NoInfer<V>>, "initial" | "equals" | "snapshot">;
}

/**
 * Creates a fixed graph-owned binding lifetime without acquiring a store.
 * @param graph - Genuine graph that will own the bound operations.
 * @returns A new independent context; component departure must not dispose a shared context.
 * @throws TypeError for a fabricated graph, or Fault when the graph is disposed.
 */
export function createBindingContext(graph: Graph): BindingContext {
  const state = graphRuntimeByGraph.get(graph);
  if (state === undefined) throw new TypeError("The value is not an Aeolia graph");
  assertGraphOpen(state);
  const activeCell = signal(true);
  let context!: BindingContextRuntime;
  const handle: BindingContext = Object.freeze({
    graph: state.graph.id,
    active: computed(() => activeCell.get()),
    dispose: () => disposeBindingContextRuntime(context),
    retire: () => retireBindingContextRuntime(context),
  });
  context = {
    retirement: createRetirementOwner(state.retirement),
    state,
    handle,
    activeCell,
    bindings: new Map(),
    streamBindings: new Map(),
    mutations: new Map(),
    mutationControllers: new Set(),
    disposed: false,
  };
  contexts.set(handle, context);
  state.bindingContexts.add(context);
  return handle;
}

function captureTarget<Q extends AnyQuery, V>(
  binding: QueryBindingRuntime,
  store: QueryStore<V>,
): TypedTarget<Q, V> | undefined {
  assertContextOpen(binding.context);
  const runtime = queryRuntimeByStore.get(store);
  if (runtime === undefined || runtime.binding !== binding)
    throw new TypeError("Query handle does not belong to this binding");
  if (runtime.disposed) throw new Fault("disposed", [String(runtime.key)]);
  return captureQueryTarget<Q, V>(binding, runtime);
}

/**
 * Registers a real query execution for one context/query/profile association.
 * @param graph - Graph containing the exact original query declaration.
 * @param definition - Original declaration; key/input/freshness semantics are preserved.
 * @param configuration - Captured completion callback and result-specific value policy.
 * @returns A binding that acquires native stores only when its member is called.
 * @throws TypeError for foreign/malformed/duplicate configuration, or Fault for a disposed lifetime.
 */
export function bindOperation<Q extends AnyQuery, V>(
  graph: Graph,
  definition: Q,
  configuration: QueryBindingConfiguration<NoInfer<Q>, V>,
): QueryBinding<Q, V>;
/**
 * Registers context-owned mutation completion and its ordered original effect plan.
 * @param graph - Graph containing the exact original mutation.
 * @param definition - Mutation whose input, selectors and effect policies are retained.
 * @param configuration - Captured run completion, execution token and complete effect plan.
 * @returns A native bound mutation; registration starts no work.
 * @throws TypeError for malformed, foreign or duplicate registration; disposed lifetimes throw Fault.
 */
export function bindOperation<M extends MutationDefinition<any, any>, R>(
  graph: Graph,
  definition: M,
  configuration: MutationBindingConfiguration<NoInfer<M>, R>,
): MutationBinding<M, R>;
export function bindOperation(
  graph: Graph,
  definition: AnyQuery | MutationDefinition<any, any>,
  configuration: QueryBindingConfiguration<any, any> | MutationBindingConfiguration<any, any>,
): QueryBinding<any, any> | MutationBinding<any, any> {
  const state = graphRuntimeByGraph.get(graph);
  if (state === undefined) throw new TypeError("The value is not an Aeolia graph");
  assertGraphOpen(state);
  const physical = beginPhysicalWork(state.retirement, "acquisition");
  try {
    const kind = definition?.kind;
    assertGraphOpen(state);
    if (kind === "mutation")
      return bindMutationOperation(
        graph,
        definition as MutationDefinition<any, any>,
        configuration as MutationBindingConfiguration<any, any>,
      );
    return bindQueryOperation(
      graph,
      definition as AnyQuery,
      configuration as QueryBindingConfiguration<any, any>,
    );
  } finally {
    finishPhysicalWork(physical);
  }
}

function bindQueryOperation<Q extends AnyQuery, V>(
  graph: Graph,
  definition: Q,
  configuration: QueryBindingConfiguration<NoInfer<Q>, V>,
): QueryBinding<Q, V> {
  const state = graphRuntimeByGraph.get(graph);
  if (state === undefined) throw new TypeError("The value is not an Aeolia graph");
  assertGraphOpen(state);
  if (configuration === null || typeof configuration !== "object" || Array.isArray(configuration))
    throw new TypeError("Invalid query binding configuration");
  const contextHandle = configuration.context;
  assertGraphOpen(state);
  const context = contexts.get(contextHandle);
  if (context === undefined || context.state !== state || !state.queryDefinitions.has(definition))
    throw new TypeError("Query binding requires its exact graph, context and declaration");
  assertContextOpen(context);
  const physical = beginPhysicalWork(context.retirement, "acquisition");
  try {
    const profile = configuration.profile;
    assertContextOpen(context);
    const fetch = configuration.fetch;
    assertContextOpen(context);
    const suppliedValue = configuration.value;
    assertContextOpen(context);
    if (
      suppliedValue !== undefined &&
      (suppliedValue === null || typeof suppliedValue !== "object" || Array.isArray(suppliedValue))
    )
      throw new TypeError("Invalid query binding value configuration");
    const value =
      suppliedValue === undefined
        ? undefined
        : Object.freeze(copyOptions(suppliedValue, () => assertContextOpen(context)));
    if (
      profile === null ||
      (typeof profile !== "object" && typeof profile !== "function") ||
      typeof fetch !== "function" ||
      (value !== undefined && !isValueOptions(value, () => assertContextOpen(context)))
    )
      throw new TypeError("Invalid query binding configuration");
    const name = definition.name;
    assertContextOpen(context);
    const key = definition.key;
    assertContextOpen(context);
    const revalidateAfterMs = definition.revalidateAfterMs;
    assertContextOpen(context);
    const snapshot = value?.snapshot ?? definition.snapshot;
    assertContextOpen(context);
    const execution: QueryDefinition<QueryInput<Q>, V, QueryKey<Q>> = Object.freeze({
      kind: "query",
      name,
      key: (input: QueryInput<Q>) => Reflect.apply(key, definition, [input]) as QueryKey<Q>,
      fetch: async (input: QueryInput<Q>, options: FetchOptions): Promise<V> =>
        fetch(input, options),
      revalidateAfterMs,
      snapshot,
      equals: value?.equals,
    });
    assertContextOpen(context);
    let profiles = context.bindings.get(definition);
    if (profiles?.has(profile)) throw new TypeError("Query binding is already registered");
    const binding: QueryBindingRuntime = {
      kind: "query",
      context,
      definition,
      profile,
      execution,
      value,
      handle: undefined!,
      stores: new Map(),
      addresses: new Map(),
    };
    const member = (...args: [input?: unknown, options?: StoreOptions<V>]) => {
      assertContextOpen(context);
      const physical = beginPhysicalWork(context.retirement, "acquisition");
      try {
        const call = normalizeQueryCall(args[0], args[1], args.length > 1, () =>
          assertContextOpen(context),
        );
        const selectedOptions =
          call.options === undefined
            ? undefined
            : copyOptions(call.options, () => assertContextOpen(context));
        assertContextOpen(context);
        if (
          selectedOptions !== undefined &&
          !isStoreOptions(selectedOptions, () => assertContextOpen(context))
        )
          throw new TypeError("Invalid query options.");
        if (
          value !== undefined &&
          Object.hasOwn(value, "initial") &&
          selectedOptions !== undefined &&
          Object.hasOwn(selectedOptions, "initial") &&
          !Object.is(value.initial, selectedOptions.initial)
        )
          throw new TypeError("Initial value conflicts with query binding configuration");
        const options = selectedOptions as StoreOptions<V> | undefined;
        assertContextOpen(context);
        return makeQueryStore(state, execution, call.input, options, binding, value);
      } finally {
        finishPhysicalWork(physical);
      }
    };
    const handle = Object.freeze({
      graph: graph.id,
      context: context.handle,
      profile,
      definition,
      member,
      target: (store: QueryStore<V>) => captureTarget<Q, V>(binding, store),
    }) as QueryBinding<Q, V>;
    binding.handle = handle;
    bindings.set(handle, binding);
    if (profiles === undefined) {
      profiles = new Map();
      context.bindings.set(definition, profiles);
    }
    profiles.set(profile, binding);
    return handle;
  } finally {
    finishPhysicalWork(physical);
  }
}
