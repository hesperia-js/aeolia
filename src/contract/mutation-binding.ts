import type {
  CallContext,
  Graph,
  GraphId,
  MutationDefinition,
  MutationOptions,
  QueryDefinition,
} from "./types.ts";
import type { BindingContext, ExistingTarget, TypedTarget } from "./query-binding.ts";
import type { queryOptions } from "./query-options.ts";
import { isMutationDefinition } from "./types.ts";
import { isAbortSignal } from "../utils.ts";
import { normalizeMutationArguments } from "./engine.ts";
import { beginBoundMutation, finishBoundMutation, runBoundMutation } from "./mutation.ts";
import { assertGraphOpen } from "./faults.ts";
import {
  assertBindingContextOpen,
  bindingContextByHandle,
  captureBoundProducer,
  graphRuntimeByGraph,
  profileEffectByHandle,
  targetPredictionByHandle,
} from "./registry.ts";
import type { MutationBindingRuntime } from "./runtime.ts";

type AnyMutation = MutationDefinition<any, any>;
type AnyQuery = QueryDefinition<any, any, any>;
type MutationInput<M extends AnyMutation> = M extends MutationDefinition<infer I, any> ? I : never;
type QueryInput<Q extends AnyQuery> = Q extends QueryDefinition<infer I, any, any> ? I : never;
type QueryValue<Q extends AnyQuery> = Q extends QueryDefinition<any, infer V, any> ? V : never;
type MaybePromise<T> = T | PromiseLike<T>;
declare const instructionBrand: unique symbol;
declare const predictionBrand: unique symbol;
declare const effectBrand: unique symbol;

/** Opaque preparation instruction; only its issuing native phase may apply it. */
export interface PreparedEffect {
  readonly [instructionBrand]: "prepare";
}
/** Opaque settled instruction; only its issuing native phase may apply it. */
export interface SettledEffect {
  readonly [instructionBrand]: "settle";
}
/** Immutable mutation/target-incarnation override, with no liveness pin. */
export interface TargetPrediction<M extends AnyMutation> {
  readonly [predictionBrand]: (mutation: M) => M;
}
/** Checked association with exactly one original mutation effect position. */
export interface ProfileEffect<M extends AnyMutation> {
  readonly [effectBrand]: (mutation: M) => M;
}

/** Native authority and original inputs for one effect resolution phase. */
export interface EffectContext<M extends AnyMutation, Q extends AnyQuery> {
  /** Validated native mutation input shared with run and prediction producers. */
  readonly input: MutationInput<M>;
  /** Original selector result, before the caller's default input pipeline. */
  readonly selectedInput: QueryInput<Q>;
  /** Cancellation hint; after dispatch it does not alone revoke successful effects. */
  readonly signal: AbortSignal;
  /**
   * Finds active existing siblings in this context and original query.
   * @param input - Native query input after the caller's default pipeline.
   * @returns Fresh nonretaining incarnation tickets; missing entries stay absent.
   * @throws Original key errors, TypeError for invalid keys, or AbortError if phase authority expired.
   */
  existing(input: QueryInput<Q>): readonly ExistingTarget<Q>[];
}

/** Constructs preparation membership and compatible synchronous predictions. */
export interface PrepareEffectContext<
  M extends AnyMutation,
  Q extends AnyQuery,
> extends EffectContext<M, Q> {
  /**
   * Records membership without selecting a producer.
   * @param target - Genuine target discovered by this phase.
   * @returns An opaque instruction; constructing it publishes nothing.
   * @throws TypeError for foreign targets or completed phases; revoked authority throws AbortError.
   */
  prepare(target: ExistingTarget<Q>): PreparedEffect;
  /**
   * Requests the exact original effect's native-value producer, if declared.
   * @param target - Checked native-value binding ticket; transformed completions need predict.
   * @returns An opaque instruction; producer evaluation stays in the native owner.
   * @throws TypeError for foreign targets or completed phases; revoked authority throws AbortError.
   */
  original(target: TypedTarget<Q, QueryValue<Q>>): PreparedEffect;
  /**
   * Captures a separately declared producer for the target's exact value type.
   * @param target - Invariant typed target discovered by this phase.
   * @param producer - Synchronous value producer; native faults diagnose and omit its prediction.
   * @returns An opaque instruction without evaluating the producer.
   * @throws TypeError for malformed producers, foreign targets or completed phases; revoked authority throws AbortError.
   */
  predict<V>(
    target: TypedTarget<Q, V>,
    producer: (current: NoInfer<V> | undefined, input: MutationInput<M>) => NoInfer<V>,
  ): PreparedEffect;
}

/** Constructs original-policy refresh/invalidation using selected or current source input. */
export interface SettleEffectContext<
  M extends AnyMutation,
  Q extends AnyQuery,
> extends EffectContext<M, Q> {
  /**
   * Uses a freshly prepared selector input and the target's retained completion.
   * @param target - Genuine target discovered by this phase.
   * @param input - Already validated native query input.
   * @returns An opaque settled instruction.
   * @throws TypeError for foreign targets or completed phases; revoked authority throws AbortError.
   */
  selected(target: ExistingTarget<Q>, input: QueryInput<Q>): SettledEffect;
  /**
   * Uses the target's current validated native source at application.
   * @param target - Genuine target discovered by this phase.
   * @returns An opaque settled instruction; no borrowed source is exposed.
   * @throws TypeError for foreign targets or completed phases; revoked authority throws AbortError.
   */
  current(target: ExistingTarget<Q>): SettledEffect;
}

/** Identity-addressed overrides; raw logical-key optimistic maps are not accepted. */
export type BindingMutationOptions<M extends AnyMutation> = Omit<MutationOptions, "optimistic"> & {
  /** Exact mutation/target instructions; duplicate physical identities fail before dispatch. */
  readonly optimistic?: readonly TargetPrediction<M>[];
};
/** Native private option brand for the inputless first-argument form. */
export type BrandedBindingMutationOptions<M extends AnyMutation> = ReturnType<
  typeof queryOptions<BindingMutationOptions<M>>
>;
/** Mutation call forms retain the original input and exact completed result. */
export type BoundMutationMember<M extends AnyMutation, R> = ((
  input: MutationInput<M>,
  options?: BindingMutationOptions<M>,
) => Promise<R>) &
  ([MutationInput<M>] extends [void | undefined]
    ? (() => Promise<R>) &
        ((options: BindingMutationOptions<M>) => Promise<R>) &
        ((
          options: BrandedBindingMutationOptions<M>,
          overrides?: BindingMutationOptions<M>,
        ) => Promise<R>)
    : {});

/** Context-owned native execution for a captured mutation plan. */
export interface MutationBinding<M extends AnyMutation, R> {
  /** Actual owning graph identity. */
  readonly graph: GraphId;
  /** Fixed native lifetime; retirement never redirects to another context. */
  readonly context: BindingContext;
  /** Exact original mutation included in the graph's contract. */
  readonly definition: M;
  /** Opaque execution selection supplied by the caller, including its plan identity. */
  readonly profile: object;
  /** Runs native preparation, completion and successful effect settlement. */
  readonly member: BoundMutationMember<M, R>;
}
/** Captured run completion and complete ordered original-effect associations. */
export interface MutationBindingConfiguration<M extends AnyMutation, R> {
  /** Active context on the same actual graph. */
  readonly context: BindingContext;
  /** Stable execution token; duplicate registration always fails. */
  readonly profile: object;
  /**
   * Runs/completes one invocation before successful native effects.
   * @param input - Validated native mutation input.
   * @param context - Native transport cancellation and graph identity.
   * @returns Completed result or thenable; rejection removes predictions without replay.
   */
  readonly run: (input: MutationInput<M>, context: CallContext) => MaybePromise<R>;
  /** One checked descriptor per original effect position, once each and in order. */
  readonly effects: readonly ProfileEffect<M>[];
}

/**
 * Captures typed resolution for one exact original mutation/effect/query association.
 * @param mutation - Original mutation; its selectors, producer and on policy are preserved.
 * @param effectIndex - Nonnegative integer position in mutation.affects.
 * @param query - Exact original query at that position.
 * @param resolver - Preparation and successful-settlement resolvers, invoked independently.
 * @returns An immutable descriptor; construction invokes neither resolver nor selector.
 * @throws TypeError for malformed configuration or a mismatched original association.
 */
export function profileEffect<M extends AnyMutation, Q extends AnyQuery>(
  mutation: M,
  effectIndex: number,
  query: Q,
  resolver: {
    readonly prepare: (
      phase: PrepareEffectContext<M, Q>,
    ) => MaybePromise<readonly PreparedEffect[]>;
    readonly settle: (phase: SettleEffectContext<M, Q>) => MaybePromise<readonly SettledEffect[]>;
  },
): ProfileEffect<M> {
  if (
    !isMutationDefinition(mutation) ||
    !Number.isInteger(effectIndex) ||
    effectIndex < 0 ||
    mutation.affects[effectIndex]?.query !== query ||
    resolver === null ||
    typeof resolver !== "object"
  )
    throw new TypeError("Effect requires its exact original mutation position and query");
  const affected = mutation.affects[effectIndex]!;
  const prepare = resolver.prepare;
  const settle = resolver.settle;
  if (typeof prepare !== "function" || typeof settle !== "function")
    throw new TypeError("Effect requires preparation and settlement resolvers");
  const handle = Object.freeze({}) as ProfileEffect<M>;
  profileEffectByHandle.set(handle, {
    mutation,
    index: effectIndex,
    query,
    affected,
    // The native phase is minted only for this checked declaration association.
    prepare: (phase) => prepare(phase as PrepareEffectContext<M, Q>),
    settle: (phase) => settle(phase as SettleEffectContext<M, Q>),
  });
  return handle;
}

/**
 * Constructs an override for an existing target incarnation without evaluating its producer.
 * @param mutation - Exact original mutation affecting this target's query.
 * @param target - Genuine invariant binding ticket; old tickets never retarget after reset.
 * @param producer - Synchronous producer for exactly the target value and native mutation input.
 * @returns An immutable instruction with no retention pin.
 * @throws TypeError for foreign/malformed associations; disposed lifetimes throw Fault.
 */
export function predictionFor<M extends AnyMutation, Q extends AnyQuery, V>(
  mutation: M,
  target: TypedTarget<Q, V>,
  producer: (current: NoInfer<V> | undefined, input: MutationInput<M>) => NoInfer<V>,
): TargetPrediction<M> {
  const prediction = captureBoundProducer(mutation, target, producer);
  const handle = Object.freeze({}) as TargetPrediction<M>;
  targetPredictionByHandle.set(handle, prediction);
  return handle;
}

/** @internal Registers the mutation branch of the public adjacent bindOperation overloads. */
export function bindMutationOperation<M extends AnyMutation, R>(
  graph: Graph,
  definition: M,
  configuration: MutationBindingConfiguration<M, R>,
): MutationBinding<M, R> {
  const state = graphRuntimeByGraph.get(graph);
  if (state === undefined) throw new TypeError("The value is not an Aeolia graph");
  assertGraphOpen(state);
  if (configuration === null || typeof configuration !== "object" || Array.isArray(configuration))
    throw new TypeError("Invalid mutation binding configuration");
  const context = bindingContextByHandle.get(configuration.context);
  if (
    context === undefined ||
    context.state !== state ||
    !state.mutationDefinitions.has(definition)
  )
    throw new TypeError("Mutation binding requires its exact graph, context and declaration");
  assertBindingContextOpen(context);
  const profile = configuration.profile;
  const run = configuration.run;
  const suppliedEffects = configuration.effects;
  if (
    profile === null ||
    (typeof profile !== "object" && typeof profile !== "function") ||
    typeof run !== "function" ||
    !Array.isArray(suppliedEffects)
  )
    throw new TypeError("Invalid mutation binding configuration");
  const effects = [...suppliedEffects].map((effect, index) => {
    const checked = profileEffectByHandle.get(effect);
    if (
      checked === undefined ||
      checked.mutation !== definition ||
      checked.index !== index ||
      checked.query !== definition.affects[index]?.query ||
      checked.affected !== definition.affects[index]
    )
      throw new TypeError("Mutation effects must cover exact original positions in order");
    return checked;
  });
  if (effects.length !== definition.affects.length)
    throw new TypeError("Mutation effect plan is incomplete");
  assertBindingContextOpen(context);
  let profiles = context.mutations.get(definition);
  if (profiles?.has(profile)) throw new TypeError("Mutation binding is already registered");
  const binding: MutationBindingRuntime = {
    context,
    definition,
    profile,
    effects: Object.freeze(effects),
    run: (input, call) => run(input as MutationInput<M>, call),
  };
  const member = (...args: [input?: unknown, options?: BindingMutationOptions<M>]): Promise<R> => {
    const attempt = beginBoundMutation(binding);
    try {
      const call = normalizeMutationArguments(args[0], args[1], args.length > 1);
      if (
        call.options !== undefined &&
        (call.options === null || typeof call.options !== "object" || Array.isArray(call.options))
      )
        throw new TypeError("Invalid bound mutation options");
      const signal: unknown =
        call.options === undefined ? undefined : Reflect.get(call.options, "abortSignal");
      const overrides: unknown =
        call.options === undefined ? undefined : Reflect.get(call.options, "optimistic");
      if (
        (signal !== undefined && !isAbortSignal(signal)) ||
        (overrides !== undefined && !Array.isArray(overrides))
      )
        throw new TypeError("Invalid bound mutation options");
      const predictions = overrides === undefined ? [] : [...overrides];
      return runBoundMutation(attempt, call.input, signal, predictions) as Promise<R>;
    } catch (error) {
      finishBoundMutation(attempt);
      throw error;
    }
  };
  const handle = Object.freeze({
    graph: state.graph.id,
    context: context.handle,
    definition,
    profile,
    member,
  }) as MutationBinding<M, R>;
  if (profiles === undefined) {
    profiles = new Map();
    context.mutations.set(definition, profiles);
  }
  profiles.set(profile, binding);
  return handle;
}
