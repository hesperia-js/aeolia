/* Typecheck-only fixture. The function is never called, so importing this file
 * does not open a stream or make a request. */
import {
  Signal,
  afterPropagation,
  affects,
  bindOperation,
  bindStream,
  inspectBoundAddress,
  peekStore,
  storeKey,
  createBindingContext,
  profileEffect,
  predictionFor,
  type BindingContext,
  type BindingMutationOptions,
  type BoundAddressStamp,
  type BoundStreamSession,
  type Graph,
  type QueryStore,
  type StoreIdentity,
  type TargetPrediction,
  type TypedTarget,
  type ValueRevision,
  createGraph,
  defineContract,
  createMutation,
  createObserver,
  createQuery,
  createStore,
  createStream,
  queryOptions,
  subscribe,
  type AffectSpec,
  type CreateMutationInput,
  type CreateQueryInput,
  type CreateStoreInput,
  type CreateStreamInput,
  type DefineContractInput,
  type FetchOptions,
  type Observer,
  withoutWrites,
} from "aeolia";
import type {
  BindingContext as ContractBindingContext,
  Graph as ContractGraph,
} from "aeolia/contract";
import { testBackend } from "aeolia/testing";

const backend = testBackend();
const store = createStore({ name: "draft", initial: 0 });
const query = createQuery({
  name: "items.get",
  key: (input: { readonly id: string }) => `items/${input.id}`,
  fetch: backend.respond<{ readonly id: string }, { readonly id: string }>("items.get"),
});
const mutation = createMutation({
  name: "items.save",
  affects: [
    affects(query, {
      select: (input: { readonly id: string }) => input,
      on: "revalidate",
    }),
  ],
  run: backend.perform<{ readonly id: string }, string>("items.save"),
});
const stream = createStream({
  name: "items.events",
  key: (input: { readonly id: string }) => `events/${input.id}`,
  open: backend.stream<{ readonly id: string }, { readonly id: string }>("items.events"),
});
const widenedQuery = createQuery({
  name: "wide.get",
  key: (input: string): string => input,
  fetch: backend.respond<string, number>("wide.get"),
});
// @ts-expect-error A required input still needs an explicit selector.
const requiredInputEffect = affects(widenedQuery, {});
const optionalInputQuery = createQuery({
  name: "optional.get",
  key: (input: string | undefined): string => input ?? "none",
  fetch: backend.respond<string | undefined, number>("optional.get"),
});
// @ts-expect-error An optional input still needs a selector; only a void query may omit it.
const optionalInputEffect = affects(optionalInputQuery, {});
const unknownInputQuery = createQuery({
  name: "unknown.get",
  key: (input: unknown): string => String(input),
  fetch: backend.respond<unknown, number>("unknown.get"),
});
// @ts-expect-error An unknown input still needs an explicit selector.
const unknownInputEffect = affects(unknownInputQuery, {});
const inputlessQuery = createQuery<void, number, string>({
  name: "inputless.get",
  key: () => "inputless",
  fetch: async () => 1,
});
const inferredInputlessQuery = createQuery({
  name: "inferred-inputless.get",
  key: () => "inferred-inputless",
  fetch: async () => 2,
});
const optionalObjectQuery = createQuery({
  name: "optional-object.get",
  key: (input?: { readonly id: string }) => `optional-object/${input?.id ?? "none"}`,
  fetch: async (input?: { readonly id: string }) => input?.id.length ?? 0,
});
const inferredInputlessMutation = createMutation({
  name: "inferred-inputless.save",
  affects: [],
  run: async () => "saved",
});
const inputlessEffect = affects(inputlessQuery, {});
const contract = defineContract({
  namespace: "types",
  operations: {
    store,
    query,
    mutation,
    stream,
    inferredInputlessQuery,
    optionalObjectQuery,
    inferredInputlessMutation,
  },
});
const widenedContract = defineContract({
  namespace: "types-wide",
  operations: { widenedQuery },
});

const collidingStore = createStore({ name: "users/1", initial: 0 });
const collidingQuery = createQuery({
  name: "users.get",
  key: (input: { readonly id: string }) => `users/${input.id}`,
  fetch: backend.respond<{ readonly id: string }, number>("users.get"),
});
// @ts-expect-error A declared store name cannot collide with a query key pattern.
const collidingContract = defineContract({
  namespace: "negative",
  operations: { collidingStore, collidingQuery },
});

function typecheckOnly(): void {
  const signalOptions: Signal.SignalOptions<number> = {
    [Signal.subtle.watched]() {
      void (this satisfies Signal<number>);
    },
  };
  const state: Signal.State<number> = new Signal.State(1, signalOptions);
  const derived: Signal.Computed<number> = new Signal.Computed(() => state.get());
  const readable: Signal<number> = derived;
  const watcher: Signal.subtle.Watcher = new Signal.subtle.Watcher(() => {});
  watcher.watch(readable);
  watcher.unwatch(readable);
  let controlled!: Observer;
  controlled = createObserver({
    notify() {
      afterPropagation(() => {
        if (controlled.check()) controlled.track(() => readable.get());
      });
    },
  });
  controlled.track(() => readable.get());
  controlled.dispose();
  withoutWrites(() => {
    void readable.get();
  });
  const storeInput: CreateStoreInput<number, "draft"> = store;
  const queryInput: CreateQueryInput<{ readonly id: string }, { readonly id: string }, string> =
    query;
  const mutationInput: CreateMutationInput<{ readonly id: string }, string> = mutation;
  const streamInput: CreateStreamInput<{ readonly id: string }, { readonly id: string }, string> =
    stream;
  const contractInput: DefineContractInput<typeof contract.operations> = contract;
  const erasedAffectSpec: AffectSpec<{ readonly id: string }, unknown, unknown> =
    mutation.affects[0]!;
  const graph = createGraph({ contract });
  const retirement: Promise<void> = graph.retire();
  void retirement;
  const declared = graph.api.store();
  const queried = graph.api.query({ id: "1" });
  graph.api.query({ id: "2" }, undefined);
  const changed = graph.api.mutation({ id: "1" });
  const streamed = graph.api.stream({ id: "1" });
  const inferredQuery = graph.api.inferredInputlessQuery();
  graph.api.inferredInputlessQuery(queryOptions({}));
  graph.api.inferredInputlessQuery(queryOptions({ initial: 2 }));
  graph.api.inferredInputlessQuery(queryOptions({ initial: 2 }), { revalidateAfterMs: 20 });
  const readonlyQueryOptions = queryOptions({ initial: 2 });
  // @ts-expect-error Branded options are a readonly shallow copy.
  readonlyQueryOptions.initial = 3;
  // @ts-expect-error The branded option retains the query's number value type.
  graph.api.inferredInputlessQuery(queryOptions({ initial: "wrong" }));
  const inferredMutation = graph.api.inferredInputlessMutation();
  graph.api.inferredInputlessMutation(queryOptions({}));
  graph.api.inferredInputlessMutation(queryOptions({}), {
    abortSignal: new AbortController().signal,
  });
  graph.api.optionalObjectQuery(undefined);
  graph.api.optionalObjectQuery({ id: "1" });
  // @ts-expect-error An optional object callback remains an input-bearing query.
  graph.api.optionalObjectQuery();
  // @ts-expect-error A branded options object is not an optional object input.
  graph.api.optionalObjectQuery(queryOptions({}));
  const addressed = graph.at("items/1");
  const unknown = graph.at("not-declared");

  void storeInput;
  void queryInput;
  void mutationInput;
  void streamInput;
  void contractInput;
  void erasedAffectSpec;
  const widenedGraph = createGraph({ contract: widenedContract });
  const widened = widenedGraph.at("anything");
  const fetchOptions: FetchOptions = {
    abortSignal: new AbortController().signal,
    graph: graph.id,
    key: queried.key,
  };

  void declared.value.get();
  void queried.status.get();
  void inferredQuery.value.get();
  void inferredMutation;
  const ready: Promise<{ readonly id: string }> = queried.ready;
  void ready;
  const stopValues = subscribe(queried.value, (value) => {
    void (value satisfies { readonly id: string } | undefined);
  });
  stopValues();
  const stopStoreValues = subscribe(queried, (value) => {
    void (value satisfies { readonly id: string });
  });
  stopStoreValues();
  const stopStreamValues = subscribe(streamed, (value) => {
    void (value satisfies { readonly id: string });
  });
  stopStreamValues();
  void queried.revalidate(fetchOptions);
  void changed;
  void streamed.pending.get();
  void addressed.value.get();

  type Equal<A, B> =
    (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;
  type AddressedValue = ReturnType<typeof addressed.value.get>;
  type DeclaredValue = ReturnType<typeof declared.value.get>;
  type UnknownValue = ReturnType<typeof unknown.value.get>;
  type WidenedValue = ReturnType<typeof widened.value.get>;
  const addressedIsTyped: Equal<AddressedValue, { readonly id: string } | undefined> = true;
  const declaredSurvivesWidePattern: Equal<DeclaredValue, number | undefined> = true;
  const widenedPatternClaimsNothing: Equal<UnknownValue, unknown> = true;
  const widenedAtIsUnknown: Equal<WidenedValue, unknown> = true;
  void addressedIsTyped;
  void declaredSurvivesWidePattern;
  void widenedPatternClaimsNothing;
  void widenedAtIsUnknown;

  // @ts-expect-error graph.at returns Store, not QueryStore.
  void unknown.status.get();
  // @ts-expect-error StreamStore has no revalidate method.
  void streamed.revalidate();
  // @ts-expect-error Readiness belongs to a query, not to an unbounded stream.
  void streamed.ready;
  // @ts-expect-error Query readiness is read-only.
  queried.ready = Promise.resolve({ id: "1" });
  // @ts-expect-error Stream member options do not accept a freshness window.
  void graph.api.stream({ id: "1" }, { revalidateAfterMs: 1000 });
  const foreign = createStore({ name: "foreign", initial: 1 });
  // @ts-expect-error A graph accepts only store definitions declared by its contract.
  void graph.store(foreign);
}

void collidingContract;
void optionalInputEffect;
void inputlessEffect;
void requiredInputEffect;
void unknownInputEffect;
void typecheckOnly;

function bindingTypesOnly(): void {
  const graph = createGraph({
    contract: defineContract({
      namespace: "bound-consumer",
      operations: { query, inputlessQuery, unknownInputQuery },
    }),
  });
  const context = createBindingContext(graph);
  const binding = bindOperation(graph, query, {
    context,
    profile: {},
    fetch: async (input) => ({ label: input.id }),
    value: { initial: { label: "loading" }, equals: (left, right) => left.label === right.label },
  });
  const handle: QueryStore<{ label: string }> = binding.member({ id: "one" });
  // @ts-expect-error Available metadata is read-only.
  handle.available.set(true);
  const capture = peekStore(handle);
  // @ts-expect-error Changed and absent captures do not expose a value.
  void capture.value;
  if (capture.kind === "stable") {
    const revision: ValueRevision = capture.valueRevision;
    // @ts-expect-error Numeric values cannot fabricate native revision provenance.
    const fabricated: ValueRevision = 1;
    if (capture.present) {
      const label: string = capture.value.label;
      void label;
    }
    void [revision, fabricated];
  }
  const identity: StoreIdentity = handle.identity;
  const completed: Promise<{ label: string }> = handle.ready;
  const ticket = binding.target(handle)!;
  const same: TypedTarget<typeof query, { label: string }> | undefined = ticket.forBinding(binding);
  // @ts-expect-error The native definition still requires its object input.
  binding.member("one");
  // @ts-expect-error Initial values describe the completed profile value.
  binding.member({ id: "one" }, { initial: { id: "raw" } });
  // @ts-expect-error A raw query store has a different value association.
  binding.target(graph.api.query({ id: "one" }));
  // @ts-expect-error Invariant ticket values cannot be widened to unknown.
  const widened: TypedTarget<typeof query, unknown> = ticket;
  const ping = bindOperation(graph, inputlessQuery, {
    context,
    profile: {},
    fetch: async () => "pong",
  });
  ping.member();
  ping.member(queryOptions({ initial: "waiting" }), { revalidateAfterMs: 1 });
  const unknown = bindOperation(graph, unknownInputQuery, {
    context,
    profile: {},
    fetch: async () => 1,
  });
  unknown.member(undefined, {});
  // @ts-expect-error Unknown remains input-bearing after binding.
  unknown.member();
  void [identity, completed, same, widened];
}

void bindingTypesOnly;

function mutationBindingTypesOnly(): void {
  const query = createQuery({
    name: "typed.query",
    key: (input: { readonly id: string }) => input.id,
    fetch: async (input) => ({ id: input.id }),
  });
  const save = createMutation({
    name: "typed.save",
    run: async (input: { readonly id: string }) => input.id,
    affects: [affects(query, { select: (input: { readonly id: string }) => input })],
  });
  const ping = createMutation({ name: "typed.ping", run: async () => 1, affects: [] });
  const unknown = createMutation({
    name: "typed.unknown",
    run: async (input: unknown) => input,
    affects: [],
  });
  const union = createMutation({
    name: "typed.union",
    run: async (input: void | string) => input,
    affects: [],
  });
  const graph = createGraph({
    contract: defineContract({
      namespace: "typed-mutation",
      operations: { query, save, ping, unknown, union },
    }),
  });
  const context = createBindingContext(graph);
  const native = bindOperation(graph, query, { context, profile: {}, fetch: query.fetch });
  const labels = bindOperation(graph, query, {
    context,
    profile: {},
    fetch: async (input) => input.id,
  });
  const numbers = bindOperation(graph, query, {
    context,
    profile: {},
    fetch: async (input) => input.id.length,
  });
  const label = labels.member({ id: "one" });
  const numeric = numbers.member({ id: "one" });
  const override: TargetPrediction<typeof save> = predictionFor(
    save,
    numbers.target(numeric)!,
    (current, input) => (current ?? 0) + input.id.length,
  );
  // @ts-expect-error The ticket fixes the output; a producer cannot widen it to string.
  predictionFor(save, numbers.target(numeric)!, () => "wrong");
  // @ts-expect-error The producer receives the original mutation's native input.
  predictionFor(save, numbers.target(numeric)!, (_value, input: string) => input.length);
  // @ts-expect-error Invariant targets cannot be widened into unknown-valued targets.
  const widened: TypedTarget<typeof query, unknown> = numbers.target(numeric)!;
  const effect = profileEffect(save, 0, query, {
    prepare: (phase) =>
      phase.existing(phase.selectedInput).map((target) => {
        const nativeValue = target.forBinding(native);
        if (nativeValue !== undefined) return phase.original(nativeValue);
        const text = target.forBinding(labels);
        if (text !== undefined) {
          // @ts-expect-error An attached string completion is not the original native object value.
          phase.original(text);
          // @ts-expect-error A typed prediction producer cannot return a different output type.
          phase.predict(text, () => 1);
          return phase.predict(text, (_current, input) => input.id);
        }
        return phase.prepare(target);
      }),
    settle: (phase) =>
      phase
        .existing(phase.selectedInput)
        .map((target) => phase.selected(target, phase.selectedInput)),
  });
  const binding = bindOperation(graph, save, {
    context,
    profile: {},
    effects: [effect],
    run: async (input, call) => ({ receipt: await save.run(input, call) }),
  });
  const result: Promise<{ receipt: string }> = binding.member(
    { id: "one" },
    { optimistic: [override] },
  );
  // @ts-expect-error Completed mutation output is not the original raw string result.
  const raw: Promise<string> = binding.member({ id: "one" });
  // @ts-expect-error Original native input remains required.
  binding.member();
  // @ts-expect-error Logical-key optimistic maps are not identity-addressed instructions.
  binding.member({ id: "one" }, { optimistic: new Map() });
  const inputless = bindOperation(graph, ping, {
    context,
    profile: {},
    effects: [],
    run: async () => ({ pong: true }),
  });
  const options = queryOptions<BindingMutationOptions<typeof ping>>({ optimistic: [] });
  inputless.member();
  inputless.member(options, { optimistic: [] });
  const required = bindOperation(graph, unknown, {
    context,
    profile: {},
    effects: [],
    run: unknown.run,
  });
  required.member(undefined, {});
  // @ts-expect-error Unknown input does not gain inputless forms.
  required.member();
  const voidUnion = bindOperation(graph, union, {
    context,
    profile: {},
    effects: [],
    run: union.run,
  });
  voidUnion.member();
  voidUnion.member("value");
  voidUnion.member(undefined, {});
  void [label, result, raw, widened];
}

void mutationBindingTypesOnly;

function boundStreamConsumer(): void {
  const graph = createGraph({
    contract: defineContract({ namespace: "typed-stream", operations: { stream, query } }),
  });
  const context = createBindingContext(graph);
  const binding = bindStream(graph, stream, graph.api.stream, { context });
  const inspected = inspectBoundAddress(binding, storeKey("events/one"));
  if (inspected.kind === "stable") {
    const stamp: BoundAddressStamp = inspected.stamp;
    inspectBoundAddress(binding, storeKey("events/one"), { expected: stamp });
    if (inspected.state === "empty") {
      const source = binding.member({ id: "one" }, { expectEmpty: inspected.condition });
      const session: BoundStreamSession<{ readonly id: string }> = binding.session(source);
      session.detached.then((receipt) => {
        if (receipt.terminalCapture.kind === "captured") {
          const capture = receipt.terminalCapture.capture;
          if (capture.kind === "stable" && capture.present) {
            const id: string = capture.value.id;
            void id;
          }
        }
      });
      // @ts-expect-error The raw member has no empty-condition contract.
      graph.api.stream({ id: "one" }, { expectEmpty: inspected.condition });
      // @ts-expect-error The query member has no empty-condition contract.
      graph.api.query({ id: "one" }, { expectEmpty: inspected.condition });
      const boundQuery = bindOperation(graph, query, { context, profile: {}, fetch: query.fetch });
      // @ts-expect-error A bound query retains its existing query options.
      boundQuery.member({ id: "one" }, { expectEmpty: inspected.condition });
    }
  }
  // @ts-expect-error Binding retains the original required input.
  binding.member({ id: 1 });
  // @ts-expect-error Stream binding requires the original stream member association.
  bindStream(graph, stream, graph.api.query, { context });
  // @ts-expect-error Empty conditions cannot be fabricated structurally.
  binding.member({ id: "one" }, { expectEmpty: {} });
  graph.dispose();
}

void boundStreamConsumer;

function publicRetirementTypes(
  graph: Graph,
  context: BindingContext,
  contractGraph: ContractGraph,
  contractContext: ContractBindingContext,
): void {
  const rootGraphRetirement: Promise<void> = graph.retire();
  const rootContextRetirement: Promise<void> = context.retire();
  const contractGraphRetirement: Promise<void> = contractGraph.retire();
  const contractContextRetirement: Promise<void> = contractContext.retire();
  void [
    rootGraphRetirement,
    rootContextRetirement,
    contractGraphRetirement,
    contractContextRetirement,
  ];
}

void publicRetirementTypes;
