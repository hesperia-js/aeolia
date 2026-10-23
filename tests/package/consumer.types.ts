/* Typecheck-only fixture. The function is never called, so importing this file
 * does not open a stream or make a request. */
import {
  adopt,
  AEOLIA_TAGGED_ENCODING,
  Signal,
  afterPropagation,
  affects,
  bindOperation,
  bindStream,
  inspectBoundAddress,
  storeKey,
  type BoundStreamSession,
  type BoundAddressStamp,
  profileEffect,
  predictionFor,
  type BindingMutationOptions,
  type TargetPrediction,
  createBindingContext,
  createGraph,
  defineContract,
  createMutation,
  createObserver,
  createQuery,
  createStore,
  createStream,
  queryOptions,
  subscribe,
  observeStore,
  peekStore,
  type ObservationLease,
  type ValueRevision,
  type AffectSpec,
  type CreateMutationInput,
  type CreateQueryInput,
  type CreateStoreInput,
  type CreateStreamInput,
  type DefineContractInput,
  type FetchOptions,
  type Graph,
  type MemberOf,
  type KeyedStores,
  type StoreNames,
  type DeclaredStores,
  type StreamStore,
  type Observer,
  type QueryStore,
  type StoreIdentity,
  type TypedTarget,
  snapshot,
  type AdoptionReport,
  type AdoptionReportV2,
  type AdoptedEntryV2,
  type AdoptOptionsV2,
  type QueryBindingHandle,
  type RawOrQuerySnapshotAddressV2,
  type Snapshot,
  type SnapshotAddress,
  type SnapshotBindingReference,
  type SnapshotEntryFieldsV2,
  type SnapshotEntryV2,
  type SnapshotLanding,
  type SnapshotOptionsV2,
  type SnapshotV2,
  type StreamBindingHandle,
  type StreamSnapshotAddressV2,
  withoutWrites,
} from "aeolia";
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

function neverStreamContractTypesOnly(): void {
  type Equal<A, B> =
    (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;
  type Input = { readonly id: string };
  const silent = createStream<Input, never, `silent/${string}`>({
    name: "silent.events",
    key: (input) => `silent/${input.id}`,
    open: async function* () {},
  });
  const hidden = createStore({ name: "silent/metadata", initial: "internal" });
  const decorated = { ...silent, metadata: { hidden } };
  const sibling = createStore({ name: "silent-sibling", initial: 1 });
  const contract = defineContract({
    namespace: "never-stream-types",
    operations: { nested: { events: decorated }, sibling },
  });
  const graph = createGraph({ contract });
  const member = graph.api.nested.events;
  const source = member({ id: "one" });
  const declared = graph.store(sibling);
  const addressed = graph.at("silent/one");

  const declarationRetained: Equal<typeof contract.operations.nested.events, typeof decorated> =
    true;
  const memberInput: Equal<Parameters<MemberOf<typeof decorated>>[0], Input> = true;
  const memberOutput: Equal<ReturnType<MemberOf<typeof decorated>>, StreamStore<never>> = true;
  const graphOutput: Equal<typeof source, StreamStore<never>> = true;
  const exactKeyedValue: Equal<
    KeyedStores<typeof decorated>,
    { readonly pattern: `silent/${string}`; readonly value: never }
  > = true;
  const exactTreeValues: Equal<
    KeyedStores<typeof contract.operations>,
    | { readonly pattern: `silent/${string}`; readonly value: never }
    | { readonly pattern: "silent-sibling"; readonly value: number }
  > = true;
  const noInternalStoreNames: Equal<StoreNames<typeof decorated>, never> = true;
  const noInternalStores: Equal<DeclaredStores<typeof decorated>, never> = true;
  const exactDeclaredStore: Equal<
    DeclaredStores<typeof contract.operations>,
    typeof sibling
  > = true;
  const siblingValue: Equal<ReturnType<typeof declared.value.get>, number | undefined> = true;
  const silentValue: Equal<ReturnType<typeof addressed.value.get>, undefined> = true;

  // @ts-expect-error Stream metadata is opaque to contract store collection.
  graph.store(hidden);
  // @ts-expect-error A never-valued member cannot accept an initial emission.
  member({ id: "one" }, { initial: "unexpected" });
  // @ts-expect-error Stream input remains the exact declared object.
  member({ id: 1 });

  const collision = createStore({ name: "silent/taken", initial: 0 });
  // @ts-expect-error A never-valued stream still reserves its narrow key pattern.
  defineContract({ namespace: "silent-collision", operations: { silent, collision } });

  const badEquals = { ...silent, equals: 1 };
  const badReturn = { ...silent, equals: (_left: never, _right: never) => "wrong" };
  const badArity = { ...silent, equals: (_left: never, _right: never, _extra: never) => true };
  const badKey = { ...silent, key: 1 };
  const badOpen = { ...silent, open: (_input: Input) => 1 };
  const badKind = { ...silent, kind: "other" };

  // @ts-expect-error Contract admission still requires a callable comparator.
  defineContract({ namespace: "bad-equals", operations: { badEquals } });
  // @ts-expect-error An erased comparator still returns boolean.
  defineContract({ namespace: "bad-return", operations: { badReturn } });
  // @ts-expect-error An erased comparator cannot require a third argument.
  defineContract({ namespace: "bad-arity", operations: { badArity } });
  // @ts-expect-error A stream key must remain callable.
  defineContract({ namespace: "bad-key", operations: { badKey } });
  // @ts-expect-error A stream source must remain an async iterable.
  defineContract({ namespace: "bad-open", operations: { badOpen } });
  // @ts-expect-error A stream retains its actual operation discriminator.
  defineContract({ namespace: "bad-kind", operations: { badKind } });

  createStream({
    name: "factory-domain",
    key: (input: Input) => `factory/${input.id}`,
    open: backend.stream<Input, number>("factory-domain"),
    // @ts-expect-error Equality cannot change the source's inferred numeric value domain.
    equals: (left: string, right: string) => left === right,
  });
  createStream<Input, never, string>({
    name: "factory-never",
    key: (input) => `factory-never/${input.id}`,
    // @ts-expect-error A never-valued stream cannot produce numeric emissions.
    open: async function* () {
      yield 1;
    },
  });

  void [
    declarationRetained,
    memberInput,
    memberOutput,
    graphOutput,
    exactKeyedValue,
    exactTreeValues,
    noInternalStoreNames,
    noInternalStores,
    exactDeclaredStore,
    siblingValue,
    silentValue,
  ];
  graph.dispose();
}

void neverStreamContractTypesOnly;

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
  const lease: ObservationLease<{ label: string }> = observeStore(graph, handle);
  lease.publish({ label: "observed" }, { transfer: "eligible" });
  // @ts-expect-error Observation values retain the completed profile's type.
  lease.publish({ id: "raw" });
  // @ts-expect-error Transfer provenance has an explicit finite literal domain.
  lease.publish({ label: "bad" }, { transfer: "automatic" });
  // @ts-expect-error Available metadata is read-only.
  handle.available.set(true);
  // @ts-expect-error Issue metadata is read-only.
  lease.issue.set({ invalid: true });
  // @ts-expect-error Activity metadata is read-only.
  lease.active.set(false);
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

function transferSnapshotTypesOnly(): void {
  const query = createQuery({
    name: "transfer.query",
    key: (input: { readonly key: string }) => input.key,
    fetch: async (input: { readonly key: string }) => input.key,
  });
  const stream = createStream({
    name: "transfer.stream",
    key: (input: { readonly key: string }) => input.key,
    open: async function* (input: { readonly key: string }) {
      yield input.key;
    },
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "typed-transfer", operations: { query, stream } }),
  });
  const context = createBindingContext(graph);
  const queryBinding = bindOperation(graph, query, {
    context,
    profile: {},
    fetch: query.fetch,
    value: { snapshot: true },
  });
  const streamBinding = bindStream(graph, stream, graph.api.stream, { context });
  const queryHandle: QueryBindingHandle = queryBinding;
  const streamHandle: StreamBindingHandle = streamBinding;
  const queryReference = { operation: query.name, profile: { result: "typed" } };
  const streamReference = { operation: stream.name, profile: { channel: "typed" } };
  const queryMapping: SnapshotBindingReference = {
    binding: queryHandle,
    reference: queryReference,
  };
  const streamMapping: SnapshotBindingReference = {
    binding: streamHandle,
    reference: streamReference,
  };
  const references: readonly SnapshotBindingReference[] = [queryMapping, streamMapping];
  const writeOptions: SnapshotOptionsV2 = { version: 2, bindings: references };
  const v1: Snapshot = snapshot(graph);
  const v1Report: AdoptionReport = adopt(graph, v1);
  const v2: SnapshotV2 = snapshot(graph, writeOptions);
  const rawOnlyV2: SnapshotV2 = snapshot(graph, { version: 2 });
  const readOptions: AdoptOptionsV2 = { bindings: references };
  const v2Report: AdoptionReportV2 = adopt(graph, v2, readOptions);
  const rawOnlyReport: AdoptionReportV2 = adopt(graph, rawOnlyV2, {});

  const rawAddress: RawOrQuerySnapshotAddressV2 = { kind: "raw", key: "same" };
  const queryAddress: RawOrQuerySnapshotAddressV2 = {
    kind: "binding",
    reference: queryReference,
    key: "same",
  };
  const streamAddress: StreamSnapshotAddressV2 = {
    kind: "stream-binding",
    reference: streamReference,
    key: "same",
  };
  const combinedAddress: SnapshotAddress = queryAddress;
  const fields = null as unknown as SnapshotEntryFieldsV2;
  const noLanding: SnapshotLanding = { kind: "none" };
  const acceptedLanding: SnapshotLanding = { kind: "accepted", age: 0 };
  const rawEntry: SnapshotEntryV2 = {
    ...fields,
    address: rawAddress,
    landing: noLanding,
  };
  const queryEntry: SnapshotEntryV2 = {
    ...fields,
    address: queryAddress,
    landing: acceptedLanding,
  };
  type StreamSnapshotEntry = Extract<
    SnapshotEntryV2,
    { readonly address: StreamSnapshotAddressV2 }
  >;
  const streamEntry: SnapshotEntryV2 = {
    ...fields,
    address: streamAddress,
    landing: { kind: "accepted", age: 0 },
  };
  const identity = null as unknown as StoreIdentity;
  const rawAdopted: AdoptedEntryV2 = { address: rawAddress, identity };
  const queryAdopted: AdoptedEntryV2 = { address: queryAddress, identity };
  const streamAdopted: AdoptedEntryV2 = {
    address: streamAddress,
    identity,
    valueRevision: fields.valueRevision,
  };
  type StreamAdopted = Extract<AdoptedEntryV2, { readonly address: StreamSnapshotAddressV2 }>;
  let streamRevision: ValueRevision | undefined;
  for (const entry of v2Report.adopted)
    if (isStreamAdopted(entry)) streamRevision = entry.valueRevision;
  const constructedV2: SnapshotV2 = {
    version: 2,
    namespace: "typed-transfer",
    encoding: AEOLIA_TAGGED_ENCODING,
    entries: [rawEntry, queryEntry, streamEntry],
  };
  const constructedReport: AdoptionReportV2 = {
    version: 2,
    adopted: [rawAdopted, queryAdopted, streamAdopted],
    skipped: [],
  };

  // @ts-expect-error Version 2 must be selected explicitly.
  snapshot(graph, { bindings: references });
  // @ts-expect-error Snapshot version 2 requires its adoption options argument.
  adopt(graph, v2);
  // @ts-expect-error Version 1 keeps its one-argument adoption form.
  adopt(graph, v1, {});
  // @ts-expect-error Unsupported snapshot versions have no overload.
  snapshot(graph, { version: 3 });
  // @ts-expect-error Stream entries require an accepted landing.
  const rejectedStreamLanding: StreamSnapshotEntry["landing"] = { kind: "none" };
  // @ts-expect-error A stream adoption row must report its local value revision.
  const missingStreamRevision: AdoptedEntryV2 = { address: streamAddress, identity };
  // @ts-expect-error Query and stream handles carry distinct native associations.
  const wrongStreamHandle: StreamBindingHandle = queryHandle;

  function isStreamAdopted(entry: AdoptedEntryV2): entry is StreamAdopted {
    return entry.address.kind === "stream-binding";
  }

  function adoptByVersion(
    target: Graph,
    value: Snapshot | SnapshotV2,
  ): AdoptionReport | AdoptionReportV2 {
    if (value.version === 1) return adopt(target, value);
    return adopt(target, value, { bindings: references });
  }

  function rejectUndiscriminatedVersion(target: Graph, value: Snapshot | SnapshotV2): void {
    // @ts-expect-error Callers narrow version 1 or version 2 before adoption.
    adopt(target, value);
  }

  void [
    v1Report,
    rawOnlyReport,
    constructedV2,
    constructedReport,
    combinedAddress,
    streamRevision,
    rejectedStreamLanding,
    missingStreamRevision,
    wrongStreamHandle,
    adoptByVersion,
    rejectUndiscriminatedVersion,
  ];
}

void transferSnapshotTypesOnly;
