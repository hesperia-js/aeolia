/* Typecheck-only fixture. The function is never called, so importing this file
 * does not open a stream or make a request. */
import {
  adopt,
  captureReactiveConstruction,
  createTransferScope,
  preparePassiveTransferClaim,
  prepareRequiredTransferAdoption,
  prepareRequiredTransferCapture,
  prepareTransferOperation,
  prepareTransferOperationRetirement,
  prepareTransferScopeSuccessors,
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
  type BindingContext,
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
  createPositionedStream,
  queryOptions,
  subscribe,
  projectPositioned,
  project,
  type ProjectionPolicy,
  capturePositionedProjection,
  capturePositionedStore,
  isPositionedStoreCheckpointCurrent,
  preparePositionedStoreRestore,
  preparePositionedRestore,
  preparePositionedSeedRestore,
  positionedFoldReceipt,
  acknowledgePositionedFold,
  observeQueryProgress,
  observeStore,
  observePositionedStoreCaptureState,
  peekStore,
  type ObservationLease,
  type ValueRevision,
  type AffectSpec,
  type CreateMutationInput,
  type CreateQueryInput,
  type CreateStoreInput,
  type CreateStreamInput,
  type Computed,
  type DefineContractInput,
  type FetchOptions,
  type QueryAcquisition,
  type QueryFetchOptions,
  type QueryProgressEvent,
  type QueryProgressObservation,
  type QueryProgressSnapshot,
  type QueryProgressStatus,
  type Graph,
  type MemberOf,
  type KeyedStores,
  type StoreNames,
  type DeclaredStores,
  type StreamStore,
  type PositionedStreamEvent,
  type PositionedStreamSource,
  type PositionedStreamOpenOptions,
  type PositionedProjectionCheckpoint,
  type PositionedFoldReceipt,
  type PositionedRestoreSeed,
  type PositionedStoreCaptureNotice,
  type PositionedStoreCheckpoint,
  type PositionedStoreRestoreSeed,
  type PositionedStoreRestoration,
  type Observer,
  type QueryStore,
  type Readable,
  type StoreIdentity,
  type TypedTarget,
  type Unsubscribe,
  snapshot,
  type AdoptionReport,
  type AdoptionReportV2,
  type ExistingNativeValueOptions,
  type MutationTransferSource,
  type NativeStoreFor,
  type NativeTransferSource,
  type PassiveTransferClaim,
  type QueryTransferSource,
  type ReactiveConstruction,
  type RequiredTransferAdoptionOptions,
  type RequiredTransferCaptureOptions,
  type RequiredTransferCodec,
  type TransferCommit,
  type TransferNotifications,
  type TransferOperation,
  type TransferOperationDispatch,
  type TransferPublication,
  type TransferScope,
  type TransferScopeSuccessor,
  type TransferSlot,
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
import { observePositionedStoreCaptureState as observeContractPositionedStoreCaptureState } from "aeolia/contract";
import type {
  BindingContext as ContractBindingContext,
  Graph as ContractGraph,
  QueryAcquisition as ContractQueryAcquisition,
  PositionedStoreCaptureNotice as ContractPositionedStoreCaptureNotice,
  PositionedStoreCheckpoint as ContractPositionedStoreCheckpoint,
  PositionedStoreRestoreSeed as ContractPositionedStoreRestoreSeed,
  PositionedStoreRestoration as ContractPositionedStoreRestoration,
} from "aeolia/contract";
import { testBackend } from "aeolia/testing";

const backend = testBackend();
const store = createStore({ name: "draft", initial: 0 });
const queryAcquisition: QueryAcquisition = "fetch";
const query = createQuery({
  name: "items.get",
  acquisition: queryAcquisition,
  key: (input: { readonly id: string }) => `items/${input.id}`,
  fetch: backend.respond<{ readonly id: string }, { readonly id: string }>("items.get"),
});
type Progress = { readonly received: number };
const progressQuery = createQuery<string, number, string, Progress>({
  name: "items.progress",
  key: (input) => `progress/${input}`,
  fetch: async (_input, { reportProgress }) => {
    reportProgress({ received: 1 });
    return 1;
  },
});
const legacyFiveGenericQuery = createQuery<
  { readonly id: string },
  number,
  string,
  (input: { readonly id: string }) => string,
  (input: { readonly id: string }, options: FetchOptions) => Promise<number>
>({
  name: "legacy.five-generics",
  key: (input) => input.id,
  fetch: async (_input, _options) => 1,
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
type PositionedInput = { readonly id: string };
type PositionedReader = { readonly token: string };
const positionedSource: PositionedStreamSource<PositionedInput, number, number, PositionedReader> =
  {
    open: async function* (
      _input: PositionedInput,
      _options: PositionedStreamOpenOptions<number, PositionedReader>,
    ) {
      yield { position: 1, value: 1 } satisfies PositionedStreamEvent<number, number>;
    },
    validateReader: (
      _input: PositionedInput,
      _reader: PositionedReader,
      _after: number,
      _checkpoint: PositionedProjectionCheckpoint<unknown, number> | undefined,
      _seed: unknown,
    ) => true,
    isReaderCurrent: (_input, _reader, _after, _seed) => true,
    acknowledge: (_input, _reader, _receipt) => undefined,
    releaseReader: (_input, _reader) => undefined,
    initialBoundary: (_input, _options) => ({ available: true, after: 0 }),
    readerLifetime: (_input, _reader, _after, _seed) => ({
      signal: new AbortController().signal,
      expiresAt: Date.now() + 60_000,
    }),
  };

type PositionedAcquisition = NonNullable<typeof positionedSource.acquireReader>;
const acquirePositioned: PositionedAcquisition = ({ input, checkpoint, signal }) => {
  const retainedInput: PositionedInput = input;
  const retainedCheckpoint: PositionedStoreRestoreSeed<number, number, readonly unknown[]> =
    checkpoint;
  const retainedSignal: AbortSignal = signal;
  void retainedInput;
  void retainedCheckpoint;
  void retainedSignal;
  return Promise.resolve({ reader: { token: "reserved" } });
};
// @ts-expect-error acquisition requires an original Promise carrying a Reader envelope
const synchronousPositionedAcquisition: PositionedAcquisition = () => ({
  reader: { token: "reserved" },
});
const barePositionedAcquisition: PositionedAcquisition = () =>
  // @ts-expect-error a bare Reader Promise would permit Reader assimilation
  Promise.resolve({ token: "reserved" });
type OpaquePositionedAcquisition = NonNullable<
  PositionedStreamSource<PositionedInput, number, number, unknown>["acquireReader"]
>;
const opaquePositionedAcquisition: OpaquePositionedAcquisition = () =>
  Promise.resolve({ reader: Promise.resolve("opaque") });
const undefinedPositionedAcquisition: OpaquePositionedAcquisition = () =>
  Promise.resolve({ reader: undefined });
const primitiveReleases: Array<
  PositionedStreamSource<PositionedInput, number, number, PositionedReader>["releaseReader"]
> = [
  () => undefined,
  () => null,
  () => false,
  () => 0,
  () => 0n,
  () => "",
  () => Symbol("released"),
  () => Promise.resolve(),
];
const existingVoidRelease: () => void = () => undefined;
const compatibleVoidRelease: PositionedStreamSource<
  PositionedInput,
  number,
  number,
  PositionedReader
>["releaseReader"] = existingVoidRelease;
void acquirePositioned;
void synchronousPositionedAcquisition;
void barePositionedAcquisition;
void opaquePositionedAcquisition;
void undefinedPositionedAcquisition;
void primitiveReleases;
void compatibleVoidRelease;

const positionedDefinition = createPositionedStream({
  name: "items.positioned",
  key: (input: PositionedInput) => input.id,
  source: positionedSource,
});
const positionedGraph = createGraph({
  contract: defineContract({
    namespace: "positioned-types",
    operations: { positioned: positionedDefinition },
  }),
});
const neverPositionedDefinition = createPositionedStream<
  PositionedInput,
  never,
  `never-positioned/${string}`,
  number,
  PositionedReader
>({
  name: "items.never-positioned",
  key: (input) => `never-positioned/${input.id}`,
  source: {
    open: async function* (
      _input: PositionedInput,
      _options: PositionedStreamOpenOptions<number, PositionedReader>,
    ) {},
    validateReader: () => true,
    isReaderCurrent: () => true,
    acknowledge: () => undefined,
    releaseReader: () => undefined,
  },
});
const neverPositionedGraph = createGraph({
  contract: defineContract({
    namespace: "never-positioned-types",
    operations: { events: neverPositionedDefinition, store },
  }),
});
const neverPositionedMember = neverPositionedGraph.api.events;
const neverPositionedSource = neverPositionedMember({ id: "one" });
const neverPositionedProjection = projectPositioned(
  neverPositionedMember,
  { id: "one" },
  { kind: "accumulate", max: 2 },
);
const neverPositionedValues: readonly never[] = neverPositionedProjection.value.get();
const neverPositionedRestore = preparePositionedSeedRestore(
  neverPositionedMember,
  { id: "one" },
  { kind: "accumulate", max: 2 },
  { after: 4, value: neverPositionedValues },
  { token: "reader" },
);
const neverPositionedRestoredValues: readonly never[] =
  neverPositionedRestore.projection.value.get();
// @ts-expect-error The never-valued member retains its declared input.
neverPositionedMember({ id: 1 });
// @ts-expect-error The never-valued member cannot accept a stored event.
neverPositionedSource.set(1);
preparePositionedSeedRestore(
  neverPositionedMember,
  { id: "one" },
  // @ts-expect-error The never-valued member retains its numeric provider position.
  { kind: "accumulate", max: 2 },
  { after: "wrong-position", value: neverPositionedValues },
  { token: "reader" },
);
preparePositionedSeedRestore(
  neverPositionedMember,
  { id: "one" },
  // @ts-expect-error The never-valued member retains its provider Reader.
  { kind: "accumulate", max: 2 },
  { after: 4, value: neverPositionedValues },
  { token: 1 },
);
const neverPositionedAddressed = neverPositionedGraph.at("never-positioned/one");
const neverPositionedKeyedValue: undefined = neverPositionedAddressed.value.get();
// @ts-expect-error A matching positioned key cannot accept an event of any value.
neverPositionedAddressed.set(1);
neverPositionedGraph.at("outside-positioned-pattern").set(1);
const neverPositionedCollision = createStore({ name: "never-positioned/taken", initial: 0 });
// @ts-expect-error The never-valued positioned stream still reserves its key pattern.
defineContract({
  namespace: "never-positioned-collision",
  operations: { events: neverPositionedDefinition, collision: neverPositionedCollision },
});
void [neverPositionedValues, neverPositionedRestoredValues, neverPositionedKeyedValue];
const positionedMember: MemberOf<typeof positionedDefinition> = positionedGraph.api.positioned;
const positionedReduce = projectPositioned(
  positionedMember,
  { id: "one" },
  {
    kind: "reduce",
    initial: { total: 0 },
    step: (accumulator, event) => ({ total: accumulator.total + event }),
  },
);
const positionedObjectValue: { total: number } = positionedReduce.value.get();
const positionedCapture: PositionedProjectionCheckpoint<{ total: number }, number> =
  capturePositionedProjection<{ total: number }, number>(positionedReduce);
const positionedReceipt: PositionedFoldReceipt<unknown> | undefined =
  positionedFoldReceipt(positionedReduce);
if (positionedReceipt !== undefined) acknowledgePositionedFold(positionedReduce, positionedReceipt);
const positionedArray = projectPositioned(
  positionedMember,
  { id: "one" },
  { kind: "accumulate", max: 4 },
);
const positionedArrayValue: readonly number[] = positionedArray.value.get();
const positionedSeed: PositionedRestoreSeed<{ total: number }, number> = {
  after: 4,
  value: positionedObjectValue,
};
const positionedRestore = preparePositionedSeedRestore(
  positionedMember,
  { id: "fresh" },
  {
    kind: "reduce",
    initial: { total: 0 },
    step: (accumulator, event) => ({ total: accumulator.total + event }),
  },
  positionedSeed,
  { token: "reader" },
);
const restoredPositionedValue: { total: number } = positionedRestore.projection.value.get();
preparePositionedSeedRestore(
  positionedGraph.api.positioned,
  { id: "wrong-position" },
  {
    kind: "reduce",
    initial: { total: 0 },
    step: (accumulator, event) => ({ total: accumulator.total + event }),
  },
  // @ts-expect-error The raw graph member retains its numeric provider position type.
  { after: "wrong-position", value: { total: 0 } },
  { token: "reader" },
);
preparePositionedSeedRestore(
  positionedGraph.api.positioned,
  { id: "wrong-reader" },
  {
    kind: "reduce",
    initial: { total: 0 },
    step: (accumulator, event) => ({ total: accumulator.total + event }),
  },
  { after: 4, value: { total: 0 } },
  // @ts-expect-error The raw graph member retains its provider reader type.
  { token: 1 },
);
const accumulatedRestore = preparePositionedSeedRestore(
  positionedMember,
  { id: "fresh-array" },
  { kind: "accumulate", max: 4 },
  { after: 4, value: [1, 2] },
  { token: "reader" },
);
const restoredPositionedArray: readonly number[] = accumulatedRestore.projection.value.get();
preparePositionedSeedRestore(
  positionedMember,
  { id: "invalid-array" },
  // @ts-expect-error Accumulation restores must seed the event array, not a scalar.
  { kind: "accumulate", max: 4 },
  { after: 4, value: 2 },
  { token: "reader" },
);
const positionedSameRuntimeRestore = preparePositionedRestore(
  positionedMember,
  { id: "same-runtime" },
  positionedCapture,
  { token: "reader" },
  { value: { total: 3 } },
);
const sameRuntimeValue: { total: number } = positionedSameRuntimeRestore.projection.value.get();
void [
  positionedObjectValue,
  positionedArrayValue,
  restoredPositionedValue,
  restoredPositionedArray,
  sameRuntimeValue,
];
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
  const contractQueryAcquisition: ContractQueryAcquisition = queryAcquisition;
  // @ts-expect-error QueryAcquisition accepts only its published cache/fetch policies.
  const unsupportedAcquisition: QueryAcquisition = "network";
  // @ts-expect-error The returned query declaration keeps its acquisition policy readonly.
  query.acquisition = "fetch";
  void unsupportedAcquisition;
  void contractQueryAcquisition;
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
  const finiteQueryFetchOptions: QueryFetchOptions = fetchOptions;
  const finiteFetchResult: Promise<{ readonly id: string }> = query.fetch(
    { id: "1" },
    fetchOptions,
  );
  const legacyFetchResult: Promise<number> = legacyFiveGenericQuery.fetch(
    { id: "1" },
    fetchOptions,
  );
  const progressFetchOptions: QueryFetchOptions<Progress> = {
    ...fetchOptions,
    reportProgress(value) {
      void (value satisfies Progress);
    },
  };
  progressFetchOptions.reportProgress({ received: 2 });
  // @ts-expect-error Progress reporters retain their declared value type.
  progressFetchOptions.reportProgress({ received: "two" });
  // @ts-expect-error Finite queries do not declare a progress value type.
  finiteQueryFetchOptions.reportProgress?.("undeclared");
  const progressFetchResult: Promise<number> = progressQuery.fetch("one", progressFetchOptions);
  const progressGraph = createGraph({
    contract: defineContract({ namespace: "progress-types", operations: { progressQuery } }),
  });
  const progressStore = progressGraph.api.progressQuery("one");
  const progressOptions: QueryFetchOptions<Progress> = {
    abortSignal: new AbortController().signal,
    graph: progressGraph.id,
    key: progressStore.key,
    reportProgress(value) {
      void (value satisfies Progress);
    },
  };
  progressOptions.reportProgress({ received: 2 });
  // @ts-expect-error Progress reporters retain their declared value type.
  progressOptions.reportProgress({ received: "two" });
  // @ts-expect-error Finite queries do not declare a progress value type.
  fetchOptions.reportProgress?.("undeclared");
  const progressStatus: QueryProgressStatus = progressStore.progress.get().status;
  const progressSnapshot: QueryProgressSnapshot<Progress> = progressStore.progress.get();
  const progressObservation: QueryProgressObservation<Progress> =
    observeQueryProgress(progressStore);
  const progressCapture: QueryProgressSnapshot<Progress> = progressObservation.capture();
  const stopProgress = progressObservation.subscribe((event: QueryProgressEvent<Progress>) => {
    const snapshot: QueryProgressSnapshot<Progress> = event.snapshot;
    void snapshot;
  });
  // @ts-expect-error Native progress snapshots do not expose Hesperia projection policy.
  void progressStore.progress.get().project;

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
  void [progressStatus, progressSnapshot, progressCapture, stopProgress];
  void changed;
  void streamed.pending.get();
  void addressed.value.get();

  type Equal<A, B> =
    (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;
  type ProgressOf<S> = S extends QueryStore<any, infer P> ? P : never;
  type AddressedValue = ReturnType<typeof addressed.value.get>;
  type DeclaredValue = ReturnType<typeof declared.value.get>;
  type UnknownValue = ReturnType<typeof unknown.value.get>;
  type WidenedValue = ReturnType<typeof widened.value.get>;
  const addressedIsTyped: Equal<AddressedValue, { readonly id: string } | undefined> = true;
  const declaredSurvivesWidePattern: Equal<DeclaredValue, number | undefined> = true;
  const widenedPatternClaimsNothing: Equal<UnknownValue, unknown> = true;
  const widenedAtIsUnknown: Equal<WidenedValue, unknown> = true;
  const finiteQueryHasNoProgress: Equal<ProgressOf<typeof queried>, never> = true;
  const progressQueryKeepsExactType: Equal<ProgressOf<typeof progressStore>, Progress> = true;

  const positionedStoreSource = positionedMember({ id: "store-source" });
  const positionedStoreTotalPolicy = {
    kind: "reduce" as const,
    initial: { total: 0 },
    step: (accumulator: { total: number }, event: number) => ({
      total: accumulator.total + event,
    }),
  };
  const positionedStoreCountPolicy = {
    kind: "reduce" as const,
    initial: 0,
    step: (count: number, _event: number) => count + 1,
  };
  const positionedStorePolicies = [positionedStoreTotalPolicy, positionedStoreCountPolicy] as const;
  const positionedStoreViews = [
    project(positionedStoreSource, positionedStoreTotalPolicy),
    project(positionedStoreSource, positionedStoreCountPolicy),
  ] as const;
  const positionedStoreCheckpoint = capturePositionedStore(
    positionedStoreSource,
    positionedStoreViews,
  );

  type PositionedStoreCaptureFailureCause = Extract<
    PositionedStoreCaptureNotice,
    { readonly kind: "failed" }
  >["cause"];
  type PositionedStoreCaptureChangedRevision = Extract<
    PositionedStoreCaptureNotice,
    { readonly kind: "changed" }
  >["revision"];
  const positionedStoreCaptureFailureCauseIsUnknown: Equal<
    PositionedStoreCaptureFailureCause,
    unknown
  > = true;
  const positionedStoreCaptureChangedRevisionIsObject: Equal<
    PositionedStoreCaptureChangedRevision,
    object
  > = true;
  const positionedStoreCaptureNoticeSubpathsMatch: Equal<
    PositionedStoreCaptureNotice,
    ContractPositionedStoreCaptureNotice
  > = true;
  const positionedStoreCaptureNotices: Readable<PositionedStoreCaptureNotice> =
    observePositionedStoreCaptureState(positionedStoreCheckpoint);
  const contractPositionedStoreCaptureNotices: Readable<ContractPositionedStoreCaptureNotice> =
    observeContractPositionedStoreCaptureState(positionedStoreCheckpoint);
  const positionedStoreCaptureNoticeObserver: (notice: PositionedStoreCaptureNotice) => void = (
    notice,
  ) => {
    if (notice.kind === "changed") {
      const revision: object = notice.revision;
      // @ts-expect-error A native revision token is opaque and cannot be used as an event position.
      const eventPosition: number = notice.revision;
      void [revision, eventPosition];
    } else if (notice.kind === "failed") {
      const cause: unknown = notice.cause;
      // @ts-expect-error An unknown failure cause must be narrowed before its fields are read.
      const failureMessage: string = notice.cause.message;
      void [cause, failureMessage];
    } else {
      const terminalKind: "closed" | "invalidated" = notice.kind;
      void terminalKind;
    }
  };
  const unsubscribePositionedStoreCapture: Unsubscribe = subscribe(
    positionedStoreCaptureNotices,
    positionedStoreCaptureNoticeObserver,
  );
  const unsubscribeContractPositionedStoreCapture: Unsubscribe = subscribe(
    contractPositionedStoreCaptureNotices,
    positionedStoreCaptureNoticeObserver,
  );
  // @ts-expect-error An ordinary Store does not carry a native positioned capture checkpoint.
  observePositionedStoreCaptureState(declared);
  void [
    positionedStoreCaptureFailureCauseIsUnknown,
    positionedStoreCaptureChangedRevisionIsObject,
    positionedStoreCaptureNoticeSubpathsMatch,
    unsubscribePositionedStoreCapture,
    unsubscribeContractPositionedStoreCapture,
  ];

  type PositionedStoreValues = readonly [{ total: number }, number];
  type CapturedPositionedStoreValues =
    typeof positionedStoreCheckpoint extends PositionedStoreCheckpoint<number, infer Values>
      ? Values
      : never;
  const capturedPositionedStoreValuesMatch: Equal<
    CapturedPositionedStoreValues,
    PositionedStoreValues
  > = true;
  const capturedPositionedStoreValues: PositionedStoreValues = positionedStoreCheckpoint.values;
  const positionedStoreCurrent: boolean =
    isPositionedStoreCheckpointCurrent(positionedStoreCheckpoint);
  if (positionedStoreCheckpoint.present) {
    const positionedLatestEvent: number = positionedStoreCheckpoint.latest;
    const positionedUnknownBoundary: unknown = positionedStoreCheckpoint.position;
    // @ts-expect-error The provider codec must narrow the opaque capture position.
    const uncheckedPosition: number = positionedStoreCheckpoint.position;
    void [positionedLatestEvent, positionedUnknownBoundary, uncheckedPosition];
  }
  const positionedStoreSeed: PositionedStoreRestoreSeed<number, number, PositionedStoreValues> = {
    after: 4,
    latest: 5,
    values: [{ total: 3 }, 2],
  };
  const undefinedLatestSeed: PositionedStoreRestoreSeed<
    number | undefined,
    number,
    PositionedStoreValues
  > = {
    after: 4,
    latest: undefined,
    values: [{ total: 3 }, 2],
  };
  function positionedStoreUndefinedEventType(
    checkpoint: PositionedStoreCheckpoint<number | undefined, PositionedStoreValues>,
  ): void {
    if (!checkpoint.present) return;
    const latestEvent: number | undefined = checkpoint.latest;
    // @ts-expect-error Presence does not narrow an event value away from undefined.
    const numericEvent: number = checkpoint.latest;
    void [latestEvent, numericEvent];
  }
  const contractCheckpointType: ContractPositionedStoreCheckpoint<number, PositionedStoreValues> =
    positionedStoreCheckpoint;
  const contractSeedType: ContractPositionedStoreRestoreSeed<
    number,
    number,
    PositionedStoreValues
  > = positionedStoreSeed;
  const inferredPositionedStoreRestoration = preparePositionedStoreRestore(
    positionedMember,
    { id: "fresh-store" },
    positionedStorePolicies,
    positionedStoreSeed,
    { token: "reader" },
  );
  type InferredPositionedStoreValues =
    typeof inferredPositionedStoreRestoration extends PositionedStoreRestoration<
      number,
      infer Values
    >
      ? Values
      : never;
  const inferredPositionedStoreValuesMatch: Equal<
    InferredPositionedStoreValues,
    PositionedStoreValues
  > = true;
  const positionedStoreRestoration: PositionedStoreRestoration<number, PositionedStoreValues> =
    inferredPositionedStoreRestoration;
  const positionedStoreWithOptions = preparePositionedStoreRestore(
    positionedMember,
    { id: "fresh-store-options" },
    positionedStorePolicies,
    positionedStoreSeed,
    { token: "reader" },
    {
      initial: 99,
      equals: (left, right) => {
        const leftEvent: number = left;
        const rightEvent: number = right;
        return Math.floor(leftEvent) === Math.floor(rightEvent);
      },
    },
  );
  const positionedStoreWithOptionsType: PositionedStoreRestoration<number, PositionedStoreValues> =
    positionedStoreWithOptions;
  preparePositionedStoreRestore(
    positionedMember,
    { id: "wrong-store-initial-option" },
    positionedStorePolicies,
    positionedStoreSeed,
    { token: "reader" },
    // @ts-expect-error Caller options cannot widen Event from the target member.
    { initial: "wrong-initial" },
  );
  preparePositionedStoreRestore(
    positionedMember,
    { id: "wrong-store-equality-option" },
    positionedStorePolicies,
    positionedStoreSeed,
    { token: "reader" },
    // @ts-expect-error Store equality parameters retain the target Event type.
    { equals: (left: string, right: string) => left === right },
  );

  const contractRestorationType: ContractPositionedStoreRestoration<number, PositionedStoreValues> =
    positionedStoreRestoration;
  const positionedRestoredSource: StreamStore<number> = positionedStoreRestoration.source;
  const positionedRestoredTotal: { total: number } =
    positionedStoreRestoration.projections[0].value.get();
  const positionedRestoredCount: number = positionedStoreRestoration.projections[1].value.get();
  const positionedActivatedSource: StreamStore<number> = positionedStoreRestoration.activate();

  const positionedStoreAccumulatePolicy = {
    kind: "accumulate" as const,
    max: 4,
  };
  const positionedStoreMixedPolicies = [
    positionedStoreTotalPolicy,
    positionedStoreAccumulatePolicy,
  ] as const;
  const positionedStoreMixedViews = [
    project(positionedStoreSource, positionedStoreTotalPolicy),
    project(positionedStoreSource, positionedStoreAccumulatePolicy),
  ] as const;
  const positionedStoreMixedCheckpoint = capturePositionedStore(
    positionedStoreSource,
    positionedStoreMixedViews,
  );
  type MixedPositionedStoreValues = readonly [{ total: number }, readonly number[]];
  type CapturedMixedPositionedStoreValues =
    typeof positionedStoreMixedCheckpoint extends PositionedStoreCheckpoint<number, infer Values>
      ? Values
      : never;
  const capturedMixedPositionedStoreValuesMatch: Equal<
    CapturedMixedPositionedStoreValues,
    MixedPositionedStoreValues
  > = true;
  const capturedMixedPositionedStoreValues: MixedPositionedStoreValues =
    positionedStoreMixedCheckpoint.values;
  const inferredMixedPositionedStoreRestoration = preparePositionedStoreRestore(
    positionedMember,
    { id: "fresh-mixed-store" },
    positionedStoreMixedPolicies,
    { after: 4, latest: 5, values: [{ total: 3 }, [1, 2]] },
    { token: "reader" },
  );
  type InferredMixedPositionedStoreValues =
    typeof inferredMixedPositionedStoreRestoration extends PositionedStoreRestoration<
      number,
      infer Values
    >
      ? Values
      : never;
  const inferredMixedPositionedStoreValuesMatch: Equal<
    InferredMixedPositionedStoreValues,
    MixedPositionedStoreValues
  > = true;
  const restoredAccumulatedEvents: readonly number[] =
    inferredMixedPositionedStoreRestoration.projections[1].value.get();
  preparePositionedStoreRestore(
    positionedMember,
    { id: "wrong-accumulated-event" },
    positionedStoreMixedPolicies,
    // @ts-expect-error Accumulation restore values keep the source event type.
    { after: 4, latest: 5, values: [{ total: 3 }, ["wrong-event"]] },
    { token: "reader" },
  );

  preparePositionedStoreRestore(
    positionedMember,
    { id: "wrong-store-position" },
    positionedStorePolicies,
    // @ts-expect-error Store restoration keeps the provider Position from its member.
    { after: "wrong-position", latest: 5, values: [{ total: 3 }, 2] },
    { token: "reader" },
  );
  preparePositionedStoreRestore(
    positionedMember,
    { id: "wrong-store-reader" },
    positionedStorePolicies,
    positionedStoreSeed,
    // @ts-expect-error Store restoration keeps the provider Reader from its member.
    { token: 1 },
  );
  preparePositionedStoreRestore(
    positionedMember,
    { id: "wrong-store-event" },
    positionedStorePolicies,
    // @ts-expect-error The source seed keeps the positioned stream event type.
    { after: 4, latest: "five", values: [{ total: 3 }, 2] },
    { token: "reader" },
  );
  preparePositionedStoreRestore(
    positionedMember,
    { id: "wrong-store-tuple" },
    positionedStorePolicies,
    // @ts-expect-error Each decoded value must match its corresponding policy.
    { after: 4, latest: 5, values: [2, { total: 3 }] },
    { token: "reader" },
  );
  preparePositionedStoreRestore(
    positionedMember,
    { id: "missing-store-event" },
    positionedStorePolicies,
    // @ts-expect-error The decoded seed must carry an own latest event property.
    { after: 4, values: [{ total: 3 }, 2] },
    { token: "reader" },
  );

  const positionedStoreDynamicPolicies: ProjectionPolicy<unknown, unknown>[] = [
    {
      kind: "reduce" as const,
      initial: undefined,
      step: (previous: unknown, event: unknown): unknown => [previous, event],
    },
    positionedStoreAccumulatePolicy,
  ];
  const positionedStoreDynamicSeed: PositionedStoreRestoreSeed<number, number, readonly unknown[]> =
    {
      after: 4,
      latest: 5,
      values: [undefined, [1, 2]],
    };
  const inferredDynamicPositionedStoreRestoration = preparePositionedStoreRestore(
    positionedMember,
    { id: "fresh-dynamic-store" },
    positionedStoreDynamicPolicies,
    positionedStoreDynamicSeed,
    { token: "reader" },
  );
  const dynamicPositionedStoreRestorationType: PositionedStoreRestoration<
    number,
    readonly unknown[]
  > = inferredDynamicPositionedStoreRestoration;

  void addressedIsTyped;
  void declaredSurvivesWidePattern;
  void widenedPatternClaimsNothing;
  void widenedAtIsUnknown;
  void finiteQueryHasNoProgress;
  void progressQueryKeepsExactType;
  void progressStatus;
  void progressSnapshot;
  void progressCapture;
  void finiteFetchResult;
  void legacyFetchResult;
  void progressFetchResult;
  void [
    capturedPositionedStoreValuesMatch,
    capturedPositionedStoreValues,
    positionedStoreCurrent,
    contractCheckpointType,
    contractSeedType,
    undefinedLatestSeed,
    positionedStoreUndefinedEventType,
    inferredPositionedStoreValuesMatch,
    capturedMixedPositionedStoreValuesMatch,
    capturedMixedPositionedStoreValues,
    inferredMixedPositionedStoreValuesMatch,
    restoredAccumulatedEvents,
    contractRestorationType,
    positionedStoreWithOptionsType,
    dynamicPositionedStoreRestorationType,
    positionedRestoredSource,
    positionedRestoredTotal,
    positionedRestoredCount,
    positionedActivatedSource,
  ];

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

function publicPassiveTransferTypeSurface<Source extends NativeTransferSource>(
  graph: Graph,
  source: Source,
  input: unknown,
  options: ExistingNativeValueOptions<Source>,
): NativeStoreFor<Source> {
  const claim: PassiveTransferClaim<Source> = preparePassiveTransferClaim<Source>(
    graph,
    source,
    input,
    options,
  );
  const store: NativeStoreFor<Source> = claim.store;
  const retirement: Promise<void> = claim.retired;
  claim.discard();
  void retirement;
  return store;
}

function publicRequiredTransferTypeSurface(
  graph: Graph,
  source: QueryTransferSource,
  target: QueryStore<unknown, unknown>,
  mutation: MutationTransferSource,
  carrier: object,
  input: unknown,
  root: Readable<unknown>,
  construction: ReactiveConstruction<unknown>,
  codec: RequiredTransferCodec,
): void {
  const scope: TransferScope = createTransferScope(graph, source, target);
  const dispatch: TransferOperationDispatch = { role: "raw", source: mutation, input: carrier };
  const operation = prepareTransferOperation([scope], [dispatch]);
  const operationHandle: TransferOperation = operation.operation;
  const successor: TransferScopeSuccessor = {
    operation: operationHandle,
    predecessor: scope,
    graph,
    source,
    input,
  };
  const operationRetirement: TransferPublication =
    prepareTransferOperationRetirement(operationHandle);
  const successors: TransferPublication & { readonly scopes: readonly TransferScope[] } =
    prepareTransferScopeSuccessors([]);
  const commit: TransferCommit | undefined = operation.seal();
  const notifications: TransferNotifications | undefined = commit?.notifications;
  const slot: TransferSlot | undefined = undefined;
  const sourceConstruction: ReactiveConstruction<unknown> = captureReactiveConstruction(
    root as Computed<unknown>,
  );
  const captureOptions: RequiredTransferCaptureOptions = {
    graphs: [graph],
    roots: [{ source: root, construction }],
    codec,
  };
  const capture = prepareRequiredTransferCapture(captureOptions);
  const adoptionOptions: RequiredTransferAdoptionOptions = {
    graphs: [graph],
    sources: [],
    reactive: [],
    codec,
    mappedStateWrites: [],
  };
  const adoption = prepareRequiredTransferAdoption(capture.image, adoptionOptions);
  void [
    operationRetirement,
    successors,
    notifications,
    slot,
    adoption,
    successor,
    sourceConstruction,
  ];
}

void publicPassiveTransferTypeSurface;
void publicRequiredTransferTypeSurface;

function positionedTransferConsumerTypesOnly(): void {
  const transferPolicy = {
    kind: "reduce" as const,
    initial: { total: 0 },
    step: (accumulator: { total: number }, event: number) => ({
      total: accumulator.total + event,
    }),
  };
  const omittedBase: PositionedStoreRestoreSeed<
    number,
    number,
    readonly [{ readonly total: number }]
  > = {
    boundary: { kind: "event", after: 4 },
    base: { present: false },
    values: [{ total: 7 }],
  };
  const presentUndefined: PositionedStoreRestoreSeed<
    number | undefined,
    number | undefined,
    readonly [number]
  > = {
    boundary: { kind: "initial", after: undefined },
    base: { present: true, value: undefined },
    values: [3],
  };
  const claim = preparePassiveTransferClaim(
    positionedGraph,
    positionedGraph.api.positioned,
    { id: "positioned-transfer" },
    undefined,
    {
      policies: [transferPolicy] as const,
      seed: omittedBase,
      reader: { token: "reader" },
    },
  );
  const restoration: PositionedStoreRestoration<number, readonly [{ readonly total: number }]> =
    claim.restoration;
  const restoredStore: StreamStore<number> = claim.store;
  const restoredValue: { readonly total: number } = restoration.projections[0].value.get();
  const registrationRetired: Promise<void> = claim.retired;
  const resourceRetired: Promise<void> = restoration.retired;
  const sameSource: boolean = restoration.source === claim.store;

  const invalidOmittedBase: PositionedStoreRestoreSeed<number, number, readonly [number]> = {
    boundary: { kind: "event", after: 4 },
    // @ts-expect-error An omitted base cannot carry an event value.
    base: { present: false, value: undefined },
    values: [3],
  };
  const invalidPresentBase: PositionedStoreRestoreSeed<
    number | undefined,
    number,
    readonly [number]
  > = {
    boundary: { kind: "event", after: 4 },
    // @ts-expect-error A present base must own a value, even when its value is undefined.
    base: { present: true },
    values: [3],
  };

  claim.discard();
  void [
    presentUndefined,
    restoredStore,
    restoredValue,
    registrationRetired,
    resourceRetired,
    sameSource,
    invalidOmittedBase,
    invalidPresentBase,
  ];
}

void positionedTransferConsumerTypesOnly;
