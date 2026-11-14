import {
  adopt as rootAdopt,
  createGraph,
  observeStore as rootObserveStore,
  observeQueryProgress as rootObserveQueryProgress,
  snapshot as rootSnapshot,
  type AdoptOptionsV2,
  type ObservationLease as RootObservationLease,
  type Signal as RootSignal,
  type SnapshotBindingReference,
  type SnapshotV2,
  type QueryProgressEvent as RootQueryProgressEvent,
  type QueryProgressObservation as RootQueryProgressObservation,
} from "aeolia";
import {
  createQuery,
  defineContract,
  observeQueryProgress,
  observeStore,
  peekStore,
  affects,
  bindOperation,
  bindStream,
  createStream,
  inspectBoundAddress,
  storeKey,
  type BoundStreamSession,
  type BoundStreamValueOptions,
  createBindingContext,
  createMutation,
  profileEffect,
  predictionFor,
  type MutationBinding,
  type TargetPrediction,
  type ObservationLease,
  type StoreCapture,
  type QueryFetchOptions,
  type QueryProgressSnapshot,
  type QueryProgressStatus,
  type QueryProgressEvent,
  type QueryProgressObservation,
  type QueryStore,
  type QueryBindingHandle,
  type StreamBindingHandle,
} from "aeolia/contract";
import { Signal, computed, signal } from "aeolia/reactive";

function subpathConsumer(): void {
  const state: RootSignal.State<number> = signal(1);
  const derived: Signal.Computed<number> = computed(() => state.get() * 2);
  // @ts-expect-error The reactive subpath retains its signal value type.
  state.set("wrong");

  const item = createQuery({
    name: "item",
    key: (id: string) => `items/${id}`,
    fetch: async (id: string) => ({ id }),
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "subpaths", operations: { item } }),
  });
  const handle = graph.api.item("one");
  const ready: Promise<{ id: string }> = handle.ready;
  const lease: ObservationLease<{ id: string }> = observeStore(graph, handle);
  const rootLease: RootObservationLease<{ id: string }> = rootObserveStore(graph, handle);
  const capture: StoreCapture<{ id: string }> = peekStore(handle);
  // @ts-expect-error The contract entry preserves the observed query's value type.
  lease.publish(1);
  void [rootLease, capture];
  // @ts-expect-error The contract subpath retains the required input type.
  graph.api.item(1);
  void ready;
  void capture;
  void derived;
}

void subpathConsumer;

function mutationSubpathConsumer(): void {
  const query = createQuery({ name: "number", key: () => "one", fetch: async () => 1 });
  const mutation = createMutation({
    name: "save",
    run: async (input: number) => input,
    affects: [affects(query, { select: (_input: number) => undefined })],
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "subpath-mutation", operations: { query, mutation } }),
  });
  const context = createBindingContext(graph);
  const binding = bindOperation(graph, query, {
    context,
    profile: {},
    fetch: async () => "completed",
  });
  const target = binding.target(binding.member())!;
  const override: TargetPrediction<typeof mutation> = predictionFor(
    mutation,
    target,
    (_current, input) => String(input),
  );
  const effect = profileEffect(mutation, 0, query, {
    prepare: (phase) => phase.existing(undefined).map((candidate) => phase.prepare(candidate)),
    settle: (phase) => phase.existing(undefined).map((candidate) => phase.current(candidate)),
  });
  const save: MutationBinding<typeof mutation, string> = bindOperation(graph, mutation, {
    context,
    profile: {},
    effects: [effect],
    run: async (input) => String(input),
  });
  const result: Promise<string> = save.member(2, { optimistic: [override] });
  // @ts-expect-error Contract-subpath producers retain the transformed target value.
  predictionFor(mutation, target, () => 1);
  void result;
}

void mutationSubpathConsumer;

function streamSubpathConsumer(): void {
  const stream = createStream({
    name: "subpath-stream",
    key: (input: string) => input,
    open: async function* (_input: string) {
      yield 1;
    },
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "subpath-stream", operations: { stream } }),
  });
  const binding = bindStream(graph, stream, graph.api.stream, {
    context: createBindingContext(graph),
  });
  const inspected = inspectBoundAddress(binding, storeKey("one"));
  if (inspected.kind === "stable" && inspected.state === "empty") {
    const options: BoundStreamValueOptions<number> = { expectEmpty: inspected.condition };
    const source = binding.member("one", options);
    const session: BoundStreamSession<number> = binding.session(source);
    const detached: Promise<number | undefined> = session.detached.then((receipt) => {
      if (receipt.terminalCapture.kind !== "captured") return undefined;
      const capture = receipt.terminalCapture.capture;
      return capture.kind === "stable" && capture.present ? capture.value : undefined;
    });
    // @ts-expect-error Contract-subpath bound values remain numeric.
    source.set("wrong");
    void detached;
  }
  graph.dispose();
}

void streamSubpathConsumer;

function versionedTransferSubpathConsumer(): void {
  const query = createQuery({
    name: "subpath-transfer.query",
    key: (input: { readonly key: string }) => input.key,
    fetch: async (input: { readonly key: string }) => input.key,
  });
  const stream = createStream({
    name: "subpath-transfer.stream",
    key: (input: { readonly key: string }) => input.key,
    open: async function* (input: { readonly key: string }) {
      yield input.key;
    },
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "subpath-transfer", operations: { query, stream } }),
  });
  const context = createBindingContext(graph);
  const queryBinding = bindOperation(graph, query, {
    context,
    profile: {},
    fetch: query.fetch,
  });
  const streamBinding = bindStream(graph, stream, graph.api.stream, { context });
  const queryHandle: QueryBindingHandle = queryBinding;
  const streamHandle: StreamBindingHandle = streamBinding;
  const bindings: readonly SnapshotBindingReference[] = [
    { binding: queryHandle, reference: { operation: query.name, profile: "subpath" } },
    { binding: streamHandle, reference: { operation: stream.name, profile: "subpath" } },
  ];
  const captured: SnapshotV2 = rootSnapshot(graph, { version: 2, bindings });
  const options: AdoptOptionsV2 = { bindings };
  rootAdopt(graph, captured, options);
  // @ts-expect-error Version 2 requires explicit root adoption options.
  rootAdopt(graph, captured);
  graph.dispose();
}

void versionedTransferSubpathConsumer;

function queryProgressSubpathConsumer(): void {
  type Progress = { readonly received: number };
  const query = createQuery<string, number, string, Progress>({
    name: "subpath-progress",
    key: (input) => input,
    fetch: async (_input, { reportProgress }) => {
      reportProgress({ received: 1 });
      return 1;
    },
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "subpath-progress", operations: { query } }),
  });
  const store: QueryStore<number, Progress> = graph.api.query("one");
  const options: QueryFetchOptions<Progress> = {
    abortSignal: new AbortController().signal,
    graph: graph.id,
    key: store.key,
    reportProgress(value) {
      void (value satisfies Progress);
    },
  };
  const result: Promise<number> = query.fetch("one", options);
  const snapshot: QueryProgressSnapshot<Progress> = store.progress.get();
  const status: QueryProgressStatus = snapshot.status;
  const observation: QueryProgressObservation<Progress> = observeQueryProgress(store);
  const rootObservation: RootQueryProgressObservation<Progress> = rootObserveQueryProgress(store);
  const captured: QueryProgressSnapshot<Progress> = observation.capture();
  const stop = rootObservation.subscribe((event: RootQueryProgressEvent<Progress>) => {
    const current: QueryProgressEvent<Progress> = event;
    void current;
  });
  // @ts-expect-error Contract subpath snapshots expose no Hesperia projection field.
  void snapshot.project;
  void [result, status, captured, stop];
  graph.dispose();
}

void queryProgressSubpathConsumer;
