import { createGraph, type Signal as RootSignal } from "aeolia";
import {
  affects,
  bindOperation,
  bindStream,
  createBindingContext,
  createMutation,
  createQuery,
  createStream,
  defineContract,
  inspectBoundAddress,
  peekStore,
  predictionFor,
  profileEffect,
  storeKey,
  type BoundStreamSession,
  type BoundStreamValueOptions,
  type MutationBinding,
  type StoreCapture,
  type TargetPrediction,
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
  const ready: Promise<{ id: string }> = graph.api.item("one").ready;
  const capture: StoreCapture<{ id: string }> = peekStore(graph.api.item("one"));
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
