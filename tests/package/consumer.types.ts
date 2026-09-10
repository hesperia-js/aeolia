/* Typecheck-only fixture. The function is never called, so importing this file
 * does not open a stream or make a request. */
import {
  Signal,
  affects,
  createGraph,
  defineContract,
  createMutation,
  createQuery,
  createStore,
  createStream,
  subscribe,
  type AffectSpec,
  type CreateMutationInput,
  type CreateQueryInput,
  type CreateStoreInput,
  type CreateStreamInput,
  type DefineContractInput,
  type FetchOptions,
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
const contract = defineContract({
  namespace: "types",
  operations: { store, query, mutation, stream },
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
  const changed = graph.api.mutation({ id: "1" });
  const streamed = graph.api.stream({ id: "1" });
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
void typecheckOnly;
