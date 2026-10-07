import { createGraph, type Signal as RootSignal } from "aeolia";
import { createQuery, defineContract } from "aeolia/contract";
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
  // @ts-expect-error The contract subpath retains the required input type.
  graph.api.item(1);
  void ready;
  void derived;
}

void subpathConsumer;
