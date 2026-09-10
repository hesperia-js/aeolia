import type { GraphId } from "./identity.ts";
import type { Graph } from "./types.ts";
import type { Unsubscribe } from "../fault.ts";
import { assertGraphOpen, reportContinuationError } from "./faults.ts";
import type { GraphRuntimeState, StoreRuntime } from "./runtime.ts";

let nextGraphId = 0;

export function allocateGraphId(): GraphId {
  return `graph-${nextGraphId++}` as GraphId;
}

/** Runtime lookup for public store shells and their prototype chain. */
export const runtimeByStore = new WeakMap<object, StoreRuntime<unknown>>();

/** Runtime lookup for graph objects used by internal adapters. */
export const graphRuntimeByGraph = new WeakMap<object, GraphRuntimeState<any>>();

export function registerProjection(graph: Graph, close: () => void): Unsubscribe {
  const state = graphRuntimeByGraph.get(graph as object);
  if (state === undefined) throw new TypeError("The value is not an Aeolia graph");
  assertGraphOpen(state);
  state.projections.add(close);
  let active = true;
  return () => {
    if (!active) return;
    active = false;
    state.projections.delete(close);
  };
}

export function reportGraphContinuationError(graph: Graph, error: unknown): void {
  const state = graphRuntimeByGraph.get(graph as object);
  if (state === undefined || state.disposed) return;
  reportContinuationError(state, error);
}

export function runtimeForStore(value: unknown): StoreRuntime<unknown> | undefined {
  if (value === null || (typeof value !== "object" && typeof value !== "function"))
    return undefined;
  let current: object | null = value;
  while (current !== null) {
    const runtime = runtimeByStore.get(current);
    if (runtime !== undefined) return runtime;
    current = Object.getPrototypeOf(current) as object | null;
  }
  return undefined;
}
