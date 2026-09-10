import { Fault } from "../fault.ts";
import type { Contract } from "./types.ts";
import type { GraphRuntimeState, StoreRuntime } from "./runtime.ts";

export function reportUnobserved(state: GraphRuntimeState<any>, error: unknown): void {
  if (state.onUnobservedFault === undefined) return;
  try {
    state.onUnobservedFault(error);
  } catch {
    /* diagnostics do not recurse */
  }
}

export function graphFault(state: GraphRuntimeState<any>, fault: Fault): void {
  const observers = [...state.faultObservers];
  if (observers.length === 0) {
    reportUnobserved(state, fault);
    return;
  }
  for (const observer of observers) {
    try {
      observer(fault);
    } catch (error) {
      reportUnobserved(state, error);
    }
  }
}

export function reportContinuationError(state: GraphRuntimeState<any>, error: unknown): void {
  if (error instanceof Fault) graphFault(state, error);
  else reportUnobserved(state, error);
}

export function assertGraphOpen<C extends Contract>(state: GraphRuntimeState<C>): void {
  if (state.disposed) throw new Fault("disposed", [String(state.graph.id)]);
}

export function assertStoreOpen<T>(runtime: StoreRuntime<T>): void {
  if (runtime.disposed || runtime.graph.__runtime.disposed)
    throw new Fault("disposed", [runtime.key as string]);
}
