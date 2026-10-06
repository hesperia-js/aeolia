import type { OperationTree, StoreDefinition } from "./types.ts";
import { isOperationDefinition, isStoreDefinition } from "./types.ts";
import { Fault } from "../fault.ts";

export function finiteNonNegative(value: number, option: string): number {
  if (!Number.isFinite(value) || value < 0) throw new Fault("contract", [option]);
  return value;
}

export function finiteBound(value: number, option: string): number {
  if (!Number.isFinite(value) || value < 1 || !Number.isInteger(value)) {
    throw new Fault("contract", [option]);
  }
  return value;
}

export function graphIdleMs(value: number): number {
  if (!Number.isFinite(value) || value < 0) throw new Fault("contract", ["idleMs"]);
  return value;
}

export function collectDeclaredStores(
  tree: OperationTree,
  into: Map<string, StoreDefinition<unknown>>,
): void {
  for (const value of Object.values(tree)) {
    if (isStoreDefinition(value)) into.set(value.name, value);
    else if (!isOperationDefinition(value) && value !== null && typeof value === "object")
      collectDeclaredStores(value as OperationTree, into);
  }
}
