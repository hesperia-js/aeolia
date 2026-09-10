import type { OperationDefinition, OperationTree, StoreDefinition } from "./types.ts";
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

export function isOperation(value: unknown): value is OperationDefinition {
  if (value === null || typeof value !== "object") return false;
  const kind = (value as { kind?: unknown }).kind;
  return kind === "query" || kind === "mutation" || kind === "stream";
}

export function isStore(value: unknown): value is StoreDefinition<unknown> {
  return (
    value !== null && typeof value === "object" && (value as { kind?: unknown }).kind === "store"
  );
}

export function collectDeclaredStores(
  tree: OperationTree,
  into: Map<string, StoreDefinition<unknown>>,
): void {
  for (const value of Object.values(tree)) {
    if (isStore(value)) into.set(value.name, value);
    else if (!isOperation(value) && value !== null && typeof value === "object")
      collectDeclaredStores(value as OperationTree, into);
  }
}
