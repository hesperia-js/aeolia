import { Fault } from "../fault.ts";
import type {
  OperationDefinition,
  OperationTree,
  MutationDefinition,
  QueryDefinition,
  StoreDefinition,
} from "./types.ts";

function isOperation(value: unknown): value is OperationDefinition {
  if (value === null || typeof value !== "object") return false;
  const kind = (value as { kind?: unknown }).kind;
  return kind === "query" || kind === "mutation" || kind === "stream";
}

function isStoreDefinition(value: unknown): value is StoreDefinition<unknown> {
  return (
    value !== null && typeof value === "object" && (value as { kind?: unknown }).kind === "store"
  );
}

interface ContractWalk {
  readonly queries: Set<QueryDefinition<any, any, any>>;
  readonly mutations: MutationDefinition<any, any>[];
}

function contractTreeFault(reason: "cycle" | "alias", name?: string): Fault {
  const fault = new Fault("contract", name === undefined ? [] : [name]);
  fault.message = name === undefined ? `contract ${reason}` : `contract ${reason}: ${name}`;
  return fault;
}

/**
 * Walk the declaration tree once. `active` distinguishes a cycle from an
 * alias, while `seen` rejects both repeated namespaces and repeated leaves.
 * Operation internals are deliberately opaque: an affected query is a
 * reference, not a second position in the operation tree.
 */
export function validateContractTree(tree: OperationTree): ContractWalk {
  const operationNames = new Set<string>();
  const storeNames = new Set<string>();
  const active = new WeakSet<object>();
  const seen = new WeakSet<object>();
  const queries = new Set<QueryDefinition<any, any, any>>();
  const mutations: MutationDefinition<any, any>[] = [];

  const visit = (node: unknown): void => {
    if (node === null || typeof node !== "object") return;
    const objectNode = node as object;
    if (active.has(objectNode)) {
      throw contractTreeFault("cycle");
    }
    if (seen.has(objectNode)) {
      const name =
        isOperation(node) || isStoreDefinition(node) ? (node as { name: string }).name : undefined;
      throw contractTreeFault("alias", name);
    }
    seen.add(objectNode);
    active.add(objectNode);

    try {
      if (isOperation(node)) {
        if (operationNames.has(node.name)) {
          throw new Fault("contract", [node.name]);
        }
        operationNames.add(node.name);
        if (node.kind === "query") queries.add(node);
        if (node.kind === "mutation") mutations.push(node);
        return;
      }

      if (isStoreDefinition(node)) {
        if (storeNames.has(node.name)) {
          throw new Fault("contract", [node.name]);
        }
        storeNames.add(node.name);
        return;
      }

      for (const value of Object.values(node)) visit(value);
    } finally {
      active.delete(objectNode);
    }
  };

  visit(tree);
  return { queries, mutations };
}

export function validateAffectedQueries(walk: ContractWalk): void {
  for (const mutation of walk.mutations) {
    for (const affected of mutation.affects) {
      if (!walk.queries.has(affected.query)) {
        throw new Fault("contract", [affected.query.name]);
      }
    }
  }
}
