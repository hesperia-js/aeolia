import { Fault } from "../fault.ts";
import type { Contract, OperationTree, MutationDefinition, QueryDefinition } from "./types.ts";
import { isOperationDefinition, isStoreDefinition } from "./types.ts";
import { isObject } from "../utils.ts";

export function validateContract(contract: Contract): void {
  if (!isObject(contract) || typeof contract.namespace !== "string")
    throw new TypeError("A contract must have a string namespace and an operation tree.");
  const walk = validateContractTree(contract.operations);
  validateAffectedQueries(walk);
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
    if (!isObject(node) || typeof node === "function" || Array.isArray(node))
      throw new TypeError("A contract operation tree must contain declarations or nested objects.");
    const objectNode = node;
    if (active.has(objectNode)) {
      throw contractTreeFault("cycle");
    }
    if (seen.has(objectNode)) {
      const name = isOperationDefinition(node) || isStoreDefinition(node) ? node.name : undefined;
      throw contractTreeFault("alias", name);
    }
    seen.add(objectNode);
    active.add(objectNode);

    try {
      if (isOperationDefinition(node)) {
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
