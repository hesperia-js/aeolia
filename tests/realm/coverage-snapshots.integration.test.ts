import { expect, it } from "bun:test";
import {
  bindOperation,
  createBindingContext,
  createGraph,
  createMutation,
  createQuery,
  createStore,
  createTransferScope,
  defineContract,
  Fault,
  prepareTransferOperation,
  prepareTransferOperationRetirement,
  prepareTransferScopeSuccessors,
  snapshot,
} from "../../src/index.ts";

for (const kind of ["raw", "bound"] as const) {
  it(`omits a covered ${kind} landing until every actual operation retires`, async () => {
    let value = "before";
    const query = createQuery({
      name: "covered.query",
      key: () => "covered",
      fetch: async () => value,
    });
    const mutation = createMutation({
      name: "covered.mutation",
      run: async (_input: object) => {},
      affects: [],
    });
    const other = createStore({ name: "unrelated", initial: "kept", snapshot: true });
    const graph = createGraph({
      contract: defineContract({
        namespace: "covered-snapshots",
        operations: { query, mutation, other },
      }),
    });
    try {
      graph.api.other();
      const binding =
        kind === "bound"
          ? bindOperation(graph, query, {
              context: createBindingContext(graph),
              profile: {},
              fetch: query.fetch,
            })
          : undefined;
      const source = binding ?? graph.api.query;
      const store = binding ? binding.member() : graph.api.query();
      await store.ready;
      const options = binding
        ? { version: 2 as const, bindings: [{ binding, reference: "profile" }] }
        : { version: 2 as const };
      expect(snapshot(graph, options).entries).toHaveLength(2);

      const scope = createTransferScope(graph, source, store);
      const inputs = [{}, {}];
      const plans = inputs.map((input) => {
        const plan = prepareTransferOperation(
          [scope],
          [{ role: "raw", source: graph.api.mutation, input }],
        );
        const commit = plan.seal();
        if (!commit) throw new Error("The operation did not seal");
        commit.publish();
        commit.notifications.deliver();
        return plan;
      });
      const results = inputs.map((input) => graph.api.mutation(input));
      value = "landed while covered";
      await store.revalidate();
      expect(store.value.get()).toBe("landed while covered");
      const during = snapshot(graph, options);
      expect(during.entries).toHaveLength(1);
      expect(during.entries[0]?.value).toBe("kept");
      const rawDuring = snapshot(graph);
      expect(Object.keys(rawDuring.entries)).toEqual(["unrelated"]);
      expect(rawDuring.entries.unrelated?.value).toBe("kept");
      if (binding) expect(snapshot(graph, { version: 2 }).entries).toHaveLength(1);

      await Promise.all(results);
      for (const [index, plan] of plans.entries()) {
        const retirement = prepareTransferOperationRetirement(plan.operation);
        const commit = retirement.seal();
        if (!commit) throw new Error("The operation did not retire");
        commit.publish();
        commit.notifications.deliver();
        await retirement.retired;
        expect(snapshot(graph, options).entries).toHaveLength(index === 0 ? 1 : 2);
      }
      if (binding) {
        expect(() => snapshot(graph)).toThrow();
        expect(() => snapshot(graph, { version: 2 })).toThrow();
      } else {
        expect(snapshot(graph).entries.covered?.value).toBe("landed while covered");
      }
    } finally {
      graph.dispose();
    }
  });
}

for (const version of [1, 2] as const) {
  it(`refuses v${version} encoding when a successor is published and retired during encoding`, async () => {
    let duringEncoding = (): void => {};
    const encodedValue = new Proxy(
      { message: "visible" },
      {
        ownKeys(target) {
          duringEncoding();
          return Reflect.ownKeys(target);
        },
      },
    );
    const query = createQuery({
      name: "coverage-transition.query",
      key: (_input: { readonly value: unknown }) => "shared",
      fetch: async (input: { readonly value: unknown }) => input.value,
    });
    const mutation = createMutation({
      name: "coverage-transition.mutation",
      run: async (_input: object) => {},
      affects: [],
    });
    const contract = defineContract({
      namespace: "coverage-transition",
      operations: { query, mutation },
    });
    const first = createGraph({ contract });
    const second = createGraph({ contract });
    try {
      const firstInput = { value: "first" };
      const secondInput = { value: encodedValue };
      const firstStore = first.api.query(firstInput);
      const secondStore = second.api.query(secondInput);
      await Promise.all([firstStore.ready, secondStore.ready]);
      const predecessor = createTransferScope(first, first.api.query, firstStore);
      const carrier = {};
      const operation = prepareTransferOperation(
        [predecessor],
        [{ role: "raw", source: first.api.mutation, input: carrier }],
      );
      const operationCommit = operation.seal();
      if (!operationCommit) throw new Error("The original operation did not seal");
      operationCommit.publish();
      operationCommit.notifications.deliver();
      await first.api.mutation(carrier);

      const retirementPreflight = prepareTransferOperationRetirement(operation.operation);
      retirementPreflight.discard();
      await retirementPreflight.retired;

      let reached = false;
      let successorPublished = false;
      let retirementPublished = false;
      const evidence: { retirement?: ReturnType<typeof prepareTransferOperationRetirement> } = {};
      duringEncoding = () => {
        if (reached) return;
        reached = true;
        const successor = prepareTransferScopeSuccessors([
          {
            operation: operation.operation,
            predecessor,
            graph: second,
            source: second.api.query,
            input: secondInput,
          },
        ]);
        const successorCommit = successor.seal();
        if (!successorCommit) throw new Error("The successor did not seal");
        successorCommit.publish();
        successorPublished = true;
        successorCommit.notifications.deliver();
        const retirement = prepareTransferOperationRetirement(operation.operation);
        evidence.retirement = retirement;
        const retirementCommit = retirement.seal();
        if (!retirementCommit) throw new Error("The operation retirement did not seal");
        retirementCommit.publish();
        retirementPublished = true;
        retirementCommit.notifications.deliver();
      };

      let failure: unknown;
      try {
        if (version === 1) snapshot(second);
        else snapshot(second, { version: 2 });
      } catch (cause) {
        failure = cause;
      }
      expect(reached).toBe(true);
      expect(successorPublished).toBe(true);
      expect(retirementPublished).toBe(true);
      if (evidence.retirement === undefined)
        throw new Error("The real retirement was not prepared");
      await evidence.retirement.retired;
      if (!(failure instanceof Fault))
        throw new Error("Expected the snapshot's native encoding fault", { cause: failure });
      expect(failure.kind).toBe("encoding");
      const after =
        version === 1
          ? snapshot(second).entries.shared?.value
          : snapshot(second, { version: 2 }).entries[0]?.value;
      expect(after).toEqual({ message: "visible" });
    } finally {
      first.dispose();
      second.dispose();
    }
  });
}

it("does not admit a new operation through an unpublished successor address", async () => {
  const query = createQuery({ name: "successor.query", key: () => "shared", fetch: async () => 1 });
  const mutation = createMutation({
    name: "successor.mutation",
    run: async (_input: object) => {},
    affects: [],
  });
  const contract = defineContract({
    namespace: "successor-coverage",
    operations: { query, mutation },
  });
  const first = createGraph({ contract });
  const second = createGraph({ contract });
  try {
    const store = first.api.query();
    await store.ready;
    const predecessor = createTransferScope(first, first.api.query, store);
    const original = prepareTransferOperation(
      [predecessor],
      [{ role: "raw", source: first.api.mutation, input: {} }],
    );
    const originalCommit = original.seal();
    if (!originalCommit) throw new Error("The original operation did not seal");
    originalCommit.publish();
    originalCommit.notifications.deliver();

    const successor = prepareTransferScopeSuccessors([
      {
        operation: original.operation,
        predecessor,
        graph: second,
        source: second.api.query,
        input: undefined,
      },
    ]);
    const successorScope = successor.scopes[0]!;
    const nextInput = {};
    const prepareNext = () =>
      prepareTransferOperation(
        [successorScope],
        [{ role: "raw", source: second.api.mutation, input: nextInput }],
      );
    expect(prepareNext).toThrow(TypeError);
    const successorCommit = successor.seal();
    if (!successorCommit) throw new Error("The successor did not seal");
    successorCommit.publish();
    successorCommit.notifications.deliver();
    const next = prepareNext();
    const nextCommit = next.seal();
    if (!nextCommit) throw new Error("The published successor did not admit an operation");
    nextCommit.publish();
    nextCommit.notifications.deliver();
    const nextStore = second.api.query();
    await nextStore.ready;
    expect(Object.keys(snapshot(second).entries)).toEqual([]);
  } finally {
    first.dispose();
    second.dispose();
  }
});

for (const version of [1, 2] as const) {
  for (const publish of [false, true]) {
    it(`handles ${publish ? "published" : "inert"} scope creation during v${version} encoding`, async () => {
      let onEncode = (): void => {};
      const value = new Proxy(
        { message: "visible" },
        {
          ownKeys(target) {
            onEncode();
            return Reflect.ownKeys(target);
          },
        },
      );
      const query = createQuery({
        name: "encoding.query",
        key: () => "encoding",
        fetch: async () => value,
      });
      const mutation = createMutation({
        name: "encoding.mutation",
        run: async (_input: object) => {},
        affects: [],
      });
      const graph = createGraph({
        contract: defineContract({
          namespace: "encoding-coverage",
          operations: { query, mutation },
        }),
      });
      try {
        const store = graph.api.query();
        await store.ready;
        let ran = false;
        onEncode = () => {
          if (ran) return;
          ran = true;
          const scope = createTransferScope(graph, graph.api.query, store);
          if (!publish) return;
          const plan = prepareTransferOperation(
            [scope],
            [{ role: "raw", source: graph.api.mutation, input: {} }],
          );
          const commit = plan.seal();
          if (!commit) throw new Error("The operation did not seal");
          commit.publish();
          commit.notifications.deliver();
        };
        if (publish) {
          expect(() =>
            version === 1 ? snapshot(graph) : snapshot(graph, { version: 2 }),
          ).toThrow();
        } else if (version === 1) {
          expect(snapshot(graph).entries.encoding?.value).toEqual({ message: "visible" });
        } else {
          expect(snapshot(graph, { version: 2 }).entries[0]?.value).toEqual({ message: "visible" });
        }
        expect(ran).toBe(true);
      } finally {
        graph.dispose();
      }
    });
  }
}
