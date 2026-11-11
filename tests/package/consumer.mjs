import {
  Fault,
  bindOperation as rootBindOperation,
  bindStream as rootBindStream,
  inspectBoundAddress as rootInspectBoundAddress,
  createGraph,
  defineContract,
  signal,
  subscribe,
} from "aeolia";
import {
  affects,
  bindOperation,
  bindStream,
  createBindingContext,
  createMutation,
  createQuery,
  createStream,
  inspectBoundAddress,
  predictionFor,
  profileEffect,
  storeKey,
} from "aeolia/contract";
import { computed, subscribe as subscribeSignal } from "aeolia/reactive";
import { testBackend } from "aeolia/testing";

function check(condition, message) {
  if (!condition) throw new Error(message);
}

export async function verifyConsumer() {
  await verifyBoundConsumer();
  await verifyBoundStreamConsumer();
  const count = signal(2);
  const doubled = computed(() => count.get() * 2);
  const signalValues = [];
  const stopSignal = subscribeSignal(doubled, (value) => signalValues.push(value));
  try {
    count.set(3);
    check(JSON.stringify(signalValues) === "[4,6]", "Cross-entry signal propagation failed");
  } finally {
    stopSignal();
  }

  const backend = testBackend();
  const item = createQuery({
    name: "item.get",
    key: (id) => `items/${id}`,
    fetch: backend.respond("item.get"),
  });
  const save = createMutation({
    name: "item.save",
    affects: [affects(item, { select: (input) => input.id, on: "invalidate" })],
    run: backend.perform("item.save"),
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "packed-consumer", operations: { item, save } }),
  });
  const pending = (name) => {
    const calls = backend.pending(name);
    check(calls.length === 1, `Expected one pending ${name} call, got ${calls.length}`);
    return calls[0];
  };
  const store = graph.api.item("one");
  const observed = [];
  const stop = subscribe(store, (value) => observed.push(value));
  try {
    backend.resolve(pending("item.get"), "initial");
    check((await store.ready) === "initial", "Packed query did not resolve");

    const mutation = graph.api.save({ id: "one" });
    backend.resolve(pending("item.save"), "saved");
    check((await mutation) === "saved", "Packed mutation did not resolve");
    check(store.status.get() === "revalidating", "Mutation did not refresh the observed query");
    backend.resolve(pending("item.get"), "updated");
    check((await store.ready) === "updated", "Refreshed data did not reach the query handle");

    const resetting = graph.reset();
    check(store.value.get() === undefined, "Reset retained the previous identity's data");
    check(graph.resetVersion.get() === 1, "Reset version did not advance");
    backend.resolve(pending("item.get"), "new identity");
    await resetting;
    check(store.value.get() === "new identity", "Reset did not refresh the preserved handle");
    check(
      JSON.stringify(observed) === '["initial","updated","new identity"]',
      "The consumer received the wrong query updates",
    );

    const refreshing = store.revalidate();
    const call = pending("item.get");
    const disposedRead = store.ready.then(
      () => {
        throw new Error("Disposed query readiness unexpectedly resolved");
      },
      (error) =>
        check(error instanceof Fault && error.kind === "disposed", "Expected a disposal fault"),
    );
    const retirement = graph.retire();
    check(graph.retire() === retirement, "Retirement did not return the same promise");
    let retired = false;
    void retirement.then(
      () => {
        retired = true;
      },
      () => {
        retired = true;
      },
    );
    check(call.aborted, "Retirement did not abort the backend request");
    await disposedRead;
    check(!retired, "Retirement finished before the original query settled");
    backend.resolve(call, "late response");
    await refreshing;
    await retirement;
    check(!observed.includes("late response"), "Disposed graph published a late response");
  } finally {
    stop();
    graph.dispose();
  }
}

async function verifyBoundConsumer() {
  check(bindOperation === rootBindOperation, "Binding constructor differs across package entries");
  const backend = testBackend();
  const query = createQuery({
    name: "bound.read",
    key: (input) => input.id,
    fetch: backend.respond("bound.read"),
  });
  const mutation = createMutation({
    name: "bound.save",
    run: backend.perform("bound.save"),
    affects: [affects(query, { select: (input) => ({ id: input.id }), on: "revalidate" })],
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "packed-bound", operations: { query, mutation } }),
  });
  try {
    const context = createBindingContext(graph);
    const sibling = createBindingContext(graph);
    const numbers = bindOperation(graph, query, { context, profile: {}, fetch: query.fetch });
    const labels = bindOperation(graph, query, {
      context,
      profile: {},
      fetch: async (input, call) => "label:" + (await query.fetch(input, call)),
    });
    const number = numbers.member({ id: "one" }, { initial: 1 });
    const label = labels.member({ id: "one" }, { initial: "label:1" });
    const effect = profileEffect(mutation, 0, query, {
      prepare: (phase) =>
        phase.existing(phase.selectedInput).map((target) => phase.prepare(target)),
      settle: (phase) =>
        phase
          .existing(phase.selectedInput)
          .map((target) => phase.selected(target, phase.selectedInput)),
    });
    const save = bindOperation(graph, mutation, {
      context,
      profile: {},
      effects: [effect],
      run: async (input, call) => ({ receipt: await mutation.run(input, call) }),
    });
    const override = predictionFor(mutation, numbers.target(number), () => 9);
    const running = save.member({ id: "one" }, { optimistic: [override] });
    check(
      number.value.get() === 9 && label.value.get() === "label:1",
      "Physical prediction override crossed profiles",
    );
    const pending = backend.pending("bound.save");
    check(pending.length === 1, "Bound mutation did not dispatch exactly once");
    backend.resolve(pending[0], "saved");
    check((await running).receipt === "saved", "Bound completion lost its exact result");
    const refreshes = backend.pending("bound.read");
    check(refreshes.length === 2, "Bound effect did not refresh both existing profiles");
    for (const call of refreshes) backend.resolve(call, 12);
    await Promise.all([number.ready, label.ready]);
    check(
      number.value.get() === 12 && label.value.get() === "label:12",
      "Refresh lost the profile completion callback",
    );
    const retirement = context.retire();
    check(context.retire() === retirement, "Context retirement did not return its cached promise");
    await retirement;
    check(sibling.active.get(), "Retiring one context disposed its sibling");
    sibling.dispose();
  } finally {
    graph.dispose();
  }
}

async function verifyBoundStreamConsumer() {
  check(
    bindStream === rootBindStream && inspectBoundAddress === rootInspectBoundAddress,
    "Stream/address constructors differ across package entries",
  );
  const backend = testBackend();
  const stream = createStream({
    name: "packed.events",
    key: (input) => input,
    open: backend.stream("packed.events"),
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "packed-stream", operations: { stream } }),
  });
  try {
    const binding = bindStream(graph, stream, graph.api.stream, {
      context: createBindingContext(graph),
    });
    const empty = rootInspectBoundAddress(binding, storeKey("one"));
    check(
      empty.kind === "stable" && empty.state === "empty",
      "Packed address did not inspect empty",
    );
    const a = binding.member("one", { expectEmpty: empty.condition });
    const ticket = binding.session(a);
    backend.emit("packed.events", undefined);
    backend.end("packed.events");
    const receipt = await ticket.detached;
    const terminal = receipt.terminalCapture;
    check(
      terminal.kind === "captured" &&
        terminal.capture.kind === "stable" &&
        terminal.capture.present &&
        terminal.capture.value === undefined,
      "Packed stream lost present undefined at native retirement",
    );
    const b = binding.member("one");
    check(
      a.identity === b.identity && ticket.identity !== binding.session(b).identity,
      "Packed stream did not separate native sessions on the physical entry",
    );
    check(
      a.available.get() && !b.available.get() && b.pending.get(),
      "Packed fixed facets followed another session",
    );
    check(
      rootInspectBoundAddress(binding, storeKey("one"), { expected: empty.stamp }).kind ===
        "changed",
      "Packed address stamp survived acquisition",
    );
    let rejected = false;
    try {
      a.set(99);
    } catch (cause) {
      rejected = cause instanceof TypeError;
    }
    check(rejected && !b.available.get(), "Retired packed wrapper wrote into its successor");
  } finally {
    graph.dispose();
  }
}
