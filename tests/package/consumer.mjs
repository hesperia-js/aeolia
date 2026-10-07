import { Fault, createGraph, defineContract, signal, subscribe } from "aeolia";
import { affects, createMutation, createQuery } from "aeolia/contract";
import { computed, subscribe as subscribeSignal } from "aeolia/reactive";
import { testBackend } from "aeolia/testing";

function check(condition, message) {
  if (!condition) throw new Error(message);
}

export async function verifyConsumer() {
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
    graph.dispose();
    check(call.aborted, "Disposal did not abort the backend request");
    await disposedRead;
    backend.resolve(call, "late response");
    await refreshing;
    check(!observed.includes("late response"), "Disposed graph published a late response");
  } finally {
    stop();
    graph.dispose();
  }
}
