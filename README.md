# Aeolia

Aeolia is a framework-agnostic reactive state library with zero runtime
dependencies. It connects your data operations to signals: queries share keyed
stores, mutations can invalidate or refresh them, and streams feed reactive
values and projections.

Your callbacks handle the I/O. Aeolia handles the state around them: pending
work, errors, freshness, optimistic updates, subscriptions, and disposal.
You can also use its signals without the managed state layer.

Aeolia is in development. The API is not yet a stable 1.0 contract.

## Usage

Define a query and give it a key. Calls with the same key share the underlying
store within a graph.

```ts
import { createGraph, createQuery, defineContract, subscribe } from "aeolia";

const item = createQuery({
  name: "items.get",
  key: (id: string) => `items/${id}`,
  // Replace this callback with your API client, database, or another data source.
  fetch: async (id) => ({ id, name: "Copper kettle" }),
});

const graph = createGraph({
  contract: defineContract({
    namespace: "shop",
    operations: { item },
  }),
});

const kettle = graph.api.item("kettle");
const stop = subscribe(kettle, (value) => console.log(value.name));

try {
  const value = await kettle.ready;
  console.log(value.id); // "kettle"
} finally {
  stop();
  graph.dispose();
}
```

The query call starts a fetch when needed. `ready` waits for committed data
without starting another request. Subscribing to the store delivers values once
data exists, then reports changes; it does not emit an empty placeholder.

For signals alone:

```ts
import { computed, signal, subscribe } from "aeolia/reactive";

const count = signal(0);
const doubled = computed(() => count.get() * 2);
const stop = subscribe(doubled, console.log); // 0
count.set(1); // 2
stop();
```

See the [storefront workflow](tests/application/storefront.integration.test.ts) for
queries, mutations, streams, and projections working together.
Development guidance lives in [CONTRIBUTING.md](CONTRIBUTING.md).
