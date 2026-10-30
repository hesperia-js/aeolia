import { afterEach, beforeEach, expect, it, jest } from "bun:test";
import {
  bindOperation,
  createBindingContext,
  createGraph,
  createQuery,
  createStream,
  computed,
  captureReactiveConstruction,
  prepareRequiredTransferCapture,
  preparePassiveTransferClaim,
  prepareRequiredTransferAdoption,
  defineContract,
  Fault,
  observeStore,
  peekStore,
  subscribe,
  withoutWrites,
  type ObservationLease,
  type QueryStore,
  type ValueOptions,
  type ClosedEncodedValue,
  type RequiredTransferCodec,
} from "../../src/index.ts";
import { testBackend } from "../../src/testing.ts";

beforeEach(() => {
  jest.useFakeTimers();
  jest.setSystemTime(0);
});
afterEach(() => {
  jest.clearAllTimers();
  jest.useRealTimers();
});

function fixture(
  value?: Pick<ValueOptions<number | undefined>, "equals" | "initial">,
  options?: ValueOptions<number | undefined>,
) {
  const backend = testBackend();
  const diagnostics: unknown[] = [];
  const query = createQuery({
    name: "observed",
    key: (_input: void) => "same/key",
    fetch: backend.respond<void, number | undefined>("observed"),
  });
  const graph = createGraph({
    contract: defineContract({ namespace: "observation", operations: { query } }),
    idleMs: 10,
    onUnobservedFault: (cause) => {
      diagnostics.push(cause);
    },
  });
  const context = createBindingContext(graph);
  const binding = bindOperation(graph, query, {
    context,
    profile: {},
    fetch: query.fetch,
    ...(value === undefined ? {} : { value }),
  });
  const store = binding.member(undefined, options);
  return { backend, diagnostics, graph, context, binding, store };
}

it("publishes present undefined while a cold request remains fetching and still owns its landing", async () => {
  const f = fixture();
  try {
    const second = f.binding.member();
    const lease = observeStore(f.graph, f.store);
    const ready = second.ready;
    expect(lease.publish(undefined)).toBe(true);
    expect(f.store.available.get()).toBe(true);
    expect(peekStore(f.store)).toMatchObject({ kind: "stable", present: true, value: undefined });
    expect(f.store.status.get()).toBe("fetching");
    expect(f.store.pending.get()).toBe(true);
    expect(f.backend.calls).toHaveLength(1);
    expect(() => observeStore(f.graph, second)).toThrow(TypeError);
    expect(f.backend.calls[0]!.aborted).toBe(false);
    f.backend.resolve(f.backend.calls[0]!, 7);
    expect(await ready).toBe(7);
    expect(f.store.value.get()).toBe(7);
    expect(lease.active.get()).toBe(true);
  } finally {
    f.graph.dispose();
  }
});

for (const recall of ["query", "stream"] as const) {
  it(`does not revive observation after a collected raw entry rejects ${recall} initial configuration`, async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "query",
      key: (_input: void) => "one",
      fetch: backend.respond<void, number>("query"),
    });
    const stream = createStream({
      name: "stream",
      key: (_input: void) => "one",
      open: backend.stream<void, number>("stream"),
    });
    const graph = createGraph({
      contract: { namespace: "failed-recall", operations: { query, stream } },
      idleMs: 10,
    });
    try {
      const store = graph.api.query(undefined, { initial: 1 });
      expect(await store.ready).toBe(1);
      jest.advanceTimersByTime(11);
      expect(peekStore(store)).toMatchObject({ kind: "stable", present: false });
      expect(() =>
        recall === "query"
          ? graph.api.query(undefined, { initial: 2 })
          : graph.api.stream(undefined, { initial: 2 }),
      ).toThrow(Fault);
      expect(() => observeStore(graph, store)).toThrow(TypeError);
      expect(backend.calls).toHaveLength(0);
      graph.at(store.key).set(3);
      expect(await store.ready).toBe(3);
      const admitted = observeStore(graph, store);
      expect(admitted.publish(4)).toBe(true);
      expect(peekStore(store)).toMatchObject({ kind: "stable", present: true, value: 4 });
      jest.advanceTimersByTime(11);
      expect(admitted.active.get()).toBe(false);
      expect(peekStore(store)).toMatchObject({ kind: "stable", present: false });
    } finally {
      graph.dispose();
    }
  });
}

for (const conflict of ["equals", "snapshot"] as const) {
  it(`validates a recalled ${conflict} conflict before installing a new initial value`, async () => {
    const backend = testBackend();
    const query = createQuery({
      name: "query",
      key: (_input: void) => "one",
      fetch: backend.respond<void, number>("query"),
    });
    const graph = createGraph({
      contract: { namespace: "configuration-admission", operations: { query } },
      idleMs: 10,
    });
    try {
      const originalEquals = (left: number, right: number) => left === right;
      const store = graph.api.query(undefined, { equals: originalEquals, snapshot: true });
      const ready = store.ready;
      backend.resolve(backend.calls[0]!, 9);
      await ready;
      await graph.reset();
      jest.advanceTimersByTime(11);
      const options =
        conflict === "equals"
          ? { initial: 2, equals: () => false }
          : { initial: 2, snapshot: false };
      expect(() => graph.api.query(undefined, options)).toThrow(Fault);
      expect(peekStore(store)).toMatchObject({ kind: "stable", present: false });
      expect(() => observeStore(graph, store)).toThrow(TypeError);
      expect(backend.calls).toHaveLength(1);
    } finally {
      graph.dispose();
    }
  });
}

it("keeps bound targets inactive after a rejected recall and admits real pending/failed work", async () => {
  const f = fixture(undefined, { initial: 1 });
  try {
    expect(await f.store.ready).toBe(1);
    const initialTicket = f.binding.target(f.store)!;
    jest.advanceTimersByTime(11);
    expect(f.binding.target(f.store)).toBeUndefined();
    expect(() => f.binding.member(undefined, { initial: 2 })).toThrow(Fault);
    expect(f.binding.target(f.store)).toBeUndefined();
    expect(() => observeStore(f.graph, f.store)).toThrow(TypeError);
    expect(f.backend.calls).toHaveLength(0);
    const admitted = f.binding.member();
    expect(initialTicket.forBinding(f.binding)).toBeUndefined();
    expect(f.binding.target(f.store)).toBeDefined();
    const lease = observeStore(f.graph, admitted);
    expect(lease.active.get()).toBe(true);
    expect(f.backend.calls).toHaveLength(1);
    const currentTicket = f.binding.target(admitted)!;
    expect(() => f.binding.member(undefined, { initial: 2 })).toThrow(Fault);
    expect(currentTicket.forBinding(f.binding)).toBe(currentTicket);
    expect(lease.active.get()).toBe(true);
    expect(f.backend.calls[0]!.aborted).toBe(false);
    const ready = admitted.ready;
    const failure = new Error("genuine request failed");
    f.backend.reject(f.backend.calls[0]!, failure);
    await expect(ready).rejects.toBe(failure);
    expect(f.binding.target(admitted)).toBeDefined();
    expect(lease.publish(5)).toBe(true);
    jest.advanceTimersByTime(11);
    expect(lease.active.get()).toBe(false);
    expect(f.binding.target(admitted)).toBeUndefined();
  } finally {
    f.graph.dispose();
  }
});

it("a rejected outer recall preserves the genuine reentrant successor at the same bound address", async () => {
  const backend = testBackend();
  let reenter = false;
  let admit!: () => void;
  let successor: QueryStore<number> | undefined;
  const query = createQuery({
    name: "query",
    key: (_input: void) => {
      if (reenter) {
        reenter = false;
        admit();
      }
      return "one";
    },
    fetch: backend.respond<void, number>("query"),
  });
  const graph = createGraph({
    contract: { namespace: "reentrant-admission", operations: { query } },
    idleMs: 10,
  });
  try {
    const binding = bindOperation(graph, query, {
      context: createBindingContext(graph),
      profile: {},
      fetch: query.fetch,
    });
    const old = binding.member(undefined, { initial: 1 });
    await old.ready;
    jest.advanceTimersByTime(11);
    admit = () => {
      successor = binding.member();
    };
    reenter = true;
    expect(() => binding.member(undefined, { initial: 2 })).toThrow(Fault);
    expect(successor).toBeDefined();
    expect(binding.target(old)).toBeDefined();
    const lease = observeStore(graph, successor!);
    expect(lease.active.get()).toBe(true);
    expect(backend.calls).toHaveLength(1);
    expect(backend.calls[0]!.aborted).toBe(false);
    const ready = successor!.ready;
    backend.resolve(backend.calls[0]!, 9);
    expect(await ready).toBe(9);
    expect(lease.active.get()).toBe(true);
  } finally {
    graph.dispose();
  }
});

it("preserves issue identity through request success and publisher replacement until valid observation", async () => {
  const f = fixture();
  try {
    const lease = observeStore(f.graph, f.store);
    const issue = Object.freeze({ kind: "invalid", fields: ["count"] });
    expect(lease.reject(issue)).toBe(true);
    const ready = f.store.ready;
    f.backend.resolve(f.backend.calls[0]!, 1);
    await ready;
    expect(lease.issue.get()).toBe(issue);
    lease.dispose();
    expect(lease.active.get()).toBe(false);
    expect(lease.issue.get()).toBe(issue);
    const successor = observeStore(f.graph, f.binding.member());
    expect(successor.issue.get()).toBe(issue);
    expect(lease.publish(99)).toBe(false);
    expect(lease.reject(Object.freeze({ old: true }))).toBe(false);
    expect(lease.report("obsolete")).toBe(false);
    expect(f.diagnostics).toEqual([]);
    expect(successor.publish(2)).toBe(true);
    expect(lease.issue.get()).toBeUndefined();
    expect(f.store.value.get()).toBe(2);
  } finally {
    f.graph.dispose();
  }
});

it("keeps failed request error and freshness authority when the observed base changes", async () => {
  const f = fixture();
  try {
    const lease = observeStore(f.graph, f.store);
    const ready = f.store.ready;
    f.backend.resolve(f.backend.calls[0]!, 1);
    await ready;
    const request = f.store.revalidate();
    const failedReady = f.store.ready;
    const failure = new Error("request failed");
    f.backend.reject(f.backend.calls[1]!, failure);
    await expect(failedReady).rejects.toBe(failure);
    await request;
    expect(lease.publish(8)).toBe(true);
    expect(f.store.value.get()).toBe(8);
    expect(f.store.status.get()).toBe("failed");
    expect(f.store.error.get()).toBe(failure);
    expect(f.store.pending.get()).toBe(false);
  } finally {
    f.graph.dispose();
  }
});

it("revokes old issue and callbacks before reset subscribers can acquire the new incarnation", async () => {
  const f = fixture();
  try {
    const ready = f.store.ready;
    f.backend.resolve(f.backend.calls[0]!, 1);
    await ready;
    const old = observeStore(f.graph, f.store);
    old.reject(Object.freeze({ invalid: true }));
    const issues: unknown[] = [];
    const stop = subscribe(old.issue, (issue) => {
      issues.push(issue);
    });
    const reset = f.graph.reset();
    expect(old.active.get()).toBe(false);
    expect(old.issue.get()).toBeUndefined();
    expect(old.publish(100)).toBe(false);
    const fresh = observeStore(f.graph, f.store);
    const nextIssue = Object.freeze({ next: true });
    fresh.reject(nextIssue);
    expect(old.issue.get()).toBeUndefined();
    expect(issues).toStrictEqual([{ invalid: true }, undefined]);
    expect(f.backend.calls).toHaveLength(2);
    f.backend.resolve(f.backend.calls[1]!, 2);
    await reset;
    expect(fresh.issue.get()).toBe(nextIssue);
    jest.advanceTimersByTime(10);
    expect(fresh.active.get()).toBe(false);
    stop();
  } finally {
    f.graph.dispose();
  }
});

it("refuses stale storage after authored equality revokes the publisher", async () => {
  let armed = false;
  let lease!: ObservationLease<number | undefined>;
  const f = fixture({
    equals: () => {
      if (armed) lease.dispose();
      return false;
    },
  });
  try {
    const ready = f.store.ready;
    f.backend.resolve(f.backend.calls[0]!, 1);
    await ready;
    lease = observeStore(f.graph, f.store);
    const issue = Object.freeze({ retained: true });
    lease.reject(issue);
    const before = peekStore(f.store);
    armed = true;
    expect(lease.publish(2)).toBe(false);
    armed = false;
    expect(peekStore(f.store)).toStrictEqual(before);
    expect(lease.issue.get()).toBe(issue);
    expect(observeStore(f.graph, f.store).issue.get()).toBe(issue);
  } finally {
    f.graph.dispose();
  }
});

it("reports a completed publication when its observer subsequently disposes the publisher", async () => {
  const f = fixture();
  try {
    const ready = f.store.ready;
    f.backend.resolve(f.backend.calls[0]!, 1);
    await ready;
    const lease = observeStore(f.graph, f.store);
    const seen: Array<number | undefined> = [];
    const stop = subscribe(f.store.value, (value) => {
      seen.push(value);
      if (value === 2) lease.dispose();
    });
    expect(lease.publish(2)).toBe(true);
    expect(seen).toContain(2);
    expect(lease.active.get()).toBe(false);
    expect(peekStore(f.store)).toMatchObject({ kind: "stable", present: true, value: 2 });
    stop();
  } finally {
    f.graph.dispose();
  }
});

it("preserves undefined diagnostic causes and rejects accessor options before invoking them", () => {
  const f = fixture();
  try {
    const lease = observeStore(f.graph, f.store);
    const issue = Object.freeze({ invalid: true });
    lease.reject(issue, { cause: undefined });
    expect(f.diagnostics).toStrictEqual([undefined]);
    lease.report("processing fault");
    expect(f.diagnostics).toStrictEqual([undefined, "processing fault"]);
    let reads = 0;
    expect(() =>
      lease.publish(2, {
        get transfer() {
          reads += 1;
          return "eligible" as const;
        },
      }),
    ).toThrow(TypeError);
    expect(reads).toBe(0);
    expect(() => withoutWrites(() => lease.publish(2))).toThrow(Fault);
    expect(lease.issue.get()).toBe(issue);
    expect(f.store.available.get()).toBe(false);
    const revokedOptions = new Proxy(
      {},
      {
        ownKeys() {
          lease.dispose();
          return [];
        },
      },
    );
    expect(lease.publish(3, revokedOptions)).toBe(false);
    expect(observeStore(f.graph, f.store).issue.get()).toBe(issue);
  } finally {
    f.graph.dispose();
  }
});

it("cannot acquire through fabricated query wrappers, another graph or a disposed context", () => {
  const f = fixture();
  const other = fixture();
  try {
    expect(() => observeStore(other.graph, f.store)).toThrow(TypeError);
    expect(() => observeStore(f.graph, Object.create(f.store))).toThrow(TypeError);
    expect(f.backend.calls).toHaveLength(1);
    const lease = observeStore(f.graph, f.store);
    lease.reject(Object.freeze({ retained: true }));
    f.context.dispose();
    expect(lease.active.get()).toBe(false);
    expect(lease.issue.get()).toBeUndefined();
    expect(lease.publish(8)).toBe(false);
    expect(() => observeStore(f.graph, f.store)).toThrow(Fault);
    const otherLease = observeStore(other.graph, other.store);
    other.graph.dispose();
    expect(otherLease.active.get()).toBe(false);
    expect(otherLease.issue.get()).toBeUndefined();
    expect(otherLease.report("late")).toBe(false);
  } finally {
    f.graph.dispose();
    other.graph.dispose();
  }
});

it("maps cached observation metadata only to an existing receiver publisher", async () => {
  const source = fixture();
  const target = fixture();
  const missing = fixture();
  const codec: RequiredTransferCodec = {
    encodeValue: (value) => value as ClosedEncodedValue,
    encodeFailure: (cause) => String(cause),
    decodeValue: (value) => value,
    decodeFailure: (failure) => new Error(String(failure)),
  };
  let capture: ReturnType<typeof prepareRequiredTransferCapture> | undefined;
  let claim: ReturnType<typeof preparePassiveTransferClaim> | undefined;
  let missingClaim: ReturnType<typeof preparePassiveTransferClaim> | undefined;
  let capturePublished = false;
  let adoptionPublished = false;
  let adoption: ReturnType<typeof prepareRequiredTransferAdoption> | undefined;
  try {
    for (const [f, value] of [
      [source, 2],
      [target, 4],
      [missing, 6],
    ] as const) {
      const ready = f.store.ready;
      f.backend.resolve(f.backend.calls[0]!, value);
      await ready;
    }

    const sourceLease = observeStore(source.graph, source.store);
    const sourceDerived = computed(
      () => sourceLease.active.get() && sourceLease.issue.get() === undefined,
    );
    expect(sourceDerived.get()).toBe(true);
    capture = prepareRequiredTransferCapture({
      graphs: [source.graph],
      roots: [{ source: sourceDerived, construction: captureReactiveConstruction(sourceDerived) }],
      codec,
    });
    const captureCommit = capture.seal();
    if (captureCommit === undefined) throw new Error("The observation capture did not seal");
    captureCommit.publish();
    capturePublished = true;
    captureCommit.notifications.deliver();
    await capture.retired;

    const targetLease = observeStore(target.graph, target.store);
    expect(targetLease.active.get()).toBe(true);
    expect(targetLease.issue.get()).toBeUndefined();
    claim = preparePassiveTransferClaim(target.graph, target.binding, undefined);
    const targetDerived = computed(
      () => targetLease.active.get() && targetLease.issue.get() === undefined,
    );
    const targetConstruction = captureReactiveConstruction(targetDerived);
    const image = capture.image;
    const nativeSource = image.stores[0];
    if (nativeSource === undefined)
      throw new Error("The observation image omitted its native source");
    adoption = prepareRequiredTransferAdoption(image, {
      graphs: [target.graph],
      sources: [{ slot: nativeSource.slot, claim }],
      reactive: [
        { slot: image.roots[0]!, target: targetDerived, construction: targetConstruction },
      ],
      codec,
      mappedStateWrites: [],
    });
    const adoptionCommit = adoption.seal();
    if (adoptionCommit === undefined) throw new Error("The observation adoption did not seal");
    adoptionCommit.publish();
    adoptionPublished = true;
    expect(targetDerived.get()).toBe(true);
    expect(targetLease.active.get()).toBe(true);
    adoptionCommit.notifications.deliver();
    await adoption.retired;

    const browserIssue = Object.freeze({ kind: "browser" });
    expect(targetLease.reject(browserIssue)).toBe(true);
    expect(targetDerived.get()).toBe(false);
    expect(sourceLease.issue.get()).toBeUndefined();

    missingClaim = preparePassiveTransferClaim(missing.graph, missing.binding, undefined);
    const missingDerived = computed(() => missingClaim!.store.value.get() !== undefined);
    const missingConstruction = captureReactiveConstruction(missingDerived);
    expect(() =>
      prepareRequiredTransferAdoption(image, {
        graphs: [missing.graph],
        sources: [{ slot: nativeSource.slot, claim: missingClaim! }],
        reactive: [
          { slot: image.roots[0]!, target: missingDerived, construction: missingConstruction },
        ],
        codec,
        mappedStateWrites: [],
      }),
    ).toThrow(TypeError);
    missingClaim.discard();
    await missingClaim.retired;
    missingClaim = undefined;
    const recoveredPublisher = observeStore(missing.graph, missing.store);
    expect(recoveredPublisher.active.get()).toBe(true);
  } finally {
    if (!adoptionPublished) adoption?.discard();
    if (!capturePublished) capture?.discard();
    if (claim !== undefined && !adoptionPublished) claim.discard();
    if (missingClaim !== undefined) missingClaim.discard();
    if (adoption !== undefined)
      await adoption.retired.then(
        () => undefined,
        () => undefined,
      );
    if (claim !== undefined)
      await claim.retired.then(
        () => undefined,
        () => undefined,
      );
    if (missingClaim !== undefined)
      await missingClaim.retired.then(
        () => undefined,
        () => undefined,
      );
    if (capture !== undefined)
      await capture.retired.then(
        () => undefined,
        () => undefined,
      );
    source.graph.dispose();
    target.graph.dispose();
    missing.graph.dispose();
  }
});
