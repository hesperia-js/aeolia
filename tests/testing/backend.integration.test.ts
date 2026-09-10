import { expect, test } from "bun:test";
import { storeKey, type GraphId } from "../../src/contract/index.ts";
import { testBackend } from "../../src/testing.ts";

test("test backend defers and settles individual calls, including abort state", async () => {
  const backend = testBackend();
  const abort = new AbortController();
  const fetch = backend.respond<{ id: number }, string>("users.get");
  const first = fetch(
    { id: 1 },
    {
      abortSignal: abort.signal,
      graph: "graph-a" as GraphId,
      key: storeKey("users/1"),
    },
  );
  const secondAbort = new AbortController();
  const second = fetch(
    { id: 2 },
    {
      abortSignal: secondAbort.signal,
      graph: "graph-a" as GraphId,
      key: storeKey("users/2"),
    },
  );

  expect(backend.calls).toHaveLength(2);
  expect(backend.calls[0]?.graph).toBe("graph-a" as GraphId);
  expect(backend.calls[0]?.key).toBe(storeKey("users/1"));
  expect(backend.calls[0]?.input).toEqual({ id: 1 });
  expect(backend.pending("users.get")).toHaveLength(2);
  expect(backend.pending()[0]).toBe(backend.calls[0]);
  secondAbort.abort();
  expect(backend.calls[1]?.aborted).toBe(true);

  const calls = backend.calls;
  backend.resolve(calls[1]!, "second");
  backend.reject(calls[0]!, new Error("first failed"));
  await expect(second).resolves.toBe("second");
  await expect(first).rejects.toThrow("first failed");
  const perform = backend.perform<{ id: number }, string>("users.rename");
  const mutation = perform(
    { id: 1 },
    {
      abortSignal: new AbortController().signal,
      graph: "graph-a" as GraphId,
    },
  );
  expect(Object.prototype.hasOwnProperty.call(backend.calls[2], "key")).toBe(false);
  backend.resolve(backend.calls[2]!, "renamed");
  await expect(mutation).resolves.toBe("renamed");
  expect(backend.pending()).toHaveLength(0);
  expect(backend.calls.every((call) => call.settled)).toBe(true);
  abort.abort();
  expect(backend.calls[0]?.aborted).toBe(false);
});

test("test backend rejects repeated and foreign settlement handles", async () => {
  const firstBackend = testBackend();
  const secondBackend = testBackend();
  const options = {
    abortSignal: new AbortController().signal,
    graph: "graph-settle" as GraphId,
    key: storeKey("settle/1"),
  };
  const firstPromise = firstBackend.respond<void, number>("settle")(undefined, options);
  void secondBackend.respond<void, number>("settle")(undefined, options);
  const firstCall = firstBackend.calls[0]!;
  const foreignCall = secondBackend.calls[0]!;

  firstBackend.resolve(firstCall, 1);
  await expect(firstPromise).resolves.toBe(1);
  expect(() => firstBackend.resolve(firstCall, 2)).toThrow();
  expect(() => firstBackend.resolve(foreignCall, 2)).toThrow();
  secondBackend.clear();
});

test("test backend broadcasts emissions, gaps, and failed termination to every matching stream", async () => {
  const backend = testBackend();
  const firstAbort = new AbortController();
  const secondAbort = new AbortController();
  let firstGaps = 0;
  let secondGaps = 0;
  const open = backend.stream<{ room: string }, number>("rooms.events");
  const firstIterator = open(
    { room: "one" },
    {
      abortSignal: firstAbort.signal,
      graph: "graph-stream" as GraphId,
      key: storeKey("rooms/one"),
      reportGap: () => {
        firstGaps += 1;
      },
    },
  )[Symbol.asyncIterator]();
  const secondIterator = open(
    { room: "two" },
    {
      abortSignal: secondAbort.signal,
      graph: "graph-stream" as GraphId,
      key: storeKey("rooms/two"),
      reportGap: () => {
        secondGaps += 1;
      },
    },
  )[Symbol.asyncIterator]();
  const first = firstIterator.next();
  const second = secondIterator.next();
  backend.emit("rooms.events", 7);
  await expect(first).resolves.toEqual({ done: false, value: 7 });
  await expect(second).resolves.toEqual({ done: false, value: 7 });
  backend.gap("rooms.events");
  expect(firstGaps).toBe(1);
  expect(secondGaps).toBe(1);

  const firstEnded = firstIterator.next();
  const secondEnded = secondIterator.next();
  const error = new Error("stream failed");
  backend.end("rooms.events", error);
  await expect(firstEnded).rejects.toBe(error);
  await expect(secondEnded).rejects.toBe(error);
  expect(backend.calls.every((call) => call.settled)).toBe(true);
  expect(backend.pending("rooms.events")).toHaveLength(0);
  firstAbort.abort();
  secondAbort.abort();
  expect(backend.calls.every((call) => !call.aborted)).toBe(true);
});

test("test backend stream return settles, and clear detaches an open stream", async () => {
  const backend = testBackend();
  const returnedAbort = new AbortController();
  const open = backend.stream<void, number>("return-me");
  const returnedIterator = open(undefined, {
    abortSignal: returnedAbort.signal,
    graph: "graph-stream" as GraphId,
    key: storeKey("return-me"),
    reportGap: () => undefined,
  })[Symbol.asyncIterator]();
  const returnedCall = backend.calls[0]!;

  await expect(returnedIterator.return!()).resolves.toEqual({ done: true, value: undefined });
  expect(returnedCall.settled).toBe(true);
  expect(backend.pending("return-me")).toHaveLength(0);
  returnedAbort.abort();
  expect(returnedCall.aborted).toBe(false);

  const clearedAbort = new AbortController();
  void backend.stream<void, number>("clear-stream")(undefined, {
    abortSignal: clearedAbort.signal,
    graph: "graph-stream" as GraphId,
    key: storeKey("clear-stream"),
    reportGap: () => undefined,
  });
  const clearedCall = backend.calls[1]!;
  backend.clear();
  clearedAbort.abort();

  expect(backend.calls).toHaveLength(0);
  expect(clearedCall.settled).toBe(false);
  expect(clearedCall.aborted).toBe(false);
});

test("test backend clear removes abort listeners without settling deferred calls", () => {
  const backend = testBackend();
  const controller = new AbortController();
  const fetch = backend.respond("clear-me");
  void fetch(undefined, {
    abortSignal: controller.signal,
    graph: "graph-clear" as GraphId,
    key: storeKey("clear-me"),
  });
  const call = backend.calls[0]!;

  backend.clear();
  controller.abort();

  expect(backend.calls).toHaveLength(0);
  expect(call.aborted).toBe(false);
  expect(call.settled).toBe(false);
});
