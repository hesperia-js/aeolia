import type {
  CallContext,
  FetchOptions,
  GraphId,
  OpenOptions,
  StoreKey,
} from "./contract/types.ts";

/**
 * The observable record for one deferred backend invocation.
 *
 * The backend updates `aborted` and `settled` in place, so a record retained by
 * a test reflects lifecycle changes after it was returned from `calls` or
 * `pending`.
 */
export interface CallRecord {
  /** Operation name supplied when the callback factory was created. */
  readonly name: string;

  /** Graph that issued the callback invocation. */
  readonly graph: GraphId;

  /** Input passed to the callback. */
  readonly input: unknown;

  /** Query or stream key supplied by the graph, when the call has one. */
  readonly key?: StoreKey;

  /** Whether the callback's graph-owned abort signal has fired. */
  readonly aborted: boolean;

  /** Whether the call was resolved, rejected, or its stream was ended. */
  readonly settled: boolean;
}

type MutableCallRecord = Omit<CallRecord, "aborted" | "settled"> & {
  aborted: boolean;
  settled: boolean;
};

interface CallRegistration {
  readonly record: MutableCallRecord;
  readonly removeAbortListener: () => void;
}

interface PendingCall {
  readonly record: MutableCallRecord;
  readonly resolve: (value: unknown) => void;
  readonly reject: (error: unknown) => void;
  readonly removeAbortListener: () => void;
  forgotten: boolean;
}

interface StreamWaiter<T> {
  readonly resolve: (result: IteratorResult<T>) => void;
  readonly reject: (error: unknown) => void;
}

interface OpenStream<T> {
  readonly record: MutableCallRecord;
  readonly queue: T[];
  readonly waiters: StreamWaiter<T>[];
  readonly reportGap: () => void;
  readonly removeAbortListener: () => void;
  ended: boolean;
  error?: unknown;
  errorDelivered: boolean;
}

function createCallRegistration<I>(
  name: string,
  input: I,
  options: CallContext,
  key?: StoreKey,
): CallRegistration {
  const record: MutableCallRecord = {
    name,
    graph: options.graph,
    input,
    ...(key === undefined ? {} : { key }),
    aborted: options.abortSignal.aborted,
    settled: false,
  };
  const onAbort = (): void => {
    record.aborted = true;
  };
  options.abortSignal.addEventListener("abort", onAbort, { once: true });
  return {
    record,
    removeAbortListener: () => options.abortSignal.removeEventListener("abort", onAbort),
  };
}

/**
 * A controlled backend double for deferred query, mutation, and stream calls.
 *
 * Factories returned by `respond`, `perform`, and `stream` record invocations
 * but leave them pending until the fixture calls `resolve`, `reject`, `emit`,
 * or `end`. Each active stream with a matching name receives every emitted
 * value. This backend performs no real I/O.
 */
export interface TestBackend {
  /** All currently remembered call records, in invocation order. */
  readonly calls: readonly CallRecord[];

  /**
   * Returns unsettled calls, optionally restricted to one operation name.
   *
   * The returned array is a snapshot; records themselves remain live objects.
   *
   * @param name - Optional operation name filter.
   */
  pending(name?: string): readonly CallRecord[];

  /**
   * Creates a deferred query callback.
   *
   * The resulting callback records its input, graph, and key, then returns a
   * promise settled by {@link TestBackend.resolve} or
   * {@link TestBackend.reject}. Aborting its signal marks the record as
   * aborted but does not settle the promise.
   *
   * @param name - Name recorded for each invocation.
   * @returns A query-compatible deferred callback.
   */
  respond<I, T>(name: string): (input: I, options: FetchOptions) => Promise<T>;

  /**
   * Creates a deferred mutation callback.
   *
   * The resulting callback records its input and graph, then returns a promise
   * settled by {@link TestBackend.resolve} or {@link TestBackend.reject}.
   * Aborting its signal marks the record as aborted but does not settle the
   * promise.
   *
   * @param name - Name recorded for each invocation.
   * @returns A mutation-compatible deferred callback.
   */
  perform<I, R>(name: string): (input: I, options: CallContext) => Promise<R>;

  /**
   * Creates a controlled stream opener.
   *
   * Each invocation records an active stream. Values emitted with the same
   * name are delivered to every active stream; values wait in that stream's
   * queue until its iterator asks for them. Aborting the supplied signal marks
   * the call record but does not by itself finish this controlled iterator.
   *
   * @param name - Name used to group active streams for control operations.
   * @returns A stream-compatible opener.
   */
  stream<I, T>(name: string): (input: I, options: OpenOptions) => AsyncIterable<T>;

  /**
   * Resolves one pending query or mutation call.
   *
   * A record from another, already settled, or cleared backend throws a
   * `TypeError`.
   *
   * @param call - Record returned by this backend's `calls` or `pending`.
   * @param value - Value delivered to the deferred callback.
   * @throws {@link TypeError} If `call` is not a live call owned by this backend.
   */
  resolve(call: CallRecord, value: unknown): void;

  /**
   * Rejects one pending query or mutation call.
   *
   * A record from another, already settled, or cleared backend throws a
   * `TypeError`.
   *
   * @param call - Record returned by this backend's `calls` or `pending`.
   * @param error - Rejection reason delivered to the deferred callback.
   * @throws {@link TypeError} If `call` is not a live call owned by this backend.
   */
  reject(call: CallRecord, error: unknown): void;

  /**
   * Emits one value to every active stream with `name`.
   *
   * If a stream is not currently waiting in `next()`, the value is queued for
   * that stream. Ended streams are ignored.
   *
   * @param name - Stream operation name to receive the value.
   * @param value - Emission delivered to matching streams.
   */
  emit(name: string, value: unknown): void;

  /**
   * Reports a gap to every active stream with `name`.
   *
   * The stream's own `reportGap` callback decides how that gap changes graph
   * state; the test backend only forwards the notification.
   *
   * @param name - Stream operation name to notify.
   */
  gap(name: string): void;

  /**
   * Ends every active stream with `name`.
   *
   * Without `error`, iterators finish normally. With an error, every currently
   * pending read rejects. If no read is pending, the first later read after any
   * queued values rejects and subsequent reads are complete.
   *
   * @param name - Stream operation name to end.
   * @param error - Optional terminal stream error.
   */
  end(name: string, error?: unknown): void;

  /**
   * Forgets all call records and pending/active backend handles.
   *
   * Pending query and mutation promises are not settled, and active stream
   * iterators are not ended. Their abort listeners are detached so the backend
   * no longer retains those handles. After clearing, resolving an old record
   * throws a `TypeError`.
   */
  clear(): void;
}

/**
 * Creates a backend fixture whose operations are controlled by the test.
 *
 * Use the returned factories in query, mutation, and stream definitions, then
 * drive settlement explicitly through the fixture. This is useful for testing
 * ordering, cancellation, retries, freshness, and shared stream lifecycles
 * without making network requests.
 *
 * @returns A new empty backend fixture with no calls recorded.
 * @example
 * ```ts
 * import { storeKey, type GraphId } from "aeolia";
 * import { testBackend } from "aeolia/testing";
 *
 * const backend = testBackend();
 * const request = backend.respond<string, number>("count.get")("1", {
 *   abortSignal: new AbortController().signal,
 *   graph: "graph-under-test" as GraphId,
 *   key: storeKey("count/1"),
 * });
 * backend.resolve(backend.pending("count.get")[0]!, 42);
 * await request;
 * ```
 */
export function testBackend(): TestBackend {
  const records: MutableCallRecord[] = [];
  const pending = new Map<CallRecord, PendingCall>();
  const streams = new Map<string, Set<OpenStream<unknown>>>();

  const defer = <I, T>(
    name: string,
    input: I,
    options: CallContext,
    key?: StoreKey,
  ): Promise<T> => {
    const { record, removeAbortListener } = createCallRegistration(name, input, options, key);
    records.push(record);

    const promise = new Promise<T>((resolve, reject) => {
      pending.set(record, {
        record,
        resolve: resolve as (value: unknown) => void,
        reject,
        removeAbortListener,
        forgotten: false,
      });
    });
    return promise;
  };

  const respond =
    <I, T>(name: string) =>
    (input: I, options: FetchOptions): Promise<T> =>
      defer<I, T>(name, input, options, options.key);

  const perform =
    <I, R>(name: string) =>
    (input: I, options: CallContext): Promise<R> =>
      defer<I, R>(name, input, options);

  const settle = (call: CallRecord, outcome: "resolve" | "reject", value: unknown): void => {
    const entry = pending.get(call);
    if (entry === undefined || entry.forgotten) {
      throw new TypeError("CallRecord does not belong to this TestBackend");
    }
    if (entry.record.settled) return;

    entry.record.settled = true;
    entry.removeAbortListener();
    pending.delete(call);
    if (outcome === "resolve") entry.resolve(value);
    else entry.reject(value);
  };

  const finishStream = <T>(stream: OpenStream<T>, error?: unknown): void => {
    if (stream.ended) return;
    stream.ended = true;
    stream.error = error;
    stream.record.settled = true;
    stream.removeAbortListener();

    const byName = streams.get(stream.record.name);
    byName?.delete(stream as OpenStream<unknown>);
    if (byName?.size === 0) streams.delete(stream.record.name);

    if (stream.waiters.length === 0) return;
    const waiters = stream.waiters.splice(0);
    if (error != null) {
      stream.errorDelivered = true;
      for (const waiter of waiters) waiter.reject(error);
    } else {
      for (const waiter of waiters) {
        waiter.resolve({ done: true, value: undefined as never });
      }
    }
  };

  const openStream =
    <I, T>(name: string) =>
    (input: I, options: OpenOptions): AsyncIterable<T> => {
      const { record, removeAbortListener } = createCallRegistration(
        name,
        input,
        options,
        options.key,
      );
      records.push(record);

      const stream: OpenStream<T> = {
        record,
        queue: [],
        waiters: [],
        reportGap: options.reportGap,
        removeAbortListener,
        ended: false,
        errorDelivered: false,
      };
      const byName = streams.get(name) ?? new Set<OpenStream<unknown>>();
      byName.add(stream as OpenStream<unknown>);
      streams.set(name, byName);

      const iterator: AsyncIterableIterator<T> = {
        [Symbol.asyncIterator](): AsyncIterableIterator<T> {
          return this;
        },
        next(): Promise<IteratorResult<T>> {
          if (stream.queue.length > 0) {
            return Promise.resolve({ done: false, value: stream.queue.shift() as T });
          }
          if (stream.ended) {
            if (stream.error != null && !stream.errorDelivered) {
              stream.errorDelivered = true;
              return Promise.reject(stream.error);
            }
            return Promise.resolve({ done: true, value: undefined as never });
          }
          return new Promise<IteratorResult<T>>((resolve, reject) => {
            stream.waiters.push({ resolve, reject });
          });
        },
        return(): Promise<IteratorResult<T>> {
          finishStream(stream);
          return Promise.resolve({ done: true, value: undefined as never });
        },
      };
      return iterator;
    };

  const result: TestBackend = {
    calls: records,
    pending(name?: string): readonly CallRecord[] {
      return records.filter(
        (record) => !record.settled && (name === undefined || record.name === name),
      );
    },
    respond,
    perform,
    stream: openStream,
    resolve(call: CallRecord, value: unknown): void {
      settle(call, "resolve", value);
    },
    reject(call: CallRecord, error: unknown): void {
      settle(call, "reject", error);
    },
    emit(name: string, value: unknown): void {
      for (const stream of Array.from(streams.get(name) ?? [])) {
        if (stream.ended) continue;
        const waiter = stream.waiters.shift();
        if (waiter != null) waiter.resolve({ done: false, value });
        else stream.queue.push(value);
      }
    },
    gap(name: string): void {
      for (const stream of Array.from(streams.get(name) ?? [])) {
        if (!stream.ended) stream.reportGap();
      }
    },
    end(name: string, error?: unknown): void {
      for (const stream of Array.from(streams.get(name) ?? [])) {
        finishStream(stream, error);
      }
    },
    clear(): void {
      for (const entry of pending.values()) {
        entry.removeAbortListener();
        entry.forgotten = true;
      }
      pending.clear();

      // `clear` forgets calls; it does not settle promises or end streams.
      // Detach listeners so the forgotten records and their closures are not
      // retained by a controller owned by the fixture.
      for (const byName of streams.values()) {
        for (const stream of byName) stream.removeAbortListener();
      }
      streams.clear();
      records.length = 0;
    },
  };
  return result;
}
