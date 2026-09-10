import type { QueryDefinition, Readable, StoreStatus } from "./types.ts";
import type { Fault } from "../fault.ts";
import { graphFault, assertStoreOpen } from "./faults.ts";
import { __internal as reactiveInternal, computed, signal } from "../reactive.ts";
import type { QueryCaller, QueryRequest, StoreRuntime } from "./runtime.ts";
import { finiteNonNegative } from "./runtime-validation.ts";

export function currentRequest<T>(runtime: StoreRuntime<T>): QueryRequest | undefined {
  for (const request of runtime.requests) {
    if (!request.settled && !request.superseded && request.generation === runtime.generation)
      return request;
  }
  return undefined;
}

export function asWindow(value: number | undefined, fallback: number | undefined): number {
  const result = value ?? fallback;
  return result === undefined
    ? Number.POSITIVE_INFINITY
    : finiteNonNegative(result, "revalidateAfterMs");
}

export function statusFor<T>(
  caller: QueryCaller<T>,
  at = Date.now(),
  landingGeneration?: number,
): StoreStatus {
  const runtime = caller.runtime;
  assertStoreOpen(runtime);
  const current = currentRequest(runtime);
  if (current != null) return runtime.hasCommitted ? "revalidating" : "fetching";
  if (!runtime.hasCommitted && runtime.failing) return "failed";
  if (!runtime.hasCommitted) return "empty";
  if (runtime.failing) return "failed";
  if (runtime.invalidated) return "stale";
  if (landingGeneration === runtime.generation) return "ready";
  const stale =
    runtime.invalidated ||
    (runtime.lastLandingAt != null && at - runtime.lastLandingAt >= caller.windowMs);
  if (stale) return "stale";
  return "ready";
}

export function writeCallerStatus<T>(
  caller: QueryCaller<T>,
  at = Date.now(),
  landingGeneration?: number,
): void {
  caller.statusCell.set(statusFor(caller, at, landingGeneration));
}

export function writeAllStatuses<T>(
  runtime: StoreRuntime<T>,
  at = Date.now(),
  landingGeneration?: number,
): void {
  const errors: unknown[] = [];
  for (const caller of runtime.callers) {
    try {
      writeCallerStatus(caller, at, landingGeneration);
    } catch (error) {
      errors.push(error);
    }
  }
  if (errors.length === 1) throw errors[0];
  if (errors.length > 1) throw new AggregateError(errors, "Several caller status writes failed");
}

export function writeElapsedStatuses<T>(runtime: StoreRuntime<T>, base: number, at: number): void {
  const errors: unknown[] = [];
  for (const caller of runtime.callers) {
    if (!Number.isFinite(caller.windowMs) || at - base < caller.windowMs) continue;
    try {
      writeCallerStatus(caller, at);
    } catch (error) {
      errors.push(error);
    }
  }
  if (errors.length === 1) throw errors[0];
  if (errors.length > 1) throw new AggregateError(errors, "Several elapsed status writes failed");
}

export function makePending(status: Readable<StoreStatus>): Readable<boolean> {
  return computed(() => {
    const value = status.get();
    return value === "fetching" || value === "revalidating";
  });
}

export function createCaller<T>(
  runtime: StoreRuntime<T>,
  query: QueryDefinition<any, T, any>,
  input: unknown,
  windowMs: number,
  abortSignal?: AbortSignal,
): QueryCaller<T> {
  const label = `query:${query.name}:${runtime.key as string}:status`;
  const statusCell = signal<StoreStatus>("empty", { label: `${label}:cell` });
  const status = computed(
    () => {
      runtime.lifecycle.get();
      assertStoreOpen(runtime);
      return statusCell.get();
    },
    { label },
  );
  const pending = makePending(status);
  const caller: QueryCaller<T> = {
    query,
    input,
    windowMs,
    runtime,
    statusCell,
    status,
    pending,
    ...(abortSignal === undefined ? {} : { abortSignal }),
  };
  const report = (fault: Fault): void => graphFault(runtime.graph.__runtime, fault);
  reactiveInternal.bindReadable(statusCell, { graphId: runtime.graph.id, reportFault: report });
  reactiveInternal.bindReadable(status, { graphId: runtime.graph.id, reportFault: report });
  writeCallerStatus(caller);
  return caller;
}

export function queryIsStale<T>(caller: QueryCaller<T>): boolean {
  const runtime = caller.runtime;
  if (!runtime.hasCommitted || runtime.failing || runtime.invalidated) return true;
  return runtime.lastLandingAt != null && Date.now() - runtime.lastLandingAt >= caller.windowMs;
}
