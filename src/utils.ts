export function isObject(value: unknown): value is object {
  return (typeof value === "object" && value != null) || typeof value === "function";
}

/** Checks the cross-runtime cancellation interface without relying on its constructor. */
export function isAbortSignal(value: unknown, afterRead?: () => void): value is AbortSignal {
  if (!isObject(value)) return false;
  const signal = value as Partial<AbortSignal>;
  const read = (key: keyof AbortSignal) => {
    const result = signal[key];
    afterRead?.();
    return result;
  };
  if (typeof read("aborted") !== "boolean") return false;
  const onabort = read("onabort");
  if (onabort !== null && typeof onabort !== "function") return false;
  return (
    typeof read("addEventListener") === "function" &&
    typeof read("removeEventListener") === "function" &&
    typeof read("dispatchEvent") === "function" &&
    typeof read("throwIfAborted") === "function"
  );
}

export function isPromiseLike(value: unknown): value is PromiseLike<unknown> {
  if (!isObject(value)) return false;
  // Read `then` exactly once and never invoke it.
  return typeof (value as { then?: unknown }).then === "function";
}

/**
 * Forwards caller cancellation to an execution's controller.
 * An already-aborted signal aborts the controller immediately. The returned
 * cleanup removes the listener without aborting the execution.
 */
export function addSignalAbort(
  signalToWatch: AbortSignal | undefined,
  controller: AbortController,
): () => void {
  if (signalToWatch === undefined) return () => undefined;
  const abort = (): void => controller.abort();
  if (signalToWatch.aborted) controller.abort();
  else signalToWatch.addEventListener("abort", abort, { once: true });
  return () => signalToWatch.removeEventListener("abort", abort);
}
