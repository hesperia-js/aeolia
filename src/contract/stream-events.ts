import { reportContinuationError } from "./faults.ts";
import type { StoreRuntime, StreamEvent, StreamSession } from "./runtime.ts";

export function publishStreamEvent<T>(
  runtime: StoreRuntime<T>,
  session: StreamSession<T>,
  event: StreamEvent<T>,
): void {
  const listeners = Array.from(session.listeners);
  for (const listener of listeners) {
    if (!session.listeners.has(listener)) continue;
    try {
      listener(event);
    } catch (error) {
      reportContinuationError(runtime.graph.__runtime, error);
    }
  }
}
