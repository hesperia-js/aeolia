/** Public reactive subsystem entry point. */
export {
  Signal,
  afterPropagation,
  computed,
  createObserver,
  readableBrand,
  signal,
  subscribe,
  withoutWrites,
  watch,
} from "./api.ts";
export type {
  Computed,
  ComputedOptions,
  LifecycleCallback,
  Observer,
  ObserverOptions,
  Readable,
  SignalOptions,
  WritableSignal,
} from "./api.ts";
