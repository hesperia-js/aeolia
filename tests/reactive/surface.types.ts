import { Signal } from "../../src/index.ts";

function accepts<T>(_value: T): void {}

function acceptsProposalSurface(
  State: new <T>(value: T, options?: Signal.SignalOptions<T>) => Signal<T>,
  Computed: new <T = unknown>(
    callback: (this: Signal.Computed<T>) => T,
    options?: Signal.SignalOptions<T>,
  ) => Signal<T>,
): void {
  void State;
  void Computed;
}

acceptsProposalSurface(Signal.State, Signal.Computed);

const state = new Signal.State(1);
const computed = new Signal.Computed(function (): number {
  accepts<Signal.Computed<number>>(this);
  return state.get() * 2;
});

const watcher = new Signal.subtle.Watcher(function (): void {
  accepts<Signal.subtle.Watcher>(this);
});

watcher.watch(state, computed);
const pending: Signal<unknown>[] = watcher.getPending();
const sources: Signal<unknown>[] = Signal.subtle.introspectSources(computed);
const sinks: (Signal<unknown> | Signal.subtle.Watcher)[] = Signal.subtle.introspectSinks(state);

void pending;
void sources;
void sinks;
