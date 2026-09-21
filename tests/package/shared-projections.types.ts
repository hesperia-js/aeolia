import { project, type Projection, type StreamStore, type Store } from "aeolia";

export function sharedProjectionTypes(source: StreamStore<number>, plain: Store<number>): void {
  const history: Projection<readonly number[]> = project(source, { kind: "accumulate", max: 10 });
  const summary: Projection<string> = project(source, {
    kind: "reduce",
    initial: "",
    step: (previous, item) => previous + item.toFixed(0),
  });
  // @ts-expect-error Numeric source items cannot satisfy a string reducer.
  project(source, {
    kind: "reduce",
    initial: "",
    step: (previous: string, item: string) => previous + item,
  });
  // @ts-expect-error A generic store has no stream session to project.
  project(plain, { kind: "accumulate", max: 10 });
  history.close();
  summary.close();
}
