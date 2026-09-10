import { expect, test } from "bun:test";

interface PackageManifest {
  readonly name: string;
  readonly dependencies?: Readonly<Record<string, string>>;
  readonly exports?: {
    readonly "."?: {
      readonly types?: string;
      readonly import?: string;
      readonly default?: string;
    };
  };
}

test("the published entry point resolves to the built ESM artifact", async () => {
  const manifest = (await Bun.file(
    new URL("../../package.json", import.meta.url),
  ).json()) as PackageManifest;
  const entry = manifest.exports?.["."];

  expect(manifest.name).toBe("aeolia");
  expect(manifest.dependencies ?? {}).toEqual({});
  expect(entry).toEqual({
    types: "./dist/index.d.ts",
    import: "./dist/index.js",
    default: "./dist/index.js",
  });

  for (const moduleName of [
    "index",
    "reactive",
    "fault",
    "contract/index",
    "contract/api",
    "contract/engine",
    "contract/query",
    "contract/mutation",
    "contract/stream",
    "contract/projection",
    "realm",
    "testing",
  ] as const) {
    expect(await Bun.file(new URL(`../../dist/${moduleName}.js`, import.meta.url)).exists()).toBe(
      true,
    );
    expect(await Bun.file(new URL(`../../dist/${moduleName}.d.ts`, import.meta.url)).exists()).toBe(
      true,
    );
  }

  const packageName = manifest.name;
  const publicApi = await import(packageName);
  expect(Object.keys(publicApi).sort()).toEqual([
    "AEOLIA_TAGGED_ENCODING",
    "Fault",
    "Signal",
    "adopt",
    "affects",
    "computed",
    "createGraph",
    "createMutation",
    "createQuery",
    "createStore",
    "createStream",
    "defineContract",
    "onFault",
    "project",
    "readableBrand",
    "signal",
    "snapshot",
    "storeKey",
    "subscribe",
    "watch",
  ]);

  const state = new publicApi.Signal.State(2);
  const doubled = new publicApi.Signal.Computed(() => state.get() * 2);
  expect(doubled.get()).toBe(4);

  for (const [subpath, names] of [
    [
      "contract",
      [
        "affects",
        "createGraph",
        "createMutation",
        "createQuery",
        "createStore",
        "createStream",
        "defineContract",
        "project",
        "storeKey",
      ],
    ],
  ] as const) {
    const subsystem = await import(`${packageName}/${subpath}`);
    expect(Object.keys(subsystem).sort()).toEqual([...names].sort());
    for (const name of names) expect(subsystem[name]).toBe(publicApi[name]);
  }

  const reactiveModule = await import(new URL("../../dist/reactive.js", import.meta.url).href);
  const reactivePackageName = `${packageName}/reactive`;
  const reactivePackage = await import(reactivePackageName);
  expect(Object.keys(reactivePackage).sort()).toEqual([
    "Signal",
    "computed",
    "readableBrand",
    "signal",
    "subscribe",
    "watch",
  ]);
  expect(reactivePackage.Signal).toBe(publicApi.Signal);
  expect(reactivePackage.signal).toBe(publicApi.signal);
  expect(reactivePackage.subscribe).not.toBe(publicApi.subscribe);
  expect(reactivePackage.readableBrand).toBe(publicApi.readableBrand);
  expect(reactiveModule.Signal).toBe(publicApi.Signal);
  expect(reactiveModule.readableBrand).toBe(publicApi.readableBrand);
  const factoryState = reactiveModule.signal(3);
  expect(publicApi.Signal.isState(factoryState)).toBe(true);
  const mixed = new publicApi.Signal.Computed(() => factoryState.get() + state.get());
  let notifications = 0;
  const stop = reactiveModule.watch(mixed, () => {
    notifications += 1;
  });
  factoryState.set(4);
  expect(notifications).toBe(1);
  expect(mixed.get()).toBe(6);
  expect(publicApi.Signal.subtle.introspectSources(mixed)).toEqual([factoryState, state]);
  stop();

  const values: number[] = [];
  const stopValues = reactivePackage.subscribe(mixed, (value: number) => values.push(value));
  state.set(3);
  expect(values).toEqual([6, 7]);
  stopValues();
});
