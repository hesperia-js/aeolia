import { expect, test } from "bun:test";

interface PackageManifest {
  readonly name: string;
  readonly dependencies?: Readonly<Record<string, string>>;
  readonly exports?: Readonly<
    Record<
      string,
      {
        readonly types?: string;
        readonly import?: string;
        readonly default?: string;
      }
    >
  >;
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

  for (const entry of Object.values(manifest.exports ?? {})) {
    for (const path of [entry.import, entry.types]) {
      expect(path).toBeDefined();
      expect(await Bun.file(new URL(`../../${path}`, import.meta.url)).exists()).toBe(true);
    }
  }

  const packageName = manifest.name;
  const publicApi = await import(packageName);
  expect(Object.keys(publicApi).sort()).toEqual([
    "AEOLIA_TAGGED_ENCODING",
    "Fault",
    "Signal",
    "adopt",
    "affects",
    "afterPropagation",
    "computed",
    "createGraph",
    "createMutation",
    "createObserver",
    "createQuery",
    "createStore",
    "createStream",
    "defineContract",
    "onFault",
    "project",
    "queryOptions",
    "readableBrand",
    "signal",
    "snapshot",
    "storeKey",
    "subscribe",
    "watch",
    "withoutWrites",
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
        "queryOptions",
        "storeKey",
      ],
    ],
  ] as const) {
    const subsystem = await import(`${packageName}/${subpath}`);
    expect(Object.keys(subsystem).sort()).toEqual([...names].sort());
    for (const name of names) expect(subsystem[name]).toBe(publicApi[name]);
  }

  const reactivePackageName = `${packageName}/reactive`;
  const reactivePackage = await import(reactivePackageName);
  expect(Object.keys(reactivePackage).sort()).toEqual([
    "Signal",
    "afterPropagation",
    "computed",
    "createObserver",
    "readableBrand",
    "signal",
    "subscribe",
    "watch",
    "withoutWrites",
  ]);
  expect(reactivePackage.Signal).toBe(publicApi.Signal);
  expect(reactivePackage.signal).toBe(publicApi.signal);
  expect(reactivePackage.subscribe).not.toBe(publicApi.subscribe);
  expect(reactivePackage.readableBrand).toBe(publicApi.readableBrand);
  const factoryState = reactivePackage.signal(3);
  expect(publicApi.Signal.isState(factoryState)).toBe(true);
  const mixed = new publicApi.Signal.Computed(() => factoryState.get() + state.get());
  let notifications = 0;
  const stop = reactivePackage.watch(mixed, () => {
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

test("bundled public functions and classes retain their runtime names", async () => {
  const packageName = "aeolia";
  const publicApi = await import(packageName);
  for (const name of ["signal", "computed", "Fault", "createGraph", "createQuery"]) {
    expect(publicApi[name].name).toBe(name);
  }
});
