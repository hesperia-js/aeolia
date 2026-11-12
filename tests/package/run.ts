import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import {
  copyFile,
  lstat,
  mkdir,
  mkdtemp,
  readdir,
  readFile,
  realpath,
  rm,
  stat,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve, sep } from "node:path";
import { chromium } from "playwright";

const root = resolve(import.meta.dir, "../..");
const archiveArguments = Bun.argv.slice(2);

if (archiveArguments.length > 1 || archiveArguments[0]?.startsWith("-"))
  throw new Error("Usage: bun run tests/package/run.ts [archive.tgz]");

const archiveArgument = archiveArguments[0];
let suppliedArchive: string | undefined;
if (archiveArgument !== undefined) {
  try {
    suppliedArchive = await realpath(resolve(process.cwd(), archiveArgument));
    if (!(await stat(suppliedArchive)).isFile()) throw new Error("Path is not a regular file");
  } catch (error) {
    const reason = error instanceof Error ? error.message : String(error);
    throw new Error(`Cannot read package archive "${archiveArgument}": ${reason}`);
  }
}
const temporary = await mkdtemp(join(tmpdir(), "aeolia-package-"));
const consumer = join(temporary, "consumer");
const archive = join(temporary, "aeolia.tgz");
const sourceManifest = await Bun.file(join(root, "package.json")).json();

async function run(args: string[], cwd: string, expectedExit = 0): Promise<string> {
  const child = Bun.spawn(args, { cwd, stdout: "pipe", stderr: "pipe", timeout: 120_000 });
  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
    child.exited,
  ]);
  assert.equal(exitCode, expectedExit, `${args.join(" ")}\n${stdout}\n${stderr}`);
  return stdout + stderr;
}

function sha256(bytes: Uint8Array): string {
  return createHash("sha256").update(bytes).digest("hex");
}

function isMissingFile(error: unknown): boolean {
  return typeof error === "object" && error !== null && "code" in error && error.code === "ENOENT";
}

type PackageExportEntry = {
  readonly types: string;
  readonly import: string;
  readonly default: string;
  readonly development: string;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isPackageExportEntry(value: unknown): value is PackageExportEntry {
  return (
    isRecord(value) &&
    typeof value.types === "string" &&
    typeof value.import === "string" &&
    typeof value.default === "string" &&
    typeof value.development === "string"
  );
}

function optionalRecord(value: unknown, field: string): Record<string, unknown> {
  if (value === undefined || value === null) return {};
  if (!isRecord(value)) throw new TypeError("Invalid package manifest field: " + field);
  return value;
}

type InstalledPeerFact = {
  name: string;
  range: string;
  optional: boolean;
  lookup: "node_modules-manifest-traversal";
  status: "resolved" | "unresolved" | "absent";
  installed: {
    name: string | undefined;
    version: string | undefined;
    realpath: string;
    symlink: boolean;
    manifestSha256: string;
    routing: { exports: unknown; main: unknown; module: unknown; types: unknown };
  } | null;
};

async function installedPeerFact(
  installed: string,
  name: string,
  range: string,
  optional: boolean,
): Promise<InstalledPeerFact> {
  const visited = new Set<string>();
  let directory = installed;
  while (true) {
    const modules =
      directory.split(sep).at(-1) === "node_modules" ? directory : join(directory, "node_modules");
    const packageRoot = join(modules, ...name.split("/"));
    if (!visited.has(packageRoot)) {
      visited.add(packageRoot);
      const manifestPath = join(packageRoot, "package.json");
      try {
        const bytes = await readFile(manifestPath);
        const peerManifest = JSON.parse(bytes.toString("utf8"));
        const realPackageRoot = await realpath(packageRoot);
        return {
          name,
          range,
          optional,
          lookup: "node_modules-manifest-traversal",
          status: peerManifest.name === name ? "resolved" : "unresolved",
          installed: {
            name: peerManifest.name,
            version: peerManifest.version,
            realpath: realPackageRoot,
            symlink: (await lstat(packageRoot)).isSymbolicLink(),
            manifestSha256: sha256(bytes),
            routing: {
              exports: peerManifest.exports ?? null,
              main: peerManifest.main ?? null,
              module: peerManifest.module ?? null,
              types: peerManifest.types ?? null,
            },
          },
        };
      } catch (error) {
        if (!isMissingFile(error)) throw error;
      }
    }
    const parent = dirname(directory);
    if (parent === directory) break;
    directory = parent;
  }

  return {
    name,
    range,
    optional,
    lookup: "node_modules-manifest-traversal",
    status: "absent",
    installed: null,
  };
}

try {
  await mkdir(consumer);
  if (suppliedArchive === undefined)
    await run([process.execPath, "pm", "pack", "--filename", archive], root);
  else await copyFile(suppliedArchive, archive);
  await writeFile(
    join(consumer, "package.json"),
    JSON.stringify({
      private: true,
      type: "module",
      dependencies: { aeolia: "file:../aeolia.tgz" },
    }),
  );
  await run([process.execPath, "install", "--ignore-scripts", "--offline"], consumer);
  const installed = join(consumer, "node_modules", "aeolia");
  const manifestValue: unknown = await Bun.file(join(installed, "package.json")).json();
  if (!isRecord(manifestValue)) throw new TypeError("Installed package manifest is not an object");
  const manifest = manifestValue;
  const installedManifestBytes = await readFile(join(installed, "package.json"));
  assert.equal(
    manifest.name,
    sourceManifest.name,
    "Archive package name does not match this checkout",
  );
  assert.equal(
    manifest.version,
    sourceManifest.version,
    "Archive package version does not match this checkout",
  );
  const installedIsSymbolicLink = (await lstat(installed)).isSymbolicLink();
  assert.equal(installedIsSymbolicLink, false, "Consumer must install the archive, not a link");
  const consumerRealpath = await realpath(consumer);
  const installedRealpath = await realpath(installed);
  assert.ok(installedRealpath.startsWith(consumerRealpath + sep));

  const installedDist: { path: string; size: number; sha256: string }[] = [];
  for await (const path of new Bun.Glob("dist/**/*").scan({ cwd: installed, onlyFiles: true })) {
    const normalizedPath = path.replaceAll("\\", "/");
    const bytes = await readFile(join(installed, path));
    installedDist.push({ path: normalizedPath, size: bytes.byteLength, sha256: sha256(bytes) });
  }
  installedDist.sort((left, right) =>
    left.path < right.path ? -1 : left.path > right.path ? 1 : 0,
  );

  const installedExports = manifest.exports;
  if (!isRecord(installedExports))
    throw new TypeError("Installed package exports are not an object");
  const exportEntries = Object.entries(installedExports);
  assert.ok(exportEntries.length > 0, "The installed archive has no public package exports");
  assert.deepEqual(
    exportEntries.map(([specifier]) => specifier).sort(),
    Object.keys(sourceManifest.exports ?? {}).sort(),
    "The installed archive changed the public export specifiers",
  );
  const profileRoutes = exportEntries.map(([specifier, rawEntry]) => {
    if (!isPackageExportEntry(rawEntry))
      throw new TypeError("Installed export is missing a required route: " + specifier);
    const entry = rawEntry;
    const sourceEntryValue: unknown = sourceManifest.exports?.[specifier];
    if (!isPackageExportEntry(sourceEntryValue))
      throw new TypeError("Source package export is missing a required route: " + specifier);
    const sourceEntry = sourceEntryValue;
    assert.equal(
      entry.types,
      sourceEntry.types,
      "The development condition changed declarations: " + specifier,
    );
    assert.equal(
      entry.import,
      sourceEntry.import,
      "The default import route changed: " + specifier,
    );
    assert.equal(entry.default, entry.import, "The default condition changed: " + specifier);
    assert.equal(
      entry.development,
      entry.import.replace(/^\.\/dist\//, "./dist/development/"),
      "The development condition did not select its configured profile directory: " + specifier,
    );
    for (const path of [entry.types, entry.import, entry.development])
      assert.ok(
        installedDist.some((file) => file.path === path.replace(/^\.\//, "")),
        "The installed export target is missing from the archive: " + path,
      );
    return { specifier, types: entry.types, default: entry.import, development: entry.development };
  });
  const stateUpdateDiagnostic = "AEOLIA-DEV-STATE-UPDATE: update() expects next to be a function";
  const offJavaScript = installedDist.filter(
    (file) =>
      file.path.startsWith("dist/") &&
      !file.path.startsWith("dist/development/") &&
      file.path.endsWith(".js"),
  );
  const developmentJavaScript = installedDist.filter(
    (file) => file.path.startsWith("dist/development/") && file.path.endsWith(".js"),
  );
  assert.ok(offJavaScript.length > 0, "The default profile has no executable JavaScript");
  assert.ok(
    developmentJavaScript.length > 0,
    "The development profile is missing from the installed archive",
  );
  // Source maps embed sourcesContent; inspect executable JavaScript bytes only.
  const offJavaScriptText = await Promise.all(
    offJavaScript.map(async (file) =>
      (await readFile(join(installed, file.path))).toString("utf8"),
    ),
  );
  const developmentJavaScriptText = await Promise.all(
    developmentJavaScript.map(async (file) =>
      (await readFile(join(installed, file.path))).toString("utf8"),
    ),
  );
  assert.ok(
    offJavaScriptText.every(
      (text) => !text.includes("__AEOLIA_DEV__") && !text.includes(stateUpdateDiagnostic),
    ),
    "The default profile retained the private development marker or State.update diagnostic",
  );
  assert.ok(
    developmentJavaScriptText.every((text) => !text.includes("__AEOLIA_DEV__")),
    "The development profile retained the private build marker",
  );
  assert.ok(
    developmentJavaScriptText.some((text) => text.includes(stateUpdateDiagnostic)),
    "The development profile omitted the State.update diagnostic",
  );

  const peerDependencies = optionalRecord(manifest.peerDependencies, "peerDependencies");
  const peerMetadata = optionalRecord(manifest.peerDependenciesMeta, "peerDependenciesMeta");
  const peerResolutions: InstalledPeerFact[] = [];
  for (const [name, range] of Object.entries(peerDependencies).sort(([left], [right]) =>
    left < right ? -1 : left > right ? 1 : 0,
  )) {
    const metadata = peerMetadata[name];
    peerResolutions.push(
      await installedPeerFact(
        installed,
        name,
        String(range),
        isRecord(metadata) && metadata.optional === true,
      ),
    );
  }

  const consumerManifestBytes = await readFile(join(consumer, "package.json"));
  const consumerManifest = JSON.parse(consumerManifestBytes.toString("utf8"));
  const lockNames = new Set([
    "bun.lock",
    "bun.lockb",
    "npm-shrinkwrap.json",
    "package-lock.json",
    "pnpm-lock.yaml",
    "yarn.lock",
  ]);
  const consumerLockFiles: { path: string; size: number; sha256: string }[] = [];
  for (const name of await readdir(consumer)) {
    if (!lockNames.has(name)) continue;
    const bytes = await readFile(join(consumer, name));
    consumerLockFiles.push({ path: name, size: bytes.byteLength, sha256: sha256(bytes) });
  }
  assert.ok(consumerLockFiles.length > 0, "Consumer installation did not retain a lockfile");
  consumerLockFiles.sort((left, right) =>
    left.path < right.path ? -1 : left.path > right.path ? 1 : 0,
  );

  const archiveBytes = await readFile(archive);
  console.log(
    JSON.stringify({
      kind: "aeolia.installed-consumer.receipt",
      archive: {
        suppliedPath: suppliedArchive,
        temporarySize: archiveBytes.byteLength,
        temporarySha256: sha256(archiveBytes),
      },
      consumer: {
        realpath: consumerRealpath,
        manifestSha256: sha256(consumerManifestBytes),
        dependencies: consumerManifest.dependencies ?? {},
        lockFiles: consumerLockFiles,
      },
      installed: {
        realpath: installedRealpath,
        symlink: installedIsSymbolicLink,
        manifest: {
          sha256: sha256(installedManifestBytes),
          name: manifest.name,
          version: manifest.version,
          main: manifest.main ?? null,
          types: manifest.types ?? null,
          exports: manifest.exports ?? {},
          profileRoutes,
          peerDependencies,
          peerDependenciesMeta: peerMetadata,
        },
        peerResolutions,
        distFiles: installedDist,
      },
    }),
  );

  const fixtures = await readdir(import.meta.dir);
  const typeFixtures = fixtures.filter((name) => name.endsWith(".types.ts"));
  assert.ok(typeFixtures.length > 0, "No consumer type fixtures found");
  for (const name of [...typeFixtures, "consumer.mjs", "mapped-error.mjs"])
    await copyFile(join(import.meta.dir, name), join(consumer, name));
  await writeFile(
    join(consumer, "tsconfig.json"),
    JSON.stringify({
      compilerOptions: {
        target: "ESNext",
        module: "NodeNext",
        moduleResolution: "NodeNext",
        strict: true,
        noEmit: true,
        skipLibCheck: false,
        types: [],
        lib: ["ESNext", "DOM"],
      },
      include: typeFixtures,
    }),
  );
  await run(
    [process.execPath, "x", "--no-install", "tsc", "--project", join(consumer, "tsconfig.json")],
    root,
  );
  await run(
    [
      process.execPath,
      "x",
      "--no-install",
      "tsc",
      "--project",
      join(consumer, "tsconfig.json"),
      "--customConditions",
      "development",
    ],
    root,
  );
  console.log("PASS packed consumer declarations under default and development conditions");

  await writeFile(
    join(consumer, "run.mjs"),
    'import { verifyConsumer } from "./consumer.mjs";\nawait verifyConsumer();\nconsole.log("PASS consumer workflow");\n',
  );
  await writeFile(
    join(consumer, "run-development.mjs"),
    [
      'import { verifyConsumer } from "./consumer.mjs";',
      "await verifyConsumer(true);",
      'console.log("PASS consumer workflow: development");',
      "",
    ].join("\n"),
  );
  for (const runtime of [process.execPath, "node"]) {
    const output = await run([runtime, "run.mjs"], consumer);
    assert.match(output, /PASS consumer workflow/);
    console.log(`PASS packed consumer workflow: ${runtime === process.execPath ? "Bun" : "Node"}`);
  }
  const developmentOutput = await run(
    ["node", "--conditions=development", "run-development.mjs"],
    consumer,
  );
  assert.match(developmentOutput, /PASS consumer workflow: development/);
  console.log("PASS packed consumer workflow: Node development condition");

  const bunDevelopmentOutput = await run(
    [process.execPath, "--conditions=development", "run-development.mjs"],
    consumer,
  );
  assert.match(bunDevelopmentOutput, /PASS consumer workflow: development/);
  console.log("PASS packed consumer workflow: Bun development condition");

  const error = await run(["node", "--enable-source-maps", "mapped-error.mjs"], consumer, 1);
  const sourceLines = (await readFile(join(root, "src/contract/api.ts"), "utf8")).split(/\r?\n/);
  const invalidInputGuard = "if (!isCreateQueryInputWithAcquisition(input, acquisition))";
  const guardLines = sourceLines.flatMap((line, index) =>
    line.trim() === invalidInputGuard ? [index] : [],
  );
  assert.ok(
    guardLines.length === 1 && guardLines[0] !== undefined,
    "Locate the unique invalid-input guard for the source-map check",
  );
  const guardThrowIndex = guardLines[0] + 1;
  assert.equal(
    sourceLines[guardThrowIndex]?.trim(),
    'throw new TypeError("Invalid query declaration.");',
    "The invalid-input guard must throw on the following source line",
  );
  const throwLine = guardThrowIndex + 1;
  assert.match(error, /TypeError: Invalid query declaration\./);
  assert.ok(
    error.replaceAll("\\", "/").includes(`/src/contract/api.ts:${throwLine}:`),
    `Source map did not locate the original throw:\n${error}`,
  );
  console.log("PASS packed source map: query error maps to its original TypeScript line");

  const stateUpdateError = await run(
    [
      "node",
      "--conditions=development",
      "--enable-source-maps",
      "mapped-error.mjs",
      "state-update",
    ],
    consumer,
    1,
  );
  const stateUpdateSourceLines = (
    await readFile(join(root, "src/reactive/classes.ts"), "utf8")
  ).split(/\r?\n/);
  const diagnosticLines = stateUpdateSourceLines.flatMap((line, index) =>
    line.includes(stateUpdateDiagnostic) ? [index] : [],
  );
  assert.ok(
    diagnosticLines.length === 1 && diagnosticLines[0] !== undefined,
    "Locate the unique State.update diagnostic for the source-map check",
  );
  const stateUpdateThrowLine = diagnosticLines[0] + 1;
  assert.ok(
    stateUpdateError.includes("TypeError: " + stateUpdateDiagnostic),
    "The installed development profile did not report the State.update diagnostic",
  );
  assert.ok(
    stateUpdateError
      .replaceAll("\\", "/")
      .includes("/src/reactive/classes.ts:" + stateUpdateThrowLine + ":"),
    "The State.update diagnostic did not map to its original TypeScript throw line:\n" +
      stateUpdateError,
  );
  console.log("PASS packed development source map: State.update diagnostic maps to TypeScript");

  const imports = {
    default: Object.fromEntries(
      profileRoutes.map((route) => [
        "aeolia" + route.specifier.slice(1),
        "/package/" + route.default.replace(/^\.\//, ""),
      ]),
    ),
    development: Object.fromEntries(
      profileRoutes.map((route) => [
        "aeolia" + route.specifier.slice(1),
        "/package/" + route.development.replace(/^\.\//, ""),
      ]),
    ),
  };
  const files = new Map<string, string>([["/consumer.mjs", join(consumer, "consumer.mjs")]]);
  for await (const path of new Bun.Glob("dist/**/*").scan({ cwd: installed, onlyFiles: true }))
    files.set("/package/" + path.replaceAll("\\", "/"), join(installed, path));
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch(request) {
      const path = new URL(request.url).pathname;
      if (path === "/" || path === "/development") {
        const selectedImports = path === "/development" ? imports.development : imports.default;
        const html =
          '<!doctype html><script type="importmap">' +
          JSON.stringify({ imports: selectedImports }) +
          "</script>";
        return new Response(html, { headers: { "content-type": "text/html" } });
      }
      const file = files.get(path);
      return file === undefined
        ? new Response("Not found", { status: 404 })
        : new Response(Bun.file(file), {
            headers: {
              "content-type":
                file.endsWith(".mjs") || file.endsWith(".js")
                  ? "text/javascript"
                  : "application/json",
            },
          });
    },
  });
  try {
    const browser = await chromium.launch();
    try {
      const runBrowserConsumer = async (path: string, development: boolean): Promise<void> => {
        const page = await browser.newPage();
        const errors: string[] = [];
        page.on("pageerror", (error) => errors.push(error.message));
        try {
          await page.goto(new URL(path, server.url.href).href);
          await page.evaluate(async (selectedDevelopment: boolean) => {
            const url = "/consumer.mjs";
            let timer: ReturnType<typeof setTimeout>;
            try {
              await Promise.race([
                import(url).then(({ verifyConsumer }) => verifyConsumer(selectedDevelopment)),
                new Promise((_, reject) => {
                  timer = setTimeout(
                    () => reject(new Error("Consumer workflow timed out")),
                    30_000,
                  );
                }),
              ]);
            } finally {
              clearTimeout(timer!);
            }
          }, development);
          assert.deepEqual(errors, [], "Browser errors during the packed consumer workflow");
          console.log(
            "PASS packed consumer workflow: Chromium " +
              browser.version() +
              " (" +
              (development ? "development" : "default") +
              ")",
          );
        } finally {
          await page.close();
        }
      };
      await runBrowserConsumer("/", false);
      await runBrowserConsumer("/development", true);
    } finally {
      await browser.close();
    }
  } finally {
    await server.stop(true);
  }
} finally {
  await rm(temporary, { recursive: true, force: true });
}
