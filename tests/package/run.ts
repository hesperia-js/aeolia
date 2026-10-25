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
  const manifest = await Bun.file(join(installed, "package.json")).json();
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

  const peerDependencies = manifest.peerDependencies ?? {};
  const peerMetadata = manifest.peerDependenciesMeta ?? {};
  const peerResolutions: InstalledPeerFact[] = [];
  for (const [name, range] of Object.entries(peerDependencies).sort(([left], [right]) =>
    left < right ? -1 : left > right ? 1 : 0,
  )) {
    peerResolutions.push(
      await installedPeerFact(
        installed,
        name,
        String(range),
        peerMetadata[name]?.optional === true,
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
  console.log("PASS packed consumer declarations (including expected type errors)");

  await writeFile(
    join(consumer, "run.mjs"),
    'import { verifyConsumer } from "./consumer.mjs";\nawait verifyConsumer();\nconsole.log("PASS consumer workflow");\n',
  );
  for (const runtime of [process.execPath, "node"]) {
    const output = await run([runtime, "run.mjs"], consumer);
    assert.match(output, /PASS consumer workflow/);
    console.log(`PASS packed consumer workflow: ${runtime === process.execPath ? "Bun" : "Node"}`);
  }

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
  console.log("PASS packed source map: real exception maps to its original TypeScript line");

  const imports = Object.fromEntries(
    Object.entries(manifest.exports as Record<string, { import: string }>).map(
      ([subpath, entry]) => [
        "aeolia" + subpath.slice(1),
        "/package/" + entry.import.replace(/^\.\//, ""),
      ],
    ),
  );
  const files = new Map<string, string>([["/consumer.mjs", join(consumer, "consumer.mjs")]]);
  for await (const path of new Bun.Glob("dist/**/*").scan({ cwd: installed, onlyFiles: true }))
    files.set("/package/" + path.replaceAll("\\", "/"), join(installed, path));
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch(request) {
      const path = new URL(request.url).pathname;
      if (path === "/")
        return new Response(
          `<!doctype html><script type="importmap">${JSON.stringify({ imports })}</script>`,
          { headers: { "content-type": "text/html" } },
        );
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
      const page = await browser.newPage();
      const errors: string[] = [];
      page.on("pageerror", (error) => errors.push(error.message));
      await page.goto(server.url.href);
      await page.evaluate(async () => {
        const url = "/consumer.mjs";
        let timer: ReturnType<typeof setTimeout>;
        try {
          await Promise.race([
            import(url).then(({ verifyConsumer }) => verifyConsumer()),
            new Promise((_, reject) => {
              timer = setTimeout(() => reject(new Error("Consumer workflow timed out")), 30_000);
            }),
          ]);
        } finally {
          clearTimeout(timer!);
        }
      });
      assert.deepEqual(errors, [], "Browser errors during the packed consumer workflow");
      console.log(`PASS packed consumer workflow: Chromium ${browser.version()}`);
    } finally {
      await browser.close();
    }
  } finally {
    await server.stop(true);
  }
} finally {
  await rm(temporary, { recursive: true, force: true });
}
