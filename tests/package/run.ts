import assert from "node:assert/strict";
import {
  copyFile,
  lstat,
  mkdir,
  mkdtemp,
  readdir,
  readFile,
  realpath,
  rm,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve, sep } from "node:path";
import { chromium } from "playwright";

const root = resolve(import.meta.dir, "../..");
const temporary = await mkdtemp(join(tmpdir(), "aeolia-package-"));
const consumer = join(temporary, "consumer");

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

try {
  await mkdir(consumer);
  await run([process.execPath, "pm", "pack", "--filename", join(temporary, "aeolia.tgz")], root);
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
  assert.equal(
    (await lstat(installed)).isSymbolicLink(),
    false,
    "Consumer must install the archive, not a link",
  );
  assert.ok((await realpath(installed)).startsWith((await realpath(consumer)) + sep));

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
  const throwLine =
    sourceLines.findIndex((line) =>
      line.includes('throw new TypeError("Invalid query declaration.")'),
    ) + 1;
  assert.ok(throwLine > 0, "Locate the deliberate declaration error for the source-map check");
  assert.match(error, /TypeError: Invalid query declaration\./);
  assert.ok(
    error.replaceAll("\\", "/").includes(`/src/contract/api.ts:${throwLine}:`),
    `Source map did not locate the original throw:\n${error}`,
  );
  console.log("PASS packed source map: real exception maps to its original TypeScript line");

  const manifest = await Bun.file(join(installed, "package.json")).json();
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
