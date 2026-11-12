import { $ } from "bun";
import { rm } from "node:fs/promises";
import { join, resolve } from "node:path";
import { build } from "tsdown";

const root = resolve(import.meta.dir, "../..");
const output = join(root, "dist");

const entries = {
  index: "src/index.ts",
  "reactive/index": "src/reactive/index.ts",
  "contract/index": "src/contract/index.ts",
  testing: "src/testing.ts",
};

const sharedOptions = {
  cwd: root,
  config: false as const,
  tsconfig: "tsconfig.build.json",
  format: "esm" as const,
  platform: "neutral" as const,
  target: "esnext",
  fixedExtension: false,
  dts: false,
  sourcemap: true,
} as const;

const buildMode = (outDir: string, marker: "true" | "false", clean: boolean) =>
  build({
    ...sharedOptions,
    entry: { ...entries },
    outDir,
    define: { __AEOLIA_DEV__: marker },
    clean,
    minify: {
      compress: true,
      mangle: false,
    },
    outputOptions: {
      keepNames: true,
      chunkFileNames: "chunks/[name]-[hash].js",
    },
  });

try {
  await buildMode(output, "false", true);
  await buildMode(join(output, "development"), "true", false);
  const compilerArgs = [
    "x",
    "tsc",
    "--project",
    join(root, "tsconfig.build.json"),
    "--emitDeclarationOnly",
    "--noEmitOnError",
  ];
  await $`${process.execPath} ${compilerArgs}`.cwd(root);
} catch (error) {
  await rm(output, { recursive: true, force: true });
  throw error;
}
