import { $ } from "bun";
import { rm } from "node:fs/promises";
import { join } from "node:path";
import { build } from "tsdown";

const root = import.meta.dir;
const output = join(root, "dist");

try {
  await build({
    cwd: root,
    config: false,
    entry: {
      index: "src/index.ts",
      "reactive/index": "src/reactive/index.ts",
      "contract/index": "src/contract/index.ts",
      testing: "src/testing.ts",
    },
    outDir: output,
    tsconfig: "tsconfig.build.json",
    format: "esm",
    platform: "neutral",
    target: "esnext",
    fixedExtension: false,
    clean: true,
    dts: false,
    sourcemap: true,
    minify: {
      compress: true,
      mangle: false,
    },
    outputOptions: {
      keepNames: true,
      chunkFileNames: "chunks/[name]-[hash].js",
    },
  });

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
