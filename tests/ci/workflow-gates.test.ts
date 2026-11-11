import assert from "node:assert/strict";
import { expect, test } from "bun:test";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../../", import.meta.url));
const workflow = Bun.YAML.parse(
  await Bun.file(new URL("../../.github/workflows/verify.yml", import.meta.url)).text(),
) as { jobs: Record<string, { steps: { name?: string; run?: string }[] }> };
const run = workflow.jobs.checks?.steps.find(
  (step) => step.name === "Check repository branch name",
)?.run;
assert.ok(run);
const match = /^bun -e '\s*([\s\S]*?)\s*'\s*$/.exec(run);
assert.ok(match?.[1]);
const branchScript = match[1];
const { version } = await Bun.file(new URL("../../package.json", import.meta.url)).json();

function accepts(branch: string) {
  return Bun.spawnSync([process.execPath, "-e", branchScript], {
    cwd: root,
    env: { ...process.env, AEOLIA_HEAD_BRANCH: branch },
    stdout: "pipe",
    stderr: "pipe",
  }).exitCode === 0;
}

test("the branch gate accepts supported branches and the authored release", () => {
  for (const branch of [
    "feature/compiler",
    "feature/compiler/scopes",
    "fix/hotfix",
    "work/m3-x",
    "work/m3-aeolia-retirement-r4",
    `release/${version}`,
  ]) {
    expect(accepts(branch)).toBe(true);
  }
});

test("the branch gate rejects other namespaces and invalid releases", () => {
  for (const branch of [
    "main",
    "feature/",
    "fix/",
    "hotfix/parser",
    "work/m3-",
    "work/m2-foo",
    "work/other",
    "release/v1.0.0",
    "release/01.0.0",
    "release/1.0.0+build",
    "release/999.0.0",
  ]) {
    expect(accepts(branch)).toBe(false);
  }
});
