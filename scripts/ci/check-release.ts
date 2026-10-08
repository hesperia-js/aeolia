import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { appendFileSync, readFileSync } from "node:fs";
import { $ } from "bun";

export function releaseVersion(value: unknown) {
  assert.ok(typeof value === "string", "package.json must contain a version");
  const match =
    /^(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)(?:-(alpha|beta|rc)(?:\.(?:0|[1-9]\d*|[0-9A-Za-z-]*[A-Za-z-][0-9A-Za-z-]*))*)?$/.exec(
      value,
    );
  assert.ok(
    match,
    `Expected a stable, alpha, beta, or rc version without build metadata: ${value}`,
  );
  return { version: value, channel: match[1] ?? "latest" };
}

export function verifyPublishedPackage(metadata: unknown, version: string, integrity: string) {
  const value = metadata as {
    name?: string;
    version?: string;
    dist?: { integrity?: string };
  } | null;
  assert.ok(
    value?.name === "aeolia" && value.version === version,
    "Published package identity differs",
  );
  assert.equal(
    value.dist?.integrity,
    integrity,
    "Published archive differs from the tested archive",
  );
}

async function checkRelease() {
  const { AEOLIA_RELEASE_BASE: base, AEOLIA_ARCHIVE: archive } = process.env;
  assert.ok(Boolean(base) !== Boolean(archive), "Supply a PR base or a tested archive");
  const manifest = await Bun.file("package.json").json();
  assert.ok(
    manifest.name === "aeolia" && manifest.private !== true,
    "Expected the public aeolia package",
  );
  const release = releaseVersion(manifest.version);

  if (base) {
    assert.match(base, /^[a-f0-9]{40}$/, "Expected the release PR base commit SHA");
    const previous = await $`git show ${`${base}:package.json`}`.quiet().json();
    assert.ok(
      Bun.semver.order(release.version, previous.version) > 0,
      "Release version must advance beyond the PR base",
    );
  }

  const registry = "https://registry.npmjs.org";
  const response = await fetch(`${registry}/aeolia/${release.version}`, {
    signal: AbortSignal.timeout(30_000),
  });
  assert.ok(
    response.ok || response.status === 404,
    `npm registry returned HTTP ${response.status}`,
  );
  const published = response.ok;
  if (base) {
    assert.ok(!published, `Version ${release.version} is already published`);
  } else if (published) {
    const integrity = `sha512-${createHash("sha512").update(readFileSync(archive!)).digest("base64")}`;
    verifyPublishedPackage(await response.json(), release.version, integrity);
  } else {
    const tagsResponse = await fetch(`${registry}/-/package/aeolia/dist-tags`, {
      signal: AbortSignal.timeout(30_000),
    });
    assert.ok(tagsResponse.ok, `npm dist-tags returned HTTP ${tagsResponse.status}`);
    const current = (await tagsResponse.json())[release.channel];
    assert.ok(
      !current || Bun.semver.order(release.version, current) > 0,
      `Refusing to move ${release.channel} backward from ${current}`,
    );
  }

  const outputs = { ...release, published: String(published) };
  if (process.env.GITHUB_OUTPUT)
    appendFileSync(
      process.env.GITHUB_OUTPUT,
      Object.entries(outputs)
        .map(([key, value]) => `${key}=${value}\n`)
        .join(""),
    );
  console.log(outputs);
}

if (import.meta.main) await checkRelease();
