import { expect, test } from "bun:test";
import { releaseVersion, verifyPublishedPackage } from "../../scripts/ci/check-release.ts";

test.each([
  ["1.0.0", "latest"],
  ["1.0.0-alpha.2", "alpha"],
  ["1.0.0-beta.1", "beta"],
  ["1.0.0-rc.3", "rc"],
])("publishes %s to its intended channel", (version, channel) => {
  expect(releaseVersion(version)).toEqual({ version, channel });
});

test.each([
  "v1.0.0",
  "01.0.0",
  "1.0",
  "1.0.0-alpha.01",
  "1.0.0-next.1",
  "1.0.0-latest.1",
  "1.0.0+local",
  "1.0.0\ntag=evil",
  null,
])("rejects an invalid or unsupported release version (%#)", (version) =>
  expect(() => releaseVersion(version)).toThrow(),
);

test("a publication retry requires the same package, version, and archive bytes", () => {
  const metadata = { name: "aeolia", version: "1.0.0", dist: { integrity: "sha512-same" } };
  expect(() => verifyPublishedPackage(metadata, "1.0.0", "sha512-same")).not.toThrow();
  expect(() => verifyPublishedPackage(metadata, "1.0.0", "sha512-other")).toThrow(
    "archive differs",
  );
  expect(() => verifyPublishedPackage(metadata, "1.0.1", "sha512-same")).toThrow(
    "identity differs",
  );
  expect(() =>
    verifyPublishedPackage({ ...metadata, name: "other" }, "1.0.0", "sha512-same"),
  ).toThrow("identity differs");
});
