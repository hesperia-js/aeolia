import { expect, test } from "bun:test";
import { checkCoverage } from "../../scripts/ci/coverage.ts";

test("coverage gates aggregate totals rather than averaging per-file percentages", () => {
  const largeCoveredFile = "LF:100\nLH:100\nFNF:100\nFNH:100\n";
  const smallUncoveredFile = "LF:1\nLH:0\nFNF:1\nFNH:0\n";
  expect(() => checkCoverage(largeCoveredFile + smallUncoveredFile)).not.toThrow();
  expect(() => checkCoverage("LF:100\nLH:90\nFNF:100\nFNH:90\n")).not.toThrow();
});

test.each([
  ["LF:100\nLH:89\nFNF:100\nFNH:100\n", "lines coverage"],
  ["LF:100\nLH:100\nFNF:100\nFNH:89\n", "functions coverage"],
  ["", "Missing or invalid lines"],
  ["LF:1\nLH:2\nFNF:1\nFNH:1\n", "Missing or invalid lines"],
  ["LF:1\nLH:1\nFNF:0\nFNH:0\n", "Missing or invalid functions"],
])("coverage rejects below-threshold or invalid reports (%#)", (lcov, message) => {
  expect(() => checkCoverage(lcov)).toThrow(message);
});
