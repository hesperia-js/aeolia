import { readFileSync } from "node:fs";

export function checkCoverage(lcov: string): void {
  const totals = { LF: 0, LH: 0, FNF: 0, FNH: 0 };
  for (const line of lcov.split(/\r?\n/)) {
    const match = /^(LF|LH|FNF|FNH):(\d+)$/.exec(line);
    if (match) totals[match[1] as keyof typeof totals] += Number(match[2]);
  }
  for (const [label, hit, found] of [
    ["lines", totals.LH, totals.LF],
    ["functions", totals.FNH, totals.FNF],
  ] as const) {
    if (found === 0 || hit > found) throw new Error(`Missing or invalid ${label} coverage totals`);
    const fraction = hit / found;
    console.log(`Coverage: ${hit}/${found} ${label} (${(fraction * 100).toFixed(2)}%)`);
    if (fraction < 0.9) throw new Error(`${label} coverage is below the required 90%`);
  }
}

if (import.meta.main) checkCoverage(readFileSync("coverage/lcov.info", "utf8"));
