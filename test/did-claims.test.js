/**
 * did-claims.test.js — the estimators versus real claims data.
 *
 * Optional suite. Runs only when data/samples/did_member_month.csv and its
 * .meta.json sidecar are present, and skips silently otherwise, so the suite is
 * green on a checkout with no local data. Given a panel, it checks the
 * estimators against real utilization noise — zeros, skew, churn — with a
 * planted treatment effect (stated in the sidecar) whose realized post-floor
 * value we compute from the panel exactly, so "close enough" has a number
 * attached.
 *
 * The synthetic coverage in did.test.js is the path that always runs; this one
 * exists to confirm the estimators survive contact with messy data.
 */

import { describe, it, expect } from "vitest";
import { existsSync, readFileSync } from "node:fs";
import { fromCSV } from "../src/index.js";
import { did, twfe, eventStudy, callawaySantAnna, checkParallelTrends } from "../src/stats/index.js";

const SAMPLE = "data/samples/did_member_month.csv";
const META = "data/samples/did_member_month.meta.json";
const available = existsSync(SAMPLE) && existsSync(META);

describe.skipIf(!available)("DiD on realistic claims panel", () => {
  if (!available) return;

  const df = fromCSV(readFileSync(SAMPLE, "utf8"), {
    dtypes: { person_id: "str", period: "i32", month: "str", group: "f64", treated_now: "i32", encounters: "f64", encounters_raw: "f64" },
  });
  const rows = df.toRows().map((r) => ({ ...r, group: r.group ?? null }));
  const meta = JSON.parse(readFileSync(META, "utf8"));

  // The truth, exactly: the mean realized (post-floor) delta on treated cells.
  const treatedCells = rows.filter((r) => r.treated_now === 1);
  const realizedEffect = treatedCells.reduce((acc, r) => acc + (r.encounters - r.encounters_raw), 0) / treatedCells.length;

  it("sample is sane and the planted effect is findable in principle", () => {
    expect(rows.length).toBeGreaterThan(10000);
    expect(realizedEffect).toBeLessThan(0);
    expect(realizedEffect).toBeGreaterThan(meta.plantedEffect - 0.01); // floor only attenuates
  });

  it("TWFE recovers the realized effect on real noise", () => {
    const r = twfe(rows, { outcome: "encounters", treatment: "treated_now", unit: "person_id", time: "period" });
    expect(r.att).toBeGreaterThan(realizedEffect - 0.3);
    expect(r.att).toBeLessThan(realizedEffect + 0.3);
    expect(r.pValue).toBeLessThan(0.01);
  });

  it("Callaway-Sant'Anna recovers it cohort by cohort", () => {
    const r = callawaySantAnna(rows, { outcome: "encounters", unit: "person_id", time: "period", group: "group" });
    expect(r.overall.att).toBeGreaterThan(realizedEffect - 0.3);
    expect(r.overall.att).toBeLessThan(realizedEffect + 0.3);
    expect(r.byGroup.length).toBe(2); // t=6 and t=8 cohorts
    // pre-period event times hover near zero relative to the effect size —
    // these are short-gap placebo comparisons on genuinely noisy claims,
    // so "near" means "well under the planted signal", not "zero"
    for (const e of r.byEventTime.filter((e) => e.eventTime < 0)) {
      expect(Math.abs(e.att)).toBeLessThan(Math.abs(realizedEffect) * 0.55);
    }
  });

  it("event study shows flat pre, treated post", () => {
    const r = eventStudy(rows, {
      outcome: "encounters", unit: "person_id", time: "period", group: "group", window: [-5, 5],
    });
    const pre = r.effects.filter((e) => e.eventTime < -1 && !e.reference);
    const post = r.effects.filter((e) => e.eventTime >= 0);
    for (const e of pre) expect(Math.abs(e.estimate)).toBeLessThan(Math.abs(realizedEffect) * 0.4);
    expect(post.length).toBeGreaterThan(2);
    expect(r.att).toBeLessThan(realizedEffect + 0.3);
  });

  it("2x2 on the t=6 cohort vs never-treated agrees", () => {
    const cohort = rows.filter((r) => r.group === 6 || r.group === null);
    const twoByTwo = cohort.map((r) => ({ ...r, treated: r.group === 6 ? 1 : 0, post: r.period >= 6 ? 1 : 0 }));
    const r = did(twoByTwo, { outcome: "encounters", treatment: "treated", time: "post", cluster: "person_id" });
    expect(r.att).toBeGreaterThan(realizedEffect - 0.35);
    expect(r.att).toBeLessThan(realizedEffect + 0.35);
  });

  it("parallel trends holds in the raw pre-period (we didn't touch it)", () => {
    const cohort = rows
      .filter((r) => r.group === 6 || r.group === null)
      .map((r) => ({ ...r, treated: r.group === 6 ? 1 : 0 }));
    const r = checkParallelTrends(cohort, {
      outcome: "encounters", treatment: "treated", time: "period", treatmentStart: 6, cluster: "person_id",
    });
    expect(Math.abs(r.slope)).toBeLessThan(0.15); // cohorts were assigned at random; trends should agree
  });
});
