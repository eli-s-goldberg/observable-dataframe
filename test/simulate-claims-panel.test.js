/**
 * simulate-claims-panel.test.js: the generated panel, and what it is for.
 *
 * Two jobs. First, hold the panel to the distributional shape it claims: the
 * counts overdispersed, a large share of months at zero, spend right-skewed,
 * high utilizers persistent, enrollment churning. A generator that quietly
 * degenerates into a smooth ramp would still build the docs site, and the
 * figures would still render, so nothing but a test catches it.
 *
 * Second, check that the panel estimators recover the effect the generator
 * planted. Because the effect is applied by thinning a draw the generator
 * already holds, the untreated counterfactual is known for every treated
 * member-month and `truth` is measured rather than assumed. That is the whole
 * argument for validating on simulated data: the answer is available.
 *
 * The tolerances below are bands, not pins. The seed is fixed, so the numbers
 * are reproducible, but a band survives an honest refactor and still fails on
 * a real regression.
 */

import { describe, it, expect } from "vitest";
import {
  CLAIMS_PANEL_COLUMNS,
  claimsPanelCsv,
  claimsSliceFromCSV,
  enrolledMemberMonths,
  simulateClaimsPanel,
} from "../src/data/index.js";
import {
  callawaySantAnna,
  checkParallelTrends,
  did,
  eventStudy,
  placeboTest,
  twfe,
} from "../src/stats/index.js";

const mean = (xs) => xs.reduce((a, b) => a + b, 0) / xs.length;
const variance = (xs) => {
  const m = mean(xs);
  return xs.reduce((a, b) => a + (b - m) ** 2, 0) / (xs.length - 1);
};
const skewness = (xs) => {
  const m = mean(xs);
  const s = Math.sqrt(variance(xs));
  return xs.reduce((a, b) => a + ((b - m) / s) ** 3, 0) / xs.length;
};

const { rows, truth, options } = simulateClaimsPanel({ seed: 42 });
const enrolled = rows.filter((r) => r.enrolled_flag === 1);
const claims = enrolled.map((r) => r.medical_claims);

describe("simulateClaimsPanel: schema and determinism", () => {
  it("emits one row per member-month with exactly the published columns", () => {
    expect(rows.length).toBe(options.members * options.months);
    expect(Object.keys(rows[0])).toEqual(CLAIMS_PANEL_COLUMNS);
  });

  it("replays digit for digit on the same seed, and differs on another", () => {
    const again = simulateClaimsPanel({ seed: 42 }).rows;
    expect(again).toEqual(rows);
    const other = simulateClaimsPanel({ seed: 43 }).rows;
    expect(other.map((r) => r.medical_claims)).not.toEqual(rows.map((r) => r.medical_claims));
  });

  it("round-trips through the CSV loader with the right dtypes", () => {
    const { csv } = claimsPanelCsv({ seed: 42, members: 20 });
    const df = claimsSliceFromCSV(csv);
    expect(df.columns).toEqual(CLAIMS_PANEL_COLUMNS);
    expect(df.height).toBe(20 * options.months);
    expect(enrolledMemberMonths(df).height).toBeLessThan(df.height);
  });

  it("carries no identifier that could be mistaken for a real one", () => {
    const uuid = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i;
    for (const id of new Set(rows.map((r) => r.person_id))) {
      expect(id).toMatch(/^sim-\d{6}$/);
      expect(id).not.toMatch(uuid);
    }
  });
});

describe("simulateClaimsPanel: the shape it claims to have", () => {
  it("produces counts far more dispersed than Poisson", () => {
    expect(variance(claims) / mean(claims)).toBeGreaterThan(5);
  });

  it("leaves a large share of member-months at zero utilization", () => {
    const zeroShare = claims.filter((c) => c === 0).length / claims.length;
    expect(zeroShare).toBeGreaterThan(0.25);
    expect(zeroShare).toBeLessThan(0.55);
  });

  it("skews the count distribution right, with a tail well past the median", () => {
    expect(skewness(claims)).toBeGreaterThan(2);
    const sorted = [...claims].sort((a, b) => a - b);
    const median = sorted[Math.floor(sorted.length / 2)];
    expect(Math.max(...claims)).toBeGreaterThan(median * 20);
  });

  it("skews spend harder than counts, most months being zero", () => {
    const paid = enrolled.map((r) => r.pharmacy_paid);
    expect(paid.filter((p) => p === 0).length / paid.length).toBeGreaterThan(0.8);
    const spent = paid.filter((p) => p > 0);
    expect(skewness(spent)).toBeGreaterThan(2);
    expect(Math.max(...spent)).toBeGreaterThan(mean(spent) * 20);
  });

  it("keeps high utilizers high: members spread widely and autocorrelate", () => {
    const byMember = new Map();
    for (const r of enrolled) {
      if (!byMember.has(r.person_id)) byMember.set(r.person_id, []);
      byMember.get(r.person_id).push(r);
    }
    const memberMeans = [...byMember.values()].map((a) => mean(a.map((r) => r.medical_claims)));
    // Coefficient of variation near 1 is the gamma mixing distribution showing.
    expect(Math.sqrt(variance(memberMeans)) / mean(memberMeans)).toBeGreaterThan(0.7);

    const pairs = [];
    for (const a of byMember.values()) {
      const s = [...a].sort((x, y) => x.period - y.period);
      for (let i = 1; i < s.length; i++) {
        if (s[i].period === s[i - 1].period + 1) pairs.push([s[i - 1].medical_claims, s[i].medical_claims]);
      }
    }
    const xs = pairs.map((p) => p[0]);
    const ys = pairs.map((p) => p[1]);
    const mx = mean(xs);
    const my = mean(ys);
    const r =
      pairs.reduce((a, p) => a + (p[0] - mx) * (p[1] - my), 0) /
      Math.sqrt(
        pairs.reduce((a, p) => a + (p[0] - mx) ** 2, 0) * pairs.reduce((a, p) => a + (p[1] - my) ** 2, 0)
      );
    expect(r).toBeGreaterThan(0.15);
  });

  it("churns enrollment, and generates nothing in an unenrolled month", () => {
    const byMember = new Map();
    for (const r of rows) {
      if (!byMember.has(r.person_id)) byMember.set(r.person_id, []);
      byMember.get(r.person_id).push(r);
    }
    const churned = [...byMember.values()].filter((a) => a.some((r) => r.enrolled_flag === 0));
    expect(churned.length).toBeGreaterThan(options.members * 0.1);
    expect(churned.length).toBeLessThan(options.members * 0.5);
    for (const r of rows) {
      if (r.enrolled_flag === 0) {
        expect(r.medical_claims).toBe(0);
        expect(r.pharmacy_fills).toBe(0);
        expect(r.pharmacy_paid).toBe(0);
      }
    }
  });

  it("staggers adoption across cohorts and keeps a never-treated group", () => {
    const cohortOf = new Map(rows.map((r) => [r.person_id, r.cohort]));
    const sizes = new Map();
    for (const c of cohortOf.values()) sizes.set(c, (sizes.get(c) ?? 0) + 1);
    for (const period of options.cohortPeriods) {
      expect(sizes.get(period)).toBe(Math.round(options.members * options.cohortShare));
    }
    expect(sizes.get(0)).toBeGreaterThan(options.members * 0.3);
    for (const r of rows) {
      expect(r.treated_now).toBe(r.cohort > 0 && r.period >= r.cohort ? 1 : 0);
    }
  });

  it("phases the effect in rather than switching it on", () => {
    const ramp = truth.attByEventTime.filter((e) => e.eventTime < options.rampMonths - 1);
    const settled = truth.attByEventTime.filter((e) => e.eventTime >= options.rampMonths - 1);
    expect(Math.abs(mean(ramp.map((e) => e.att)))).toBeLessThan(Math.abs(mean(settled.map((e) => e.att))));
    expect(truth.att).toBeLessThan(0);
    expect(truth.attFullyPhasedIn).toBeLessThan(truth.att);
  });
});

describe("panel estimators recover the planted effect", () => {
  const panel = enrolled;

  it("TWFE lands close, attenuated as staggered adoption with dynamics predicts", () => {
    const r = twfe(panel, {
      outcome: "medical_claims", treatment: "treated_now", unit: "person_id", time: "period",
    });
    expect(r.pValue).toBeLessThan(0.001);
    expect(r.att).toBeGreaterThan(truth.att - 0.3);
    expect(r.att).toBeLessThan(truth.att + 0.3);
    expect(truth.att).toBeGreaterThan(r.ci[0]);
    expect(truth.att).toBeLessThan(r.ci[1]);
  });

  it("Callaway-Sant'Anna covers the truth cohort by cohort with bootstrap SEs", () => {
    const r = callawaySantAnna(panel, {
      outcome: "medical_claims", unit: "person_id", time: "period", group: "cohort",
    });
    expect(r.byGroup.length).toBe(options.cohortPeriods.length);
    expect(r.overall.att).toBeGreaterThan(truth.att - 0.6);
    expect(r.overall.att).toBeLessThan(truth.att + 0.6);
    // Pre-period cells are short-gap placebo comparisons: individually noisy,
    // centered on nothing, which is the property that matters.
    const pre = r.byEventTime.filter((e) => e.eventTime < 0);
    expect(Math.abs(mean(pre.map((e) => e.att)))).toBeLessThan(Math.abs(truth.att) * 0.25);
  });

  it("the event study shows a flat pre-period and a ramp after adoption", () => {
    const r = eventStudy(panel, {
      outcome: "medical_claims", unit: "person_id", time: "period", group: "cohort", window: [-6, 7],
    });
    const pre = r.effects.filter((e) => e.eventTime < -1 && !e.reference);
    for (const e of pre) {
      expect(Math.abs(e.estimate)).toBeLessThan(Math.abs(truth.att) * 0.5);
      expect(e.pValue).toBeGreaterThan(0.05);
    }
    const settled = r.effects.filter((e) => e.eventTime >= options.rampMonths - 1);
    expect(mean(settled.map((e) => e.estimate))).toBeLessThan(truth.attFullyPhasedIn + 0.4);
    expect(mean(settled.map((e) => e.estimate))).toBeGreaterThan(truth.attFullyPhasedIn - 0.4);
  });

  it("the 2x2 on the first cohort recovers that cohort's own effect", () => {
    const cohort = panel
      .filter((r) => r.cohort === options.cohortPeriods[0] || r.cohort === 0)
      .map((r) => ({
        ...r,
        treated: r.cohort === options.cohortPeriods[0] ? 1 : 0,
        post: r.period >= options.cohortPeriods[0] ? 1 : 0,
      }));
    const r = did(cohort, {
      outcome: "medical_claims", treatment: "treated", time: "post", cluster: "person_id",
    });
    const cohortTruth = truth.attByCohort.find((c) => c.cohort === options.cohortPeriods[0]).att;
    expect(cohortTruth).toBeGreaterThan(r.ci[0]);
    expect(cohortTruth).toBeLessThan(r.ci[1]);
  });

  it("finds nothing where nothing was planted", () => {
    const cohort = panel
      .filter((r) => r.cohort === options.cohortPeriods[0] || r.cohort === 0)
      .map((r) => ({ ...r, treated: r.cohort === options.cohortPeriods[0] ? 1 : 0 }));
    const start = options.cohortPeriods[0];
    const trends = checkParallelTrends(cohort, {
      outcome: "medical_claims", treatment: "treated", time: "period",
      treatmentStart: start, cluster: "person_id",
    });
    expect(trends.passed).toBe(true);
    expect(Math.abs(trends.slope)).toBeLessThan(0.1);

    const placebo = placeboTest(cohort, {
      outcome: "medical_claims", treatment: "treated", time: "period",
      treatmentStart: start, cluster: "person_id",
    });
    expect(placebo.passed).toBe(true);
    expect(Math.abs(placebo.att)).toBeLessThan(Math.abs(truth.att) * 0.25);
  });
});
