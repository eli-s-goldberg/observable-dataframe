/**
 * did.test.js — the estimators must recover effects we planted ourselves.
 * If you can't find the treasure you buried, stop drawing maps.
 */

import { describe, it, expect } from "vitest";
import { DataFrame, random } from "../src/index.js";
import {
  fitOLS,
  withinTransform,
  did,
  twfe,
  eventStudy,
  callawaySantAnna,
  checkParallelTrends,
  placeboTest,
} from "../src/stats/index.js";

// Deterministic LCG so test failures are reproducible, not astrological.
function rng(seed = 42) {
  let s = seed;
  return () => (s = (s * 1664525 + 1013904223) % 4294967296) / 4294967296;
}
function gauss(rand) {
  return Math.sqrt(-2 * Math.log(1 - rand())) * Math.cos(2 * Math.PI * rand());
}

/** Panel with unit + time effects and a constant treatment effect. */
function makePanel({ nUnits = 200, nPeriods = 8, treatStart = 4, effect = 5, seed = 7, staggered = false }) {
  const rand = rng(seed);
  const rows = [];
  for (let u = 0; u < nUnits; u++) {
    const treatedUnit = u < nUnits / 2;
    const g = staggered
      ? treatedUnit
        ? treatStart + (u % 3) // cohorts adopt at treatStart, +1, +2
        : null
      : treatedUnit
        ? treatStart
        : null;
    const unitEffect = gauss(rand) * 3;
    for (let t = 0; t < nPeriods; t++) {
      const timeEffect = t * 0.8 + Math.sin(t) * 0.5;
      const treatedNow = g != null && t >= g;
      rows.push({
        unit: `u${u}`,
        period: t,
        group: g,
        treated: treatedUnit ? 1 : 0,
        post: t >= treatStart ? 1 : 0,
        d: treatedNow ? 1 : 0,
        y: 10 + unitEffect + timeEffect + (treatedNow ? effect : 0) + gauss(rand) * 1.5,
      });
    }
  }
  return rows;
}

describe("fitOLS", () => {
  it("recovers coefficients with sensible HC1 SEs", () => {
    const rand = rng(1);
    const X = [];
    const y = [];
    for (let i = 0; i < 500; i++) {
      const x1 = rand() * 10;
      X.push([1, x1]);
      y.push(2 + 3 * x1 + gauss(rand));
    }
    const m = fitOLS(X, y, { terms: ["const", "x1"] });
    expect(m.beta[0]).toBeCloseTo(2, 0);
    expect(m.beta[1]).toBeCloseTo(3, 1);
    expect(m.pValues[1]).toBeLessThan(0.001);
    expect(m.vcovType).toBe("hc1");
  });

  it("cluster-robust SEs widen when errors are cluster-correlated", () => {
    const rand = rng(2);
    const X = [];
    const y = [];
    const clusters = [];
    for (let c = 0; c < 40; c++) {
      const shock = gauss(rand) * 3; // shared within cluster
      for (let i = 0; i < 25; i++) {
        const x = rand();
        X.push([1, x]);
        y.push(1 + 0.5 * x + shock + gauss(rand) * 0.3);
        clusters.push(c);
      }
    }
    const hc1 = fitOLS(X, y);
    const cr1 = fitOLS(X, y, { vcov: "cluster", clusters });
    expect(cr1.nClusters).toBe(40);
    // ignoring the cluster shock understates uncertainty; CR1 should not
    expect(cr1.se[0]).toBeGreaterThan(hc1.se[0]);
  });

  it("throws on singular designs with a diagnosis", () => {
    const X = [[1, 2, 4], [1, 3, 6], [1, 4, 8], [1, 5, 10]];
    expect(() => fitOLS(X, [1, 2, 3, 4])).toThrow(/Collinearity/);
  });

  it("withinTransform demeans by both dimensions", () => {
    const values = [1, 2, 3, 4, 5, 6];
    const units = ["a", "a", "a", "b", "b", "b"];
    const times = [0, 1, 2, 0, 1, 2];
    const out = withinTransform(values, units, times);
    // fully explained by unit + time means: residuals collapse to zero
    for (const v of out) expect(Math.abs(v)).toBeLessThan(1e-9);
  });
});

describe("did (2×2)", () => {
  it("recovers the planted ATT on the textbook example", () => {
    // diff-diff's own quick-start numbers: ATT = 3
    const rows = [
      { outcome: 10, treated: 1, post: 0 }, { outcome: 11, treated: 1, post: 0 },
      { outcome: 15, treated: 1, post: 1 }, { outcome: 18, treated: 1, post: 1 },
      { outcome: 9, treated: 0, post: 0 }, { outcome: 10, treated: 0, post: 0 },
      { outcome: 12, treated: 0, post: 1 }, { outcome: 13, treated: 0, post: 1 },
    ];
    const r = did(rows, { outcome: "outcome", treatment: "treated", time: "post" });
    expect(r.att).toBeCloseTo(3.0, 6);
    expect(r.n).toBe(8);
    expect(r.summary()).toContain("ATT: 3.0000");
  });

  it("matches diff-diff's published quick-start output digit for digit", () => {
    // The reference implementation's README prints:
    //   DiDResults(ATT=3.0000, SE=1.7321, p=0.1583)
    // (classical OLS SEs, t with n-k df). Same numbers or it didn't happen.
    const rows = [
      { outcome: 10, treated: 1, post: 0 }, { outcome: 11, treated: 1, post: 0 },
      { outcome: 15, treated: 1, post: 1 }, { outcome: 18, treated: 1, post: 1 },
      { outcome: 9, treated: 0, post: 0 }, { outcome: 10, treated: 0, post: 0 },
      { outcome: 12, treated: 0, post: 1 }, { outcome: 13, treated: 0, post: 1 },
    ];
    const r = did(rows, { outcome: "outcome", treatment: "treated", time: "post", vcov: "classical" });
    expect(r.att).toBeCloseTo(3.0, 4);
    expect(r.se).toBeCloseTo(1.7321, 4);
    expect(r.pValue).toBeCloseTo(0.1583, 4);
  });

  it("the 2x2 ATT equals the four-means arithmetic exactly", () => {
    // DiD's whole identity: (ȳ_T,post − ȳ_T,pre) − (ȳ_C,post − ȳ_C,pre).
    // The regression must agree with the arithmetic to machine precision.
    const rows = makePanel({ effect: 5 });
    const m = (t, p) =>
      rows.filter((r) => r.treated === t && r.post === p).reduce((a, r) => a + r.y, 0) /
      rows.filter((r) => r.treated === t && r.post === p).length;
    const arithmetic = m(1, 1) - m(1, 0) - (m(0, 1) - m(0, 0));
    const r = did(rows, { outcome: "y", treatment: "treated", time: "post" });
    expect(r.att).toBeCloseTo(arithmetic, 10);
  });

  it("recovers a planted effect at scale, with covariates and clusters", () => {
    const rows = makePanel({ effect: 5 });
    const df = DataFrame.fromRows(rows);
    const r = did(df, { outcome: "y", treatment: "treated", time: "post", cluster: "unit" });
    expect(r.att).toBeGreaterThan(4.3);
    expect(r.att).toBeLessThan(5.7);
    expect(r.pValue).toBeLessThan(0.001);
    expect(r.nClusters).toBe(200);
  });
});

describe("twfe", () => {
  it("matches the planted effect with unit and time FE absorbed", () => {
    const rows = makePanel({ effect: 5 });
    const r = twfe(rows, { outcome: "y", treatment: "d", unit: "unit", time: "period" });
    expect(r.att).toBeGreaterThan(4.5);
    expect(r.att).toBeLessThan(5.5);
    expect(r.pValue).toBeLessThan(0.001);
    expect(r.estimator).toBe("TwoWayFixedEffects");
  });
});

describe("eventStudy", () => {
  it("finds flat pre-trends and the post effect, reference pinned at zero", () => {
    const rows = makePanel({ effect: 5 });
    const r = eventStudy(rows, { outcome: "y", unit: "unit", time: "period", group: "group" });
    const ref = r.effects.find((e) => e.reference);
    expect(ref.eventTime).toBe(-1);
    expect(ref.estimate).toBe(0);

    for (const e of r.effects.filter((e) => e.eventTime < -1)) {
      expect(Math.abs(e.estimate)).toBeLessThan(1); // pre-trends: quiet
    }
    for (const e of r.effects.filter((e) => e.eventTime >= 0)) {
      expect(e.estimate).toBeGreaterThan(3.8); // post: the planted 5, roughly
    }
    expect(r.att).toBeGreaterThan(4.3);
    expect(r.att).toBeLessThan(5.7);
  });

  it("clamps event times into the window", () => {
    const rows = makePanel({ effect: 5, nPeriods: 10 });
    const r = eventStudy(rows, { outcome: "y", unit: "unit", time: "period", group: "group", window: [-2, 2] });
    const eventTimes = r.effects.map((e) => e.eventTime);
    expect(Math.min(...eventTimes)).toBeGreaterThanOrEqual(-2);
    expect(Math.max(...eventTimes)).toBeLessThanOrEqual(2);
  });
});

describe("callawaySantAnna", () => {
  it("recovers the planted effect per cohort under staggered adoption", () => {
    const rows = makePanel({ effect: 4, staggered: true, nUnits: 300, nPeriods: 9 });
    const r = callawaySantAnna(rows, { outcome: "y", unit: "unit", time: "period", group: "group" });
    expect(r.overall.att).toBeGreaterThan(3.4);
    expect(r.overall.att).toBeLessThan(4.6);
    expect(r.overall.pValue).toBeLessThan(0.01);
    // every adopting cohort found, each near the truth
    expect(r.byGroup.length).toBe(3);
    for (const g of r.byGroup) {
      expect(g.att).toBeGreaterThan(3);
      expect(g.att).toBeLessThan(5);
    }
    // pre-period event-time estimates hover near zero
    for (const e of r.byEventTime.filter((e) => e.eventTime < 0)) {
      expect(Math.abs(e.att)).toBeLessThan(1);
    }
  });

  it("not-yet-treated controls work when never-treated are scarce", () => {
    const thin = random(17);
    const rows = makePanel({ effect: 4, staggered: true, nUnits: 300, nPeriods: 9 }).filter(
      (r) => r.group != null || thin() < 0.2 // thin the never-treated herd, actually deterministically
    );
    const r = callawaySantAnna(rows, { outcome: "y", unit: "unit", time: "period", group: "group", control: "notyet" });
    expect(r.overall.att).toBeGreaterThan(3);
    expect(r.overall.att).toBeLessThan(5);
  });

  it("bootstrap SEs run and stay in the same universe as analytic ones", () => {
    const rows = makePanel({ effect: 4, staggered: true, nUnits: 150, nPeriods: 8 });
    const analytic = callawaySantAnna(rows, { outcome: "y", unit: "unit", time: "period", group: "group" });
    const rand = rng(11);
    const boot = callawaySantAnna(rows, {
      outcome: "y", unit: "unit", time: "period", group: "group",
      bootstrap: 99, random: rand,
    });
    expect(boot.overall.bootstrapped).toBe(true);
    expect(boot.overall.se).toBeGreaterThan(analytic.overall.se * 0.3);
    expect(boot.overall.se).toBeLessThan(analytic.overall.se * 3);
  });
});

describe("diagnostics", () => {
  it("parallel trends passes on parallel data, fails on diverging data", () => {
    const clean = makePanel({ effect: 5 });
    const ok = checkParallelTrends(clean, { outcome: "y", treatment: "treated", time: "period", treatmentStart: 4 });
    expect(ok.passed).toBe(true);

    // sabotage: give treated units their own pre-trend
    const rigged = clean.map((r) => ({ ...r, y: r.y + (r.treated ? r.period * 2 : 0) }));
    const bad = checkParallelTrends(rigged, { outcome: "y", treatment: "treated", time: "period", treatmentStart: 4 });
    expect(bad.passed).toBe(false);
    expect(bad.slope).toBeGreaterThan(1.5);
  });

  it("placebo timing finds nothing where nothing was planted", () => {
    const rows = makePanel({ effect: 5 });
    const r = placeboTest(rows, { outcome: "y", treatment: "treated", time: "period", treatmentStart: 4, cluster: "unit" });
    expect(r.passed).toBe(true);
    expect(Math.abs(r.att)).toBeLessThan(1);
  });
});
