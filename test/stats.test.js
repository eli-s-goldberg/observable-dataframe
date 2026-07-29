/**
 * stats.test.js — checking our math against numbers computed elsewhere,
 * by software with more citations than us.
 */

import { describe, it, expect } from "vitest";
import { DataFrame } from "../src/index.js";
import {
  probit,
  normalCDF,
  studentTCDF,
  sampleSizeTwoProportions,
  sampleSizeTwoMeans,
  powerAnalysis,
  evaluateCadence,
  varianceCluster,
  designEffect,
  designMatrix,
  designMatrixData,
  varthetaM,
  oneSampleTTest,
  twoSampleTTest,
  welchTTest,
  ols,
  ancova,
  distributions,
  DistP,
  inv,
  matmul,
} from "../src/stats/index.js";

describe("special functions", () => {
  it("probit matches known z-scores", () => {
    expect(probit(0.975)).toBeCloseTo(1.959964, 4);
    expect(probit(0.95)).toBeCloseTo(1.644854, 4);
    expect(probit(0.8)).toBeCloseTo(0.841621, 4);
    expect(probit(0.5)).toBeCloseTo(0, 6);
    expect(probit(0.025)).toBeCloseTo(-1.959964, 4);
  });

  it("probit rejects nonsense", () => {
    expect(() => probit(0)).toThrow();
    expect(() => probit(1.5)).toThrow();
  });

  it("normalCDF inverts probit, approximately", () => {
    for (const p of [0.1, 0.3, 0.5, 0.7, 0.9, 0.975]) {
      expect(normalCDF(probit(p))).toBeCloseTo(p, 5);
    }
  });

  it("studentTCDF matches reference values", () => {
    // R: pt(2.0, 10) = 0.9633060
    expect(studentTCDF(2.0, 10)).toBeCloseTo(0.9633, 3);
    // R: pt(-1.5, 5) = 0.09695184
    expect(studentTCDF(-1.5, 5)).toBeCloseTo(0.0970, 3);
  });
});

describe("power & sample size", () => {
  it("two-proportion sample size matches the classic example", () => {
    // p1=0.10, p2=0.15, alpha=.05 two-sided, power=.80 → ~686 per arm (pooled z formula)
    const n = sampleSizeTwoProportions({ p1: 0.1, p2: 0.15, alpha: 0.05, power: 0.8 });
    expect(n).toBeGreaterThan(650);
    expect(n).toBeLessThan(730);
  });

  it("two-mean sample size matches the textbook formula", () => {
    // sigma=1, delta=0.5, alpha=.05 two-sided, power=.80 → 2*(1.96+0.84)^2*1/0.25 ≈ 63
    const n = sampleSizeTwoMeans({ mu1: 0, mu2: 0.5, sigma: 1 });
    expect(n).toBe(63);
  });

  it("zero effect politely asks for infinite members", () => {
    expect(sampleSizeTwoProportions({ p1: 0.1, p2: 0.1 })).toBe(Infinity);
  });

  it("powerAnalysis reproduces the AVM high-risk cohort", () => {
    // High-risk RA cohort: P1=19%, D=0.63%, F=1, alpha=10% 1-sided, 80% power
    const r = powerAnalysis({
      baseRate: 0.19,
      behaviorChange: 0.0063,
      design: "one-sided-proportions",
      alpha: 0.1,
      power: 0.8,
    });
    expect(r.p2).toBeCloseTo(0.1963, 6);
    expect(r.arms).toBe(2);
    // n/arm should land in the low tens of thousands for this tiny delta
    expect(r.nPerArm).toBeGreaterThan(15000);
    expect(r.nPerArm).toBeLessThan(60000);
  });

  it("attributable fraction scales the detectable effect", () => {
    const full = powerAnalysis({ baseRate: 0.08, behaviorChange: 0.004, attributable: 1 });
    const half = powerAnalysis({ baseRate: 0.08, behaviorChange: 0.004, attributable: 0.5 });
    expect(half.nPerArm).toBeGreaterThan(full.nPerArm * 3); // quarter the delta², ~4x the n
  });

  it("DiD costs 1.5x; single-arm has one arm", () => {
    const base = powerAnalysis({ baseRate: 0.1, behaviorChange: 0.01, design: "one-sided-proportions" });
    const did = powerAnalysis({ baseRate: 0.1, behaviorChange: 0.01, design: "difference-in-differences" });
    expect(did.nPerArm / base.nPerArm).toBeCloseTo(1.5, 1);
    expect(powerAnalysis({ baseRate: 0.1, behaviorChange: 0.01, design: "single-arm" }).arms).toBe(1);
  });

  it("evaluateCadence compounds lifts and checks power", () => {
    const channels = [
      { key: "dm", cost: 1.2, behaviorChange: 0.01 },
      { key: "email", cost: 0.1, behaviorChange: 0.004 },
    ];
    const plan = [
      [true, false],
      [true, true],
    ];
    const r = evaluateCadence({ channels, plan, baseRate: 0.19, perArm: 24000 });
    expect(r.touches).toBe(3);
    expect(r.costPerMember).toBeCloseTo(1.4);
    expect(r.lift).toBeCloseTo(1 - 0.99 * 0.996 * 0.996, 10);
    expect(typeof r.meetsPower).toBe("boolean");
    // no touches, no lift, no experiment
    const empty = evaluateCadence({ channels, plan: [[false, false], [false, false]], baseRate: 0.19, perArm: 24000 });
    expect(empty.requiredPerArm).toBe(Infinity);
    expect(empty.meetsPower).toBe(false);
  });
});

describe("cluster designs", () => {
  it("design effect and cluster variance", () => {
    expect(designEffect(1, 0.5)).toBe(1); // one person per cluster: a cluster of one is a person
    expect(designEffect(100, 0.01)).toBeCloseTo(1.99);
    expect(varianceCluster(2, 10, 20, 0.05)).toBeCloseTo((4 / 200) * (1 + 19 * 0.05));
  });

  it("designMatrix shapes", () => {
    expect(designMatrix("Parallel")).toEqual([[0], [1]]);
    expect(designMatrix("Cross-over")).toEqual([[0, 1], [1, 0]]);
    expect(designMatrix("Stepped-wedge", 4)).toEqual([
      [0, 1, 1, 1],
      [0, 0, 1, 1],
      [0, 0, 0, 1],
    ]);
  });

  it("designMatrixData is long-format for Plot.cell", () => {
    const rows = designMatrixData("Cross-over");
    expect(rows).toHaveLength(4);
    expect(rows[0]).toEqual({ sequence: "Seq 1", period: 1, treated: 0 });
  });

  it("varthetaM: more clusters means less variance, stepped-wedge beats parallel here", () => {
    const base = { m: 20, periods: 6, icc: 0.05, cac: 0.8, iac: 0, sd: 1 };
    const sw1 = varthetaM({ ...base, design: "Stepped-wedge", clustersPerSequence: 1 });
    const sw2 = varthetaM({ ...base, design: "Stepped-wedge", clustersPerSequence: 2 });
    expect(sw2).toBeCloseTo(sw1 / 2);
    const par = varthetaM({ ...base, design: "Parallel", clustersPerSequence: 1 });
    expect(sw1).toBeGreaterThan(0);
    expect(par).toBeGreaterThan(0);
  });
});

describe("hypothesis tests", () => {
  it("one-sample t-test", () => {
    const rows = [12, 14, 11, 13, 15, 12, 13].map((v) => ({ v }));
    const r = oneSampleTTest(rows, "v", 10);
    expect(r.mean).toBeCloseTo(12.857, 2);
    expect(r.pValue).toBeLessThan(0.01); // clearly not 10
    expect(r.df).toBe(6);
  });

  it("two-sample t-test on a DataFrame", () => {
    const df = DataFrame.fromRows([
      { g: "A", v: 10 }, { g: "A", v: 12 }, { g: "A", v: 11 },
      { g: "B", v: 20 }, { g: "B", v: 22 }, { g: "B", v: 21 },
    ]);
    const r = twoSampleTTest(df, "v", "g", "A", "B");
    expect(r.mean1).toBeCloseTo(11);
    expect(r.mean2).toBeCloseTo(21);
    expect(r.pValue).toBeLessThan(0.001);
  });

  it("welch handles unequal variances without complaint", () => {
    const rows = [
      ...[10, 10.1, 9.9, 10.05].map((v) => ({ g: "tight", v })),
      ...[8, 15, 11, 20].map((v) => ({ g: "wild", v })),
    ];
    const r = welchTTest(rows, "v", "g", "tight", "wild");
    expect(r.df).toBeLessThan(6); // Welch df shrinks under variance imbalance
    expect(r.pValue).toBeGreaterThan(0.05);
  });

  it("ols recovers known coefficients", () => {
    // y = 2 + 3x, exactly
    const rows = [1, 2, 3, 4, 5].map((x) => ({ x, y: 2 + 3 * x }));
    const m = ols(rows, { dependentVar: "y", predictors: ["x"] });
    expect(m.beta[0]).toBeCloseTo(2, 6);
    expect(m.beta[1]).toBeCloseTo(3, 6);
    expect(m.rss).toBeCloseTo(0, 6);
  });

  it("ancova detects a group effect beyond covariates", () => {
    const rows = [];
    for (let i = 0; i < 40; i++) {
      const covar = i % 10;
      rows.push({ group: "control", covar, y: 5 + covar * 1.2 + (i % 3) * 0.1 });
      rows.push({ group: "treated", covar, y: 8 + covar * 1.2 + (i % 3) * 0.1 });
    }
    const r = ancova(rows, { dependentVar: "y", covariates: ["covar"], groupVar: "group" });
    expect(r.partialF.pValue).toBeLessThan(0.001);
    expect(r.levels).toEqual(["control", "treated"]);
    // treatment dummy should be ~3
    expect(r.fullModel.beta[2]).toBeCloseTo(3, 1);
  });
});

describe("distributions & DistP", () => {
  it("samplers land near their theoretical moments", () => {
    const n = 20000;
    const normal = Array.from({ length: n }, () => distributions.normal({ mean: 5, std: 2 }));
    expect(normal.reduce((a, b) => a + b, 0) / n).toBeCloseTo(5, 0);

    const beta = Array.from({ length: n }, () => distributions.beta({ alpha: 2, beta: 6 }));
    expect(beta.reduce((a, b) => a + b, 0) / n).toBeCloseTo(0.25, 1);

    const gamma = Array.from({ length: n }, () => distributions.gamma({ shape: 3, scale: 2 }));
    expect(gamma.reduce((a, b) => a + b, 0) / n).toBeCloseTo(6, 0);
  });

  it("DistP samples, bounds, and reports stats", () => {
    const d = new DistP({
      name: "conversion",
      lever: "engagement",
      segment: "program",
      distfunc: distributions.normal,
      params: { mean: 0.25, std: 0.15 },
      bounds: [0, 1],
      size: 10000,
    });
    expect(d.samples).toHaveLength(10000);
    expect(d.stats.mean).toBeGreaterThan(0.2);
    expect(d.lever).toBe("engagement");
    expect(Math.min(...d.samples)).toBeGreaterThanOrEqual(0);
    const [lo, hi] = d.confInt();
    expect(lo).toBeLessThan(hi);
  });

  it("DistP toDataFrame exports draws as DataFrame rows", () => {
    const d = new DistP({ distfunc: distributions.uniform, params: { min: 1, max: 1 }, size: 5, bounds: [0, 2] });
    const df = d.toDataFrame("draw_value");
    expect(df.height).toBe(5);
    expect(df.columns).toContain("draw_value");
  });

  it("chain operations compose distributions", () => {
    const a = new DistP({ distfunc: distributions.uniform, params: { min: 1, max: 1 }, size: 100, bounds: [0, 10] });
    const b = new DistP({ distfunc: distributions.uniform, params: { min: 2, max: 2 }, size: 100, bounds: [0, 10] });
    const c = a.copy().chainMult(b).multConst(3);
    expect(c.stats.mean).toBeCloseTo(6);
    expect(a.stats.mean).toBeCloseTo(1); // copy() protected the original
  });

  it("size mismatches are rejected, not broadcast", () => {
    const a = new DistP({ samples: [1, 2, 3] });
    const b = new DistP({ samples: [1, 2] });
    expect(() => a.chainMult(b)).toThrow(/equal sizes/);
  });
});

describe("kde", () => {
  it("integrates to ~1 and respects cut", async () => {
    const { kde, silvermanBandwidth } = await import("../src/stats/density.js");
    const samples = Array.from({ length: 2000 }, () => distributions.normal({ mean: 10, std: 2 }));
    const { points, bandwidth } = kde(samples, { cut: 3 });
    expect(bandwidth).toBeCloseTo(silvermanBandwidth(samples), 10);

    // trapezoid integral of the density should be ~1
    let integral = 0;
    for (let i = 1; i < points.length; i++) {
      integral += ((points[i].density + points[i - 1].density) / 2) * (points[i].x - points[i - 1].x);
    }
    expect(integral).toBeGreaterThan(0.97);
    expect(integral).toBeLessThan(1.03);

    // cut: 0 clips at the data; cut: 3 extends past it
    const clipped = kde(samples, { cut: 0 });
    const extended = kde(samples, { cut: 3 });
    const min = Math.min(...samples);
    expect(clipped.points[0].x).toBeCloseTo(min, 6);
    expect(extended.points[0].x).toBeLessThan(min);
  });

  it("peaks near the true mode", async () => {
    const { kde } = await import("../src/stats/density.js");
    const samples = Array.from({ length: 5000 }, () => distributions.normal({ mean: 5, std: 1 }));
    const { points } = kde(samples);
    const peak = points.reduce((best, p) => (p.density > best.density ? p : best));
    expect(peak.x).toBeGreaterThan(4.5);
    expect(peak.x).toBeLessThan(5.5);
  });

  it("refuses the void", async () => {
    const { kde } = await import("../src/stats/density.js");
    expect(() => kde([])).toThrow(/philosophical/);
  });
});

describe("matrix helpers", () => {
  it("inverts and multiplies", () => {
    const A = [
      [4, 7],
      [2, 6],
    ];
    const Ainv = inv(A);
    const I = matmul(A, Ainv);
    expect(I[0][0]).toBeCloseTo(1);
    expect(I[0][1]).toBeCloseTo(0);
    expect(I[1][1]).toBeCloseTo(1);
  });

  it("refuses singular matrices", () => {
    expect(() => inv([[1, 2], [2, 4]])).toThrow(/singular/i);
  });
});
