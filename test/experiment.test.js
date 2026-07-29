// @vitest-environment jsdom
/**
 * experiment.test.js — the experimental design system, end to end.
 * Panel data in, strata measured, arms defined, sample sizes computed,
 * feasibility checked, plots rendered, business case priced, and the
 * whole thing validated against its own Monte Carlo. One system.
 */

import { describe, it, expect } from "vitest";
import { DataFrame } from "../src/index.js";
import {
  ExperimentDesign,
  sampleSizePerArm,
  multiArmAdjustment,
  channelCascade,
} from "../src/stats/index.js";
import { experimentDesignTree, measurementTimeline, consortDiagram } from "../src/plots/index.js";

function rng(seed = 3) {
  let s = seed;
  return () => (s = (s * 1664525 + 1013904223) % 4294967296) / 4294967296;
}

/** A member-month panel with a risk score that actually predicts events. */
function makeRiskPanel({ nUnits = 2000, nPeriods = 12, seed = 3 } = {}) {
  const rand = rng(seed);
  const rows = [];
  for (let u = 0; u < nUnits; u++) {
    const score = rand(); // uniform risk score
    // wide rate spread so per-unit rates persist visibly across windows,
    // which is what makes the CUPED correlation estimable from 12 months
    const monthlyRate = 0.005 + score * 0.12;
    for (let t = 0; t < nPeriods; t++) {
      rows.push({
        person_id: `m${u}`,
        period: t,
        risk_score: score,
        event: rand() < monthlyRate ? 1 : 0,
      });
    }
  }
  return rows;
}

describe("power calculators", () => {
  it("arcsine (Cohen's h) sample size matches the Python framework's arithmetic", () => {
    // h = 2(asin√.18 − asin√.162); n/arm = 2((z_.975+z_.8)/h)²
    const r = sampleSizePerArm({ p1: 0.18, p2: 0.162, alpha: 0.05, power: 0.8 });
    const h = 2 * (Math.asin(Math.sqrt(0.18)) - Math.asin(Math.sqrt(0.162)));
    const expected = Math.ceil(2 * ((1.959964 + 0.841621) / h) ** 2);
    expect(r.nPerArm).toBe(expected);
  });

  it("CUPED with ρ=0.5 cuts n by 25%, as advertised", () => {
    const base = sampleSizePerArm({ p1: 0.18, p2: 0.162 });
    const cuped = sampleSizePerArm({ p1: 0.18, p2: 0.162, method: "cuped", correlation: 0.5 });
    expect(cuped.nPerArm / base.nPerArm).toBeCloseTo(0.75, 2);
    expect(cuped.varianceReduction).toBeCloseTo(0.25);
  });

  it("zero effect asks for infinity, politely", () => {
    expect(sampleSizePerArm({ p1: 0.1, p2: 0.1 }).nPerArm).toBe(Infinity);
  });

  it("multi-arm adjustment matches the ported table", () => {
    expect(multiArmAdjustment(2)).toBe(1);
    expect(multiArmAdjustment(3)).toBe(1.5);
    expect(multiArmAdjustment(4)).toBeCloseTo(4 / 3);
    expect(multiArmAdjustment(12)).toBe(2); // capped
  });

  it("channel cascade compounds multiplicatively and sums costs", () => {
    const { relativeEffect, costPerMember } = channelCascade([
      { reach: 1.0, open: 0.7, efficacy: 0.03, cost: 1 },
      { reach: 0.41, open: 0.56, efficacy: 0.03, cost: 0 },
    ]);
    const expected = 1 - (1 - 0.7 * 0.03) * (1 - 0.41 * 0.56 * 0.03);
    expect(relativeEffect).toBeCloseTo(expected, 10);
    expect(costPerMember).toBe(1);
  });
});

describe("ExperimentDesign from panel data", () => {
  const panel = DataFrame.fromRows(makeRiskPanel());

  it("measures strata rates and population from the data", () => {
    const design = ExperimentDesign.fromPanel(panel, {
      unit: "person_id", outcome: "event", riskScore: "risk_score",
    }).stratify({ high: [0.7, 1], low: [0.2, 0.7] });

    const [high, low] = design._strata;
    expect(high.name).toBe("high");
    expect(high.available).toBeGreaterThan(500); // top 30% of 2000
    expect(high.available).toBeLessThan(700);
    // the score predicts events, so measured rates must be ordered
    expect(high.baseRate).toBeGreaterThan(low.baseRate);
  });

  it("estimates the CUPED correlation from pre/post windows", () => {
    const design = ExperimentDesign.fromPanel(panel, {
      unit: "person_id", outcome: "event", period: "period",
      riskScore: "risk_score", prePeriods: [0, 1, 2, 3, 4, 5],
    });
    // persistent per-unit rates → positive pre/post correlation, from data
    expect(design._power.correlation).toBeGreaterThan(0.05);
    expect(design._power.correlation).toBeLessThan(0.9);
  });

  it("builds a feasible RCT and an infeasible factorial from the same panel", () => {
    const base = () =>
      ExperimentDesign.fromPanel(panel, { unit: "person_id", outcome: "event", riskScore: "risk_score" })
        .stratify({ high: [0.7, 1] })
        .power({ alpha: 0.05, power: 0.8, method: "cuped", correlation: 0.5 });

    const rct = base().arms([{ name: "PT+MRI", relativeEffect: -0.35 }]).design("rct").build();
    expect(rct.nArms).toBe(2);
    expect(rct.cells[0].nPerArm).toBeGreaterThan(0);
    expect(rct.describe()).toContain("feasible");

    // three arms with a small effect: the multi-arm tax makes it worse
    const threeArm = base()
      .arms([{ name: "PT", relativeEffect: -0.2 }, { name: "PT+MRI", relativeEffect: -0.35 }])
      .design("three-arm")
      .build();
    expect(threeArm.adjustment).toBe(1.5);
    expect(threeArm.requiredInStratum("high")).toBeGreaterThan(rct.requiredInStratum("high"));
  });
});

describe("Experiment: one system, every output", () => {
  const experiment = ExperimentDesign.fromAssumptions()
    .stratify([
      { name: "High Risk", baseRate: 0.18, available: 40000 },
      { name: "Low Risk", baseRate: 0.08, available: 100000 },
    ])
    .arms([
      {
        name: "Outreach",
        channels: [
          { name: "dm", reach: 1.0, open: 0.7, efficacy: 0.03, cost: 1 },
          { name: "cm_call", reach: 0.9, open: 0.3, efficacy: 0.07, cost: 3 },
        ],
      },
    ])
    .power({ alpha: 0.05, power: 0.8, method: "cuped", correlation: 0.5 })
    .design("rct")
    .build();

  it("channel arms derive effect and cost from the cascade", () => {
    expect(experiment.arms[0].relativeEffect).toBeLessThan(0); // prevention
    expect(experiment.arms[0].costPerMember).toBe(4);
  });

  it("toDataFrame returns a real DataFrame the core can query", () => {
    const df = experiment.toDataFrame();
    expect(df.height).toBe(2); // 2 strata × 1 arm
    expect(df.columns).toContain("n_per_arm");
    const sorted = df.sort("base_rate", { descending: true });
    expect(sorted.row(0).stratum).toBe("High Risk");
  });

  it("feasibility reconciles required vs available", () => {
    for (const f of experiment.feasibility()) {
      expect(f.required).toBe(experiment.requiredInStratum(f.stratum));
      expect(typeof f.feasible).toBe("boolean");
    }
  });

  it("renders the design tree, timeline, and consort from its own configs", () => {
    const tree = experimentDesignTree(experiment.toDesignTree({ months: { "High Risk": "~6", "Low Risk": "~12" } }));
    expect(tree.textContent).toContain("High Risk");
    expect(tree.textContent).toContain("n per arm");

    const timeline = measurementTimeline(experiment.toTimeline({ horizonMonths: 12 }));
    expect(timeline.textContent).toContain("High Risk endpoint observable");
    expect(timeline.textContent).toContain("Claims lag");

    const consort = consortDiagram(experiment.toConsort({ screened: 200000 }));
    expect(consort.textContent).toContain("Assessed for eligibility (n = 200,000)");
    expect(consort.textContent).toContain("Outreach");
    expect(consort.textContent).toContain("Control");
  });

  it("prices the business case with ROI defined as (value − cost) / cost", () => {
    const bc = experiment.businessCase({ eventValue: 10000 });
    expect(bc.byCell.height).toBe(2);
    const totals = bc.totals;
    expect(totals.events_prevented).toBeGreaterThan(0);
    expect(totals.net).toBeCloseTo(totals.value - totals.program_cost, 6);
    expect(totals.roi).toBeCloseTo((totals.value - totals.program_cost) / totals.program_cost, 6);
  });

  it("Monte Carlo lands near the design power", () => {
    const rand = rng(7);
    const results = experiment.simulatePower({ nSimulations: 300, random: rand });
    for (const r of results) {
      expect(r.empiricalPower).toBeGreaterThan(0.68); // target 0.8, sampling noise allowed
      expect(r.empiricalPower).toBeLessThanOrEqual(1);
    }
  });

  it("refuses to build without strata or arms, with directions", () => {
    expect(() => ExperimentDesign.fromAssumptions().build()).toThrow(/stratify/);
    expect(() =>
      ExperimentDesign.fromAssumptions().stratify([{ name: "x", baseRate: 0.1, available: 100 }]).build()
    ).toThrow(/arms/);
  });
});
