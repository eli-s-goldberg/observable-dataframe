// @vitest-environment jsdom
/**
 * did-plots.test.js — the regression figures agree with the regressions.
 * A plot that disagrees with its own estimator is a very confident lie.
 */

import { describe, it, expect } from "vitest";
import { did, eventStudy, callawaySantAnna } from "../src/stats/index.js";
import { didPlot, eventStudyPlot } from "../src/plots/index.js";

const isNode = (el) => el?.nodeType != null;

// diff-diff's quick-start rows: ATT = 3, visible from orbit.
const rows2x2 = [
  { outcome: 10, treated: 1, post: 0 }, { outcome: 11, treated: 1, post: 0 },
  { outcome: 15, treated: 1, post: 1 }, { outcome: 18, treated: 1, post: 1 },
  { outcome: 9, treated: 0, post: 0 }, { outcome: 10, treated: 0, post: 0 },
  { outcome: 12, treated: 0, post: 1 }, { outcome: 13, treated: 0, post: 1 },
];

function makeStaggered(effect = 4) {
  let s = 5;
  const rand = () => (s = (s * 1664525 + 1013904223) % 4294967296) / 4294967296;
  const gauss = () => Math.sqrt(-2 * Math.log(1 - rand())) * Math.cos(2 * Math.PI * rand());
  const rows = [];
  for (let u = 0; u < 150; u++) {
    const g = u % 2 === 0 ? (u % 4 === 0 ? 4 : 5) : null;
    const level = gauss() * 2;
    for (let t = 0; t < 8; t++) {
      rows.push({ unit: `u${u}`, period: t, group: g, y: 10 + level + t * 0.5 + (g != null && t >= g ? effect : 0) + gauss() });
    }
  }
  return rows;
}

describe("didPlot", () => {
  it("draws the four means, counterfactual, and an ATT that matches did()", () => {
    const el = didPlot(rows2x2, { outcome: "outcome", treatment: "treated", time: "post" });
    expect(isNode(el)).toBe(true);
    expect(el.textContent).toContain("treated");
    expect(el.textContent).toContain("control");
    expect(el.textContent).toContain("counterfactual");
    // the bracket shows the same ATT the regression reports
    const r = did(rows2x2, { outcome: "outcome", treatment: "treated", time: "post" });
    expect(el.textContent).toContain(`ATT +${r.att.toFixed(2)}`);
  });

  it("negative effects bracket downward with a sign", () => {
    const flipped = rows2x2.map((r) => ({ ...r, outcome: r.treated && r.post ? r.outcome - 8 : r.outcome }));
    const el = didPlot(flipped, { outcome: "outcome", treatment: "treated", time: "post" });
    expect(el.textContent).toMatch(/ATT -/);
  });

  it("refuses incomplete 2x2s by name", () => {
    const missing = rows2x2.filter((r) => !(r.treated === 1 && r.post === 1));
    expect(() => didPlot(missing, { outcome: "outcome", treatment: "treated", time: "post" })).toThrow(/four cells/);
  });
});

describe("eventStudyPlot", () => {
  it("renders eventStudy() output with reference and CIs", () => {
    const r = eventStudy(makeStaggered(), { outcome: "y", unit: "unit", time: "period", group: "group" });
    const el = eventStudyPlot(r);
    expect(isNode(el)).toBe(true);
    expect(el.textContent).toContain("adoption");
    expect(el.textContent).toContain("event time");
  });

  it("renders callawaySantAnna().byEventTime interchangeably", () => {
    const cs = callawaySantAnna(makeStaggered(), { outcome: "y", unit: "unit", time: "period", group: "group" });
    const el = eventStudyPlot(cs, { yLabel: "ATT" });
    expect(isNode(el)).toBe(true);
    expect(el.textContent).toContain("ATT");
  });

  it("demands effects", () => {
    expect(() => eventStudyPlot([])).toThrow(/Estimate first/);
  });
});
