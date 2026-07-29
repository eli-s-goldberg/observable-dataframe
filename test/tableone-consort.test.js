// @vitest-environment jsdom
/**
 * tableone-consort.test.js — the paper furniture: Table 1 and the CONSORT
 * flow diagram. Reviewers check these before the findings; so do we.
 */

import { describe, it, expect } from "vitest";
import { DataFrame } from "../src/index.js";
import { tableOne, chi2CDF } from "../src/stats/index.js";
import { consortDiagram } from "../src/plots/index.js";

function makeBaseline(n = 400) {
  let s = 9;
  const rand = () => (s = (s * 1664525 + 1013904223) % 4294967296) / 4294967296;
  const gauss = () => Math.sqrt(-2 * Math.log(1 - rand())) * Math.cos(2 * Math.PI * rand());
  return Array.from({ length: n }, (_, i) => ({
    person_id: `m${i}`,
    arm: i % 2 === 0 ? "intervention" : "control",
    age: Math.round(52 + gauss() * 12),
    baseline_encounters: Math.max(0, 6 + gauss() * 3),
    sex: rand() < 0.55 ? "F" : "M",
    region: ["East", "West", "Central"][i % 3],
  }));
}

describe("chi2CDF", () => {
  it("matches reference values", () => {
    // R: pchisq(3.841, 1) = 0.9500
    expect(chi2CDF(3.841, 1)).toBeCloseTo(0.95, 3);
    // R: pchisq(9.488, 4) = 0.95
    expect(chi2CDF(9.488, 4)).toBeCloseTo(0.95, 3);
    expect(chi2CDF(0, 3)).toBe(0);
  });
});

describe("tableOne", () => {
  const df = DataFrame.fromRows(makeBaseline());

  it("builds arm sizes, mean (SD), and n (%) rows", () => {
    const t1 = tableOne(df, {
      by: "arm",
      continuous: ["age", "baseline_encounters"],
      categorical: ["sex", "region"],
      labels: { baseline_encounters: "Baseline encounters" },
    });
    expect(t1.groups.map((g) => g.label).sort()).toEqual(["control", "intervention"]);
    expect(t1.rows[0]).toMatchObject({ characteristic: "n" });
    expect(t1.rows[0].intervention).toBe("200");

    const age = t1.rows.find((r) => r.characteristic.startsWith("age"));
    expect(age.intervention).toMatch(/^\d+\.\d \(\d+\.\d\)$/); // "52.3 (11.8)" shape
    expect(age.p).toMatch(/^(<0\.001|0\.\d{3})$/);

    const sexHeader = t1.rows.find((r) => r.characteristic.startsWith("sex"));
    expect(sexHeader.p).toBeTruthy();
    const female = t1.rows.find((r) => r.characteristic.trim() === "F");
    expect(female.control).toMatch(/^\d+ \(\d+\.\d%\)$/); // "108 (54.0%)" shape
  });

  it("randomized arms produce unremarkable p-values, as they should", () => {
    const t1 = tableOne(df, { by: "arm", continuous: ["age"], categorical: ["sex"] });
    const ps = t1.rows.map((r) => r.p).filter((p) => p && p !== "");
    // alternating assignment ≈ randomization: expect no tiny p-values
    for (const p of ps) expect(p === "<0.001").toBe(false);
  });

  it("refuses one-armed comparisons", () => {
    const solo = makeBaseline(50).map((r) => ({ ...r, arm: "only" }));
    expect(() => tableOne(solo, { by: "arm", continuous: ["age"] })).toThrow(/at least 2/);
  });
});

describe("consortDiagram", () => {
  const config = {
    title: "Participant flow",
    steps: [
      { label: "Assessed for eligibility", n: 64632 },
      {
        label: "Randomized",
        n: 6000,
        excluded: {
          label: "Excluded",
          n: 58632,
          reasons: [
            { label: "Insufficient enrollment span", n: 41210 },
            { label: "No claims activity", n: 17422 },
          ],
        },
      },
    ],
    arms: [
      {
        label: "Intervention",
        n: 3000,
        steps: [
          { label: "Received outreach", n: 2874, excluded: { label: "Did not receive", n: 126, reasons: [{ label: "Disenrolled", n: 126 }] } },
          { label: "Analyzed", n: 2874 },
        ],
      },
      { label: "Usual care", n: 3000, steps: [{ label: "Analyzed", n: 3000 }] },
    ],
  };

  it("renders the spine, exclusions with reasons, and both arms", () => {
    const el = consortDiagram(config);
    expect(el.tagName.toLowerCase()).toBe("svg");
    const text = el.textContent;
    expect(text).toContain("Assessed for eligibility (n = 64,632)");
    expect(text).toContain("Excluded (n = 58,632)");
    // wrapped lines concatenate in textContent, so match the pieces
    expect(text).toContain("Insufficient enrollment span");
    expect(text).toContain("41,210");
    expect(text).toContain("Intervention (n = 3,000)");
    expect(text).toContain("Usual care (n = 3,000)");
    expect(text).toContain("Analyzed (n = 2,874)");
    // boxes and arrows exist in publication-plausible quantity
    expect(el.querySelectorAll("rect").length).toBeGreaterThan(6);
    expect(el.querySelectorAll("path").length).toBeGreaterThan(3); // arrowheads
  });

  it("works spine-only for single-arm flows", () => {
    const el = consortDiagram({ steps: [{ label: "Screened", n: 100 }, { label: "Analyzed", n: 80 }] });
    expect(el.textContent).toContain("Screened (n = 100)");
    expect(el.querySelectorAll("rect")).toHaveLength(2);
  });

  it("rejects emptiness", () => {
    expect(() => consortDiagram({})).toThrow(/flows nowhere/);
  });
});
