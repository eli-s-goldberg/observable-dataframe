// @vitest-environment jsdom
/**
 * plots.test.js — smoke tests: every primitive renders a real DOM node
 * from real data without throwing. Pixel-perfection is the docs site's
 * problem; existence is ours.
 */

import { describe, it, expect } from "vitest";
import { DataFrame } from "../src/index.js";
import { DistP, distributions, designMatrixData } from "../src/stats/index.js";
import {
  corrPlot,
  distPlot,
  forestPlot,
  funnelChart,
  boxPlot,
  timeline,
  designMatrixPlot,
  summaryTable,
  experimentDesignTree,
  powerTable,
  measurementTimeline,
  dotPlot,
} from "../src/plots/index.js";
import {
  splitPanel,
  kpiCard,
  cardRow,
  prose,
  sparkbar,
  formatStatus,
  formatTwoLevel,
  withRowHeight,
  formatWrappedText,
  formatCurrency,
  formatPercent,
  injectPageStyle,
} from "../src/layouts/index.js";

const df = DataFrame.fromRows(
  Array.from({ length: 200 }, (_, i) => ({
    x: Math.sin(i / 10) * 10 + i / 20,
    y: i / 10 + (i % 7),
    z: (i % 13) - 6,
    city: ["NYC", "SF", "LA"][i % 3],
    when: new Date(2024, 0, 1 + i),
    ok: i % 2 === 0,
  }))
);

const isNode = (el) => el instanceof Element || el instanceof DocumentFragment || el?.nodeType != null;

describe("plot primitives render", () => {
  it("corrPlot from a DataFrame", () => {
    expect(isNode(corrPlot(df))).toBe(true);
  });

  it("corrPlot cells are square by default; explicit height opts out", () => {
    const sq = corrPlot(df);
    const cell = sq.querySelector('[aria-label="cell"] rect');
    expect(Number(cell.getAttribute("width"))).toBeCloseTo(Number(cell.getAttribute("height")), 0);

    const rect = corrPlot(df, { height: 160 });
    expect(Number(rect.getAttribute("height"))).toBe(160);
    const squished = rect.querySelector('[aria-label="cell"] rect');
    expect(Number(squished.getAttribute("width"))).not.toBeCloseTo(Number(squished.getAttribute("height")), 0);
  });

  it("distPlot from DistP, array, and DataFrame column", () => {
    const d = new DistP({ distfunc: distributions.normal, params: { mean: 5, std: 1 }, size: 500, bounds: [0, 10] });
    expect(isNode(distPlot(d))).toBe(true);
    expect(isNode(distPlot([1, 2, 2, 3, 3, 3]))).toBe(true);
    expect(isNode(distPlot(df, { column: "x" }))).toBe(true);
    expect(() => distPlot(df)).toThrow(/column/);
  });

  it("distPlot label positions: top (default), bottom, none", () => {
    const samples = [1, 2, 2, 3, 3, 3, 4];
    const meanText = "mean 2.57"; // mean of the samples above, to 2 digits
    expect(distPlot(samples).textContent).toContain(meanText);
    expect(distPlot(samples, { labelPosition: "bottom" }).textContent).toContain(meanText);
    expect(distPlot(samples, { labelPosition: "none" }).textContent).not.toContain("2.57");
    expect(distPlot(samples, { labelDigits: 0 }).textContent).toContain("mean 3");
  });

  it("distPlot kind: 'kde' renders a density with markers intact", () => {
    const samples = Array.from({ length: 500 }, (_, i) => Math.sin(i) * 10 + 50);
    const el = distPlot(samples, { kind: "kde", markers: ["mean", "median"], labelDigits: 1 });
    expect(isNode(el)).toBe(true);
    expect(el.textContent).toContain("density"); // y-axis label switches
    expect(el.textContent).toContain("mean");
    const tight = distPlot(samples, { kind: "kde", cut: 0, bandwidth: 2 });
    expect(isNode(tight)).toBe(true);
  });

  it("distPlot stacks colliding marker labels (mean ≈ median)", () => {
    // symmetric data: mean and median land on the same pixel, guaranteed collision
    const symmetric = Array.from({ length: 200 }, (_, i) => 50 + Math.sin(i) * 5);
    const stacked = distPlot(symmetric, { markers: ["mean", "median"] });
    // stacking splits labels into multiple text marks (one per tier)
    expect(stacked.querySelectorAll('[aria-label="text"]').length).toBeGreaterThan(1);

    const overlapping = distPlot(symmetric, { markers: ["mean", "median"], labelCollision: "none" });
    expect(overlapping.querySelectorAll('[aria-label="text"]').length).toBe(1);

    // far-apart markers don't stack: one tier, one mark
    const spread = Array.from({ length: 400 }, (_, i) => (i % 2 ? i : i * 3));
    const apart = distPlot(spread, { markers: [0.05, 0.95] });
    expect(apart.querySelectorAll('[aria-label="text"]').length).toBe(1);
  });

  it("distPlot justify mode leans near labels apart instead of stacking", () => {
    const symmetric = Array.from({ length: 200 }, (_, i) => 50 + Math.sin(i) * 5);
    const el = distPlot(symmetric, { markers: ["mean", "median"], labelCollision: "justify" });
    const textGroups = el.querySelectorAll('[aria-label="text"]');
    // two anchors (end + start) → two marks, both on tier 0
    expect(textGroups.length).toBe(2);
    const anchors = [...textGroups].map((g) => g.getAttribute("text-anchor") ?? g.querySelector("text")?.getAttribute("text-anchor"));
    expect(anchors).toContain("end");
    expect(anchors).toContain("start");
  });

  it("distPlot markers: median and arbitrary percentiles as ticks", () => {
    const samples = Array.from({ length: 101 }, (_, i) => i); // 0..100: quantiles you can do in your head
    const el = distPlot(samples, { markers: ["mean", "median", 0.25, 0.75], labelDigits: 0 });
    const text = el.textContent;
    expect(text).toContain("mean 50");
    expect(text).toContain("median 50");
    expect(text).toContain("p25 25");
    expect(text).toContain("p75 75");
    // unlabeled ticks still render the marks, just silently
    const silent = distPlot(samples, { markers: [0.25, 0.75], labelPosition: "none" });
    expect(silent.textContent).not.toContain("p25");
    expect(() => distPlot(samples, { markers: [1.5] })).toThrow(/menu/);
  });

  it("forestPlot with CIs and p-values", () => {
    const rows = [
      { site: "Clinic A", est: 0.02, lo: -0.01, hi: 0.05, p: 0.2 },
      { site: "Clinic B", est: 0.08, lo: 0.03, hi: 0.13, p: 0.002 },
    ];
    expect(isNode(forestPlot(rows, { category: "site", value: "est", lower: "lo", upper: "hi", pValue: "p" }))).toBe(true);
  });

  it("forestPlot sizes the right margin for p labels and takes pLabel styling", () => {
    const rows = [
      { site: "A", est: 0.02, lo: 0.01, hi: 0.03, p: 0.0004 },
      { site: "B", est: 0.01, lo: 0.0, hi: 0.02, p: 0.31 },
    ];
    const base = { category: "site", value: "est", lower: "lo", upper: "hi" };
    // with p labels, the right margin grows beyond the bare default of 20
    const withP = forestPlot(rows, { ...base, pValue: "p" });
    const withoutP = forestPlot(rows, base);
    expect(Number(withP.getAttribute("width"))).toBe(Number(withoutP.getAttribute("width")));
    // custom format + styling flow through
    const styled = forestPlot(rows, {
      ...base,
      pValue: "p",
      pLabel: { format: (p) => `significance: ${p}`, fontSize: 12, fill: "purple" },
    });
    expect(styled.textContent).toContain("significance: 0.31");
    // explicit marginRight wins over the computed one
    expect(isNode(forestPlot(rows, { ...base, pValue: "p", marginRight: 5 }))).toBe(true);
  });

  it("funnelChart, boxPlot, timeline, designMatrixPlot", () => {
    expect(isNode(funnelChart([
      { group: "Eligible", value: 100000 },
      { group: "Reached", value: 42000 },
      { group: "Engaged", value: 9000 },
    ]))).toBe(true);
  });

  it("boxPlot is tufte by default: whisker rules, median dot, interactive tip", () => {
    const el = boxPlot(df, { x: "city", y: "y" });
    expect(isNode(el)).toBe(true);
    // tip: true mounts Plot's interactive tip mark (content appears on hover)
    expect(el.querySelector('[aria-label="tip"]')).toBeTruthy();
    // whiskers are rule marks; the median is a dot
    expect(el.querySelector('[aria-label="rule"]')).toBeTruthy();
    expect(el.querySelector('[aria-label="dot"]')).toBeTruthy();
    // no tip requested → no tip mark
    expect(boxPlot(df, { x: "city", y: "y", tip: false }).querySelector('[aria-label="tip"]')).toBeNull();
    // the classic variant survives for the traditionalists
    expect(isNode(boxPlot(df, { x: "city", y: "y", variant: "box" }))).toBe(true);
  });

  it("dotPlot stacks dense events and optional count labels", () => {
    const events = Array.from({ length: 50 }, (_, i) => ({
      x: i % 10,
      y: `m${i % 8}`,
      kind: i % 2 ? "a" : "b",
    }));
    expect(isNode(dotPlot(events, { x: "x", y: "y", fill: "kind" }))).toBe(true);
    expect(
      isNode(dotPlot(events, { countValues: ["a", "b"], countField: "kind", width: 480 }))
    ).toBe(true);
  });

  it("dotPlot mirror layout for balance charts", () => {
    const rows = Array.from({ length: 80 }, (_, i) => ({
      age: 50 + (i % 30),
      arm: i % 2 ? "Treatment" : "Control",
    }));
    expect(
      isNode(
        dotPlot(rows, {
          x: "age",
          y: (d) => (d.arm === "Treatment" ? 1 : -1),
          fill: "arm",
          layout: "mirror",
          yDomain: [-2, 2],
          width: 400,
        })
      )
    ).toBe(true);
  });

  it("waterfall, twoGroupBoxPlot, facetedDensityPlot render", async () => {
    const { waterfallPlot, twoGroupBoxPlot, facetedDensityPlot } = await import("../src/plots/index.js");
    const steps = [
      { step_num: 0, step: "Fees", start: 0, end: -100, color: "#E57373" },
      { step_num: 1, step: "Savings", start: -100, end: 50, color: "#81D4FA" },
      { step_num: 2, step: "Net", start: 0, end: 50, color: "#A5D6A7" },
    ];
    expect(isNode(waterfallPlot(steps, { width: 400, height: 200 }))).toBe(true);

    const month = "Jan-22";
    const a = Array.from({ length: 20 }, (_, i) => ({ date_category: month, effect: -500 + i * 10 }));
    const b = Array.from({ length: 20 }, (_, i) => ({ date_category: month, effect: -200 + i * 8 }));
    expect(isNode(twoGroupBoxPlot(a, b, { width: 420, height: 220 }))).toBe(true);

    const dose = Array.from({ length: 120 }, (_, i) => ({
      treatment_dose: 1 + (i % 4),
      effect: -800 + Math.sin(i) * 400,
    }));
    expect(isNode(facetedDensityPlot(dose, { width: 480, height: 280 }))).toBe(true);
  });

  it("boxPlot flags outliers instead of hiding them", () => {
    const rows = [
      ...Array.from({ length: 20 }, (_, i) => ({ g: "a", v: 10 + (i % 5) })),
      { g: "a", v: 500 }, // the outlier that would have ended up in production
    ];
    const withOutliers = boxPlot(rows, { x: "g", y: "v" });
    const without = boxPlot(rows, { x: "g", y: "v", showOutliers: false });
    expect(withOutliers.querySelectorAll("circle").length).toBeGreaterThan(
      without.querySelectorAll("circle").length
    );
  });

  it("the tip lexicon: true, function, and off", async () => {
    const { resolveTip, tipHTML, createTooltip } = await import("../src/plots/index.js");
    expect(resolveTip(false)).toEqual({});
    expect(resolveTip(true, (d) => d.name)).toMatchObject({ tip: true });
    const fn = (d) => `custom ${d.x}`;
    expect(resolveTip(fn)).toMatchObject({ tip: true, title: fn });
    expect(resolveTip({ anchor: "top" })).toEqual({ tip: { anchor: "top" } });

    // tips mount Plot's interactive tip mark on the figure
    const el = forestPlot(
      [{ site: "A", est: 1, lo: 0, hi: 2, p: 0.01 }],
      { category: "site", value: "est", lower: "lo", upper: "hi", pValue: "p", tip: (d) => `hello ${d.site}` }
    );
    expect(el.querySelector('[aria-label="tip"]')).toBeTruthy();

    // the D3 shared tooltip mounts, shows, hides, and cleans up
    const tooltip = createTooltip();
    tooltip.show({ pageX: 10, pageY: 10 }, tipHTML({ metric: "value" }));
    const mounted = [...document.querySelectorAll("div")].find((d) => d.textContent.includes("metric"));
    expect(mounted).toBeTruthy();
    expect(mounted.style.opacity).toBe("1");
    tooltip.hide();
    expect(mounted.style.opacity).toBe("0");
    tooltip.remove();
  });

  it("funnelChart moves labels outside bars too small to hold them", () => {
    const el = funnelChart([
      { group: "Eligible", value: 120000 },
      { group: "Behavior change", value: 1100 },
    ]);
    // the tiny bar's value renders outside; its rate follows in muted annotation style
    expect(el.textContent).toContain("1,100");
    expect(el.textContent).toContain("1% of prior");
    // value and rate are separate marks now, so the rate isn't bolded along for the ride
    const rateText = [...el.querySelectorAll("text")].find((t) => t.textContent.includes("1% of prior"));
    expect(rateText.getAttribute("font-weight") ?? rateText.closest("[font-weight]")?.getAttribute("font-weight")).not.toBe("700");
    // the big bar keeps its plain inside label
    expect(el.textContent).toContain("120,000");
    expect(isNode(boxPlot(df, { x: "city", y: "y" }))).toBe(true);
    expect(isNode(timeline([{ year: 2024, y: 1, name: "Launch" }]))).toBe(true);
    expect(isNode(designMatrixPlot(designMatrixData("Stepped-wedge", 5)))).toBe(true);
  });

  it("summaryTable summarizes every dtype without incident", () => {
    const el = summaryTable(df);
    expect(isNode(el)).toBe(true);
    expect(el.textContent).toContain("200 rows");
    expect(el.textContent).toContain("city");
    expect(() => summaryTable([{ a: 1 }])).toThrow(/DataFrame/);
  });
});

describe("experiment design figures", () => {
  const cohorts = [
    { label: "Low-Risk Cohort", baseRate: 0.08, behaviorChange: 0.0037, months: "~12", channels: "2 DM + 2 Email" },
    { label: "High-Risk Cohort", baseRate: 0.19, behaviorChange: 0.0063, months: "~6", channels: "2 DM + 2 Email + call", ops: "600 calls/mo ≈ 1 FTE" },
  ];

  it("experimentDesignTree renders both cohorts with power numbers", () => {
    const el = experimentDesignTree({ cohorts, inputs: { alpha: 0.1, power: 0.8 } });
    expect(isNode(el)).toBe(true);
    expect(el.textContent).toContain("High-Risk Cohort");
    expect(el.textContent).toContain("n per arm");
    expect(el.textContent).toContain("P1 = 19.0%");
  });

  it("single-arm collapses to one box per cohort", () => {
    const el = experimentDesignTree({ cohorts, inputs: { design: "single-arm" } });
    expect(el.textContent).toContain("Single arm");
    expect(el.textContent).not.toContain("Control —");
  });

  it("powerTable walks the parameters", () => {
    const el = powerTable(cohorts[1], { alpha: 0.1, power: 0.8 });
    expect(el.textContent).toContain("Z₁₋α");
    expect(el.textContent).toContain("n per arm");
  });

  it("measurementTimeline renders phases, ops markers, and flags", () => {
    const el = measurementTimeline({
      months: ["Nov-25", "Dec-25", "Jan-26", "Feb-26", "Mar-26", "Apr-26", "May-26", "Jun-26"],
      rows: [
        {
          group: "Group 1",
          risk: "High",
          segments: [
            { start: 0, len: 1, type: "identify", label: "Identified" },
            { start: 1, len: 2, type: "outreach", label: "Email + DM" },
            { start: 3, len: 3, type: "observe", label: "Obs" },
            { start: 6, len: 1, type: "lag", label: "Claims lag" },
          ],
          opsMonths: [1, 2],
          clinicalMonth: 7,
          clinicalLabel: "DMARD initiation observable",
        },
      ],
      financialStartMonth: 7,
    });
    expect(isNode(el)).toBe(true);
    expect(el.textContent).toContain("Operational");
    expect(el.textContent).toContain("DMARD initiation observable");
    expect(el.textContent).toContain("Financial");
  });
});

describe("layouts", () => {
  it("splitPanel composes slots", () => {
    const el = splitPanel({
      title: "A section",
      left: prose("Some words"),
      right: kpiCard({ label: "Value at stake", value: "$4.2M", flavor: "valueAtStake" }),
    });
    expect(isNode(el)).toBe(true);
    expect(el.textContent).toContain("A section");
    expect(el.textContent).toContain("$4.2M");
  });

  it("cardRow spreads cards", () => {
    const el = cardRow(kpiCard({ label: "a", value: "1" }), kpiCard({ label: "b", value: "2" }));
    expect(el.textContent).toContain("a");
    expect(el.textContent).toContain("b");
  });

  it("table formatters produce cells", () => {
    expect(isNode(sparkbar(100)(42))).toBe(true);
    expect(isNode(formatStatus()("Deprecated"))).toBe(true);
    expect(isNode(formatTwoLevel()({ header: "v2", description: "the rewrite" }))).toBe(true);
    expect(isNode(withRowHeight("45px", "top")(formatWrappedText())("long text"))).toBe(true);
    expect(isNode(formatCurrency()(-1200))).toBe(true);
    expect(isNode(formatPercent()(0.123))).toBe(true);
  });

  it("bulleted list formatters split, mark, and respect existing markers", async () => {
    const { formatBulletedList, formatProsList, formatConsList } = await import("../src/layouts/index.js");
    const cell = formatBulletedList()("fast | cheap | reproducible");
    expect(cell.textContent).toContain("fast");
    expect(cell.textContent).toContain("•");
    expect(cell.querySelectorAll("span").length).toBeGreaterThan(3);

    const pros = formatProsList()("clear power story | reusable engine");
    expect(pros.textContent).toContain("✓");
    const cons = formatConsList()("claims lag | attribution fights");
    expect(cons.textContent).toContain("×");

    // values with their own checkmarks don't get double-bulleted
    const marked = formatBulletedList()("✓ shipped | ✓ tested");
    expect(marked.textContent.match(/✓/g)).toHaveLength(2);

    // arrays work too
    expect(formatBulletedList()(["a", "b"]).textContent).toContain("b");
    // single items stay plain
    expect(formatBulletedList()("just text").textContent).not.toContain("•");
  });

  it("formatTextBold bullets packed strings, wraps plain ones", async () => {
    const { formatTextBold } = await import("../src/layouts/index.js");
    expect(formatTextBold()("alpha | beta").textContent).toContain("•");
    expect(formatTextBold()("plain").textContent).toContain("plain");
  });

  it("injectPageStyle is idempotent and layers chrome + numbering", () => {
    injectPageStyle({ numberedHeadings: true });
    injectPageStyle(); // second call: no-op
    const styles = document.querySelectorAll("#odf-page-style");
    expect(styles).toHaveLength(1);
    expect(styles[0].textContent).toContain("#observablehq-sidebar"); // chrome on by default
    expect(styles[0].textContent).toContain("h1counter"); // numbering requested
  });

  it("createNumberHeadings keeps the 1.2.3 bookkeeping", async () => {
    const { createNumberHeadings } = await import("../src/layouts/index.js");
    const number = createNumberHeadings();
    expect(number("Intro", 1)).toBe("1 Intro");
    expect(number("Methods", 1)).toBe("2 Methods");
    expect(number("Cohorts", 2)).toBe("2.1 Cohorts");
    expect(number("Power", 2)).toBe("2.2 Power");
    expect(number("Results", 1)).toBe("3 Results");
    expect(number("Subgroup", 2)).toBe("3.1 Subgroup");
  });
});
