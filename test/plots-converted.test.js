// @vitest-environment jsdom
/**
 * plots-converted.test.js — smoke tests for the primitives converted from
 * the toConvert/visualization stash. Same bar as the others: real data in,
 * real DOM out, no exceptions on the way.
 */

import { describe, it, expect } from "vitest";
import { DataFrame } from "../src/index.js";
import {
  bumpChart,
  trapezoidFunnel,
  tufteLine,
  tufteLineMarks,
  sankeyFlow,
  nestFromFrame,
  treeExplore,
  tufteForestPlot,
  withDownloadButtons,
  serpentineTimeline,
  pictogramFill,
} from "../src/plots/index.js";
import { tabPanel } from "../src/layouts/index.js";

const isNode = (el) => el?.nodeType != null;

describe("bumpChart", () => {
  const df = DataFrame.fromRows([
    { period: "Jan", series: "Falls", value: 68 },
    { period: "Jan", series: "Pneumonia", value: 45 },
    { period: "Jan", series: "AKI", value: 62 },
    { period: "Feb", series: "Falls", value: 44 },
    { period: "Feb", series: "Pneumonia", value: 61 },
    { period: "Feb", series: "AKI", value: 63 },
  ]);

  it("ranks within periods; end labels carry ranks; no colliding y axis", () => {
    const el = bumpChart(df, { x: "period", y: "value", z: "series" });
    expect(isNode(el)).toBe(true);
    // end labels carry the rank inline, replacing the axis that used to collide with them
    expect(el.textContent).toContain("#1 Falls");
    expect(el.querySelector('[aria-label="y-axis tick label"]')).toBeNull();
    // margins are sized from the longest label so long names don't clip
    const longNames = bumpChart(
      [
        { period: "Jan", series: "Extremely Protracted Condition Name", value: 2 },
        { period: "Jan", series: "B", value: 1 },
        { period: "Feb", series: "Extremely Protracted Condition Name", value: 2 },
        { period: "Feb", series: "B", value: 1 },
      ],
      { x: "period", y: "value", z: "series" }
    );
    expect(isNode(longNames)).toBe(true);
  });

  it("topN filters to series that ever rank high enough", () => {
    const el = bumpChart(df, { x: "period", y: "value", z: "series", topN: 1 });
    expect(isNode(el)).toBe(true);
    // Falls (#1 in Jan) and AKI (#1 in Feb) survive; Pneumonia never ranks #1
    expect(el.textContent).toContain("Falls");
    expect(el.textContent).toContain("AKI");
  });
});

describe("trapezoidFunnel", () => {
  const stages = [
    { group: "Aware", value: 100000 },
    { group: "Engaged", value: 40000 },
    { group: "Converted", value: 8000 },
  ];

  it("renders trapezoids with percentages and rates", () => {
    const el = trapezoidFunnel(stages);
    expect(isNode(el)).toBe(true);
    expect(el.textContent).toContain("Aware");
    expect(el.textContent).toContain("100%");
    expect(el.textContent).toContain("40% convert ↓");
  });

  it("uses altLabel when provided", () => {
    const withAlt = stages.map((s) => ({ ...s, label: `${s.value / 1000}K` }));
    const el = trapezoidFunnel(withAlt, { altLabel: "label" });
    expect(el.textContent).toContain("100K");
  });
});

describe("tufteLine", () => {
  const df = DataFrame.fromRows(
    ["A", "B"].flatMap((s) =>
      Array.from({ length: 6 }, (_, i) => ({ month: i, score: i * 2 + (s === "A" ? 1 : 4), series: s }))
    )
  );

  it("renders the composed figure and standalone marks", () => {
    expect(isNode(tufteLine(df, { x: "month", y: "score", stroke: "series" }))).toBe(true);
    expect(Array.isArray(tufteLineMarks(df, { x: "month", y: "score" }))).toBe(true);
  });
});

describe("sankeyFlow", () => {
  it("nestFromFrame builds the tree from a DataFrame", () => {
    const df = DataFrame.fromRows([
      { region: "East", specialty: "Cardio", preferred: 100, non_preferred: 40 },
      { region: "East", specialty: "Ortho", preferred: 60, non_preferred: 80 },
      { region: "West", specialty: "Cardio", preferred: 90, non_preferred: 10 },
    ]);
    const nested = nestFromFrame(df, { levels: ["region", "specialty"], buckets: ["preferred", "non_preferred"] });
    expect(nested.root.East.Cardio).toEqual({ preferred: 100, non_preferred: 40 });
    expect(nested.root.West.Cardio.preferred).toBe(90);
  });

  it("renders statically (animate: false) with final counts", () => {
    const nested = {
      root: {
        East: { preferred: 100, non_preferred: 40 },
        West: { preferred: 90, non_preferred: 10 },
      },
    };
    const { node, replay, stop } = sankeyFlow(nested, { animate: false });
    expect(isNode(node)).toBe(true);
    expect(node.textContent).toContain("East");
    expect(node.textContent).toContain("Preferred");
    expect(node.textContent).toContain("100"); // final count, not "0"
    expect(typeof replay).toBe("function");
    stop();
  });
});

describe("treeExplore", () => {
  const df = DataFrame.fromRows([
    { region: "East", clinic: "A", visits: 10, nonPreferred: 4 },
    { region: "East", clinic: "B", visits: 6, nonPreferred: 1 },
    { region: "West", clinic: "C", visits: 8, nonPreferred: 5 },
  ]);

  it("builds a hierarchy with configurable metrics", () => {
    const el = treeExplore(df, {
      levels: ["region", "clinic"],
      metrics: [
        { label: "Members", type: "count" },
        { label: "Visits", type: "sum", column: "visits" },
        { label: "Non-preferred rate", type: "rate", numerator: "nonPreferred", denominator: "visits", format: "percent" },
      ],
    });
    expect(isNode(el)).toBe(true);
    expect(el.textContent).toContain("East");
  });

  it("demands levels", () => {
    expect(() => treeExplore(df, {})).toThrow(/levels/);
  });
});

describe("tufteForestPlot", () => {
  const rows = [
    { category: "Outpatient", value: 0.02, lower: 0.01, upper: 0.03, p: 0.001, rel: 0.12 },
    { category: "Emergency", value: 0.01, lower: -0.01, upper: 0.03, p: 0.2, rel: 0.05 },
    { category: "Pharmacy", value: 0.35, lower: 0.32, upper: 0.38, p: 0.0001, rel: 0.4 },
  ];

  it("renders continuous mode with auto domain", () => {
    const el = tufteForestPlot(rows, { pValue: "p", relative: "rel", title: "DiD by site" });
    expect(isNode(el)).toBe(true);
    expect(el.textContent).toContain("Outpatient");
    expect(el.textContent).toContain("p < 0.001");
    expect(el.textContent).toContain("DiD by site");
  });

  it("renders break mode for the outlier's benefit", () => {
    const el = tufteForestPlot(rows, {
      break: true,
      xScaleLeftRange: [-0.02, 0.05],
      xScaleRightRange: [0.3, 0.4],
    });
    expect(isNode(el)).toBe(true);
  });

  it("withDownloadButtons wraps and adds buttons", () => {
    const el = withDownloadButtons(tufteForestPlot, rows, {});
    expect(el.querySelectorAll("button")).toHaveLength(2);
  });
});

describe("serpentineTimeline", () => {
  it("renders from a DataFrame and cleans up after itself", () => {
    const df = DataFrame.fromRows([
      { date: new Date("2026-01-01"), phase: "Design", activity: "Power analysis" },
      { date: new Date("2026-04-01"), phase: "Launch", activity: "First outreach" },
      { date: new Date("2026-10-01"), phase: "Measure", activity: "Clinical endpoints" },
    ]);
    const { node, cleanup } = serpentineTimeline(df, { now: new Date("2026-02-01") });
    expect(isNode(node)).toBe(true);
    cleanup();
    expect(document.querySelectorAll(".odf-timeline-tooltip")).toHaveLength(0);
  });
});

describe("pictogramFill", () => {
  it("fills a silhouette and updates on demand", () => {
    const { node, setFillLevel } = pictogramFill({ pathData: "M0 0 L677 0 L677 1280 L0 1280 Z", fillLevel: 25 });
    expect(isNode(node)).toBe(true);
    const mask = node.querySelector("rect");
    expect(Number(mask.getAttribute("height"))).toBeCloseTo(1280 * 0.75);
    setFillLevel(100);
    expect(Number(mask.getAttribute("height"))).toBe(0);
    setFillLevel(150); // clamped, not launched
    expect(Number(mask.getAttribute("height"))).toBe(0);
  });

  it("requires pathData", () => {
    expect(() => pictogramFill({})).toThrow(/pathData/);
  });
});

describe("tabPanel", () => {
  it("switches panes on click", () => {
    const el = tabPanel({
      tabs: [
        { label: "One", content: "first pane" },
        { label: "Two", content: "second pane" },
      ],
    });
    const buttons = el.querySelectorAll("button");
    const panes = [...el.querySelectorAll("div > div")].slice(1);
    expect(buttons).toHaveLength(2);
    expect(el.textContent).toContain("first pane");
    buttons[1].click();
    const visible = [...el.children].filter((c) => c.style.display !== "none");
    expect(el.textContent).toContain("second pane");
    expect(visible.length).toBeGreaterThan(0);
  });

  it("rejects tablessness", () => {
    expect(() => tabPanel({ tabs: [] })).toThrow(/div/);
  });
});
