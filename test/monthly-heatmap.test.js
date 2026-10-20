// @vitest-environment jsdom
import { describe, it, expect } from "vitest";
import { DataFrame } from "../src/index.js";
import { tufteMonthlyHeatmap } from "../src/plots/index.js";

const rows = [
  {code: "A", date: "2024-01-03", enc: "one"},
  {code: "A", date: "2024-01-03", enc: "one"}, // duplicate claim line
  {code: "B", date: "2024-01-03", enc: "one"}, // co-occurrence, not a new encounter
  {code: "B", date: "2024-01-08", enc: "two"},
  {code: "B", date: "2024-03-08", enc: "three"},
];
const cell = (el, code, month) => el.querySelector(`[data-code="${code}"][data-month="${month}"]`);

describe("tufteMonthlyHeatmap", () => {
  it("counts distinct encounters and uses all monthly encounters as the denominator", () => {
    const el = tufteMonthlyHeatmap(DataFrame.fromRows(rows));
    expect(cell(el, "A", "2024-01").textContent).toContain("1 of 2 distinct encounters (50.0%)");
    expect(cell(el, "B", "2024-01").textContent).toContain("2 of 2 distinct encounters (100.0%)");
    expect(cell(el, "A", "2024-03").textContent).toContain("0 of 1 distinct encounters (0.0%)");
    expect(cell(el, "A", "2024-02").textContent).toContain("No encounters");
  });
  it("toggles the measure, legend and accessible state without changing cell positions", () => {
    const el = tufteMonthlyHeatmap(rows);
    const before = [...el.querySelectorAll("[data-code]")].map(n => n.getAttribute("transform"));
    const buttons = el.querySelectorAll("button");
    const original = cell(el, "A", "2024-01").querySelector("rect").getAttribute("fill");
    buttons[1].click();
    expect(buttons[1].getAttribute("aria-pressed")).toBe("true");
    expect(buttons[0].getAttribute("aria-pressed")).toBe("false");
    expect(el.textContent).toContain("Fixed 0–100% scale");
    expect(el.querySelector("svg").getAttribute("aria-label")).toContain("percentages");
    expect([...el.querySelectorAll("[data-code]")].map(n => n.getAttribute("transform"))).toEqual(before);
    expect(cell(el, "A", "2024-01").querySelector("rect").getAttribute("fill")).not.toBe(original);
    expect(cell(el, "A", "2024-02").querySelector("circle")).not.toBeNull();
    buttons[0].click();
    expect(cell(el, "A", "2024-01").querySelector("rect").getAttribute("fill")).toBe(original);
  });
  it("uses a fixed percentage scale across datasets, supports columns and static SVG", () => {
    const a = tufteMonthlyHeatmap(rows, {mode: "percent", toggle: false});
    const b = tufteMonthlyHeatmap([
      {diagnosis: "A", when: "2024-01-01", visit: 1},
      {diagnosis: "B", when: "2024-01-02", visit: 2},
    ], {code: "diagnosis", date: "when", encounter: "visit", mode: "percent", toggle: false});
    expect(a.tagName.toLowerCase()).toBe("svg");
    expect(cell(a, "A", "2024-01").querySelector("rect").getAttribute("fill"))
      .toBe(cell(b, "A", "2024-01").querySelector("rect").getAttribute("fill"));
  });
  it("orders earliest codes at bottom with deterministic ties, independent of input order", () => {
    const svg = tufteMonthlyHeatmap([...rows].reverse(), {toggle: false});
    const y = code => +cell(svg, code, "2024-01").getAttribute("transform").match(/,(\d+)\)/)[1];
    expect(y("A")).toBeGreaterThan(y("B"));
  });
  it("handles UTC boundaries, empty and singleton data and rejects invalid options", () => {
    expect(tufteMonthlyHeatmap([]).textContent).toContain("No recorded contacts");
    const svg = tufteMonthlyHeatmap([{code:"A", enc:0, date:"2024-02-01T00:30:00+01:00"}], {toggle:false});
    expect(cell(svg, "A", "2024-01")).not.toBeNull();
    expect(svg.outerHTML).not.toMatch(/NaN|undefined/);
    for (const date of [null, "", "invalid"]) expect(() => tufteMonthlyHeatmap([{code:"A", enc:1, date}])).toThrow();
    expect(() => tufteMonthlyHeatmap(rows, {mode:"bad"})).toThrow();
    expect(() => tufteMonthlyHeatmap(rows, {width:NaN})).toThrow();
  });
});
