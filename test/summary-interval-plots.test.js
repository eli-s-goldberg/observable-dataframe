// @vitest-environment jsdom
import { describe, it, expect } from "vitest";
import { DataFrame } from "../src/index.js";
import { summaryBoxPlot, intervalTimeline, forestPlot } from "../src/plots/index.js";

describe("summaryBoxPlot", () => {
  const row = { position: 1, lower: -7, q1: 1, median: 2, q3: 3, upper: 13, estimate: 4 };
  it("uses supplied endpoints rather than 1.5-IQR fences and keeps estimate separate", () => {
    const data = DataFrame.fromRows([row]);
    const p = summaryBoxPlot(data, { estimate: "estimate", tip: false, y: {domain: [-10, 15]} });
    const rule = p.querySelector('[aria-label="rule"] line');
    expect(+rule.getAttribute("y1")).toBeCloseTo(p.scale("y").apply(row.lower));
    expect(+rule.getAttribute("y2")).toBeCloseTo(p.scale("y").apply(row.upper));
    const point = p.querySelector('[aria-label="dot"] circle');
    expect(+point.getAttribute("cy")).toBeCloseTo(p.scale("y").apply(row.estimate));
    expect(data.toRows()).toEqual([row]);
  });
  it("preserves zero-width intervals and estimates outside the interval", () => {
    expect(summaryBoxPlot([{...row, lower: 2, q1: 2, median: 2, q3: 2, upper: 2}], {estimate: "estimate", tip: false})).toBeTruthy();
  });
  it("rejects invalid or unordered summaries without dropping rows", () => {
    for (const patch of [{lower: null}, {q1: NaN}, {upper: Infinity}, {median: 5}, {position: "1"}])
      expect(() => summaryBoxPlot([{...row, ...patch}])).toThrow(/row 0/);
    expect(() => summaryBoxPlot([row], {boxWidth: 0})).toThrow(/boxWidth/);
  });
});

describe("intervalTimeline", () => {
  it("preserves negative baseline time, a zero milestone, and the exclusive end", () => {
    const p = intervalTimeline(DataFrame.fromRows([
      {row: "Baseline", start: -365, end: 0},
      {row: "Index", start: 0, end: 0},
      {row: "Follow-up", start: 1, end: 365},
    ]), {tip: false, x: {domain: [-365, 365]}});
    const bars = [...p.querySelectorAll('[aria-label="bar"] rect')];
    expect(bars).toHaveLength(2);
    expect(+bars[0].getAttribute("x")).toBeCloseTo(p.scale("x").apply(-365));
    expect(+bars[1].getAttribute("x") + +bars[1].getAttribute("width")).toBeCloseTo(p.scale("x").apply(365));
    expect(p.querySelectorAll('[aria-label="dot"] path')).toHaveLength(1);
  });
  it("uses UTC dates without mutating the supplied dates", () => {
    const a = new Date("2020-02-28T00:00:00Z"), b = new Date("2020-03-01T00:00:00Z");
    const p = intervalTimeline([{row:"Leap year", start:a, end:b}], {tip:false});
    expect(p.scale("x").type).toBe("utc");
    expect(+b - +a).toBe(2 * 86400000);
  });
  it("refuses reversed, missing, nonfinite or mixed-type endpoints", () => {
    for (const r of [{start:2,end:1}, {start:null,end:1}, {start:0,end:Infinity},
      {start:new Date("invalid"),end:new Date()}, {start:0,end:new Date()}])
      expect(() => intervalTimeline([{row:"A",...r}])).toThrow(/intervalTimeline/);
  });
});

it("forestPlot can mark the ratio null at one on a log scale and apply a series color", () => {
  const data = [{name:"Group", value:2, low:0.5, high:4}];
  const options = {category:"name", value:"value", lower:"low", upper:"high", tip:false,
    reference:1, stroke:"#137f88", x:{type:"log", domain:[0.25,8]}};
  const p = forestPlot(data, options);
  const rule = p.querySelector('[aria-label="rule"] line');
  expect(+rule.getAttribute("x1")).toBeCloseTo(p.scale("x").apply(1));
  expect(p.querySelector('[aria-label="dot"]').getAttribute("fill")).toBe("#137f88");
  expect(forestPlot(data,{...options,reference:null}).querySelector('[aria-label="rule"]')).toBeNull();
});
