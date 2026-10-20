// @vitest-environment jsdom
import {describe, it, expect} from "vitest";
import {DataFrame} from "../src/index.js";
import {tufteJourney, tufteMonthlyHeatmap} from "../src/plots/index.js";
const e = (code, day, enc = `${code}-${day}`, setting = "Office") => ({code, date: `2024-01-${String(day).padStart(2,"0")}`, enc, setting});
const parent = (svg, code) => svg.querySelector(`[data-child="${code}"]`)?.getAttribute("data-parent");

describe("tufteJourney", () => {
  it("accepts DataFrames, custom columns and labels without mutating input", () => {
    const data = [{dx:"A", when:"2024-01-01", visit:0, place:"ER"}];
    const before = JSON.stringify(data);
    const svg = tufteJourney(DataFrame.fromRows(data), {code:"dx", date:"when", encounter:"visit", setting:"place", meta:new Map([["A", {label:"A label"}]])});
    expect(svg.tagName.toLowerCase()).toBe("svg");
    expect(svg.textContent).toContain("A label");
    expect(svg.querySelector('[data-code="A"] circle')).not.toBeNull();
    expect(JSON.stringify(data)).toBe(before);
  });
  it("deduplicates encounters at the earliest date while retaining acute status", () => {
    const svg = tufteJourney([e("A",3,"same"),e("A",4,"same","inpatient"),e("A",5,"other")]);
    expect(svg.querySelectorAll("[data-encounter]")).toHaveLength(2);
    const mark = svg.querySelector('[data-encounter="same"]');
    expect(mark.querySelector("circle")).not.toBeNull();
    expect(mark.textContent).toContain("Jan 03, 2024");
  });
  it("prefers block then chapter then onset encounter, choosing the most recent earlier code", () => {
    const rows=[e("A",1),e("B",2),e("C",3),e("D",4),e("E",5),e("E",6,"shared"),e("F",6,"shared")];
    const meta=new Map([["A",{block:"one",chapter:"x"}],["B",{block:"one",chapter:"x"}],["C",{block:"two",chapter:"x"}],["D",{block:"one",chapter:"x"}],["E",{chapter:"x"}]]);
    const svg=tufteJourney(rows,{meta});
    expect(parent(svg,"D")).toBe("B");
    expect(parent(svg,"E")).toBe("D");
    expect(parent(svg,"F")).toBe("E");
  });
  it("does not invent parents from missing metadata, unrelated codes or simultaneous onsets", () => {
    expect(tufteJourney([e("A",1),e("B",2)]).querySelectorAll("path")).toHaveLength(0);
    expect(tufteJourney([e("A",1,"shared"),e("B",1,"shared")],{meta:new Map([["A",{block:"x"}],["B",{block:"x"}]])}).querySelectorAll("path")).toHaveLength(0);
  });
  it("anchors branches within the parent span and preserves heatmap row order", () => {
    const rows=[e("A",1),e("A",2),e("B",8),e("C",8)];
    const svg=tufteJourney(rows,{meta:new Map([["A",{chapter:"x"}],["B",{chapter:"x"}]])});
    const lane=svg.querySelector('[data-lane="A"]');
    const start=+svg.querySelector('path').getAttribute('d').match(/^M([^,]+)/)[1];
    expect(start).toBeGreaterThanOrEqual(+lane.getAttribute('x1'));
    expect(start).toBeLessThanOrEqual(+lane.getAttribute('x2'));
    const heat=tufteMonthlyHeatmap(rows,{toggle:false});
    const getY=node=>+node.getAttribute('transform').match(/,([^)]*)/)[1];
    for (const code of ['A','B','C']) expect(getY(svg.querySelector(`[data-code="${code}"]`))).toBe(getY(heat.querySelector(`[data-code="${code}"]`)));
  });
  it("handles empty and singleton inputs with finite geometry and one date label", () => {
    expect(tufteJourney([]).textContent).toContain("No recorded contacts");
    const svg=tufteJourney([e("A",1)]);
    expect(svg.outerHTML).not.toMatch(/NaN|undefined/);
    expect([...svg.querySelectorAll("text")].filter(n=>n.textContent==='01 Jan 2024')).toHaveLength(1);
    expect(svg.getAttribute('aria-label')).toContain('not clinical progression');
  });
  it("rejects invalid dates, missing identifiers and invalid dimensions", () => {
    for (const date of [null,"","bad"]) expect(()=>tufteJourney([{...e('A',1),date}])).toThrow();
    expect(()=>tufteJourney([{...e('A',1),enc:null}])).toThrow();
    expect(()=>tufteJourney([e('A',1)],{width:Infinity})).toThrow();
  });
});
