// @vitest-environment jsdom
/**
 * plot-geometry.test.js — the tests that "it renders without throwing"
 * could never fail. Every case here is a defect that shipped: a figure
 * that produced a perfectly valid DOM node containing no marks, or drew
 * its content outside the box it told the browser to draw.
 *
 * The bar is geometric. Count the marks. Measure the extents.
 */

import { describe, it, expect } from "vitest";
import {
  dotPlot,
  consortDiagram,
  serpentineTimeline,
  pictogramFill,
  trapezoidFunnel,
} from "../src/plots/index.js";

const events = Array.from({ length: 400 }, (_, i) => ({
  month: (i % 24) + 1,
  member: `m${i % 40}`,
  lane: ["imaging", "pt", "surgery"][i % 3],
  when: new Date(2026, i % 12, 1),
}));

const dots = (el) => el.querySelectorAll('[aria-label="dot"] circle');

describe("dotPlot draws marks", () => {
  // The bug: x/y domains were computed by hand with Math.min/Math.max over
  // raw column values. A categorical column gave [NaN, NaN], every dot got
  // cy="NaN", and Plot emitted an empty figure that passed a smoke test.
  it("renders a dot per row for a categorical y", () => {
    const el = dotPlot(events, { x: "month", y: "member", fill: "lane" });
    expect(dots(el).length).toBe(events.length);
  });

  it("renders a dot per row for a temporal x", () => {
    const el = dotPlot(events, { x: "when", y: "member" });
    expect(dots(el).length).toBe(events.length);
  });

  it("stacks a numeric y instead of piling every dot on one row", () => {
    const el = dotPlot(events, { x: "month", y: () => 1 });
    const cys = [...dots(el)].map((c) => c.getAttribute("cy"));
    expect(cys.length).toBe(events.length);
    expect(cys.every((v) => Number.isFinite(Number(v)))).toBe(true);
    // 400 events over 24 months: the tallest column is well clear of one.
    expect(new Set(cys).size).toBeGreaterThan(10);
  });

  it("subsamples without dropping the marks or breaking the count axis", () => {
    const el = dotPlot(events, { x: "month", y: () => 1, interval: 4 });
    expect(dots(el).length).toBe(100);
    // ticks are multiplied back up to real counts, and stay numeric
    const ticks = [...el.querySelectorAll('[aria-label="y-axis tick label"] text')].map((t) => t.textContent);
    expect(ticks.length).toBeGreaterThan(0);
    expect(ticks.every((t) => /^\d+$/.test(t))).toBe(true);
  });

  it("keeps the zero baseline off a categorical axis", () => {
    const tickText = (el) =>
      [...el.querySelectorAll('[aria-label="y-axis tick label"] text')].map((t) => t.textContent);
    expect(tickText(dotPlot(events, { x: "month", y: "member", interval: 8 }))).not.toContain("0");
    expect(tickText(dotPlot(events, { x: "month", y: () => 1 }))).toContain("0");
  });

  it("honors explicit domains and the mirror layout", () => {
    const rows = Array.from({ length: 80 }, (_, i) => ({ age: 50 + (i % 30), side: i % 2 ? 1 : -1 }));
    const el = dotPlot(rows, { x: "age", y: "side", layout: "mirror", yDomain: [-2, 2] });
    expect(dots(el).length).toBe(rows.length);
  });

  it("prints category totals in the right margin", () => {
    const el = dotPlot(events, {
      x: "month",
      y: () => 1,
      interval: 2,
      countValues: ["imaging", "pt", "surgery"],
      countField: "lane",
    });
    expect(dots(el).length).toBe(200);
    expect(el.textContent).toContain("imaging: 134");
    expect(el.textContent).toContain("surgery: 134");
  });
});

/** Every x extent the SVG actually draws, in viewBox units. */
function drawnExtent(svg) {
  let left = Infinity;
  let right = -Infinity;
  const see = (a, b) => {
    left = Math.min(left, a);
    right = Math.max(right, b);
  };
  for (const r of svg.querySelectorAll("rect")) {
    const x = Number(r.getAttribute("x"));
    const stroke = Number(r.getAttribute("stroke-width") ?? 0) / 2;
    see(x - stroke, x + Number(r.getAttribute("width")) + stroke);
  }
  for (const l of svg.querySelectorAll("line")) {
    see(Math.min(+l.getAttribute("x1"), +l.getAttribute("x2")), Math.max(+l.getAttribute("x1"), +l.getAttribute("x2")));
  }
  return { left, right };
}

const consortConfig = {
  steps: [
    { label: "Assessed for eligibility", n: 1000 },
    {
      label: "Randomized",
      n: 400,
      excluded: {
        label: "Excluded",
        n: 600,
        reasons: [
          { label: "No claims activity", n: 350 },
          { label: "Enrolled under 6 months", n: 250 },
        ],
      },
    },
  ],
  arms: [
    { label: "Intervention", n: 200, steps: [{ label: "Analyzed", n: 194, excluded: { label: "Lost to follow-up", n: 6 } }] },
    { label: "Usual care", n: 200, steps: [{ label: "Analyzed", n: 200 }] },
  ],
};

describe("consortDiagram stays inside its own frame", () => {
  // The bug: the exclusion column was placed at a fixed offset from the
  // spine, putting its right edge 15px past a viewBox derived from `width`.
  // The "Excluded" box was clipped in half on the live gallery.
  it("draws nothing outside the viewBox", () => {
    for (const width of [600, 760, 900, 1100]) {
      const svg = consortDiagram({ ...consortConfig, width });
      const [vbX, , vbW] = svg.getAttribute("viewBox").split(/[\s,]+/).map(Number);
      const { left, right } = drawnExtent(svg);
      expect(left).toBeGreaterThanOrEqual(vbX);
      expect(right).toBeLessThanOrEqual(vbX + vbW);
    }
  });

  it("fits the requested width rather than growing past it", () => {
    for (const width of [600, 760, 900]) {
      const svg = consortDiagram({ ...consortConfig, width });
      expect(drawnExtent(svg).right).toBeLessThanOrEqual(width);
      expect(Number(svg.getAttribute("width"))).toBe(width);
    }
  });

  it("keeps the exclusion box beside the spine, not on top of it", () => {
    const svg = consortDiagram({ ...consortConfig, width: 760 });
    const boxes = [...svg.querySelectorAll("rect")].map((r) => ({
      left: Number(r.getAttribute("x")),
      right: Number(r.getAttribute("x")) + Number(r.getAttribute("width")),
    }));
    const spine = boxes.find((b) => b.right - b.left === 250);
    const exclusion = boxes.find((b) => b.left > spine.right);
    expect(exclusion).toBeDefined();
    expect(exclusion.left).toBeGreaterThan(spine.right);
    expect(exclusion.right).toBeLessThanOrEqual(760);
  });

  it("widens the viewBox rather than clipping when the content cannot fit", () => {
    // Three arms, each shedding participants, into a canvas built for two.
    const crowded = consortDiagram({
      ...consortConfig,
      width: 420,
      arms: ["Wave 1", "Wave 2", "Usual care"].map((label) => ({
        label,
        n: 100,
        steps: [{ label: "Analyzed", n: 90, excluded: { label: "Lost to follow-up", n: 10 } }],
      })),
    });
    const [vbX, , vbW] = crowded.getAttribute("viewBox").split(/[\s,]+/).map(Number);
    const { left, right } = drawnExtent(crowded);
    expect(left).toBeGreaterThanOrEqual(vbX);
    expect(right).toBeLessThanOrEqual(vbX + vbW);
    expect(crowded.getAttribute("style")).toContain("max-width: 100%");
  });
});

describe("serpentineTimeline respects the width it was given", () => {
  // The bug: the ResizeObserver multiplied every drawn size by
  // containerWidth/width, on top of the scaling the viewBox already does.
  // In a column wider than nominal the figure grew twice over.
  const milestones = [
    { date: "2026-09-01", phase: "Design", activity: "Cohorts & power analysis" },
    { date: "2027-03-01", phase: "Observe", activity: "Observation window" },
    { date: "2027-12-01", phase: "Financial", activity: "PMPM impact in TME" },
  ];

  it("caps at the requested width and scales with it", () => {
    for (const width of [640, 820, 1200]) {
      const { node, cleanup } = serpentineTimeline(milestones, { width, height: 440 });
      expect(node.style.maxWidth).toBe(`${width}px`);
      expect(node.style.width).toBe("100%");
      const svg = node.querySelector("svg");
      expect(svg.getAttribute("viewBox").split(/[\s,]+/).map(Number)[2]).toBe(width);
      // fluid down, never taller than its own aspect ratio demands
      expect(svg.style.width).toBe("100%");
      expect(svg.style.height).toBe("auto");
      cleanup();
    }
  });

  it("does not exceed a sane default footprint", () => {
    const { node, cleanup } = serpentineTimeline(milestones);
    expect(Number.parseInt(node.style.maxWidth, 10)).toBeLessThanOrEqual(1200);
    cleanup();
  });
});

describe("pictogramFill actually tracks the percentage", () => {
  // The bug: a hardcoded scale(0.1) drew the silhouette at a tenth of its
  // size in the corner of the box, while the mask covered the whole box.
  // The mask never reached the shape, so the gauge read full at every value.
  const soldier =
    "M338 0 C 150 0 100 180 100 320 C 100 430 170 500 240 520 L 200 1280 L 477 1280 L 437 520 C 507 500 577 430 577 320 C 577 180 527 0 338 0 Z";

  const maskOf = (fillLevel, options = {}) => {
    const { node } = pictogramFill({ pathData: soldier, fillLevel, ...options });
    const rect = node.querySelector("rect");
    return {
      y: Number(rect.getAttribute("y")),
      height: Number(rect.getAttribute("height")),
      x: Number(rect.getAttribute("x")),
      width: Number(rect.getAttribute("width")),
    };
  };

  it("empties monotonically as the value rises", () => {
    const heights = [0, 25, 50, 75, 90, 100].map((v) => maskOf(v).height);
    expect(new Set(heights).size).toBe(heights.length);
    for (let i = 1; i < heights.length; i++) expect(heights[i]).toBeLessThan(heights[i - 1]);
  });

  it("spans the full shape at 0% and none of it at 100%", () => {
    const empty = maskOf(0);
    const full = maskOf(100);
    expect(full.height).toBe(0);
    expect(empty.height).toBeGreaterThan(0);
    // half full is half the shape, not half the canvas
    expect(maskOf(50).height).toBeCloseTo(empty.height / 2, 6);
  });

  it("covers the silhouette rather than the whole canvas", () => {
    const { width, x } = maskOf(50);
    // the soldier spans x 100..577 of a 677-wide box: the mask tracks the
    // shape, so it is narrower than the canvas and inset from its edge
    expect(width).toBeCloseTo(477, 6);
    expect(x).toBeGreaterThan(0);
  });

  it("fits path data whatever units it arrived in", () => {
    const tenfold = soldier.replace(/\d+/g, (n) => String(Number(n) * 10));
    expect(maskOf(50, { pathData: tenfold })).toEqual(maskOf(50));
  });

  it("setFillLevel moves the mask after construction", () => {
    const { node, setFillLevel } = pictogramFill({ pathData: soldier, fillLevel: 10 });
    const rect = node.querySelector("rect");
    const before = Number(rect.getAttribute("height"));
    setFillLevel(80);
    expect(Number(rect.getAttribute("height"))).toBeLessThan(before);
  });
});

describe("trapezoidFunnel", () => {
  const stages = [
    { group: "Eligible members", value: 120000 },
    { group: "Targeted", value: 72000 },
    { group: "Reached", value: 31000 },
    { group: "Engaged", value: 8200 },
  ];
  const bandWidths = (el) =>
    [...el.querySelectorAll("path.band")].map((b) => {
      const [, x1, , x2] = b.getAttribute("d").match(/^M([-\d.]+),([-\d.]+) L([-\d.]+),/).map(Number);
      return x2 - x1;
    });

  it("draws one band per stage, narrowing all the way down", () => {
    const el = trapezoidFunnel(stages, { width: 680, height: 340 });
    const widths = bandWidths(el);
    expect(widths).toHaveLength(stages.length);
    for (let i = 1; i < widths.length; i++) expect(widths[i]).toBeLessThan(widths[i - 1]);
  });

  it("labels every stage directly, with its count and its conversion", () => {
    const text = trapezoidFunnel(stages, { width: 680, height: 340 }).textContent;
    for (const s of stages) {
      expect(text).toContain(s.group);
      expect(text).toContain(s.value.toLocaleString("en-US"));
    }
    expect(text).toContain("↓ 60%"); // 72,000 of 120,000
    expect(text).toContain("6.8% of top"); // 8,200 of 120,000
    expect(text).not.toContain("100% of top"); // the first stage is not news
  });

  it("moves a count outside its band when the band is too narrow to hold it", () => {
    const el = trapezoidFunnel(stages, { width: 680, height: 340 });
    const counts = [...el.querySelectorAll("text.count")];
    const wide = counts.find((t) => t.textContent === "120,000");
    const narrow = counts.find((t) => t.textContent === "8,200");
    expect(wide.getAttribute("text-anchor")).toBe("middle");
    expect(narrow.getAttribute("text-anchor")).toBe("start");
  });

  it("keeps chartjunk out: no strokes, gradients, or shadows on the bands", () => {
    const el = trapezoidFunnel(stages);
    for (const band of el.querySelectorAll("path.band")) {
      expect(band.getAttribute("stroke")).toBeNull();
      expect(band.getAttribute("filter")).toBeNull();
    }
    expect(el.querySelector("linearGradient")).toBeNull();
    expect(el.querySelector("defs")).toBeNull();
  });

  it("still honors an explicit palette, altLabel, and showRates", () => {
    const paletted = trapezoidFunnel(stages, { palette: ["#20B2AA", "#48D1CC", "#90EE90", "#BBF7D0"] });
    expect([...paletted.querySelectorAll("path.band")].map((b) => b.getAttribute("fill"))).toEqual([
      "#20B2AA",
      "#48D1CC",
      "#90EE90",
      "#BBF7D0",
    ]);
    const alt = trapezoidFunnel(
      stages.map((s) => ({ ...s, alt: `${s.value / 1000}K` })),
      { altLabel: "alt" }
    );
    expect(alt.textContent).toContain("120K");
    expect(trapezoidFunnel(stages, { showRates: false }).querySelectorAll("text.rate")).toHaveLength(0);
  });

  it("picks readable label colors for a palette given in rgba", () => {
    // The old contrast math sliced hex digits out of the string and got NaN
    // luminance, so every label defaulted to black, including on dark bands.
    const dark = trapezoidFunnel(stages, { palette: ["rgba(13,115,119,0.95)"] });
    const inside = [...dark.querySelectorAll("text.count")].filter((t) => t.getAttribute("text-anchor") === "middle");
    expect(inside.length).toBeGreaterThan(0);
    for (const t of inside) expect(t.style.fill).toBe("rgb(255, 255, 255)");

    const light = trapezoidFunnel(stages, { palette: ["rgb(240,240,235)"] });
    for (const t of [...light.querySelectorAll("text.count")].filter((t) => t.getAttribute("text-anchor") === "middle")) {
      expect(t.style.fill).toBe("rgb(0, 0, 0)");
    }
  });

  it("refuses an empty funnel with a real message", () => {
    expect(() => trapezoidFunnel([])).toThrow(/stages/);
  });
});
