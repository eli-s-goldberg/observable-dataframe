---
toc: true
theme: [air, wide]
title: Plot gallery
---

```js
import { injectPageStyle } from "observable-dataframe/layouts";
injectPageStyle();
```

<div class="page-title-header">
  <div class="page-title">Plot gallery</div>
  <div class="divider"></div>
  Every primitive takes a DataFrame or plain rows and returns a live figure.
  Options merge over sensible defaults; the theme keeps the palette honest.
</div>

## The common lexicon

Every plot speaks one options vocabulary, and it's deliberately
Observable-shaped — for the Plot-based primitives your options are spread
straight into `Plot.plot()`, so anything Plot understands
(`title`, `subtitle`, `caption`, `marginLeft`, `x: {domain}`, …) just
works. The house additions:

| Option | Meaning |
|---|---|
| `width`, `height` | pixels |
| `title`, `subtitle`, `caption` | figure-level text (Plot renders natively) |
| `label` / axis labels | axis text; column names by default |
| `labelPosition` | `"top"` / `"bottom"` / `"none"` for value & marker labels |
| `labelDigits` | decimals on numeric labels |
| `markers` | statistical marks where distributions appear |
| `break`, `xScaleLeftRange`, … | axis-break controls |
| `sort` | `"ascending"` / `"descending"` / `null` |
| `tip` | tooltips: `true` for a sensible default, `(d) => string` for your own text, or a Plot tip options object for full control |

The D3-rendered primitives (`trapezoidFunnel`, `tufteForestPlot`, …)
implement the same names by hand, including `tip`, which there takes a
function returning HTML — `tipHTML({label: value, ...})` does the
typesetting if you'd rather not.

```js
import { DataFrame, col } from "observable-dataframe";
import {
  corrPlot, distPlot, forestPlot, funnelChart, boxPlot,
  designMatrixPlot, summaryTable,
  bumpChart, trapezoidFunnel, tufteLine, sankeyFlow, nestFromFrame,
  treeExplore, tufteForestPlot, serpentineTimeline, pictogramFill,
  dotPlot,
} from "observable-dataframe/plots";
import { DistP, distributions, designMatrixData } from "observable-dataframe/stats";
```

```js
// A synthetic clinical-ish dataset shared by the examples below.
const members = DataFrame.fromRows(
  Array.from({ length: 800 }, (_, i) => {
    const risk = i % 5 === 0 ? "high" : "low";
    const age = 25 + ((i * 13) % 55);
    const visits = Math.max(0, Math.round((risk === "high" ? 7 : 3) + Math.sin(i) * 2));
    return {
      risk,
      age,
      visits,
      pmpm: Math.round(180 + age * 3 + visits * 42 + ((i * 89) % 120)),
      engaged: i % 3 === 0,
    };
  })
);
```

## summaryTable

The look-before-you-model table.

```js echo
display(summaryTable(members, { label: "Member panel" }));
```

## corrPlot

Feeds `df.corrMatrix()` to `Plot.cell` with printed coefficients.

```js echo
display(corrPlot(members, { width: 380 }));
```

## distPlot

Tufte-styled distributions for DataFrame columns, raw arrays, or `DistP`
Monte Carlo outputs: thin binned outline, faint fill, no grid, and
statistical markers as baseline ticks. The mean keeps its full-height red
rule; `markers` takes `"mean"`, `"median"`, or any percentile in (0, 1).

```js echo
display(distPlot(members, {
  column: "pmpm",
  label: "PMPM ($)",
  markers: ["mean", "median", 0.25, 0.75],
  labelDigits: 0,
}));
```

Unlabeled, outline-only, if the figure is headed somewhere dense:

```js echo
display(distPlot(members, {
  column: "pmpm", label: "PMPM ($)",
  markers: [0.1, 0.5, 0.9], labelPosition: "none", fill: false, height: 160,
}));
```

`kind: "kde"` swaps the binned counts for a Gaussian kernel density
estimate. `cut` is the seaborn convention — how many bandwidths the curve
extends past the data extremes (0 clips at the data, 3 shows the tails):

```js echo
display(distPlot(members, {
  column: "pmpm", label: "PMPM ($)",
  kind: "kde", cut: 3,
  markers: ["mean", 0.25, 0.75], labelDigits: 0,
}));
```

```js echo
const savings = new DistP({
  name: "savings per engaged member ($)",
  distfunc: distributions.lognormal,
  params: { mean: 220, shape: 0.5 },
  bounds: [0, 2000],
  size: 20000,
});
display(distPlot(savings));
```

## forestPlot

Point estimates with CI whiskers; significant rows get the clinical red.

```js echo
display(forestPlot([
  { site: "Outpatient", est: 0.031, lo: 0.012, hi: 0.050, p: 0.001 },
  { site: "Emergency", est: 0.012, lo: -0.004, hi: 0.028, p: 0.14 },
  { site: "Inpatient", est: 0.044, lo: 0.021, hi: 0.067, p: 0.0004 },
  { site: "Pharmacy", est: -0.006, lo: -0.019, hi: 0.007, p: 0.36 },
  { site: "Imaging", est: 0.019, lo: 0.002, hi: 0.036, p: 0.03 },
], { category: "site", value: "est", lower: "lo", upper: "hi", pValue: "p" }));
```

## funnelChart

Stage sizes with conversion-versus-prior labels, which is the part people
actually argue about.

```js echo
display(funnelChart([
  { group: "Eligible", value: 120000 },
  { group: "Targeted", value: 72000 },
  { group: "Reached", value: 31000 },
  { group: "Engaged", value: 8200 },
  { group: "Behavior change", value: 1100 },
]));
```

## boxPlot

Tufte's redesign: the box was most of the ink, so it's gone. Whiskers run
min→Q1 and Q3→max, the interquartile range is the *gap*, the median is the
dot — hover it for the five-number summary. Outliers beyond 1.5×IQR render
as faint dots rather than being quietly cropped. `variant: "box"` restores
the classic, if a reviewer insists.

```js echo
display(boxPlot(members, { x: "risk", y: "pmpm", width: 420 }));
```

```js echo
display(boxPlot(members, { x: "risk", y: "pmpm", width: 420, variant: "box", tip: false }));
```

## dotPlot

Stacked dots for dense event timelines: each row is one observation, dots at
the same x pile vertically via `Plot.stackY2`. Subsample with `interval` when
the series is too thick; `countValues` adds right-margin totals by category.

Stacking is arithmetic, so it needs a numeric `y` — `y: () => 1` is the
usual "count the events" case, and the axis is multiplied back up to real
counts when you subsample.

```js echo
const events = Array.from({ length: 400 }, (_, i) => ({
  month: (i % 24) + 1,
  member: `m${i % 40}`,
  lane: ["imaging", "pt", "surgery"][i % 3],
}));
display(dotPlot(events, {
  x: "month",
  y: () => 1,
  fill: "lane",
  interval: 2,
  countValues: ["imaging", "pt", "surgery"],
  countField: "lane",
  xLabel: "months from index",
  yLabel: "events",
  width: 520,
  height: 320,
}));
```

Hand it a categorical `y` instead and the dots are placed straight onto an
ordinal axis, which is what a category asked for anyway:

```js echo
display(dotPlot(events, {
  x: "month",
  y: "member",
  fill: "lane",
  interval: 4,
  xLabel: "months from index",
  yLabel: "member",
  width: 520,
  height: 320,
}));
```

## designMatrixPlot

Trial layouts as Plot.cell staircases — parallel, cross-over,
stepped-wedge. Pair with `varthetaM` from stats to put a variance number
next to each shape.

```js echo
display(designMatrixPlot(designMatrixData("Stepped-wedge", 6), { width: 460 }));
```

```js echo
display(designMatrixPlot(designMatrixData("Cross-over", 2), { width: 300 }));
```

## bumpChart

Rank trajectories over time — what mattered most, when, and what overtook it.

```js echo
const conditions = DataFrame.fromRows(
  ["Jan", "Feb", "Mar", "Apr"].flatMap((period, t) => [
    { period, series: "Falls", value: 68 - t * 8 },
    { period, series: "Pneumonia", value: 45 + t * 6 },
    { period, series: "Acute Kidney Failure", value: 62 + t },
    { period, series: "Sepsis", value: 30 + t * 12 },
  ])
);
display(bumpChart(conditions, { x: "period", y: "value", z: "series" }));
```

## trapezoidFunnel

The classic centered funnel, for meetings where a bar chart isn't what
people picture when they say "funnel". One hue darkening down the taper,
whitespace instead of borders, stage names in a fixed column, counts in
the band (or just outside it when the band gets too narrow to hold one),
and stage-to-stage conversion in its own right-hand column. Pass
`palette` if you need the bands to carry category instead.

```js echo
display(trapezoidFunnel([
  { group: "Eligible members", value: 120000 },
  { group: "Targeted", value: 72000 },
  { group: "Reached", value: 31000 },
  { group: "Engaged", value: 8200 },
], { width: 680, height: 340 }));
```

## tufteLine

Lines with the quiet gap-dot treatment: each observation breaks the line,
so the data reads as data.

```js echo
const trend = DataFrame.fromRows(
  ["Treatment", "Control"].flatMap((arm) =>
    Array.from({ length: 8 }, (_, q) => ({
      quarter: `Q${q + 1}`,
      arm,
      rate: arm === "Treatment" ? 8 + q * 1.4 : 8 + q * 0.5,
    }))
  )
);
display(tufteLine(trend, { x: "quarter", y: "rate", stroke: "arm", height: 260 }));
```

## sankeyFlow

An animated Sankey with particles and live counters. `nestFromFrame`
builds the flow tree straight from a DataFrame; press replay to run the
particles again.

```js echo
const referrals = DataFrame.fromRows([
  { region: "East", specialty: "Cardiology", preferred: 120, non_preferred: 45 },
  { region: "East", specialty: "Orthopedics", preferred: 60, non_preferred: 85 },
  { region: "West", specialty: "Cardiology", preferred: 95, non_preferred: 15 },
  { region: "West", specialty: "Orthopedics", preferred: 40, non_preferred: 30 },
]);

const flow = sankeyFlow(nestFromFrame(referrals, {
  levels: ["region", "specialty"],
  buckets: ["preferred", "non_preferred"],
}));
display(flow.node);
display(Inputs.button("Replay", { reduce: () => flow.replay() }));
```

## treeExplore

Collapsible drill-down tree with a configurable metrics panel — click a
node to expand it and see its numbers, hover to trace the branch.

```js echo
const panel = DataFrame.fromRows(
  Array.from({ length: 400 }, (_, i) => ({
    region: ["East", "West", "Central"][i % 3],
    lineOfBusiness: ["Commercial", "Medicare"][i % 2],
    clinic: `Clinic ${1 + (i % 6)}`,
    visits: 1 + (i % 9),
    nonPreferred: i % 4 === 0 ? 1 + (i % 3) : 0,
  }))
);
display(treeExplore(panel, {
  levels: ["region", "lineOfBusiness"],
  metrics: [
    { label: "Members", type: "count" },
    { label: "Total visits", type: "sum", column: "visits" },
    { label: "Non-preferred rate", type: "rate", numerator: "nonPreferred", denominator: "visits", format: "percent" },
  ],
}));
```

## tufteForestPlot

The publication forest plot — zebra striping, p-value annotations, and an
optional broken x-axis for when one outlier would otherwise squash the
interesting cluster into 40 pixels.

```js echo
display(tufteForestPlot([
  { category: "Outpatient", value: 0.021, lower: 0.008, upper: 0.034, p: 0.002, rel: 0.11 },
  { category: "Emergency", value: 0.009, lower: -0.006, upper: 0.024, p: 0.21, rel: 0.05 },
  { category: "Inpatient", value: 0.015, lower: 0.001, upper: 0.029, p: 0.04, rel: 0.08 },
  { category: "Pharmacy (outlier)", value: 0.35, lower: 0.31, upper: 0.39, p: 0.0001, rel: 0.42 },
], {
  pValue: "p", relative: "rel", sort: "ascending",
  title: "Absolute prevalence DiD by site of care",
  break: true,
  xScaleLeftRange: [-0.02, 0.06],
  xScaleRightRange: [0.3, 0.4],
}));
```

## serpentineTimeline

A program timeline that snakes across the page instead of demanding a
wall. Hover the points; the "Now" marker and dashed lead-in are options.
The figure is fluid down to the column and never drawn larger than the
`width` you asked for.

```js echo
const milestones = DataFrame.fromRows([
  { date: "2026-09-01", phase: "Design", activity: "Cohorts & power analysis", strokeColor: "#1C2B3A" },
  { date: "2026-11-01", phase: "Launch", activity: "First outreach wave", strokeColor: "#0D7377" },
  { date: "2027-03-01", phase: "Observe", activity: "Observation window", strokeColor: "#6A9BB5" },
  { date: "2027-08-01", phase: "Clinical", activity: "Endpoints measurable", strokeColor: "#A32020" },
  { date: "2027-12-01", phase: "Financial", activity: "PMPM impact in TME", strokeColor: "#B7860B" },
]);
const serpentine = serpentineTimeline(milestones, {
  width: 820, height: 440, turns: 2,
  now: new Date("2026-08-01"),
  displayLevels: ["date", "phase", "activity"],
});
display(serpentine.node);
```

## consortDiagram

Publication-grade participant flow: enrollment spine, itemized exclusion
boxes, parallel arms. Pair with `stats.tableOne` for the companion
baseline-characteristics table; this is the shape of the API.

```js echo
import { consortDiagram } from "observable-dataframe/plots";
display(consortDiagram({
  steps: [
    { label: "Assessed for eligibility", n: 1000 },
    { label: "Randomized", n: 400, excluded: { label: "Excluded", n: 600, reasons: [
      { label: "No claims activity", n: 350 },
      { label: "Enrolled under 6 months", n: 250 },
    ] } },
  ],
  arms: [
    { label: "Intervention", n: 200, steps: [{ label: "Analyzed", n: 194, excluded: { label: "Lost to follow-up", n: 6 } }] },
    { label: "Usual care", n: 200, steps: [{ label: "Analyzed", n: 200 }] },
  ],
}));
```

## pictogramFill

Fill any silhouette to a percentage. "1.5 of every 3.9" lands differently
than a bar at 38%; supply your own path data and the shape is your unit.

```js echo
const fillPct = view(Inputs.range([0, 100], { label: "Fill %", value: 38, step: 1 }));
```

```js echo
const gauge = pictogramFill({
  pathData: "M338 0 C 150 0 100 180 100 320 C 100 430 170 500 240 520 L 200 1280 L 477 1280 L 437 520 C 507 500 577 430 577 320 C 577 180 527 0 338 0 Z",
  fillLevel: fillPct,
  fillColor: "#0D7377",
  width: "140px",
  height: "260px",
});
display(gauge.node);
```
