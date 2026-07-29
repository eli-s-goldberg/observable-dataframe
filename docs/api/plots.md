---
toc: true
theme: [air, wide]
title: "API: plots"
---

```js
import { injectPageStyle } from "observable-dataframe/layouts";
injectPageStyle();
```

<div class="page-title-header">
  <div class="page-title">API reference: observable-dataframe/plots</div>
  <div class="divider"></div>
  Every primitive, the theme and tooltip machinery underneath, and the
  two levels of use: high-level one-liners, and low-level composition
  with raw Observable Plot when the figure needs to be yours. Live
  examples of each primitive are in the <a href="../gallery">gallery</a>;
  this page is the contract.
</div>

Figures must share one set of numbers with the business case and the power
calculation; no copy-paste between deck and notebook. Rendered examples of
every primitive: [plot gallery](../gallery).

```js
import * as Plot from "npm:@observablehq/plot";
import { DataFrame, col } from "observable-dataframe";
import * as P from "observable-dataframe/plots";
```

## The common lexicon

One options vocabulary, everywhere. Plot-based primitives spread your
options into `Plot.plot()`, so Plot's own names (`title`, `subtitle`,
`caption`, `marginLeft`, `x: {domain}`, …) all work. House additions:
`label`/`labelPosition`/`labelDigits`, `markers`, `sort`,
`break`/`xScaleLeftRange`, and `tip`.

### The tip option

`tip: false | true | (d) => string | {plot tip options}` — every
primitive. `true` gives a per-plot default; a function receives the datum.

```js echo
const df = DataFrame.fromRows(Array.from({ length: 60 }, (_, i) => ({
  x: i, y: Math.sin(i / 8) * 10 + i / 4, g: i % 2 ? "a" : "b",
})));
display(P.tufteLine(df, { x: "x", y: "y", stroke: "g", height: 200,
  tip: (d) => `${d.g} at x=${d.x}: ${d.y.toFixed(1)}` }));
```

## Theme & utilities

### colors / fonts / typography / tufteAxis / plotDefaults

The tokens every figure draws from: semantic palette (operational teal,
clinical red, financial amber, navy structure, yellow highlight), the
house sans/serif, the five-size type scale, and the Tufte axis fragment
(`{ticks: 4, tickSize: 4, grid: false}`) for spreading into scales.

```js echo
({ operational: P.colors.operational, clinical: P.colors.clinical,
   typeScale: P.typography, tufteAxis: P.tufteAxis })
```

### resolveTip(tip, defaultTitle) / createTooltip() / tipHTML(fields)

The tooltip machinery, exported for building your own primitives:
`resolveTip` maps the lexicon's tip option onto Plot mark options;
`createTooltip` is the shared floating div the D3 primitives use;
`tipHTML` typesets a label/value grid.

```js echo
P.resolveTip((d) => d.name)
```

### asRows(data) / fmtK(n) / fmtPct(p, digits)

DataFrame-or-rows normalization and the slide-format numbers.

```js echo
[P.fmtK(24000), P.fmtPct(0.196, 1)]
```

## Distribution & correlation

### distPlot(data, options)

Tufte-styled distributions: DistP, arrays, or DataFrame + `column`.
`kind: "hist" | "kde"` (Silverman bandwidth, seaborn `cut`), `markers`
(`"mean"`, `"median"`, percentiles in (0,1)) as labeled baseline ticks
with collision handling (`labelCollision: "stack" | "justify" | "none"`).

```js echo
display(P.distPlot(df, { column: "y", kind: "kde", markers: ["mean", 0.25, 0.75], height: 200 }));
```

### corrPlot(data, options)

Correlation heatmap; calls `corrMatrix()` on a DataFrame for you. Square
cells by default; explicit `height` opts out.

```js echo
display(P.corrPlot(df.drop("g"), { width: 220 }));
```

### boxPlot(data, {x, y, variant, showOutliers, tip})

Tufte quartile plot by default (whisker-gap-dot, outliers as faint dots,
five-number tooltip); `variant: "box"` restores the classic.

```js echo
display(P.boxPlot(df, { x: "g", y: "y", width: 300, height: 200 }));
```

### dotPlot(data, {x, y, fill, interval, countValues, countField, r, tip})

Stacked dot plot for dense event data (`Plot.stackY2`). Subsample with
`interval`; annotate category totals on the right with `countValues` +
`countField`.

```js echo
const pts = df.toRows().map((r, i) => ({ ...r, lane: ["a", "b", "c"][i % 3] }));
display(P.dotPlot(pts, { x: "x", y: "city", fill: "lane", width: 360, height: 220 }));
```

## Comparison & flow

### forestPlot(data, {category, value, lower, upper, pValue, pLabel, sort, tip})

Plot-based dot-and-whisker CIs; significance coloring; p-annotations with
`pLabel` styling control and an auto-sized right margin.

### tufteForestPlot(data, options)

The D3 publication version: zebra striping, relative-effect annotations,
optional broken x-axis (`break: true` with panel ranges), hover tips.
`withDownloadButtons(plotFn, data, options)` wraps any SVG plot with
SVG/PNG export.

### funnelChart(data, {group, value, showRates, tip}) / trapezoidFunnel(data, options)

The honest bar funnel (labels hop outside narrow bars) and the classic
centered trapezoid with contrast-checked labels.

### sankeyFlow(nested, config) / nestFromFrame(df, {levels, buckets})

The animated particle Sankey with live counters; `nestFromFrame` builds
its tree from a DataFrame. `animate: false` for static rendering.

### bumpChart(data, {x, y, z, topN, labelEnds, tip})

Rank trajectories; ranks computed per period, rank-prefixed end labels,
margins sized to the longest name.

```js echo
display(P.bumpChart(
  ["Q1", "Q2", "Q3"].flatMap((period, t) => [
    { period, series: "Imaging", value: 40 - t * 6 },
    { period, series: "Pharmacy", value: 30 + t * 8 },
    { period, series: "Inpatient", value: 35 + t },
  ]),
  { x: "period", y: "value", z: "series", width: 480 }
));
```

## Time & structure

### tufteLine(data, options) / tufteLineMarks(data, options)

The gap-dot line. `tufteLineMarks` returns bare marks for low-level
composition — the pattern for building your own figure on our styling:

```js echo
// Low level: our marks inside your Plot, alongside your own annotations
display(Plot.plot({
  height: 220,
  y: { label: "score", ...P.tufteAxis },
  marks: [
    Plot.ruleY([15], { stroke: P.colors.clinical, strokeDasharray: "4,3" }),
    Plot.text([{ x: 2, y: 15 }], { x: "x", y: "y", text: ["target"], dy: -8, fill: P.colors.clinical, fontSize: P.typography.annotation }),
    P.tufteLineMarks(df, { x: "x", y: "y", stroke: "g" }),
  ],
}));
```

### timeline(data, options) / serpentineTimeline(data, options)

Minimal labeled events, and the S-curve program timeline with tooltips,
"Now" marker, and per-phase styling (returns `{node, cleanup}`).

### designMatrixPlot(rows, options)

Trial layouts (from `stats.designMatrixData`) as Plot.cell staircases.

### treeExplore(data, {levels, metrics, width})

Collapsible drill-down tree with a configurable metrics panel
(count/sum/rate/custom per node), hover branch tracing.

### pictogramFill({pathData, fillLevel, ...})

Any silhouette as a percentage gauge; returns `{node, setFillLevel}`.

## Tables & summaries

### summaryTable(df, {label, width})

Per-column EDA: dtype, mini distribution, missingness, summary stats.

```js echo
display(P.summaryTable(df, { label: "Running example" }));
```

## Experiment & causal figures

These consume the stats module's output directly: pass `did()` or
`eventStudy()` results straight in, or drive the design tree from
`stats.powerAnalysis` inputs.

### experimentDesignTree(config) / powerTable(cohort, inputs)

The power H-tree (population → cohorts → arms with P1/P2/n/months) and
its auditable parameter-walk table; both run on `stats.powerAnalysis`,
and both accept `Experiment.toDesignTree()` output.

### measurementTimeline(config)

The operational/clinical/financial impact Gantt; accepts
`Experiment.toTimeline()`.

### consortDiagram(config)

Publication-grade participant flow; accepts `Experiment.toConsort()`.

### didPlot(rows, {outcome, treatment, time})

The four-means 2×2 picture: group lines, dashed counterfactual, ATT
bracket, arithmetic shared with `stats.did`.

### eventStudyPlot(result, options)

Event-study coefficients with CIs; accepts `eventStudy()` output or
`callawaySantAnna()` output interchangeably.

```js echo
display(P.eventStudyPlot([
  { eventTime: -2, estimate: 0.1, ci: [-0.4, 0.6] },
  { eventTime: -1, estimate: 0, ci: [0, 0], reference: true },
  { eventTime: 0, estimate: -1.8, ci: [-2.3, -1.3] },
  { eventTime: 1, estimate: -2.1, ci: [-2.6, -1.6] },
], { height: 220, yLabel: "effect" }));
```

## Writing your own primitive

The house pattern, start to finish: normalize input with `asRows`, style
with the tokens, honor the lexicon, resolve tips.

```js echo
function lollipop(data, { x, y, tip = true, width = 480, height = 200, ...options } = {}) {
  const rows = P.asRows(data);
  return Plot.plot({
    ...P.plotDefaults, width, height,
    x: { label: null, tickSize: 0 },
    y: { label: y, ...P.tufteAxis },
    marks: [
      Plot.ruleX(rows, { x, y1: 0, y2: y, stroke: P.colors.navy }),
      Plot.dot(rows, { x, y, fill: P.colors.navy, r: 4, ...P.resolveTip(tip, (d) => `${d[x]}: ${d[y]}`) }),
      Plot.ruleY([0]),
    ],
    ...options,
  });
}
display(lollipop([{ k: "a", v: 4 }, { k: "b", v: 7 }, { k: "c", v: 2 }], { x: "k", y: "v" }));
```
