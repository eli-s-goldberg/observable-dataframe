---
title: Module catalog
---

# Module catalog

Every export path, one motivating case, one entry point. The **DataFrame** is
the primitive; everything else produces, transforms, or visualizes DataFrames.

## `observable-dataframe` — Core

**Case:** You have member-month claims rows and need grouped PMPM without
shipping data to a server.

| Piece | Motivating case | Guide |
|-------|-----------------|-------|
| `DataFrame` | Columnar storage; polars-style `filter` / `groupBy` / `join` | [Tour](./dataframe) · [API](./api/dataframe) |
| `col` / `lit` / `when` | Describe work once, execute vectorized | [API](./api/expressions) |
| `Column` | Typed arrays + null mask; dictionary strings | [API](./api/column-io) |
| `fromCSV` / `toCSV` | Load the claims slice; export results | [API](./api/column-io) |
| `groupByDynamic` | Member-month → calendar month / quarter rollups | [DataFrame tour](./dataframe#temporal-aggregation-group-by-dynamic) |

## `observable-dataframe/data` — Healthcare primitives

**Case:** Real-shaped eligibility + medical + pharmacy joined at `person_id`,
small enough for the browser, honest enough for method development.

| Export | Motivating case | Guide |
|--------|-----------------|-------|
| `claimsSliceFromCSV` | Member-month panel as typed DataFrame | [Data panel](./data-panel) |
| `memberRollup` | Who drives utilization in the slice? | [Data panel](./data-panel) |
| `monthlyTrend` | Cohort-level seasonality check | [Data panel](./data-panel) |

The loader emits a synthetic panel of the same shape when no local sample is
present, so preview works without any private extract.

## `observable-dataframe/stats` — Proof

**Case:** Before the study runs, stats says *could we detect it*; after it runs,
*did it happen*.

| Area | Key exports | Motivating case | Guide |
|------|-------------|-----------------|-------|
| Power | `powerAnalysis`, `sampleSizePerArm`, `varthetaM` | How many members for 80% power? | [API](./api/stats) |
| Experiment | `ExperimentDesign`, `Experiment` | One object → tree, timeline, CONSORT | [API](./api/stats) |
| Distributions | `DistP`, `distributions`, `kde` | Every input is a guess with a shape | [API](./api/stats) |
| Causal | `did`, `twfe`, `eventStudy`, `callawaySantAnna` | Staggered adoption on a member-month panel | [Statistics](./statistics) |
| Regression | `fitOLS`, `ols`, `ancova` | Adjusted comparisons with robust SEs | [Statistics](./statistics) |
| Baseline | `tableOne` | Do the arms look alike? (Table 1) | [Statistics](./statistics) |
| Tests | `welchTTest`, `twoSampleTTest` | Quick arm comparisons | [API](./api/stats) |

## `observable-dataframe/plots` — Figures

**Case:** The design and the power calculation must share one set of
numbers — linked figures, not copy-paste.

| Primitive | Motivating case | Gallery |
|-----------|-----------------|---------|
| `experimentDesignTree` | Design ↔ power ↔ sample size (live) | [Gallery](./gallery) |
| `measurementTimeline` | When operational / clinical / financial signal lands | [Gallery](./gallery) |
| `consortDiagram` | Who entered, who left, who analyzed | [Statistics](./statistics) |
| `dotPlot` | DistP / column / array → Tufte distribution | [Gallery](./gallery) |
| `waterfallPlot` | Contribution bridge (start → steps → net) | [API](./api/plots) |
| `twoGroupBoxPlot` | Group A vs group B monthly box comparison | [API](./api/plots) |
| `facetedDensityPlot` | Effect KDE by treatment dose | [API](./api/plots) |
| `didPlot` / `eventStudyPlot` | Causal estimates with CIs | [Statistics](./statistics) |
| `tufteForestPlot` | Publication forest with download | [Gallery](./gallery) |
| `summaryTable` | First look at any new DataFrame | [Getting started](./index) |

Full list: [Plot gallery](./gallery) · [API](./api/plots).

## `observable-dataframe/layouts` — Presentation

**Case:** Presentation-ready Observable Framework pages without rebuilding
chrome for every report.

| Export | Motivating case |
|--------|-----------------|
| `injectPageStyle` | Dark sidebar, numbered headings, hero blocks |
| `splitPanel` / `tabPanel` | Exhibit layout (control left, figure right) |
| `formatProsList` / `sparkbar` | `Inputs.table` cells that read like a brief |

[API](./api/layouts)

## Scripts (local, not imported)

| Script | Output |
|--------|--------|
| `npm run bench` | Core performance vs Arquero |
| `npm run docs:dev` | Docs site preview; loaders emit synthetic panels |

