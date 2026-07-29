# observable-dataframe

A columnar, expression-based DataFrame for the browser, built for Observable
Framework. Polars-style expressions over typed-array storage, plus the
extension modules that turn frames into analyses: statistics and causal
inference, plot primitives, and page layouts. ODE dynamics and business
modeling build on it as separate packages. Data science in the browser, no
server harmed.

```js
import { DataFrame, col } from "observable-dataframe";

const summary = DataFrame.fromRows(rows)
  .filter(col("age").gt(25).and(col("city").isIn(["NYC", "SF"])))
  .withColumns(col("income").div(12).round(0).alias("monthly"))
  .groupBy("city")
  .agg(col("income").mean().alias("avg_income"), col("id").count().alias("n"))
  .sort("avg_income", { descending: true });

Plot.plot({ marks: [Plot.barY(summary.toRows(), { x: "city", y: "avg_income" })] });
```

## Why

- **Columnar storage.** Numbers live in `Float64Array`/`Int32Array`, strings
  are dictionary-encoded (each unique string stored once), dates are epoch
  milliseconds, and nulls live in a validity mask instead of pretending to be
  `NaN` or `""`.
- **Expression engine.** `col("age").gt(25)` builds a tree; the engine
  executes it in one vectorized pass. Filters produce index vectors, groupbys
  compute group ids once and reuse them for every aggregation, joins hash on
  dictionary codes. No row objects until `toRows()`, the deliberate exit ramp
  to Observable Plot and `Inputs.table`.
- **Measured, not asserted.** `npm run bench` compares against Arquero.
  Grouped aggregation runs roughly 4-13x faster at 100k-1M rows; filters,
  sorts, and joins are competitive. Numbers, machine, and methodology in the
  docs.

## Install

```bash
npm install observable-dataframe
```

The core has one dependency (`d3-dsv`) and works out of the box. The `/plots`
and `/layouts` subpaths render figures and HTML, so they need the Observable
rendering stack, which is declared as optional peer dependencies and must be
installed by you:

```bash
npm install @observablehq/plot htl d3        # required for /plots and /layouts
npm install @observablehq/inputs             # optional, for table formatters
```

The package is ESM-only and ships TypeScript declarations generated from the
JSDoc.

## The pieces

| Import | Contents |
|---|---|
| `observable-dataframe` | `DataFrame`, `Column`, `col`, `lit`, `when`, `fromCSV`/`toCSV` |
| `observable-dataframe/stats` | t-tests, `ols`, `ancova`, `fitOLS` (HC1/cluster SEs), the DiD family (`did`, `twfe`, `eventStudy`, `callawaySantAnna`), `powerAnalysis`, `tableOne`, `DistP` Monte Carlo distributions, `kde`, stepped-wedge machinery, the `ExperimentDesign` system |
| `observable-dataframe/plots` | ~25 plot primitives, `(data, options) => figure`; see the tour below |
| `observable-dataframe/layouts` | `injectPageStyle`, `splitPanel`, `tabPanel`, `kpiCard`, table formatters for `Inputs.table` |
| `observable-dataframe/data` | member-month claims slice helpers for the docs panel |

Every example below that touches the core or stats modules runs as written;
paste it into a page or a Node script and it works.

## Quick start

```js
import { DataFrame, col } from "observable-dataframe";

const members = DataFrame.fromRows([
  { name: "Alice", age: 34, city: "NYC", income: 91000 },
  { name: "Bob", age: 27, city: "SF", income: 78000 },
  { name: "Carol", age: 41, city: "NYC", income: 122000 },
  { name: "Dave", age: 29, city: "SF", income: 74000 },
  { name: "Eve", age: 52, city: "CHI", income: 101000 },
]);

members
  .filter(col("age").gt(28))
  .groupBy("city")
  .agg(col("income").mean().alias("avg_income"), col("name").count().alias("n"))
  .sort("avg_income", { descending: true })
  .toRows();
// [{ city: "NYC", avg_income: 106500, n: 2 }, { city: "CHI", ... }, ...]
```

## Core tour

### Expressions

Arithmetic, comparison, boolean logic, null handling, string and date
namespaces, conditionals, aggregations, and window operations, all composable
and all evaluated in vectorized passes:

```js
import { DataFrame, col, lit, when } from "observable-dataframe";

const df = DataFrame.fromRows([
  { name: "Alice", income: 91000, joined: new Date("2021-03-15") },
  { name: "Bob", income: null, joined: new Date("2022-07-01") },
]);

df.withColumns(
  col("income").fillNull(0).div(12).round(0).alias("monthly"),
  when(col("income").gt(80000)).then(lit("high")).otherwise(lit("standard")).alias("band"),
  col("name").str.upper().alias("shout"),
  col("joined").dt.year().alias("cohort_year")
);
```

The full expression menu: arithmetic (`add/sub/mul/div/mod/pow/neg/abs/log/exp/sqrt/round`),
comparison (`gt/gte/lt/lte/eq/neq`), boolean (`and/or/not`), nulls
(`isNull/isNotNull/fillNull`), membership (`isIn/between`), strings
(`.str.lower/upper/contains/startsWith/endsWith/replace/strip/slice/len`),
dates (`.dt.year/month/day/weekday/hour`), aggregations
(`sum/mean/min/max/count/nUnique/first/last/median/std/var/quantile/implode`),
windows (`shift/cumSum/diff/rollingMean`), and `when(cond).then(a).otherwise(b)`.
The escape hatches are `col("x").map(fn)` for element-wise JS and
`df.filter(row => ...)` for row predicates, both documented as the slow lane
they are.

### Joins

Hash joins in all four flavors, keyed on shared names or `leftOn`/`rightOn`:

```js
const claims = DataFrame.fromRows([
  { member_id: 1, paid: 1200 },
  { member_id: 1, paid: 300 },
  { member_id: 2, paid: 950 },
]);
const plans = DataFrame.fromRows([
  { member_id: 1, book: "PPO" },
  { member_id: 2, book: "HMO" },
  { member_id: 3, book: "POS" },
]);

claims.join(plans, { on: "member_id", how: "left" });
// inner | left | right | outer; name collisions on the right get a suffix
```

### Reshaping: pivot and melt

```js
const long = DataFrame.fromRows([
  { year: 2023, book: "PPO", pmpm: 412 },
  { year: 2023, book: "HMO", pmpm: 380 },
  { year: 2024, book: "PPO", pmpm: 431 },
  { year: 2024, book: "HMO", pmpm: 395 },
]);

const wide = long.pivot({ index: "year", columns: "book", values: "pmpm" });
// [{ year: 2023, PPO: 412, HMO: 380 }, { year: 2024, PPO: 431, HMO: 395 }]

wide.melt({ idVars: ["year"], variableName: "book", valueName: "pmpm" });
// back to long, which is the shape Observable Plot usually wants anyway
```

### Time windows: groupByDynamic

The polars `group_by_dynamic`: bucket a date (or numeric) index into stepped
or overlapping windows and aggregate per window. Calendar durations
(`"1mo"`, `"1q"`, `"1y"`) respect month boundaries in UTC; fixed durations
(`"7d"`, `"12h"`) are exact milliseconds. This is the panel-builder's
workhorse: claims in, member-months out.

```js
const events = DataFrame.fromRows([
  { person_id: "a", date: new Date("2024-01-05"), paid: 100 },
  { person_id: "a", date: new Date("2024-01-20"), paid: 250 },
  { person_id: "a", date: new Date("2024-02-11"), paid: 90 },
  { person_id: "b", date: new Date("2024-01-09"), paid: 400 },
]);

events
  .groupByDynamic({ indexColumn: "date", every: "1mo", by: ["person_id"] })
  .agg(col("paid").sum().alias("paid"), col("paid").count().alias("claims"));
// one row per (person, month), window starts in the date column
```

### CSV in, CSV out

Per-column type inference with a `dtypes` override for the columns inference
would get wrong (the zip-code defense). Empty cells become nulls, not empty
strings.

```js
import { fromCSV, toCSV } from "observable-dataframe";

const df = fromCSV("name,age,zip\nAlice,34,02134\nBob,,10001", { dtypes: { zip: "str" } });
df.dtypes;      // { name: "str", age: "f64", zip: "str" }
df.row(1).age;  // null, a real null
toCSV(df);      // round trips, nulls and all
```

`fromCSVUrl(url)` is the fetch-then-parse convenience for
`FileAttachment(...).url()` in Observable pages.

### Column access without hazards

`getColumn(name)` returns the typed Column: `dtype`, `data`, `validity`, and
`dict` are public read-only storage for zero-copy work. When you want logical
values rather than storage, `values()` is the sanctioned accessor: strings
decoded from the dictionary, `Date` objects for date columns, null where the
validity mask says so.

```js
const g = df.getColumn("name");
g.values();                       // ["Alice", "Bob"]
g.values({ validOnly: true });    // nulls skipped, what every statistic wants
```

Also on the frame: `describe()`, `valueCounts(name)`, `corr(a, b)`,
`corrMatrix()`, `unique()`, `dropNulls()`, `head/tail/slice`, `rename`,
`drop`, `concat`, and the exits `toRows()` / `toColumns()`.

## /stats: tests, regression, causal inference, power

Every function accepts a DataFrame or plain rows.

```js
import { welchTTest, ols, did, powerAnalysis, tableOne } from "observable-dataframe/stats";

// Hypothesis test: Welch's t, the one that doesn't assume equal variances
const t = welchTTest(rows, "pmpm", "arm", "control", "treatment");
// { mean1, mean2, tValue, pValue, df, n1, n2 }

// Regression with inference
const m = ols(rows, { dependentVar: "pmpm", predictors: ["age"] });
// { beta, se, tStats, pValues, r2, rss, ... }

// Difference-in-differences: the classic 2x2, HC1 or clustered SEs
const r = did(panel, { outcome: "outcome", treatment: "treated", time: "post" });
r.att;        // the treated x post interaction, i.e. the coefficient the meeting is about
r.summary();  // printable estimator summary

// Power: base rate, behavior change, attributable fraction -> n per arm
const power = powerAnalysis({
  baseRate: 0.19,
  behaviorChange: 0.0063,
  design: "one-sided-proportions",
  alpha: 0.1,
  power: 0.8,
});
power.nPerArm;  // 35400; the number that ends the underpowered-pilot conversation

// Table 1: baseline characteristics by arm, with the tests reviewers expect
const t1 = tableOne(rows, { by: "arm", continuous: ["age", "pmpm"], categorical: ["sex"] });
t1.rows;  // ready for Inputs.table
```

The panel/causal set continues with `twfe` (two-way fixed effects via within
transformation), `eventStudy` (period-specific effects around adoption),
`callawaySantAnna` (group-time ATT for staggered adoption, with bootstrap
SEs), `checkParallelTrends`, and `placeboTest`. Beyond those:
`sampleSizeTwoProportions`, `sampleSizeTwoMeans`, `evaluateCadence`,
stepped-wedge design matrices and `varthetaM`, `kde` with Silverman
bandwidth, `DistP` Monte Carlo distributions, and the unified
`ExperimentDesign` system (panel-measured strata, CUPED/Bayesian/simulation
power methods, channel cascades, multi-arm adjustment, Monte Carlo
self-validation).

## /plots: figures with a house style

Every primitive is `(data, options) => figure`, where data is a DataFrame or
plain rows. These render DOM, so they run in Observable pages (or jsdom),
and they need the optional peers installed.

```js
import { summaryTable, distPlot, tufteForestPlot, experimentDesignTree } from "observable-dataframe/plots";

// Per-column EDA: dtype, mini distribution, missingness, summary stats.
// The recommended first move with any new dataset.
summaryTable(df);

// Tufte-styled distribution with statistical markers
distPlot(df, { column: "pmpm", markers: ["mean", "median", 0.25, 0.75] });

// Publication forest plot with p-value annotations and broken axes
tufteForestPlot(effects, { estimate: "att", lo: "ci_lo", hi: "ci_hi", label: "cohort" });

// Cohorts -> event rates -> power -> n per arm in one H-tree,
// driven by stats.powerAnalysis
experimentDesignTree({ baseRate: 0.19, behaviorChange: 0.0063 });
```

The catalog: `summaryTable`, `distPlot`, `corrPlot`, `boxPlot`,
`twoGroupBoxPlot`, `facetedDensityPlot`, `dotPlot`, `bumpChart`,
`tufteLine`, `timeline`, `serpentineTimeline`, `waterfallPlot`,
`funnelChart`, `trapezoidFunnel`, `sankeyFlow`, `treeExplore`,
`forestPlot`, `tufteForestPlot` (with `withDownloadButtons` for SVG/PNG
export), `pictogramFill`, `consortDiagram`, `designMatrixPlot`,
`experimentDesignTree`, `powerTable`, `measurementTimeline`, and the
regression figures `didPlot` / `eventStudyPlot`, which consume estimator
output from `/stats` directly. Live examples of each are in the docs gallery
(`npm run dev`, then Figures, then Plot gallery).

## /layouts: page scaffolding

Layout helpers for Observable pages: `injectPageStyle()` applies the house
typography, `splitPanel` / `stackPanel` / `tabPanel` arrange content,
`kpiCard` and `cardRow` build the numbers row, and the table formatters
(`sparkbar`, `formatStatus`, and friends) plug into `Inputs.table` columns.

```js
import { injectPageStyle, splitPanel, kpiCard } from "observable-dataframe/layouts";
injectPageStyle();
splitPanel(leftFigure, rightProse, { ratio: 0.6 });
```

## /data: the claims panel

Helpers for the member-month claims slice used throughout the docs:
`claimsSliceFromCSV(text)` parses the panel with correct dtypes,
`enrolledMemberMonths(df)` filters to enrolled rows, `memberRollup(df)` and
`monthlyTrend(df)` aggregate per member and per month. The panel the docs read
is produced by a data loader that emits a synthetic slice of the right shape
when no local sample is present, so the docs site previews with no extract.

## Built to be built on

The core is designed as a foundation for downstream packages, the way the
stats and plots modules here are built on it. The rules a
third-party extension can rely on are documented in the extension contract
([docs/api/extension-contract.md](docs/api/extension-contract.md)): the
Column physical layout (`data`, `validity`, `dict`) is public read-only, the
expression node schema and evaluator entry points are stable API, and
`Column.values()` is the sanctioned logical accessor. Underscore-prefixed
fields are internal.

**One API, one spelling.** There are no pandas aliases: `groupBy`, not
`groupby`; `sort`, not `sort_values`; `withColumns`, not `with_columns`;
`toRows`, not `to_data`.

## Development

```bash
npm install
npm test              # vitest: core, stats, plot smoke tests
npm run bench         # observable-dataframe vs Arquero
npm run build:types   # TypeScript declarations from JSDoc
npm run docs:dev      # live docs site (Observable Framework)
```

The docs site (`docs/`) publishes the landing page, the API reference, and
the plot gallery; the guide pages (the journey, module catalog, data panel,
experiment design, DiD walkthrough, benchmarks) remain in the repo as
drafts. API references for every module live in `docs/api/`.

## Acknowledgments

The panel-data/DiD section (`did`, `twfe`, `eventStudy`, `callawaySantAnna`,
diagnostics) is modeled on
[diff-diff](https://github.com/igerber/diff-diff) by Isaac Gerber and
contributors; our 2x2 reproduces their published quick-start output digit
for digit, and their estimator taxonomy and practitioner workflow shaped
this API. For estimators beyond our core four (Synthetic DiD, Honest DiD,
Sun-Abraham, survey designs), use the original.

## License

Apache-2.0. See [LICENSE](LICENSE).
