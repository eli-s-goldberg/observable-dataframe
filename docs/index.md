---
toc: false
theme: [air, wide]
title: Getting started
---

```js
import { injectPageStyle } from "observable-dataframe/layouts";
injectPageStyle();
```

<div class="page-title-header">
  <div class="page-title">observable-dataframe</div>
  <div class="divider"></div>
  A columnar, expression-based DataFrame for the browser: polars-style syntax,
  typed-array internals, and companion modules for statistics, dynamics,
  business modeling, plotting, and page layout.
</div>

## Install

```bash
npm install observable-dataframe
```

The core has one dependency (`d3-dsv`). The `/plots` and `/layouts` subpaths
render figures and HTML, so they need the Observable rendering stack,
declared as optional peer dependencies you install yourself:

```bash
npm install @observablehq/plot htl d3
```

The package is ESM-only and ships TypeScript declarations generated from the
JSDoc.

## The thirty-second tour

Data goes in as rows or columns, gets stored as typed arrays (numbers in
`Float64Array`/`Int32Array`, strings dictionary-encoded, dates as epoch
milliseconds, nulls in a validity mask), and every operation is a vectorized
pass. No per-row objects until you explicitly ask for them at the edges.

```js echo
import { DataFrame, col } from "observable-dataframe";

const df = DataFrame.fromRows([
  { name: "Alice", age: 30, city: "NYC", income: 90000 },
  { name: "Bob", age: 25, city: "SF", income: 80000 },
  { name: "Carol", age: 35, city: "NYC", income: 120000 },
  { name: "Dave", age: 28, city: "SF", income: 75000 },
  { name: "Eve", age: null, city: "NYC", income: null },
]);

const summary = df
  .filter(col("age").isNotNull())
  .withColumns(col("income").div(12).round(0).alias("monthly"))
  .groupBy("city")
  .agg(
    col("income").mean().alias("avg_income"),
    col("age").count().alias("n"),
  )
  .sort("avg_income", { descending: true });
```

```js echo
Inputs.table(summary.toRows())
```

Expressions are the whole trick: `col("age").gt(25)` builds a description of
work, and the engine executes it in one pass over typed arrays. Filters
produce index vectors, groupbys reuse their group ids for every aggregation,
and strings are compared as dictionary codes. Window operations partition
with `.over()`: `col("paid").shift(1).over("member_id")` lags each member's
own series.

## The pieces

| Import | Contents |
|---|---|
| `observable-dataframe` | `DataFrame`, `Column`, `col`, `lit`, `when`, CSV IO |
| `observable-dataframe/stats` | t-tests, OLS, ANCOVA, the DiD family, power analysis, `tableOne`, Monte Carlo distributions |
| `observable-dataframe/plots` | ~25 plot primitives, `(data, options) => figure` |
| `observable-dataframe/layouts` | page style, split panels, KPI cards, table formatters |
| `observable-dataframe/data` | member-month claims panel helpers |

## API reference

Every method with a live example:

- [DataFrame & GroupBy](./api/dataframe): frame verbs, grouping, joins, reshaping
- [Expressions](./api/expressions): the expression system, end to end
- [Column & IO](./api/column-io): the dtype system, Column, CSV in and out
- [Statistics](./api/stats): tests, regression, DiD, power, Table 1
- [Data](./api/data): the claims panel helpers
- [Plots](./api/plots): every figure primitive
- [Layouts](./api/layouts): page scaffolding
- [Extension contract](./api/extension-contract): what downstream packages can rely on

For rendered figures, see the [plot gallery](./gallery).

## A first look at the data

`summaryTable` is the recommended first move with any new dataset: one row
per column with dtype, distribution, and missingness.

```js echo
import { DataFrame } from "observable-dataframe";
import { summaryTable } from "observable-dataframe/plots";

const flights = DataFrame.fromRows(
  Array.from({ length: 500 }, (_, i) => ({
    carrier: ["UA", "AA", "DL", "WN"][i % 4],
    delay: Math.round((Math.sin(i / 7) + 1) * 30 + (i % 11) * 4 - 10),
    distance: 200 + ((i * 37) % 2400),
    date: new Date(2024, 0, 1 + (i % 365)),
    cancelled: i % 29 === 0,
  }))
);

display(summaryTable(flights, { label: "Flights (synthetic)" }));
```
