---
toc: true
theme: [air, wide]
title: The DataFrame
---

```js
import { injectPageStyle } from "observable-dataframe/layouts";
injectPageStyle();
```

<div class="page-title-header">
  <div class="page-title">The DataFrame</div>
  <div class="divider"></div>
  Expression-based, immutable, columnar. Everything returns a new frame;
  nothing mutates; strings are integers in a trench coat.
</div>

```js echo
import { DataFrame, col, lit, when } from "observable-dataframe";
```

```js
const people = DataFrame.fromRows([
  { name: "Alice", age: 30, city: "NYC", income: 90000, joined: new Date("2021-03-15") },
  { name: "Bob", age: 25, city: "SF", income: 80000, joined: new Date("2022-07-01") },
  { name: "Carol", age: 35, city: "NYC", income: 120000, joined: new Date("2020-01-20") },
  { name: "Dave", age: 28, city: "SF", income: 75000, joined: new Date("2023-02-11") },
  { name: "Eve", age: null, city: "NYC", income: null, joined: new Date("2021-11-30") },
]);
display(Inputs.table(people.toRows()));
```

## Expressions

`col()` references a column; methods build a tree; the frame executes it in
one vectorized pass. Aggregations (`mean`, `sum`, `count`, `median`,
`quantile`, `std`, `nUnique`, …) work in `select` (one-row result) and in
`groupBy().agg()` (one row per group).

```js echo
people
  .filter(col("age").between(25, 32).and(col("city").isIn(["NYC", "SF"])))
  .withColumns(
    col("income").div(12).round(0).alias("monthly"),
    when(col("age").gte(30)).then("senior").otherwise("junior").alias("tier"),
    col("name").str.upper().alias("shouting"),
    col("joined").dt.year().alias("cohort_year"),
  )
  .toRows()
```

```js echo
people.select(
  col("income").mean().alias("avg_income"),
  col("income").sum().div(col("age").sum()).alias("income_per_year_lived"),
).toRows()
```

## GroupBy

Group ids are computed once and reused for every aggregation in the call.
Multi-column keys are collision-safe (no string-joining with `"_"` — we read
that postmortem).

```js echo
people
  .groupBy("city")
  .agg(
    col("income").mean().alias("avg_income"),
    col("income").max().alias("top"),
    col("name").count().alias("n"),
  )
  .sort("avg_income", { descending: true })
  .toRows()
```

The object form gives you named expressions — the closest JavaScript gets
to polars' keyword arguments:

```js echo
people.withColumns({ monthly: col("income").div(12).round(0) }).select("name", "monthly").toRows()
```

## Joins

Hash joins, all four flavors, null keys match nothing:

```js echo
const scores = DataFrame.fromRows([
  { name: "Alice", score: 0.92 },
  { name: "Dave", score: 0.71 },
  { name: "Zed", score: 0.55 },
]);

people.join(scores, { on: "name", how: "left" }).select("name", "city", "score").toRows()
```

## Reshaping

`pivot` (long → wide), `melt` (wide → long), `concat`, `unique`:

```js echo
const long = DataFrame.fromRows([
  { year: 2023, city: "NYC", revenue: 10 },
  { year: 2023, city: "SF", revenue: 20 },
  { year: 2024, city: "NYC", revenue: 30 },
  { year: 2024, city: "SF", revenue: 44 },
]);
display(Inputs.table(long.pivot({ index: "year", columns: "city", values: "revenue" }).toRows()));
```

## Temporal aggregation: groupByDynamic

The polars `group_by_dynamic`, for the step every panel begins with:
events with timestamps in, fixed windows out. `every` sets the stride,
`period` the span (longer than the stride → overlapping, rolling
windows), `offset` shifts boundaries, `closed` picks the inclusive edge,
and `by` adds group keys. Calendar durations (`"1mo"`, `"1q"`, `"1y"`)
respect real month boundaries in UTC; fixed durations (`"7d"`, `"12h"`)
are exact.

```js echo
import * as Plot from "npm:@observablehq/plot";
import { tufteLine } from "observable-dataframe/plots";

// Claim lines with dispensing dates, the shape claims actually arrive in.
const claimLines = DataFrame.fromRows((() => {
  let s = 11;
  const rand = () => (s = (s * 1664525 + 1013904223) % 4294967296) / 4294967296;
  return Array.from({ length: 900 }, () => {
    const member = `m${(rand() * 40) | 0}`;
    const day = (rand() * 365) | 0;
    return {
      member,
      date: new Date(Date.UTC(2024, 0, 1 + day)),
      paid: Math.round(20 + rand() * 300),
    };
  });
})());

// Claims → member-month panel, one call.
const memberMonths = claimLines
  .groupByDynamic({ indexColumn: "date", every: "1mo", by: ["member"] })
  .agg(col("paid").sum().alias("paid"), col("paid").count().alias("claims"));
display(Inputs.table(memberMonths.head(8).toRows()));
```

The same call without `by` gives the book-of-business trend, and a
quarterly stride is a one-token change:

```js echo
const monthly = claimLines
  .groupByDynamic({ indexColumn: "date", every: "1mo" })
  .agg(col("paid").sum().alias("paid"));
const quarterly = claimLines
  .groupByDynamic({ indexColumn: "date", every: "1q" })
  .agg(col("paid").sum().alias("paid"));

display(tufteLine(monthly.withColumns(col("paid").alias("monthly paid ($)")), {
  x: "date", y: "monthly paid ($)", height: 220,
}));
display(Inputs.table(quarterly.toRows()));
```

Overlapping windows are the rolling variant: a `period` of three months
stepping `every` one month gives each window a quarter of context.

```js echo
const rollingQuarter = claimLines
  .groupByDynamic({ indexColumn: "date", every: "1mo", period: "3mo" })
  .agg(col("paid").sum().alias("trailing_3mo_paid"));
display(Inputs.table(rollingQuarter.head(6).toRows()));
```

## Window operations

Order-sensitive column ops for time series work — `shift`, `diff`,
`cumSum`, `rollingMean`:

```js echo
DataFrame.fromRows([4, 7, 5, 9, 12, 10].map((v, i) => ({ t: i, v })))
  .withColumns(
    col("v").diff().alias("delta"),
    col("v").cumSum().alias("running"),
    col("v").rollingMean(3).alias("smooth"),
  )
  .toRows()
```

## IO and the Plot bridge

```js run=false
import { fromCSV } from "observable-dataframe";
const df = fromCSV(await FileAttachment("data/mydata.csv").text());
```

`toRows()` is the deliberate exit ramp to Observable Plot and
`Inputs.table` — the only place row objects are ever materialized:

```js run=false
Plot.plot({
  marks: [Plot.dot(df.toRows(), { x: "age", y: "income", stroke: "city" })],
})
```

## Escape hatches, ranked by shame

1. `col("x").map(fn)` — arbitrary JS per element, still columnar on the outside.
2. `df.filter(row => ...)` — row-predicate filter; materializes row objects. The emergency exit, not the door.

There is no third option. v1's pandas spellings (`groupby`, `sort_values`,
`with_columns`, `to_data`, `loc`) were removed in v2 — one API, one
spelling. Migration is mechanical: `groupby` → `groupBy`, `sort_values(by, asc)` →
`sort(by, { descending })`, `with_columns` → `withColumns` (expressions, not
`fn(row)`), `to_data()` → `toRows()`.

## describe, valueCounts, corr

```js echo
Inputs.table(people.describe().toRows())
```

```js echo
people.valueCounts("city").toRows()
```
