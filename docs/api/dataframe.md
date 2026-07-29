---
toc: true
theme: [air, wide]
title: "API: DataFrame"
---

```js
import { injectPageStyle } from "observable-dataframe/layouts";
injectPageStyle();
```

<div class="page-title-header">
  <div class="page-title">API reference: DataFrame &amp; GroupBy</div>
  <div class="divider"></div>
  Every method on the frame, with a running example. Construction,
  inspection, transformation, grouping, joining, reshaping, statistics,
  and export, plus the low-level and high-level plotting path for each
  result shape.
</div>

Every downstream module assumes you can hold member-month claims in a typed
frame and aggregate without a server round-trip. This page is that
assumption, method by method.

```js
import * as Plot from "npm:@observablehq/plot";
import { DataFrame, col, lit, when } from "observable-dataframe";
import { boxPlot, tufteLine, corrPlot } from "observable-dataframe/plots";
```

```js
// The running example: a small member panel used by every method below.
const members = DataFrame.fromRows([
  { name: "Alice", age: 34, city: "NYC", income: 91000, joined: new Date(Date.UTC(2021, 2, 15)) },
  { name: "Bob", age: 27, city: "SF", income: 78000, joined: new Date(Date.UTC(2022, 6, 1)) },
  { name: "Carol", age: 41, city: "NYC", income: 122000, joined: new Date(Date.UTC(2020, 0, 20)) },
  { name: "Dave", age: 29, city: "SF", income: 74000, joined: new Date(Date.UTC(2023, 1, 11)) },
  { name: "Eve", age: null, city: "CHI", income: 67000, joined: new Date(Date.UTC(2021, 10, 30)) },
  { name: "Frank", age: 52, city: "CHI", income: 101000, joined: new Date(Date.UTC(2019, 8, 2)) },
]);
display(Inputs.table(members.toRows()));
```

## Construction

### new DataFrame(data, options?)

Rows (`[{a: 1}, ...]`) or columns (`{a: [1, 2]}`). Types are inferred per
column; `options.dtypes` overrides inference when it guesses wrong.

```js echo
new DataFrame({ zip: ["02134", "10001"] }, { dtypes: { zip: "str" } }).dtypes
```

### DataFrame.fromRows(rows) / DataFrame.fromColumns(columns)

Named constructors for the two input shapes. Identical to the constructor,
with the intent in the name.

```js echo
DataFrame.fromColumns({ x: [1, 2, 3], y: [4, 5, 6] }).shape
```

### DataFrame.concat(frames)

Vertical stack, aligned by column name; columns missing from any input
become null there.

```js echo
DataFrame.concat([
  DataFrame.fromRows([{ a: 1, b: "x" }]),
  DataFrame.fromRows([{ a: 2, c: true }]),
]).toRows()
```

## Inspection

### height, width, shape, columns, dtypes

```js echo
({ height: members.height, width: members.width, shape: members.shape,
   columns: members.columns, dtypes: members.dtypes })
```

### getColumn(name)

The underlying typed Column: the zero-copy accessor plots and stats use to
skip row materialization. See the [Column & IO page](./column-io) for the
Column API.

```js echo
members.getColumn("age").nullCount()
```

### row(i) and iteration

`row(i)` returns one plain object; the frame is iterable row-wise. Both
are edge conveniences, priced accordingly.

```js echo
members.row(0)
```

```js echo
[...members].filter((r) => r.city === "CHI").map((r) => r.name)
```

## Selection & transformation

### select(...namesOrExprs)

Choose or compute columns. All-aggregation selects collapse to one row.

```js echo
members.select("name", col("income").div(12).round(0).alias("monthly")).toRows()
```

```js echo
members.select(col("income").mean().alias("avg"), col("age").max().alias("oldest")).toRows()
```

### withColumns(...exprs)

Add or replace columns; the rest ride along. Named-expression object form
is the JavaScript spelling of polars' keyword arguments.

```js echo
members.withColumns(
  when(col("age").gte(40)).then("senior").otherwise("standard").alias("tier"),
  { joined_year: col("joined").dt.year() },
).select("name", "tier", "joined_year").toRows()
```

### rename(mapping) / drop(...names)

```js echo
members.rename({ name: "member" }).drop("joined", "income").columns
```

## Filtering

### filter(expr)

Boolean expression in, surviving rows out; nulls never pass. A row
predicate function is the documented escape hatch for logic the
expression API cannot express.

```js echo
members.filter(col("city").eq("NYC").and(col("income").gt(100000))).toRows()
```

```js echo
members.filter((r) => r.name.length === 3).toRows() // the slow lane, labeled
```

### dropNulls(options?)

Drop rows with nulls, everywhere or in a `subset`.

```js echo
[members.dropNulls().height, members.dropNulls({ subset: ["income"] }).height]
```

## Sorting

### sort(by, options?)

Stable, dtype-aware, nulls last. Multi-key with per-key direction.

```js echo
members.sort(["city", "income"], { descending: [false, true] })
  .select("city", "name", "income").toRows()
```

## Grouping

### groupBy(...names) → GroupBy

Group ids are computed once and reused for every aggregation in the call.
Multi-column keys are collision-safe.

```js echo
const byCity = members.groupBy("city").agg(
  col("income").mean().alias("avg_income"),
  col("income").sum().div(col("age").sum()).alias("income_per_year_lived"),
  col("name").count().alias("n"),
).sort("avg_income", { descending: true });
display(Inputs.table(byCity.toRows()));
```

#### GroupBy.count()

The aggregation everyone actually wanted, as a shortcut.

```js echo
members.groupBy("city").count().toRows()
```

#### Iterating groups

`for (const [key, subframe] of groupBy)` yields each group as its own
DataFrame. Costs more than `agg()`; worth it less often than it feels.

```js echo
[...members.groupBy("city")].map(([key, sub]) => `${key.city}: ${sub.height} members`)
```

**Plotting grouped results, both levels.** Low level: `toRows()` into raw
Observable Plot, full mark control. High level: the primitives accept the
frame directly.

```js echo
// Low level: raw Plot on the aggregated rows
display(Plot.plot({
  height: 200,
  marginLeft: 60,
  x: { label: "avg income ($)" },
  y: { label: null },
  marks: [
    Plot.barX(byCity.toRows(), { x: "avg_income", y: "city", fill: "#1C2B3A" }),
    Plot.ruleX([0]),
  ],
}));
```

```js echo
// High level: a primitive consumes the raw frame and does the summary itself
display(boxPlot(members, { x: "city", y: "income", width: 420, height: 240 }));
```

### groupByDynamic(options) → GroupBy

Time-window grouping, the polars `group_by_dynamic`: `every` sets the
stride (calendar `"1mo"`/`"1q"`/`"1y"` in UTC, fixed `"7d"`/`"12h"`, or
numbers for numeric indexes), `period` the span (longer → overlapping,
rolling windows), `offset` shifts boundaries, `closed` picks the edge,
`by` adds keys. Output keeps the index column's name, sorted.

```js echo
const events = DataFrame.fromRows(
  Array.from({ length: 120 }, (_, i) => ({
    date: new Date(Date.UTC(2024, 0, 1 + i * 3)),
    paid: 50 + (i % 7) * 30,
  }))
);
const monthlyPaid = events.groupByDynamic({ indexColumn: "date", every: "1mo" })
  .agg(col("paid").sum().alias("paid"));
display(Inputs.table(monthlyPaid.toRows()));
```

```js echo
// Low level: window starts are real dates; Plot's time scale just works
display(Plot.plot({
  height: 200,
  y: { label: "paid ($)", grid: false, ticks: 4 },
  marks: [Plot.line(monthlyPaid.toRows(), { x: "date", y: "paid", curve: "step-after" }), Plot.ruleY([0])],
}));
```

```js echo
// High level: the tufte line, one call
display(tufteLine(monthlyPaid, { x: "date", y: "paid", height: 200 }));
```

## Joining

### join(other, options)

Hash join: `on` or `leftOn`/`rightOn`, `how` of inner/left/right/outer,
`suffix` for non-key collisions. Null keys match nothing.

```js echo
const scores = DataFrame.fromRows([
  { name: "Alice", score: 0.91 }, { name: "Dave", score: 0.72 }, { name: "Zed", score: 0.55 },
]);
({
  inner: members.join(scores, { on: "name" }).height,
  left: members.join(scores, { on: "name", how: "left" }).height,
  outer: members.join(scores, { on: "name", how: "outer" }).height,
})
```

### unique(options?)

Distinct rows, optionally by a `subset` (first occurrence wins).

```js echo
members.unique({ subset: ["city"] }).select("city").toRows()
```

## Reshaping

### pivot({index, columns, values, agg?})

Long to wide; duplicate cells resolved by `agg` (`first` default, or
`sum`/`mean`/`min`/`max`/`count`).

```js echo
const long = DataFrame.fromRows([
  { year: 2023, city: "NYC", revenue: 12 }, { year: 2023, city: "SF", revenue: 9 },
  { year: 2024, city: "NYC", revenue: 15 }, { year: 2024, city: "SF", revenue: 11 },
]);
display(Inputs.table(long.pivot({ index: "year", columns: "city", values: "revenue" }).toRows()));
```

### melt({idVars, valueVars?, variableName?, valueName?})

Wide to long, the shape Observable Plot prefers anyway.

```js echo
DataFrame.fromRows([{ id: 1, q1: 10, q2: 14 }]).melt({ idVars: ["id"] }).toRows()
```

**Plotting reshaped results.** Pivot feeds heatmaps; melt feeds
multi-series marks with a stroke channel.

```js echo
// Low level: long format feeds Plot's series channels directly
display(Plot.plot({
  height: 180,
  marks: [Plot.line(long.toRows(), { x: "year", y: "revenue", stroke: "city", marker: true })],
  x: { tickFormat: "d" },
  color: { legend: true },
}));
```

## Built-in statistics

### describe()

Count, null count, mean, SD, min, quartiles, max per numeric column. The
five-second health check.

```js echo
Inputs.table(members.describe().toRows())
```

### valueCounts(name)

Counts per distinct value, sorted descending.

```js echo
members.valueCounts("city").toRows()
```

### corr(a, b) / corrMatrix()

Pearson correlation, pairwise or all numeric columns in long format
(`a`, `b`, `corr`), the shape `Plot.cell` and `corrPlot` both eat.

```js echo
members.corr("age", "income")
```

```js echo
// High level: corrPlot calls corrMatrix() itself and squares the cells
display(corrPlot(members, { width: 260 }));
```

## Slicing & export

### head(n) / tail(n) / slice(start, end)

```js echo
[members.head(2).height, members.tail(1).row(0).name, members.slice(2, 4).height]
```

### sample(n, {seed?, withReplacement?})

Random rows. A seed makes the draw reproducible (the same seed replays the
same rows, backed by the core `random()` PRNG); without one it uses
Math.random. Without replacement (the default) each row appears at most
once; with replacement rows may repeat.

```js echo
members.sample(3, { seed: 7 }).toRows().map((r) => r.name)
```

```js echo
// Same seed, same draw: reproducibility you can put in a methods section.
members.sample(3, { seed: 7 }).toRows().map((r) => r.name)
```

### toRows() / toColumns()

The deliberate exit ramps: plain row objects for Plot and `Inputs.table`,
or a `{name: values[]}` dictionary. Copies everything; make it the last
step.

```js echo
members.head(2).toColumns()
```
