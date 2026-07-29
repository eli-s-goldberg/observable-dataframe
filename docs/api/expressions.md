---
toc: true
theme: [air, wide]
title: "API: Expressions"
---

```js
import { injectPageStyle } from "observable-dataframe/layouts";
injectPageStyle();
```

<div class="page-title-header">
  <div class="page-title">API reference: expressions</div>
  <div class="divider"></div>
  The whole trick: <code>col("age").gt(25)</code> builds a description of
  work, and the engine executes it in one vectorized pass over typed
  arrays. Every operator, with an example that runs.
</div>

Expressions keep every module on one substrate: the same `col("pmpm").mean()`
runs on a claims panel, a power table, or a DiD coefficient frame.

```js
import * as Plot from "npm:@observablehq/plot";
import { DataFrame, col, lit, when } from "observable-dataframe";
import { distPlot } from "observable-dataframe/plots";
```

```js
const orders = DataFrame.fromRows(
  Array.from({ length: 12 }, (_, i) => ({
    id: i + 1,
    sku: ["WIDGET-A", "WIDGET-B", "GADGET-A", null][i % 4],
    qty: [3, 1, 7, 2, 5, 4, 8, 2, 6, 1, 9, 3][i],
    price: [19.99, 45.5, 7.25, 19.99, 45.5, 7.25, 19.99, 45.5, 7.25, 19.99, 45.5, 7.25][i],
    placed: new Date(Date.UTC(2024, i % 6, 3 + i)),
  }))
);
display(Inputs.table(orders.head(4).toRows()));
```

## The atoms

### col(name)

A column reference. Every expression grows from one (or from a literal).

```js echo
orders.select(col("qty")).head(3).toRows()
```

### lit(value)

An explicit literal. Rarely needed — scalars auto-wrap — but available for
the explicitness enthusiasts.

```js echo
orders.select(col("qty").mul(lit(10)).alias("qty10")).head(3).toRows()
```

### when(cond).then(a).otherwise(b)

The vectorized ternary. Both branches accept expressions or literals.

```js echo
orders.withColumns(
  when(col("qty").gte(5)).then("bulk").otherwise("single").alias("size")
).select("qty", "size").head(6).toRows()
```

## Arithmetic

`add`, `sub`, `mul`, `div`, `mod`, `pow`, `neg`, `abs`, `log`, `exp`,
`sqrt`, `round(decimals)`. Numeric output is `f64`; division by zero is
`Infinity`, as tradition demands.

```js echo
orders.select(
  col("qty").mul(col("price")).round(2).alias("total"),
  col("qty").pow(2).alias("qty_sq"),
  col("price").log().round(3).alias("log_price"),
).head(4).toRows()
```

## Comparison & boolean

`gt`, `gte`, `lt`, `lte`, `eq`, `neq` produce boolean columns; `and`,
`or`, `not` combine them. Strings compare through the dictionary, so
`eq` on two string columns never allocates per row.

```js echo
orders.filter(col("qty").gte(5).and(col("price").lt(20)).not()).height
```

## Null handling

### isNull() / isNotNull() / fillNull(value)

Nulls live in a validity mask, never as sentinels; these are the three
verbs for dealing with them.

```js echo
({
  nullSkus: orders.filter(col("sku").isNull()).height,
  filled: orders.withColumns(col("sku").fillNull("UNKNOWN").alias("sku"))
    .valueCounts("sku").toRows(),
})
```

## Membership & ranges

### isIn(values) / between(lo, hi)

Set-backed membership (dictionary-tested once for strings, not per row)
and inclusive ranges.

```js echo
orders.filter(col("sku").isIn(["WIDGET-A", "WIDGET-B"]).and(col("qty").between(2, 6)))
  .select("sku", "qty").toRows()
```

## Shape-shifting

### cast(dtype) / alias(name)

Cast between `"f64" | "i32" | "bool" | "str" | "date"` (string→number
failures become nulls, not exceptions); alias names the output.

```js echo
orders.select(col("qty").cast("str").alias("qty_label")).dtypes
```

## String operations: .str

`lower`, `upper`, `strip`, `len`, `contains(sub|regex)`, `startsWith`,
`endsWith`, `replace(search, replacement)`, `slice(start, end)`. All
computed on the dictionary — one operation per unique string, then an
integer gather — so a million-row lowercase costs about forty lowercases.

```js echo
orders.dropNulls({ subset: ["sku"] }).select(
  col("sku").str.lower().alias("lower"),
  col("sku").str.slice(0, 6).alias("family"),
  col("sku").str.contains("WIDGET").alias("is_widget"),
  col("sku").str.replace("-", "_").alias("snake"),
  col("sku").str.len().alias("len"),
).unique({ subset: ["lower"] }).toRows()
```

## Date operations: .dt

`year`, `month` (1–12, siding with humans), `day`, `weekday` (0=Sunday),
`hour`.

```js echo
orders.select(
  col("placed").dt.year().alias("y"),
  col("placed").dt.month().alias("m"),
  col("placed").dt.weekday().alias("dow"),
).head(4).toRows()
```

## Aggregations

`sum`, `mean`, `min`, `max`, `count` (non-null), `nUnique`, `first`,
`last`, `median`, `std`, `var` (both ddof=1), `quantile(p)`, `implode`
(group values into an array). They work in three contexts: `select`
(one-row frame), `groupBy().agg()` (one row per group), and arithmetic
over aggregations composes (`sum(a).div(sum(b))`).

```js echo
orders.select(
  col("qty").sum().alias("units"),
  col("price").quantile(0.9).round(2).alias("p90_price"),
  col("sku").nUnique().alias("skus"),
  col("qty").mul(col("price")).sum().round(0).alias("revenue"),
).toRows()
```

```js echo
orders.groupBy("sku").agg(
  col("qty").implode().alias("qty_list"),
  col("qty").std().round(2).alias("qty_sd"),
).toRows()
```

## Window operations

Order-sensitive whole-column ops: `shift(n)` (vacated slots become null),
`cumSum`, `diff(n)`, `rollingMean(n)` (trailing; warm-up rows null), and
`rank()` (ascending, ties averaged, nulls rank as null).

```js echo
DataFrame.fromRows([4, 7, 5, 9, 12, 10].map((v, t) => ({ t, v }))).withColumns(
  col("v").shift(1).alias("prev"),
  col("v").diff().alias("delta"),
  col("v").cumSum().alias("running"),
  col("v").rollingMean(3).alias("smooth3"),
).toRows()
```

## Grouped windows: over()

Any window operation takes `.over(...keys)`: the window runs within each
partition in row order, and results land back on their original rows.
Null keys form a partition of their own; multiple keys partition by the
tuple. This is the member-month panel workhorse: lag each member's paid
amounts without ever splitting the frame.

```js echo
const panel = DataFrame.fromRows([
  { member: "m1", month: "2024-01", paid: 120 },
  { member: "m2", month: "2024-01", paid: 900 },
  { member: "m1", month: "2024-02", paid: 80 },
  { member: "m2", month: "2024-02", paid: 450 },
  { member: "m1", month: "2024-03", paid: 200 },
  { member: "m2", month: "2024-03", paid: 610 },
]);
panel.withColumns(
  col("paid").shift(1).over("member").alias("prev_paid"),
  col("paid").diff(1).over("member").alias("mom_change"),
  col("paid").cumSum().over("member").alias("paid_to_date"),
).toRows()
```

The frame must already be sorted the way the window should read it
(chronologically, for a lag); `.over()` partitions rows, it does not
reorder them.

## The escape hatch

### map(fn, dtype?)

Arbitrary JS per element. Abandons the vectorized kernels, so reach for
it when the operators above genuinely cannot express the logic, not
because `.mul(2)` felt like ceremony.

```js echo
orders.dropNulls({ subset: ["sku"] })
  .select(col("sku").map((s) => s.split("-")[1]).alias("variant"))
  .valueCounts("variant").toRows()
```

## Plotting expression results, both levels

Expressions produce frames; frames plot. Low level composes raw Plot
marks from `toRows()`; high level hands a column straight to a primitive.

```js echo
const revenue = orders.withColumns(col("qty").mul(col("price")).alias("line_total"));
// Low level: full mark control
display(Plot.plot({
  height: 200,
  x: { label: "line total ($)" },
  y: { label: "orders", ticks: 3 },
  marks: [
    Plot.rectY(revenue.toRows(), Plot.binX({ y: "count" }, { x: "line_total", fill: "#1C2B3A", fillOpacity: 0.35 })),
    Plot.ruleY([0]),
  ],
}));
```

```js echo
// High level: distPlot takes the frame + column, adds markers and the house style
display(distPlot(revenue, { column: "line_total", markers: ["mean", "median"], labelDigits: 0, height: 200 }));
```
