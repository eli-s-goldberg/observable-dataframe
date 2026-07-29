---
toc: true
theme: [air, wide]
title: "API: Extension contract"
---

```js
import { injectPageStyle } from "observable-dataframe/layouts";
injectPageStyle();
```

<div class="page-title-header">
  <div class="page-title">API reference: the extension contract</div>
  <div class="divider"></div>
  What a module built on top of the core may rely on: the Column physical
  layout (read-only), the sanctioned <code>values()</code> accessor, and the
  expression node schema the evaluator executes. The stats and plots modules
  in this package are held to the same rules as any third-party extension.
</div>

If you are writing a package on top of observable-dataframe, everything on
this page is public API and covered by semver. Anything not on this page or
the other API pages (underscore-prefixed fields like `_columns` and `_take`,
helper functions inside `eval.js`) is internal and may change without notice.

```js
import { DataFrame, Column, col, lit } from "observable-dataframe";
```

## Column physical layout: public, read-only

Every Column exposes its storage directly. Extensions may read these fields
for zero-copy numeric work; they must never write to them. Every frame verb
returns a new frame, and Columns are shared freely between frames, so a
write into one buffer corrupts every frame that shares it.

| field | type | contents |
|---|---|---|
| `dtype` | string | `"f64"`, `"i32"`, `"bool"`, `"str"`, or `"date"` |
| `data` | typed array | the physical values: numbers for `f64`/`i32`, 0/1 for `bool`, dictionary codes for `str`, epoch ms for `date` |
| `validity` | `Uint8Array` or `null` | 1 = valid, 0 = null; `null` means every slot is valid |
| `dict` | `string[]` or `null` | the dictionary for `str` columns; `data[i]` indexes into it |

The trap to respect: `data` holds *physical* values, not logical ones. For a
`str` column, `data[i]` is an integer code, and for a `date` column it is a
number of milliseconds. Reading `data` without checking `dtype` first is how
dictionary codes end up in a mean.

```js echo
const c = Column.from(["a", "b", null, "a"]);
({ dtype: c.dtype, data: Array.from(c.data), validity: Array.from(c.validity), dict: c.dict })
```

### The dtype-checked fast path

The pattern the built-in stats module uses: take the typed array only when
the dtype says the physical values are the logical values.

```js echo
function numericValues(column) {
  if (column.dtype === "str") throw new Error(`"${column.dtype}" is not numeric.`);
  const out = [];
  for (let i = 0; i < column.length; i++) {
    if (column.isValid(i)) out.push(column.data[i]);
  }
  return out; // f64/i32 numbers, bool 0/1, date epoch ms
}
numericValues(Column.from([1.5, null, 3]))
```

## Column.values(options?)

The sanctioned logical accessor, for when you want values rather than
storage: strings decoded from the dictionary, `Date` objects for `date`
columns, booleans for `bool`, and `null` where the validity mask says so.
`validOnly: true` skips the nulls, which is what every statistic wants
anyway.

```js echo
Column.from(["a", null, "b"]).values()
```

```js echo
Column.from([1, null, 3]).values({ validOnly: true })
```

Use `values()` when dtype handling matters or the column is small; use the
dtype-checked fast path above when you are inside a hot loop over a numeric
column and have already checked `dtype`.

## The expression node schema

Expressions are immutable trees. `col("age").gt(25)` builds nodes; the
evaluator in the core executes them in vectorized passes. Every `Expr`
exposes its tree at `.node`, and each node is a plain object with a `kind`
plus kind-specific fields:

| kind | fields | built by |
|---|---|---|
| `col` | `name` | `col(name)` |
| `lit` | `value` | `lit(value)`, or auto-wrapped scalars |
| `binary` | `op`, `left`, `right` | `add sub mul div mod pow`, `gt gte lt lte eq neq`, `and or`, `fillNull` |
| `unary` | `op`, `input`, `param?` | `neg abs log exp sqrt round not isNull isNotNull` |
| `agg` | `op`, `input`, `param?` | `sum mean min max count nUnique first last median std var quantile implode` |
| `window` | `op`, `input`, `param?`, `over?` | `shift cumSum diff rollingMean rank`, partitioned by `.over(...keys)` |
| `str` | `op`, `input`, `args` | the `.str` namespace: `lower upper len contains startsWith endsWith replace strip slice` |
| `dt` | `op`, `input` | the `.dt` namespace: `year month day weekday hour` |
| `ternary` | `cond`, `then`, `otherwise` | `when(cond).then(a).otherwise(b)` |
| `isin` | `input`, `values` | `isIn(values)` |
| `between` | `input`, `lo`, `hi` | `between(lo, hi)` |
| `cast` | `input`, `dtype` | `cast(dtype)` |
| `alias` | `input`, `name` | `alias(name)` |
| `map` | `input`, `fn`, `dtype?` | `map(fn, dtype?)`, the escape hatch |

Child positions (`input`, `left`, `right`, `cond`, `then`, `otherwise`) hold
nodes, not `Expr` wrappers. To build nodes programmatically, compose the
fluent API and take `.node` at the end; constructing raw node objects by
hand is possible but the fluent builders are the supported route.

On `window` nodes, the optional `over` field is an array of partition
column names, set by `.over(...keys)`. The evaluator groups row indices by
the key tuple (null keys share a partition), runs the window within each
partition in row order, and scatters results back to their original rows.

```js echo
col("age").gt(25).and(col("city").isIn(["NYC"])).node
```

```js echo
col("paid").shift(1).over("member_id").node
```

## The evaluator contract

Three entry points, all exported from the core's `eval.js` and consumed by
`DataFrame` itself. The `frame` argument is anything with
`getColumn(name) -> Column` and a `height` property; a DataFrame qualifies,
and so does a duck-typed stand-in.

### evalExpr(node, frame) -> Column

Evaluates a non-aggregating expression to a Column of `frame.height` rows.
Aggregations that appear here are computed once and broadcast to full
length. This is what `select`, `withColumns`, and `filter` call.

### evalAggScalar(node, frame) -> value

Evaluates an expression whose spine aggregates (`isAggExpr(node)` is true)
to a single scalar. Arithmetic over aggregations, such as
`col("a").sum().div(col("b").sum())`, recurses with scalar semantics.

### evalAggGrouped(node, frame, ids, nGroups, cache?) -> Array

Evaluates an aggregating expression once per group and returns a plain
array of `nGroups` values. `ids` is an `Int32Array` assigning a group id in
`[0, nGroups)` to each row, exactly what `GroupBy` computes. The optional
`cache` object is shared across a single `agg(...)` call so order
statistics (median, quantile, implode) build their per-group row lists at
most once.

Null semantics, stated once for all three: aggregations skip nulls,
comparisons and arithmetic propagate them through the validity mask, and a
null never passes a filter.

```js echo
const df = DataFrame.fromRows([
  { city: "NYC", income: 90000 },
  { city: "NYC", income: 120000 },
  { city: "SF", income: 80000 },
]);
df.groupBy("city").agg(col("income").mean().alias("avg")).toRows()
```

## What this buys you

The modules built on the core are the proof the contract holds: `/stats`
consumes columns through `getColumn` plus the dtype-checked fast path, `/plots`
renders from `values()` and `toRows()`, and `/data` builds frames only through
`DataFrame.fromColumns` and public verbs. A third-party package following
the same rules gets the same stability.
