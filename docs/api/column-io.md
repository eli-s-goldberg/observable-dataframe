---
toc: true
theme: [air, wide]
title: "API: Column & IO"
---

```js
import { injectPageStyle } from "observable-dataframe/layouts";
injectPageStyle();
```

<div class="page-title-header">
  <div class="page-title">API reference: Column, dtypes &amp; IO</div>
  <div class="divider"></div>
  The storage layer underneath the frame, and the CSV doors in and out.
  You will rarely construct a Column by hand; you will often be glad it
  works the way it does.
</div>

Load any CSV with `fromCSV`; every column is a typed array before the first
`groupBy`.

```js
import { DataFrame, Column, inferDtype, col, fromCSV, fromCSVUrl, toCSV } from "observable-dataframe";
```

## The dtype system

Five dtypes, each with a typed backing store and a validity mask for
nulls (no `NaN`-as-null, no `""`-as-null):

| dtype | storage | notes |
|---|---|---|
| `f64` | `Float64Array` | any numeric with fractions or large magnitude |
| `i32` | `Int32Array` | numerics that are all safe 32-bit integers |
| `bool` | `Uint8Array` | 0/1 |
| `str` | `Uint32Array` codes + dictionary | each unique string stored once |
| `date` | `Float64Array` epoch ms | real temporal math, not formatted strings |

### inferDtype(values)

The inference rules, callable directly: strings taint everything, mixed
types go to `str`, all-null picks `f64` harmlessly.

```js echo
[
  inferDtype([1, 2, 3]),
  inferDtype([1.5, 2]),
  inferDtype([true, false]),
  inferDtype([new Date()]),
  inferDtype([1, "2"]),
]
```

## The Column class

### Column.from(values, dtype?)

Build from a plain array, inferring or asserting the dtype. Null and
undefined become nulls in the validity mask.

```js echo
const c = Column.from(["a", "b", null, "a", "a"]);
({ dtype: c.dtype, length: c.length, dict: c.dict, codes: Array.from(c.data), nulls: c.nullCount() })
```

Dictionary encoding is why string operations are cheap: five rows, two
dictionary entries, and `"a" === "a"` becomes `0 === 0`.

### get(i) / isValid(i) / nullCount()

`get` returns the JS value (string decoded, Date constructed, null when
absent); `isValid` asks without materializing.

```js echo
[c.get(0), c.get(2), c.isValid(2), c.nullCount()]
```

### take(indices) / slice(start, end)

The row-shuffle primitives every frame operation is built on: gather by
index (dictionary shared, not copied) and contiguous slice.

```js echo
Column.from([10, 20, 30, 40]).take(Uint32Array.from([3, 0, 3])).toArray()
```

### toArray()

Materialize as plain JS values. The exit ramp; use it at the edges.

```js echo
Column.from([new Date(Date.UTC(2024, 0, 1))]).toArray()
```

### cast(dtype)

Convert dtypes with least-surprise semantics: string→number failures
become nulls rather than exceptions.

```js echo
Column.from(["1", "2", "oops"]).cast("f64").toArray()
```

### groupCodes()

Dense integer codes per row, used by groupBy and join internally. String
columns reuse their dictionary codes for free; nulls share one code and
group together.

```js echo
Column.from(["x", "y", "x", null]).groupCodes()
```

## CSV IO

### fromCSV(text, options?)

Parse with per-column type inference: all-numeric → numbers, ISO-looking
→ dates, `true`/`false` → booleans, otherwise strings. Empty cells become
nulls. `dtypes` overrides per column — the zip-code defense.

```js echo
const df = fromCSV(`name,age,joined,zip\nAlice,34,2021-03-15,02134\nBob,,2022-07-01,10001`,
  { dtypes: { zip: "str" } });
display(Inputs.table(df.toRows()));
display(df.dtypes);
```

### fromCSVUrl(url, options?)

The fetch-then-parse convenience for Observable pages:

```js run=false
const claims = await fromCSVUrl(FileAttachment("data/claims.csv").url());
```

### toCSV(df, options?)

Serialize back: dates as ISO strings, nulls as empty cells, quoting where
the delimiter demands it.

```js echo
toCSV(df.head(2))
```

Round trips preserve the data (and the nulls):

```js echo
fromCSV(toCSV(df), { dtypes: { zip: "str" } }).row(1).age === null
```
