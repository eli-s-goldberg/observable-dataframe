---
toc: true
theme: [air, wide]
title: "API: Data"
---

```js
import { injectPageStyle } from "observable-dataframe/layouts";
injectPageStyle();
```

<div class="page-title-header">
  <div class="page-title">API reference: observable-dataframe/data</div>
  <div class="divider"></div>
  Real-shaped eligibility + medical + pharmacy joined at <code>person_id</code>,
  small enough for the browser, honest enough for method development.
</div>

The panel joins eligibility, medical claims, and pharmacy on one `person_id`
axis at member-month grain. The docs read it from a data loader that emits a
synthetic slice of the same shape, so no extract is needed to follow along.

```js
import {
  claimsSliceFromCSV,
  memberRollup,
  monthlyTrend,
  enrolledMemberMonths,
} from "observable-dataframe/data";
```

## `claimsSliceFromCSV(text)`

Parse CSV text → typed `DataFrame`. Use an Observable Framework [data loader](https://observablehq.com/framework/data-loaders) (`files/claims_member_month.csv.js`) and `FileAttachment` in pages — not `node:fs`.

```js
const panel = claimsSliceFromCSV(await FileAttachment("../files/claims_member_month.csv").text());
```

## `memberRollup(df)`

Per-member totals → DataFrame.

## `monthlyTrend(df)`

Cohort monthly aggregates → DataFrame.

## `enrolledMemberMonths(df)`

Filter to enrolled rows (`enrolled_flag === 1`).

## Where the panel comes from

The loader at `docs/files/claims_member_month.csv.js` reads
`data/samples/claims_member_month.csv` when that file is present, and otherwise
emits a synthetic panel with the same columns and dtypes. The synthetic path is
the supported one: `npm run docs:dev` and `npm run docs:build` both work with no
local data. To develop against your own extract, write a CSV with the columns
above to `data/samples/claims_member_month.csv` and the loader prefers it.
