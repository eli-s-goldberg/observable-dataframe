---
title: Data panel
---

```js
import { injectPageStyle } from "observable-dataframe/layouts";
injectPageStyle();
```

<div class="page-title-header">
  <div class="page-title">Healthcare data panel</div>
  <div class="divider"></div>
  Eligibility, medical claims, and pharmacy on one <code>person_id</code> axis —
  member-month grain, loaded into a DataFrame. The substrate for experiment
  design, DiD, and cohort sizing without a warehouse round-trip.
</div>

```js
import { claimsSliceFromCSV, memberRollup, monthlyTrend, enrolledMemberMonths } from "observable-dataframe/data";
import { summaryTable } from "observable-dataframe/plots";
```

## Why a slice

A full payer claims extract runs to tens of millions of service lines, which no
browser will scan interactively, and which has no business in a source
repository. A [data loader](https://observablehq.com/framework/data-loaders)
builds the published CSV at preview/build time instead:

| File | Role |
|------|------|
| `docs/files/claims_member_month.csv.js` | Loader: reads `data/samples/` or emits synthetic fallback |
| `FileAttachment("files/claims_member_month.csv")` | What pages import in the browser |

No extract is required to work through this page:

```bash
npm run docs:dev
```

With no `data/samples/claims_member_month.csv` present, the loader emits a
synthetic panel of the same shape and column types, which is the supported path
for anyone without a local extract. If you do drop a real sample at that path,
the loader prefers it on the next preview run.

Columns:

| Column | Meaning |
|--------|---------|
| `person_id` | Cross-source member key |
| `month` | `YYYY-MM` service month |
| `medical_claims` | Distinct claim-day count |
| `pharmacy_fills` | Fill count |
| `pharmacy_paid` | Sum paid amount |
| `enrolled_flag` | 1 if month falls in eligibility span |

## Load into DataFrame

```js
const claimsCsv = FileAttachment("files/claims_member_month.csv");
```

```js echo
const panel = claimsSliceFromCSV(await claimsCsv.text());
({
  rows: panel.height,
  members: new Set(panel.toRows().map((r) => r.person_id)).size,
})
```

```js echo
display(summaryTable(panel, { label: "Claims member-month slice" }));
```

## Standard rollups

```js echo
display(memberRollup(panel).head(10));
```

```js echo
display(monthlyTrend(panel));
```

## Bridge to design and analysis

The panel supplies the inputs a design needs: population size, prevalence
proxies, and observed event rates for the power calculation.

```js echo
enrolledMemberMonths(panel).height
```

Downstream: [Panel data & DiD](./statistics) · [Statistics API](./api/stats)
