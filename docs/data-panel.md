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
import {
  claimsSliceFromCSV, memberRollup, monthlyTrend, enrolledMemberMonths,
} from "observable-dataframe/data";
import { summaryTable } from "observable-dataframe/plots";
```

## Why a slice

A full payer claims extract runs to tens of millions of service lines, which no
browser will scan interactively, and which has no business in a source
repository. A [data loader](https://observablehq.com/framework/data-loaders)
builds the published CSV at preview/build time instead:

| File | Role |
|------|------|
| `docs/files/claims_panel.csv.js` | Loader: calls `claimsPanelCsv({ seed: 42 })` |
| `FileAttachment("files/claims_panel.csv")` | What pages import in the browser |

Nothing is required to work through this page:

```bash
npm run docs:dev
```

The loader always simulates, and deliberately does not look for a local extract.
A public site that prefers a local file publishes whatever happens to sit on the
machine that ran the build, and a seeded panel is byte-identical everywhere, so
the numbers quoted in prose on [Panel data & DiD](./statistics) stay true on
every checkout. The generator is
[`simulateClaimsPanel()`](./api/data#simulate-claims-panel-options), and it is
built to reproduce the shape utilization has rather than a smooth ramp:
overdispersed counts, roughly two fifths of member-months at zero, spend with a
lognormal tail, persistent high utilizers, enrollment churn, and mild
seasonality. To work against your own extract, point your own page at your own
loader.

Columns:

| Column | Meaning |
|--------|---------|
| `person_id` | Cross-source member key |
| `month` | `YYYY-MM` service month |
| `period` | Month index, 0 through 23 |
| `cohort` | Adoption period, or 0 for never treated |
| `treated_now` | 1 when this member is treated in this month |
| `medical_claims` | Encounter count, the DiD outcome |
| `pharmacy_fills` | Fill count |
| `pharmacy_paid` | Sum paid amount |
| `enrolled_flag` | 1 if month falls in eligibility span |

## Load into DataFrame

```js
const claimsCsv = FileAttachment("files/claims_panel.csv");
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
