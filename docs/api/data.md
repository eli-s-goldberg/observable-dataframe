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
  A simulated eligibility + medical + pharmacy panel joined at
  <code>person_id</code>, small enough for the browser, shaped closely enough to
  utilization for method development.
</div>

The panel joins eligibility, medical claims, and pharmacy on one `person_id`
axis at member-month grain. It is generated from a seed, so no extract is needed
to follow along and the same seed gives the same panel everywhere.

```js
import {
  claimsSliceFromCSV,
  memberRollup,
  monthlyTrend,
  enrolledMemberMonths,
  simulateClaimsPanel,
  claimsPanelCsv,
} from "observable-dataframe/data";
```

## `claimsSliceFromCSV(text)`

Parse CSV text → typed `DataFrame`. Use an Observable Framework [data loader](https://observablehq.com/framework/data-loaders) (`files/claims_panel.csv.js`) and `FileAttachment` in pages, not `node:fs`.

```js
const panel = claimsSliceFromCSV(await FileAttachment("../files/claims_panel.csv").text());
```

## `memberRollup(df)`

Per-member totals → DataFrame.

## `monthlyTrend(df)`

Cohort monthly aggregates → DataFrame.

## `enrolledMemberMonths(df)`

Filter to enrolled rows (`enrolled_flag === 1`).

## `simulateClaimsPanel(options)`

Generate a member-month panel from a seed, with a treatment effect planted in it.
Returns `{rows, truth, options}`. Nothing it produces is real; every default is
an illustrative round number, chosen so the output has the shape utilization has
rather than the values of any population.

```js
const { rows, truth } = simulateClaimsPanel({ seed: 42 });
```

The mechanisms, and the shape each one buys:

| Mechanism | Shape it produces |
|---|---|
| member intensity ~ Gamma, reused every month | counts overdispersed, high utilizers persistent |
| monthly Gamma shock into a Poisson draw | bursts of care rather than a steady rate |
| a share of member-months drawn inactive | a large mass at zero, on top of Poisson zeros |
| lognormal paid amount per fill | spend with a long right tail |
| late starts, early ends, and mid-window gaps | `enrolled_flag` churn |
| cosine on the calendar month | mild winter seasonality |

Treatment is planted by thinning: on a treated member-month, each would-be
encounter is dropped independently with probability `1 - rateRatio`, phased in
over `rampMonths`. Because the thinning removes encounters from a draw the
generator already holds, the untreated counterfactual is known for every treated
cell, and `truth` reports the average treatment effect on the treated as
measured rather than assumed: `truth.att`, `truth.attFullyPhasedIn`,
`truth.attByCohort`, and `truth.attByEventTime`. Cohorts are assigned
independently of member intensity, so parallel trends holds by construction.

Defaults live in `CLAIMS_PANEL_DEFAULTS`; the published column order lives in
`CLAIMS_PANEL_COLUMNS`. See [Panel data & DiD](../statistics) for what the
estimators recover from it.

## `claimsPanelCsv(options)`

The same panel as CSV text, for a data loader to write to stdout. Returns
`{csv, truth, options}`.

```js
import { claimsPanelCsv } from "observable-dataframe/data";
process.stdout.write(claimsPanelCsv({ seed: 42 }).csv);
```

## Where the panel comes from

The loader at `docs/files/claims_panel.csv.js` always simulates, and deliberately
does not look for a local extract, so `npm run docs:dev` and `npm run docs:build`
work identically on every checkout and the site never publishes whatever happened
to sit on the machine that built it. To work against your own extract, point your
own page at your own loader.

One trap worth naming, because it is silent: Framework serves a static file in
preference to a loader targeting the same path. A stale CSV left in `docs/files/`
next to a loader of the same name wins, and the build reports nothing unusual.
Name loaders so they cannot collide with anything written there by hand.
