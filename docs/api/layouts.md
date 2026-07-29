---
toc: true
theme: [air, wide]
title: "API: layouts"
---

```js
import { injectPageStyle } from "observable-dataframe/layouts";
injectPageStyle();
```

<div class="page-title-header">
  <div class="page-title">API reference: observable-dataframe/layouts</div>
  <div class="divider"></div>
  Page structure and table polish: the site style, split panels, KPI
  cards, tabs, and the cell formatters that turn Inputs.table into
  something a person reads on purpose.
</div>

Frame the study for your audience, then export a reproducible record: page
chrome, KPI cards, and table formatters that make `Inputs.table`
presentation-ready.

```js
import * as d3 from "npm:d3";
import { DataFrame, col } from "observable-dataframe";
import * as L from "observable-dataframe/layouts";
```

## Page style

### injectPageStyle(options?)

Appends the site stylesheet once per page (idempotent). Layers:

```js run=false
injectPageStyle();                            // page styles + dark sidebar chrome
injectPageStyle({ chrome: false });           // keep Framework's default nav
injectPageStyle({ numberedHeadings: true });  // 1., 1.1, 1.1.1 via CSS counters
injectPageStyle({ extra: ".hero h1 { color: red; }" }); // appended last, wins
```

The raw stylesheets export separately for inspection or à-la-carte use:
`pageStyle`, `siteChromeStyle`, `numberedHeadingsStyle`. Classes the page
layer provides: `.page-title`, `.page-title-header`, `.divider`, `.hero`,
`.pull-quote` / `.pull-quote-highlight`, `.card` (+ `-yellow`, `-gray`),
`.full-width-section`, `.layout-grid`, `.quote-box`, `.bibliography`.

### createNumberHeadings(maxLevel?)

The runtime numberer for dynamically generated markdown, where CSS
counters cannot reach.

```js echo
const number = L.createNumberHeadings();
[number("Introduction", 1), number("Methods", 1), number("Cohorts", 2), number("Results", 1)]
```

## Panels & cards

### splitPanel({title, subtitle, gridSplit, left, right, theme, cards})

The declarative two-column section: any content in the slots, `gridSplit`
in fr units, `theme: "gray"` for the full-bleed band.

```js echo
display(L.splitPanel({
  title: "Where is the cohort?",
  subtitle: "A panel composed from slots.",
  gridSplit: [40, 60],
  left: L.prose(html`We identified <span class="pull-quote-highlight">3 segments</span>
    worth separate treatment, detailed at right.`),
  right: L.cardRow(
    L.kpiCard({ label: "Members", value: "24.0K" }),
    L.kpiCard({ label: "Value at stake", value: "$4.2M", flavor: "valueAtStake" }),
  ),
}));
```

### stackPanel({title, subtitle, top, bottom, theme})

The vertical variant: cards row on top, wide content below. The executive
dashboard shape.

### prose(content)

Pull-quote styled body text for panel slots; mark the hot phrase with
`class="pull-quote-highlight"`.

### kpiCard({label, value, note, flavor}) / cardRow(...cards)

KPI tiles in three flavors: `plain`, `keyTakeaway` (yellow),
`valueAtStake` (navy); `cardRow` spreads them evenly.

```js echo
display(L.cardRow(
  L.kpiCard({ label: "Events prevented", value: "1,040 / yr" }),
  L.kpiCard({ label: "ROI", value: "212%", flavor: "keyTakeaway", note: "pro forma" }),
  L.kpiCard({ label: "Program cost", value: "$380K", flavor: "valueAtStake" }),
));
```

## Tabs

### tabPanel({tabs, active, accent})

Self-contained tabs: labeled slots in, working tab bar out.

```js echo
display(L.tabPanel({
  tabs: [
    { label: "Summary", content: html`<p>The headline view.</p>` },
    { label: "Detail", content: html`<p>The rows behind it.</p>` },
  ],
}));
```

## Table formatters

Higher-order cell formatters for `Inputs.table(rows, { format: {...} })`.
The demo table exercises all of them at once:

```js echo
const campaigns = [
  { campaign: "Imaging shift", status: "Active", outreach: 4200, spendDelta: -125000, rate: 0.062,
    version: { header: "v3", description: "DM + email + microsite for predicted savers" },
    pros: "clear power story | reusable engine | cheap channels",
    cons: "claims lag | attribution fights" },
  { campaign: "MSK prevention", status: "Underpowered", outreach: 1100, spendDelta: 43000, rate: 0.021,
    version: { header: "v1", description: "Pilot wave, high-risk only" },
    pros: "high event value", cons: "small cohort | long horizon | consent friction" },
];

display(Inputs.table(campaigns, {
  format: {
    campaign: L.formatTextBold(),
    status: L.formatStatus(),
    outreach: L.sparkbar(d3.max(campaigns, (d) => d.outreach)),
    spendDelta: L.formatCurrency(),
    rate: L.formatPercent(1),
    version: L.formatTwoLevel(),
    pros: L.formatProsList(),
    cons: L.formatConsList(),
  },
  rows: 6,
}));
```

The contracts, one line each:

- **`sparkbar(max, color?)`** — inline bar scaled against `max`.
- **`formatStatus({bad})`** — green/red pill; states listed in `bad` go red.
- **`formatTwoLevel()`** — `{header, description}` cells render bold-over-muted.
- **`formatWrappedText(options)`** — CSS-wrapped text; `charLimit` sets an
  em-based max width, plus font/alignment/background knobs.
- **`formatTextBold(options)`** — bold wrap; packed strings (`"a | b"`)
  become bulleted lists automatically.
- **`formatHeaderTextBold(text, charLimit)`** — for table headers.
- **`formatBulletedList(options)`** — arrays or delimited strings to
  bullets; auto-detects existing ✓/× markers and preserves them; any
  bullet glyph, color, spacing.
- **`formatProsList()` / `formatConsList()` / `formatNeutralList()`** —
  green-check, red-×, and muted-bullet presets.
- **`withRowHeight(height, verticalAlign)(formatter)`** — composable
  min-height + alignment wrapper around any formatter.
- **`formatCurrency({currency, digits})`** — right-aligned tabular
  currency; negatives in clinical red.
- **`formatPercent(digits)`** — right-aligned fixed-digit percent.

## Composing a page

The full stack in order, which is also the order the docs pages
themselves use:

```js run=false
// 1. front matter: theme: [air, wide], toc as needed
// 2. style, once
import { injectPageStyle, splitPanel, prose, kpiCard, cardRow } from "observable-dataframe/layouts";
injectPageStyle();
// 3. hero
html`<div class="page-title-header">
  <div class="page-title">The page's one claim</div>
  <div class="divider"></div>
  The sentence that earns the scroll.
</div>`
// 4. data → frames → figures (plots) → tables (formatters)
// 5. sections composed with splitPanel / stackPanel / tabPanel
```
