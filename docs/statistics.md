---
toc: true
theme: [air, wide]
title: Panel data & DiD
---

```js
import { injectPageStyle } from "observable-dataframe/layouts";
injectPageStyle();
```

<div class="page-title-header">
  <div class="page-title">Did the program work? A difference-in-differences walkthrough</div>
  <div class="divider"></div>
  Claims data in, causal estimate out, every step visible. Built on the
  DataFrame, validated against
  <a href="https://github.com/igerber/diff-diff">diff-diff</a>, and run
  entirely in your browser.
</div>

```js
import { DataFrame, col } from "observable-dataframe";
import {
  did, twfe, eventStudy, callawaySantAnna,
  checkParallelTrends, placeboTest, tableOne,
} from "observable-dataframe/stats";
import {
  didPlot, eventStudyPlot, tufteLine, consortDiagram,
} from "observable-dataframe/plots";
```

I want to convince you of one thing before we go anywhere else: **you can
run a publication-shaped causal analysis on claims data without leaving
the browser**, and the discipline that makes it publication-shaped is not
the estimator. It is the boring furniture around the estimator: knowing
exactly who is in your study, showing that your arms looked alike before
you touched anything, and checking that your design finds nothing where
nothing exists. The estimate is the last five percent. By the end of this
page you will have watched a member-month panel go from raw cohort to a
defended treatment effect, and every number along the way will reconcile.

Here is what I am going to do:

1. Introduce the claims data and build one unifying member-month panel.
2. Draw the CONSORT flow diagram, so the denominators are public.
3. Build Table 1, so baseline balance is public too.
4. Estimate the effect four ways: the 2×2, two-way fixed effects, an
   event study, and Callaway–Sant'Anna for the staggered rollout.
5. Try to break my own result with pre-trend and placebo diagnostics.

One dataset, start to finish. If a number appears in a figure, it came
from the same rows as every other figure on this page.

## The data

The setting is a care-management program rolled out in waves. Some
members get outreach starting in month 6, a second wave starts in month
8, and the rest get usual care. The outcome is monthly encounter counts,
the workhorse utilization measure: how many times did this member touch
the system this month?

Claims data does not ship with this package, and the estimators are held to
a standard that does not require it. This page runs on a **simulated panel**:
the schema, cohort structure, and staggered design a member-month panel has,
with a planted effect of −2 encounters per member per month. The
[validation section](#validation-and-a-thank-you) below repeats the whole
exercise on a larger simulated panel whose counts are deliberately as
overdispersed, zero-heavy, and skewed as utilization actually is.
**We know the answer is −2 because we buried it ourselves.** That is the
entire evaluation strategy: if the machinery cannot find treasure it
buried, it has no business hunting anyone else's.

```js echo
// 1,000 members screened, exclusions applied the way a claims pipeline
// applies them, 400 enrolled into a 12-month panel.
const study = (() => {
  let s = 42;
  const rand = () => (s = (s * 1664525 + 1013904223) % 4294967296) / 4294967296;
  const gauss = () => Math.sqrt(-2 * Math.log(1 - rand())) * Math.cos(2 * Math.PI * rand());

  const screened = Array.from({ length: 1000 }, (_, i) => ({
    person_id: `m${i}`,
    age: Math.round(54 + gauss() * 11),
    sex: rand() < 0.55 ? "F" : "M",
    region: ["East", "West", "Central"][i % 3],
    hasClaims: rand() > 0.35,          // 35% have no claims activity in window
    monthsEnrolled: rand() > 0.25 ? 12 : Math.floor(rand() * 6), // 25% churn early
  }));

  const eligible = screened.filter((m) => m.hasClaims && m.monthsEnrolled >= 6);
  const enrolled = eligible.slice(0, 400).map((m, i) => {
    const u = i / 400;
    return { ...m, group: u < 0.25 ? 6 : u < 0.5 ? 8 : null };
  });

  const panelRows = enrolled.flatMap((m) => {
    const level = 6 + gauss() * 2.5 + (m.age - 54) * 0.03;
    return Array.from({ length: 12 }, (_, t) => {
      const seasonal = Math.sin((t / 12) * 2 * Math.PI) * 0.7 + t * 0.05;
      const treatedNow = m.group != null && t >= m.group;
      return {
        person_id: m.person_id, period: t, group: m.group,
        treated: m.group != null ? 1 : 0,
        treated_now: treatedNow ? 1 : 0,
        post: t >= 6 ? 1 : 0,
        encounters: Math.max(0, level + seasonal + (treatedNow ? -2 : 0) + gauss() * 1.8),
      };
    });
  });

  return { screened, eligible, enrolled, panelRows };
})();
const panel = DataFrame.fromRows(study.panelRows);
const baseline = DataFrame.fromRows(
  study.enrolled.map((m) => ({
    ...m,
    arm: m.group == null ? "usual care" : `outreach (t=${m.group})`,
  }))
);
```

The panel is long format, one row per member per month, and every method
on this page consumes exactly this shape:

| column | meaning |
|---|---|
| `person_id` | the unit: who |
| `period` | the time: which month, 0 through 11 |
| `group` | adoption month for treated members; null for usual care |
| `treated_now` | 1 when this member is treated in this month |
| `encounters` | the outcome: encounters this month |

Why panels carry causal weight: observing the same member repeatedly lets
us subtract away everything constant about that member (their level of
illness, their zip code, their fondness for urgent care), and observing
many members in the same month lets us subtract away everything common to
the month (flu season, a formulary change). Two subtractions, and what
remains, **if the parallel trends assumption holds**, is the effect.

## Who is in this study? The CONSORT diagram

Before any estimate: who did we screen, who did we drop, and why? The
CONSORT flow diagram is how trials publish that arithmetic, and claims
studies deserve the same discipline. Every exclusion is a decision that
shapes what population the answer applies to.

```js echo
const excludedNoClaims = study.screened.filter((m) => !m.hasClaims).length;
const excludedChurn = study.screened.filter((m) => m.hasClaims && m.monthsEnrolled < 6).length;
const notSampled = study.eligible.length - study.enrolled.length;

display(consortDiagram({
  title: "Participant flow",
  steps: [
    { label: "Assessed for eligibility", n: study.screened.length },
    {
      label: "Enrolled in panel", n: study.enrolled.length,
      excluded: {
        label: "Excluded", n: excludedNoClaims + excludedChurn + notSampled,
        reasons: [
          { label: "No claims activity in window", n: excludedNoClaims },
          { label: "Enrolled fewer than 6 months", n: excludedChurn },
          { label: "Not sampled", n: notSampled },
        ],
      },
    },
  ],
  arms: [
    { label: "Outreach, wave 1 (t=6)", n: study.enrolled.filter((m) => m.group === 6).length,
      steps: [{ label: "Analyzed", n: study.enrolled.filter((m) => m.group === 6).length }] },
    { label: "Outreach, wave 2 (t=8)", n: study.enrolled.filter((m) => m.group === 8).length,
      steps: [{ label: "Analyzed", n: study.enrolled.filter((m) => m.group === 8).length }] },
    { label: "Usual care", n: study.enrolled.filter((m) => m.group == null).length,
      steps: [{ label: "Analyzed", n: study.enrolled.filter((m) => m.group == null).length }] },
  ],
}));
```

<figure>
<figcaption><strong>Figure 1.</strong> Participant flow. Of 1,000 members
assessed, exclusions for absent claims activity and short enrollment
leave the eligible pool, from which 400 enter the panel: two outreach
waves of 100 (adopting at months 6 and 8) and 200 usual-care members.
Counts at each node reconcile with the node above; the diagram is drawn
by <code>consortDiagram()</code> from the same objects that build the
panel, so it cannot silently disagree with the analysis.</figcaption>
</figure>

The exclusions are doing real work here, and it is worth being blunt
about what they cost: dropping members with no claims activity means the
answer applies to **members who use care**, not to the whole book. That
is usually the population the program targets, but the sentence belongs
in the limitations section, not in a drawer.

## Do the arms look alike? Table 1

The first table of every trial paper answers one question: before
anything happened, were the groups comparable? Means and spreads for
continuous characteristics, counts and percentages for categorical ones,
one column per arm.

```js echo
const t1 = tableOne(baseline, {
  by: "arm",
  continuous: ["age", "monthsEnrolled"],
  categorical: ["sex", "region"],
  labels: { monthsEnrolled: "Months enrolled" },
});
display(Inputs.table(t1.rows, { format: { characteristic: (d) => d } }));
if (t1.note) display(html`<div style="font-size: 11px; color: #888; font-style: italic;">${t1.note}</div>`);
```

<figure>
<figcaption><strong>Figure 2.</strong> Baseline characteristics by arm
(Table 1). Continuous rows report mean (SD) with a Welch two-sample test;
categorical rows report n (%) per level with a chi-square test across all
arms. Assignment here is effectively random, so the arms match on age,
sex, region, and enrollment, and the p-values are unremarkable, which is
exactly what they should be.</figcaption>
</figure>

A caveat I will keep making: under true randomization, baseline p-values
test a null hypothesis you already know to be true, so a "significant"
row is a guaranteed false positive at rate alpha. Reviewers want the
column anyway. The column is there. What actually matters is the size of
the differences, not their stars.

## Step 0: look at the raw trends

*Can you see the effect before any regression touches the data?* If the
cohort means do not show roughly parallel lines before adoption, no
estimator downstream will conjure them.

```js echo
const trends = panel
  .withColumns({ cohort: col("group").fillNull(0).map((g) => (g === 0 ? "usual care" : `outreach t=${g}`)) })
  .groupBy("cohort", "period")
  .agg(col("encounters").mean().alias("mean"))
  .sort("period");
display(tufteLine(trends, { x: "period", y: "mean", stroke: "cohort", height: 280, haloRadius: 5 }));
```

<figure>
<figcaption><strong>Figure 3.</strong> Mean encounters per member per
month by cohort. The three lines track each other through month 5, wave 1
drops after month 6, wave 2 drops after month 8, and usual care keeps its
seasonal wiggle. The vertical gaps after adoption preview the estimate;
the parallel stretch before adoption is the identification argument,
visible to the naked eye.</figcaption>
</figure>

## The 2×2: one difference of differences

Take the simplest slice first: wave 1 versus usual care, before versus
after month 6. Four cell means and one identity. The estimator is

```tex
\widehat{\text{ATT}} = (\bar{y}_{T,\text{post}} - \bar{y}_{T,\text{pre}}) - (\bar{y}_{C,\text{post}} - \bar{y}_{C,\text{pre}})
```

in words: how much the treated group changed, minus how much the control
group changed. The control group's change stands in for what would have
happened to the treated group without the program. That substitution is
the parallel trends assumption wearing its work clothes.

```js echo
const cohort6 = study.panelRows
  .filter((r) => r.group === 6 || r.group === null)
  .map((r) => ({ ...r, treated: r.group === 6 ? 1 : 0 }));

const basic = did(cohort6, {
  outcome: "encounters", treatment: "treated", time: "post", cluster: "person_id",
});
display(html`<pre>${basic.summary()}</pre>`);
```

Grounding the coefficient: an ATT near −2 means treated members average
about two fewer encounters per month after outreach begins, against a
baseline near 6.5, roughly a 30% reduction in monthly utilization. The
standard errors cluster on the member, because a member's months are
correlated with each other and pretending otherwise buys you confidence
you did not earn.

```js echo
display(didPlot(cohort6, {
  outcome: "encounters", treatment: "treated", time: "post",
  yLabel: "encounters / member / month",
}));
```

<figure>
<figcaption><strong>Figure 4.</strong> The 2×2 in one picture. Solid
lines are observed group means, pre and post. The dashed line is the
counterfactual: the treated group's pre-period level advanced along the
control group's slope. The red bracket measures the vertical gap between
observed and counterfactual, which is the ATT; the number on the bracket
is computed from the same four means as the regression, so figure and
table cannot drift apart.</figcaption>
</figure>

## Try to break it: diagnostics

*Would this design find an effect where none exists?* Two checks, both
cheap, both mandatory.

First, differential pre-trends: regress the outcome on time, treatment,
and their interaction, using pre-period data only. The interaction slope
estimates how fast the arms were diverging before anyone did anything.

```js echo
const pt = checkParallelTrends(cohort6, {
  outcome: "encounters", treatment: "treated", time: "period",
  treatmentStart: 6, cluster: "person_id",
});
display(html`<pre>pre-trend slope: ${pt.slope.toFixed(4)} (p = ${pt.pValue.toFixed(3)})
${pt.passed ? "no divergence detected" : "arms were diverging before treatment: stop here"}</pre>`);
```

Second, placebo timing: rerun the 2×2 entirely inside the pre-period with
an invented treatment date. A design that finds a "significant" effect of
a treatment that never happened will happily find one of a treatment that
did.

```js echo
const placebo = placeboTest(cohort6, {
  outcome: "encounters", treatment: "treated", time: "period",
  treatmentStart: 6, cluster: "person_id",
});
display(html`<pre>placebo ATT at fake t=${placebo.placeboStart}: ${placebo.att.toFixed(3)} (p = ${placebo.pValue.toFixed(3)})
${placebo.passed ? "nothing found where nothing was planted" : "the design finds ghosts"}</pre>`);
```

The honest caveat, stated once and meant everywhere: an insignificant
pre-trend test is evidence of absence of evidence, not proof that
parallel trends holds. **Passing these checks earns you the right to
proceed, not the right to stop doubting.**

## Two-way fixed effects

The panel version: absorb a fixed effect for every member and every
month, then ask what the treatment indicator explains on top. We
implement it by iterated demeaning rather than ten thousand dummy
columns, which produces the identical estimate and a much happier matrix.

```js echo
const fe = twfe(panel, {
  outcome: "encounters", treatment: "treated_now", unit: "person_id", time: "period",
});
display(html`<pre>${fe.summary()}</pre>`);
```

TWFE earns a warning label under staggered adoption: with two waves and
heterogeneous effects it quietly uses early adopters as controls for late
ones, with weights nobody agreed to (Goodman-Bacon 2021). Our two waves
have the same planted effect, so TWFE lands close here. Real programs
rarely extend that courtesy, which is why the next two estimators exist.

## The event study

*When does the effect arrive, and was anything moving before adoption?*
Instead of one pooled coefficient, estimate one effect per period
relative to each member's own adoption month, with the month before
adoption (e = −1) as the reference.

```js echo
const es = eventStudy(panel, {
  outcome: "encounters", unit: "person_id", time: "period", group: "group", window: [-5, 5],
});
display(eventStudyPlot(es, { yLabel: "Δ encounters / member / month" }));
```

<figure>
<figcaption><strong>Figure 5.</strong> Event-study coefficients with 95%
confidence intervals, periods indexed relative to adoption. Gray points
left of the adoption rule are pre-period placebo effects and should
straddle zero; navy points from e = 0 onward are the dynamic treatment
effects. The flat left side is the credibility exhibit. The drop of about
two encounters on the right, arriving at e = 0 and holding, is the
finding.</figcaption>
</figure>

The left half of this figure is the pre-trend test with its clothes off:
instead of one summary slope, every pre-period gets its own coefficient
and its own confidence interval, and you can inspect exactly which
periods behave.

## Callaway–Sant'Anna for the staggered rollout

With two waves adopting at different times, the clean approach estimates
a separate effect for every cohort at every period, comparing each
adopting cohort only against members not yet treated or never treated.
No early adopter ever serves as a control for a late one.

```js echo
const cs = callawaySantAnna(panel, {
  outcome: "encounters", unit: "person_id", time: "period", group: "group",
});
display(Inputs.table(cs.byGroup.map((g) => ({
  cohort: `adopts t=${g.group}`,
  att: +g.att.toFixed(3),
  se: +g.se.toFixed(3),
  "95% CI": `[${g.ci[0].toFixed(2)}, ${g.ci[1].toFixed(2)}]`,
  cells: g.nCells,
}))));
display(html`<pre>overall ATT: ${cs.overall.att.toFixed(3)} (se ${cs.overall.se.toFixed(3)}); planted truth: −2</pre>`);
```

```js echo
display(eventStudyPlot(cs, { yLabel: "ATT by event time (Callaway–Sant'Anna)" }));
```

<figure>
<figcaption><strong>Figure 6.</strong> Callaway–Sant'Anna group-time ATTs
aggregated by event time, with 95% intervals. Each point averages the
cohort-specific effects at that distance from adoption, weighted by
cohort size. Both waves recover the planted −2 individually (table
above), and the aggregation agrees with the event study in Figure 5,
which is what two valid estimators looking at the same data owe each
other.</figcaption>
</figure>

For decks and papers, pass `bootstrap: 999` to replace the analytic
aggregate standard errors (which treat cells as independent, and say so
in their docstring) with unit-level bootstrap intervals that respect
cross-cell covariance.

## The plotting API, in one table

Every regression figure speaks the same options lexicon as the rest of
the library (`width`, `tip`, `caption`, scale overrides passed through to
Observable Plot), and each one consumes estimator output directly. No
reshaping step, no transcription step, no transcription errors.

| Figure | Feed it | Shows |
|---|---|---|
| `consortDiagram(config)` | screening counts | participant flow with exclusions |
| `tableOne(df, {by, ...})` | baseline rows | arm comparability (render with `Inputs.table`) |
| `didPlot(rows, {outcome, treatment, time})` | the same rows as `did()` | four means, counterfactual, ATT bracket |
| `eventStudyPlot(result)` | `eventStudy()` or `callawaySantAnna()` output | period effects with CIs |
| `tufteLine(df, {x, y, stroke})` | cohort means from `groupBy` | the raw trends |

## Validation, and a thank-you

The 2×2 reproduces the reference implementation's published quick-start
output digit for digit (ATT = 3.0000, SE = 1.7321, p = 0.1583, with
`vcov: "classical"`).

Every estimator on this page is then checked against a simulated claims panel,
and simulation is the stronger test here rather than the weaker one. On
observational data you can compare estimators to each other and hope they agree;
the quantity they are all estimating is never available. On a panel you
generated, it is a number you can print.

`simulateClaimsPanel()` from `observable-dataframe/data` draws a baseline
intensity per member from a gamma distribution, mixes it into Poisson counts
through a monthly shock, forces a share of member-months to no utilization at
all, and gives spend a lognormal tail. The counts that come out are
overdispersed rather than Poisson, roughly two fifths of member-months are zero,
high utilizers stay high, and enrollment churns. It plants the effect by
thinning: on a treated member-month each would-be encounter is dropped
independently with probability 0.4, phased in over three months. Because the
thinning removes encounters from a draw the generator is already holding, the
untreated counterfactual survives for every treated cell, so the average
treatment effect on the treated is **measured, not assumed**.

The panel the [data panel page](./data-panel) publishes is that generator at
seed 42: 1,500 members over 24 months, three cohorts of 300 adopting at months
8, 12, and 16, and 600 members who never adopt. Cohort assignment is independent
of member intensity, so parallel trends holds by construction. Restricted to
enrolled member-months, the planted effect works out to **−1.643 encounters per
treated member-month** across the whole post period, and **−1.802** once the
phase-in completes.

| Estimator | Recovered | 95% interval | Planted truth | Covers it |
|---|---|---|---|---|
| 2×2, first cohort vs never-treated | −1.795 (SE 0.256) | [−2.30, −1.29] | −1.862, that cohort | yes |
| Two-way fixed effects | −1.523 (SE 0.129) | [−1.78, −1.27] | −1.643 | yes |
| Event study, average over e ≥ 2 | −1.999 | per-period, below | −1.802 | yes |
| Callaway–Sant'Anna, analytic SE | −2.059 (SE 0.106) | [−2.27, −1.85] | −1.643 | **no** |
| Callaway–Sant'Anna, `bootstrap: 999` | −2.059 (SE 0.290) | [−2.63, −1.49] | −1.643 | yes |

Two of those rows deserve more than a checkmark.

**TWFE comes back about 7% short, and that is the literature rather than a
bug.** Repeating the exercise across twenty seeds, TWFE is biased toward zero by
0.08 encounters on average, about four standard errors of that mean, while
Callaway–Sant'Anna and the event study show no bias detectable at this panel
size. Collapsing the phase-in to a single period removes the TWFE bias entirely
(−0.002). So does collapsing the three cohorts to one adoption date (+0.046, not
distinguishable from zero). The attenuation therefore requires both staggered
adoption and dynamic effects together, which is exactly the condition
Goodman-Bacon (2021) describes, and exactly why `callawaySantAnna()` exists.

**Callaway–Sant'Anna's analytic interval misses the truth, and its own
docstring says why.** Aggregating ATT(g,t) cells analytically treats them as
independent, and they are not: every cell for a cohort is differenced against
the same base period, so noise in that one base mean shifts the whole cohort
together and never averages out across periods. The independence approximation
reports SE 0.106 where the unit-level bootstrap reports 0.290, and only the
bootstrap interval covers the truth. Each cohort's own planted effect (−1.862,
−1.527, and −1.375, falling because later adopters spend less of the window
fully phased in) does land inside its bootstrap interval. Pass `bootstrap: 999`
whenever the interval is load-bearing.

The diagnostics have nothing to find, which is the point of running them where
nothing is buried. The pre-trend slope on the first cohort is 0.021 (SE 0.083,
p = 0.80). The placebo, with an invented treatment date at month 4, returns
−0.065 (p = 0.86). Every event-study pre-period coefficient sits within 0.483 of
zero and none reaches significance at the 5% level, against a real effect near
−1.8. Across twenty seeds the pre-trend test rejected at the 5% level zero times
and the placebo once, which is the nominal rate behaving as advertised.

One honest limit. Callaway–Sant'Anna's individual pre-period cells are noisy at
this panel size, reaching 1.7 in absolute value even though they average to
−0.06. Cohort precision scales with members per cohort and not at all with the
number of months, for the base-period reason above, so the remedy is more
members rather than a longer window. 1,500 is where we stopped: a panel large
enough to pin those cells down individually is larger than a documentation site
should ship as a static file. Read the aggregates, and bootstrap them.

The estimator selection, the API shape, and the practitioner workflow
here are modeled on
[diff-diff](https://github.com/igerber/diff-diff) by Isaac Gerber and
contributors: Difference-in-Differences causal inference in Python, with
Callaway–Sant'Anna, Synthetic DiD, Honest DiD, event studies, and far
more, validated against R. We implemented the core four estimators and
diagnostics in JavaScript because we wanted them next to the DataFrame;
for anything beyond (Sun–Abraham, Synthetic DiD, Honest DiD sensitivity
bounds, survey designs), use the original. It is excellent, and this page
exists because their design was worth imitating. Thank you.

## Reproducing this

Everything on this page is generated, and generated deterministically. The
walkthrough above runs on the inline panel in the first code block; the
validation table runs on `simulateClaimsPanel({ seed: 42 })`, which is the same
panel the docs site publishes and the same panel
`test/simulate-claims-panel.test.js` holds to both its distributional shape and
its planted effect. No warehouse, no extract, no credential:

```bash
npm test
npm run docs:build
```

One dataset, one flow diagram, one Table 1, four estimators, two
diagnostics, and an answer we can check because we planted it. The estimator was
never the hard part.
