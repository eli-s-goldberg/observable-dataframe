---
toc: true
theme: [air, wide]
title: "API: stats"
---

```js
import { injectPageStyle } from "observable-dataframe/layouts";
injectPageStyle();
```

<div class="page-title-header">
  <div class="page-title">API reference: observable-dataframe/stats</div>
  <div class="divider"></div>
  Every function and class in the statistics module: special functions,
  power and experiment design, hypothesis tests, regression, panel-data
  causal inference, Monte Carlo distributions, and the unified experiment
  designer. Pure functions, no DOM, no CDN.
</div>

This module answers *could we detect it* (power) and *did it happen* (DiD
and friends). The `DistP` distributions here are the building block for
propagating uncertainty through any downstream calculation.

```js
import * as Plot from "npm:@observablehq/plot";
import { DataFrame, col } from "observable-dataframe";
import * as stats from "observable-dataframe/stats";
import { eventStudyPlot, distPlot, designMatrixPlot, tufteLine } from "observable-dataframe/plots";
```

## Special functions

The polynomial bedrock every p-value stands on.

### probit(p) / normalCDF(z)

Inverse and forward standard normal CDF.

```js echo
[stats.probit(0.975).toFixed(4), stats.normalCDF(1.96).toFixed(4)]
```

### studentTCDF(t, df) / fCDF(f, d1, d2) / tTwoSidedP(t, df)

Student-t and F distribution functions, and the two-sided p-value people
skip to.

```js echo
[stats.studentTCDF(2.0, 10).toFixed(4), stats.tTwoSidedP(2.0, 10).toFixed(4), stats.fCDF(4.0, 2, 20).toFixed(4)]
```

### gammaln(x) / gammainc(a, x) / chi2CDF(x, k) / ibeta(x, a, b)

Log-gamma, regularized incomplete gamma, chi-square CDF (what Table 1's
categorical tests run on), and the incomplete beta underneath t and F.

```js echo
[stats.chi2CDF(3.841, 1).toFixed(4), stats.gammaln(5).toFixed(4)]
```

## Power & sample size

### sampleSizeTwoProportions({p1, p2, alpha, power, sided})

The classic per-arm n for a difference in rates, pooled-variance z
formula.

```js echo
stats.sampleSizeTwoProportions({ p1: 0.10, p2: 0.15, alpha: 0.05, power: 0.8 })
```

### sampleSizeTwoMeans({mu1, mu2, sigma, alpha, power, sided})

```js echo
stats.sampleSizeTwoMeans({ mu1: 0, mu2: 0.5, sigma: 1 })
```

### standardizedMeanDifference(mean1, mean2, sd)

Cohen's d.

```js echo
stats.standardizedMeanDifference(10, 12, 4)
```

### powerAnalysis(options)

The campaign engine behind the experiment-design tree: base rate P1,
behavior change D, attributable fraction F → P2 and n per arm, across
designs (one/two-sided proportions, chi-square, difference-in-differences
with 1.5× inflation, single-arm).

```js echo
stats.powerAnalysis({ baseRate: 0.19, behaviorChange: 0.0063, alpha: 0.1, power: 0.8, design: "one-sided-proportions" })
```

### evaluateCadence({channels, plan, baseRate, perArm})

Channel touches → compounded lift → required n → can your cohort measure
what you're buying.

```js echo
stats.evaluateCadence({
  channels: [{ key: "dm", cost: 1.2, behaviorChange: 0.01 }, { key: "email", cost: 0.1, behaviorChange: 0.004 }],
  plan: [[true], [true]],
  baseRate: 0.19,
  perArm: 24000,
})
```

## Cluster & stepped-wedge designs

### varianceIndividual(sigma, n) / varianceCluster(sigma, k, m, rho) / designEffect(m, rho)

The design-effect arithmetic: 1 + (m−1)ρ is the tax for randomizing
clinics when you wanted to randomize people.

```js echo
[stats.designEffect(100, 0.01).toFixed(2), stats.varianceCluster(2, 10, 20, 0.05).toFixed(4)]
```

### varianceComparisonData(variable, range, fixed)

Long-format sweep data for plotting individual vs cluster variance.

```js echo
const sweep = stats.varianceComparisonData("rho", [0, 0.05, 0.1, 0.15, 0.2], { sigma: 2, k: 10, m: 20, n: 200 });
display(Plot.plot({
  height: 200, y: { label: "variance", ticks: 4 },
  color: { legend: true },
  marks: [Plot.line(sweep, { x: "x", y: "variance", stroke: "variance_type" })],
}));
```

### designMatrix(type, periods) / designMatrixData(type, periods)

Trial layouts as 0/1 matrices (parallel, before-after, cross-over,
stepped-wedge, multi cross-over), and the long-format rows the plot eats.

```js echo
stats.designMatrix("Stepped-wedge", 4)
```

```js echo
display(designMatrixPlot(stats.designMatrixData("Stepped-wedge", 5), { width: 380 }));
```

### varthetaM(options)

GLS variance of the treatment effect for repeated-measures cluster
designs with correlation decay (after Hemming et al.). Smaller is better;
compare designs at fixed resources.

```js echo
({
  steppedWedge: stats.varthetaM({ m: 20, design: "Stepped-wedge", periods: 6, icc: 0.05, cac: 0.8 }).toFixed(4),
  parallel: stats.varthetaM({ m: 20, design: "Parallel", periods: 6, icc: 0.05, cac: 0.8 }).toFixed(4),
})
```

## Hypothesis tests & regression

All accept a DataFrame or rows.

### oneSampleTTest(data, column, mu) / twoSampleTTest(...) / welchTTest(...)

```js echo
const groups = DataFrame.fromRows([
  ...[10, 12, 11, 13].map((v) => ({ g: "A", v })),
  ...[15, 17, 16, 18].map((v) => ({ g: "B", v })),
]);
({
  one: stats.oneSampleTTest(groups, "v", 12).pValue.toFixed(4),
  pooled: stats.twoSampleTTest(groups, "v", "g", "A", "B").pValue.toFixed(5),
  welch: stats.welchTTest(groups, "v", "g", "A", "B").df.toFixed(1),
})
```

### ols(data, {dependentVar, predictors}) / ancova(data, {dependentVar, covariates, groupVar})

OLS with intercept, and the covariate-adjusted group test via partial F.

```js echo
const fit = stats.ols(
  [1, 2, 3, 4, 5].map((x) => ({ x, y: 2 + 3 * x + (x % 2) * 0.1 })),
  { dependentVar: "y", predictors: ["x"] }
);
fit.terms.map((t, i) => `${t}: ${fit.beta[i].toFixed(3)} (se ${fit.se[i].toFixed(3)})`)
```

### fitOLS(X, y, {vcov, clusters, terms})

The raw engine: design matrix in, coefficients out, with classical, HC1,
or CR1 cluster-robust covariance. Everything in the DiD module runs on it.

```js echo
stats.fitOLS([[1, 0], [1, 1], [1, 2], [1, 3]], [1, 3, 5, 7], { vcov: "classical", terms: ["const", "x"] }).beta
```

### withinTransform(values, unitIds, timeIds)

Two-way demeaning for panels: how TWFE absorbs ten thousand fixed effects
without materializing one dummy.

```js echo
stats.withinTransform([1, 2, 3, 4], ["a", "a", "b", "b"], [0, 1, 0, 1]).map((v) => +v.toFixed(6))
```

### tableOne(data, {by, continuous, categorical, labels})

The baseline characteristics table: mean (SD) with Welch tests, n (%)
with chi-square.

```js echo
const baseline = DataFrame.fromRows(Array.from({ length: 100 }, (_, i) => ({
  arm: i % 2 ? "treat" : "control", age: 50 + (i % 17), sex: i % 3 ? "F" : "M",
})));
Inputs.table(stats.tableOne(baseline, { by: "arm", continuous: ["age"], categorical: ["sex"] }).rows)
```

## Panel data & causal inference

The DiD estimator set. Signatures and a compact example each:

```js
const panelRows = (() => {
  let s = 5;
  const rand = () => (s = (s * 1664525 + 1013904223) % 4294967296) / 4294967296;
  const g = () => Math.sqrt(-2 * Math.log(1 - rand())) * Math.cos(2 * Math.PI * rand());
  const rows = [];
  for (let u = 0; u < 200; u++) {
    const grp = u % 2 ? (u % 4 === 1 ? 4 : 6) : null;
    const level = g() * 2;
    for (let t = 0; t < 10; t++) {
      rows.push({ unit: `u${u}`, t, group: grp, treated: grp != null ? 1 : 0,
        d: grp != null && t >= grp ? 1 : 0, post: t >= 4 ? 1 : 0,
        y: 10 + level + t * 0.4 + (grp != null && t >= grp ? 3 : 0) + g() });
    }
  }
  return rows;
})();
```

### did(data, {outcome, treatment, time, covariates, cluster, vcov})

The 2×2. `vcov: "classical"` reproduces diff-diff's published output.

```js echo
stats.did(panelRows.filter((r) => r.group === 4 || r.group === null),
  { outcome: "y", treatment: "treated", time: "post", cluster: "unit" }).summary()
```

### twfe(data, {outcome, treatment, unit, time, cluster})

```js echo
stats.twfe(panelRows, { outcome: "y", treatment: "d", unit: "unit", time: "t" }).att.toFixed(3)
```

### eventStudy(data, {outcome, unit, time, group, reference, window})

```js echo
const es = stats.eventStudy(panelRows, { outcome: "y", unit: "unit", time: "t", group: "group", window: [-3, 4] });
display(eventStudyPlot(es, { height: 240 }));
```

### callawaySantAnna(data, {outcome, unit, time, group, control, bootstrap})

```js echo
const cs = stats.callawaySantAnna(panelRows, { outcome: "y", unit: "unit", time: "t", group: "group" });
({ overall: cs.overall.att.toFixed(3), cohorts: cs.byGroup.map((g) => `t=${g.group}: ${g.att.toFixed(2)}`) })
```

### checkParallelTrends(...) / placeboTest(...)

```js echo
({
  preTrend: stats.checkParallelTrends(
    panelRows.filter((r) => r.group === 4 || r.group === null),
    { outcome: "y", treatment: "treated", time: "t", treatmentStart: 4 }
  ).passed,
  placebo: stats.placeboTest(
    panelRows.filter((r) => r.group === 6 || r.group === null),
    { outcome: "y", treatment: "treated", time: "t", treatmentStart: 6 }
  ).passed,
})
```

## Distributions & Monte Carlo

### distributions

Native samplers: `normal`, `uniform`, `exponential`, `lognormal`
(real-space mean + shape), `triangular`, `beta`, `gamma`, `weibull`,
`custom` (resample an array). Each takes a params object, returns a draw.

```js echo
Array.from({ length: 5 }, () => +stats.distributions.beta({ alpha: 2, beta: 6 }).toFixed(3))
```

### DistP

The Monte Carlo distribution class: sample, bound, chain element-wise
(`chainMult/Divide/Add/Sub`, `multConst`), inspect (`stats`, `confInt`),
`copy()` before mutating chains.

```js echo
const conversion = new stats.DistP({ name: "conversion", distfunc: stats.distributions.beta, params: { alpha: 20, beta: 180 }, size: 8000, bounds: [0, 1] });
const value = new stats.DistP({ name: "value", distfunc: stats.distributions.lognormal, params: { mean: 120, shape: 0.4 }, size: 8000, bounds: [0, 2000] });
const impact = conversion.copy().chainMult(value).multConst(10000);
({ mean: impact.stats.mean.toFixed(0), ci: impact.confInt().map((v) => v.toFixed(0)) })
```

```js echo
display(distPlot(impact, { label: "program impact ($)", markers: ["mean", 0.05, 0.95], labelDigits: 0, height: 200 }));
```

### kde(samples, {bandwidth, cut, n}) / silvermanBandwidth(samples)

Gaussian kernel density on a grid, Silverman's rule by default, seaborn's
`cut` convention for tail extension. `distPlot`'s `kind: "kde"` runs on it.

```js echo
stats.kde([1, 2, 2, 3, 3, 3, 4], { cut: 0, n: 5 }).points.map((p) => ({ x: +p.x.toFixed(2), density: +p.density.toFixed(3) }))
```

## The experiment design system

### ExperimentDesign / Experiment

The unified designer: `fromPanel` measures strata rates, population, and
the CUPED correlation from data; `stratify`, `arms` (direct effects or
channel cascades), `power` (standard/cuped/bayesian), `design`, `build`.
The built Experiment renders the design tree, timeline, and CONSORT
configs, prices the business case, and Monte Carlo-validates itself.

```js echo
const experiment = stats.ExperimentDesign.fromAssumptions()
  .stratify([{ name: "High Risk", baseRate: 0.18, available: 40000 }])
  .arms([{ name: "Outreach", relativeEffect: -0.1, costPerMember: 4 }])
  .power({ alpha: 0.05, power: 0.8, method: "cuped", correlation: 0.5 })
  .design("rct")
  .build();
display(html`<pre>${experiment.describe()}</pre>`);
```

### sampleSizePerArm({p1, p2, alpha, power, effectSize, method, correlation})

The designer's calculator, standalone: Cohen's h (arcsine) or pooled
effect sizes; CUPED and Bayesian n reductions.

```js echo
({
  standard: stats.sampleSizePerArm({ p1: 0.18, p2: 0.162 }).nPerArm,
  cuped: stats.sampleSizePerArm({ p1: 0.18, p2: 0.162, method: "cuped", correlation: 0.5 }).nPerArm,
})
```

### multiArmAdjustment(nArms) / channelCascade(channels)

The k/(k−1)-family multi-arm tax and the reach × open × efficacy cascade.

```js echo
({
  threeArm: stats.multiArmAdjustment(3),
  cascade: stats.channelCascade([
    { reach: 1, open: 0.7, efficacy: 0.03, cost: 1 },
    { reach: 0.9, open: 0.3, efficacy: 0.07, cost: 3 },
  ]),
})
```

## Matrix helpers

Dense row-major arrays, experiment-design sized: `identity(n)`,
`matmul(A, B)`, `matvec(A, v)`, `dot(a, b)`, `inv(A)` (throws on
singular), `transpose(A)`.

```js echo
stats.matmul(stats.inv([[4, 7], [2, 6]]), [[4, 7], [2, 6]]).map((row) => row.map((v) => +v.toFixed(6)))
```
