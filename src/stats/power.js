/**
 * power.js — sample size and power for experiments people actually run.
 *
 * Three layers, roughly in order of how often you'll reach for them:
 *
 *   1. Classic closed-form sample sizes (two proportions, two means).
 *   2. The campaign power engine — base event rate, behavior change,
 *      attributable fraction, design choice → n per arm. This is the math
 *      behind the "experiment design tree" figure: the one that finally
 *      makes leadership understand why the underpowered pilot they want
 *      cannot be measured.
 *   3. Cluster/stepped-wedge machinery: design matrices, ICC-adjusted
 *      variances, and GLS treatment-effect variance (vartheta) for
 *      repeated-measures cluster designs (after Hemming et al.).
 */

import { probit } from "./special.js";
import { identity, inv, matvec, dot } from "./matrix.js";

// ---------------------------------------------------------------------------
// 1. Classic closed forms
// ---------------------------------------------------------------------------

/**
 * Sample size per arm to detect a difference between two proportions.
 * The workhorse formula, with the pooled-variance z-approximation.
 *
 * @param {{p1: number, p2: number, alpha?: number, power?: number, sided?: 1|2}} options
 * @returns {number} n per arm, ceiled, because 0.4 of a member can't consent
 */
export function sampleSizeTwoProportions({ p1, p2, alpha = 0.05, power = 0.8, sided = 2 }) {
  const zA = probit(1 - alpha / sided);
  const zB = probit(power);
  const delta = Math.abs(p2 - p1);
  if (delta === 0) return Infinity; // no effect, no experiment, no amount of n
  const pbar = (p1 + p2) / 2;
  const num = zA * Math.sqrt(2 * pbar * (1 - pbar)) + zB * Math.sqrt(p1 * (1 - p1) + p2 * (1 - p2));
  return Math.ceil((num * num) / (delta * delta));
}

/**
 * Sample size per arm to detect a difference between two means.
 *
 * @param {{mu1: number, mu2: number, sigma: number, alpha?: number, power?: number, sided?: 1|2}} options
 */
export function sampleSizeTwoMeans({ mu1, mu2, sigma, alpha = 0.05, power = 0.8, sided = 2 }) {
  const zA = probit(1 - alpha / sided);
  const zB = probit(power);
  const delta = Math.abs(mu2 - mu1);
  if (delta === 0) return Infinity;
  return Math.ceil((2 * (zA + zB) ** 2 * sigma ** 2) / delta ** 2);
}

/** Cohen's d, i.e. how big the effect is once you stop flattering it with raw units. */
export function standardizedMeanDifference(mean1, mean2, sd) {
  return (mean2 - mean1) / sd;
}

// ---------------------------------------------------------------------------
// 2. Campaign power engine
// ---------------------------------------------------------------------------

/** @typedef {"one-sided-proportions"|"two-sided-proportions"|"chi-square"|"difference-in-differences"|"single-arm"} PowerDesign */

/**
 * Power calculation linking experimental design, event rates, and required
 * sample size — the engine behind the experiment-design-tree figure.
 *
 * The model: a base population converts at rate `baseRate` (P1). Your
 * intervention shifts behavior by `behaviorChange` (D), of which only
 * `attributable` (F) is detectable in your metric, giving a target rate
 * P2 = P1 + D×F. From there it's the two-proportion formula, with a 1.5×
 * inflation for difference-in-differences (two differences, twice the
 * noise, more than twice the meetings).
 *
 * @param {object} options
 * @param {number} options.baseRate P1 — event rate in the base population
 * @param {number} options.behaviorChange D — absolute rate lift you believe your intervention causes
 * @param {number} [options.attributable=1] F — fraction of D observable in this metric (0–1)
 * @param {PowerDesign} [options.design="one-sided-proportions"]
 * @param {number} [options.alpha=0.05]
 * @param {number} [options.power=0.8]
 * @returns {{zAlpha: number, zBeta: number, p1: number, p2: number, delta: number,
 *            attributable: number, notObserved: number, nPerArm: number, arms: number,
 *            totalN: number, design: PowerDesign}}
 */
export function powerAnalysis({
  baseRate,
  behaviorChange,
  attributable = 1,
  design = "one-sided-proportions",
  alpha = 0.05,
  power = 0.8,
}) {
  const twoSided = design === "two-sided-proportions" || design === "chi-square";
  const singleArm = design === "single-arm";
  const did = design === "difference-in-differences";

  const effAlpha = twoSided ? alpha / 2 : alpha;
  const zAlpha = probit(1 - effAlpha);
  const zBeta = probit(power);

  const p1 = baseRate;
  const effChange = behaviorChange * attributable;
  const p2 = p1 + effChange;
  const delta = Math.abs(effChange) || 1e-6;
  const pbar = (p1 + p2) / 2;

  let nPerArm;
  let arms;
  if (singleArm) {
    nPerArm = Math.ceil(((zAlpha + zBeta) ** 2 * pbar * (1 - pbar)) / delta ** 2);
    arms = 1;
  } else {
    const num =
      zAlpha * Math.sqrt(2 * pbar * (1 - pbar)) +
      zBeta * Math.sqrt(p1 * (1 - p1) + p2 * (1 - p2));
    let n = (num * num) / (delta * delta);
    if (did) n *= 1.5; // two differences means extra variance; the discount rack has no free lunch
    nPerArm = Math.ceil(n);
    arms = 2;
  }

  return {
    zAlpha,
    zBeta,
    p1,
    p2,
    delta,
    attributable,
    notObserved: 1 - attributable,
    nPerArm,
    arms,
    totalN: nPerArm * arms,
    design,
  };
}

/**
 * Evaluate a channel-cadence plan: which outreach touches are on, what they
 * cost, what lift they compound to, and whether the cohort you have is big
 * enough to measure the lift you're buying.
 *
 * The lift model is multiplicative survival: each touch independently
 * converts a fraction of the not-yet-converted, so D = 1 − Π(1 − bc). Naive?
 * Yes. Wrong? Also probably. But it's monotone, bounded, and explainable in
 * one slide, which beats most alternatives on the criteria that matter here.
 *
 * @param {object} options
 * @param {Array<{key: string, cost: number, behaviorChange: number}>} options.channels
 * @param {boolean[][]} options.plan plan[channelIndex][waveIndex] — is this touch on?
 * @param {number} options.baseRate P1 for the cohort
 * @param {number} options.perArm members available per arm
 * @param {number} [options.alpha=0.1]
 * @param {number} [options.power=0.8]
 * @returns {{costPerMember: number, lift: number, requiredPerArm: number, meetsPower: boolean,
 *            campaignCost: number, touches: number}}
 */
export function evaluateCadence({ channels, plan, baseRate, perArm, alpha = 0.1, power = 0.8 }) {
  let costPerMember = 0;
  let keep = 1;
  let touches = 0;
  channels.forEach((ch, ci) => {
    for (const on of plan[ci]) {
      if (!on) continue;
      costPerMember += ch.cost;
      keep *= 1 - ch.behaviorChange;
      touches++;
    }
  });
  const lift = 1 - keep;
  const requiredPerArm =
    lift > 0
      ? sampleSizeTwoProportions({ p1: baseRate, p2: baseRate + lift, alpha, power, sided: 1 })
      : Infinity;
  return {
    costPerMember,
    lift,
    requiredPerArm,
    meetsPower: perArm >= requiredPerArm,
    campaignCost: costPerMember * perArm,
    touches,
  };
}

// ---------------------------------------------------------------------------
// 3. Cluster & stepped-wedge designs
// ---------------------------------------------------------------------------

/**
 * Variance of a mean under individual randomization: σ²/n. Included mostly
 * so the cluster version below has something to be compared against.
 */
export function varianceIndividual(sigma, n) {
  return (sigma * sigma) / n;
}

/**
 * Variance of a mean under cluster randomization: (σ²/km)·(1 + (m−1)ρ).
 * The (1 + (m−1)ρ) term is the design effect — the tax you pay for
 * randomizing clinics when you wanted to randomize people.
 *
 * @param {number} sigma total SD
 * @param {number} k number of clusters
 * @param {number} m individuals per cluster
 * @param {number} rho intraclass correlation (ICC)
 */
export function varianceCluster(sigma, k, m, rho) {
  return ((sigma * sigma) / (k * m)) * (1 + (m - 1) * rho);
}

/** The design effect on its own: 1 + (m−1)ρ. Small ρ, big m, big regret. */
export function designEffect(m, rho) {
  return 1 + (m - 1) * rho;
}

/**
 * Long-format variance comparison data for plotting individual vs cluster
 * randomization as one of {n, k, m, rho} sweeps and the rest stay fixed.
 * Output rows: {x, variance, variance_type} — Plot.line-ready.
 *
 * @param {"n"|"k"|"m"|"rho"} variable which knob sweeps
 * @param {number[]} range values for the sweeping knob
 * @param {{sigma: number, k: number, m: number, rho: number, n: number}} fixed the other knobs
 * @returns {Array<{x: number, variance: number, variance_type: string}>}
 */
export function varianceComparisonData(variable, range, { sigma, k, m, rho, n }) {
  const out = [];
  for (const value of range) {
    const vi =
      variable === "n" ? varianceIndividual(sigma, value) : varianceIndividual(sigma, n);
    const vc = varianceCluster(
      sigma,
      variable === "k" ? value : k,
      variable === "m" ? value : m,
      variable === "rho" ? value : rho
    );
    out.push(
      { x: value, variance: vi, variance_type: "Individual Variance" },
      { x: value, variance: vc, variance_type: "Cluster Variance" },
      { x: value, variance: vc - vi, variance_type: "Variance Delta" }
    );
  }
  return out;
}

/**
 * 0/1 design matrix for a trial layout: rows are sequences (cluster groups),
 * columns are periods, 1 means "treated". Feed it to Plot.cell for the
 * classic stepped-wedge staircase, or to varthetaM below for variance.
 *
 * @param {"Parallel"|"Before and After"|"Cross-over"|"Stepped-wedge"|"Multi cross-over"} type
 * @param {number} [periods=1]
 * @returns {number[][]}
 */
export function designMatrix(type, periods = 1) {
  switch (type) {
    case "Parallel":
      return periods === 1
        ? [[0], [1]]
        : [Array(periods).fill(0), Array(periods).fill(1)];
    case "Before and After":
      return [
        [0, 0],
        [0, 1],
      ];
    case "Cross-over":
      return [
        [0, 1],
        [1, 0],
      ];
    case "Stepped-wedge": {
      // The staircase: sequence i turns on at period i+1 and stays on.
      const rows = Array.from({ length: periods - 1 }, () => Array(periods).fill(0));
      for (let i = 0; i < rows.length; i++) {
        for (let j = i + 1; j < periods; j++) rows[i][j] = 1;
      }
      return rows;
    }
    case "Multi cross-over":
      return [
        Array.from({ length: periods }, (_, i) => i % 2),
        Array.from({ length: periods }, (_, i) => (i + 1) % 2),
      ];
    default:
      throw new Error(`Unknown design type "${type}".`);
  }
}

/**
 * Design matrix as long-format rows for plotting:
 * {sequence, period, treated}. Because Plot.cell doesn't read matrices,
 * and you shouldn't have to write this loop again.
 */
export function designMatrixData(type, periods = 1) {
  const matrix = designMatrix(type, periods);
  const out = [];
  matrix.forEach((row, i) => {
    row.forEach((treated, j) => {
      out.push({ sequence: `Seq ${i + 1}`, period: j + 1, treated });
    });
  });
  return out;
}

/**
 * GLS variance of the treatment effect for repeated-measures cluster
 * designs with correlation decay — vartheta, after Hemming/Hughes and
 * Karla Hemming's lecture notes. Smaller is better; compare designs at
 * fixed resources and let the matrix algebra referee.
 *
 * @param {object} options
 * @param {number} options.m individuals per cluster-period
 * @param {string} options.design design type for designMatrix()
 * @param {number} options.periods number of periods
 * @param {number} [options.clustersPerSequence=1] Krep — clusters per sequence
 * @param {number} options.icc within-period intraclass correlation
 * @param {number} [options.cac=1] cluster autocorrelation (decay r between periods)
 * @param {number} [options.iac=0] individual autocorrelation (cohort vs cross-section)
 * @param {number} [options.sd=1] total SD
 * @returns {number} variance of the treatment effect estimate
 */
export function varthetaM({
  m,
  design,
  periods,
  clustersPerSequence = 1,
  icc,
  cac = 1,
  iac = 0,
  sd = 1,
}) {
  const totalvar = sd * sd;
  const sig2CP = icc * totalvar;
  const r = cac;

  let sig2E = (1 - iac) * (totalvar - sig2CP);
  let sig2 = sig2E / m;
  let sigindiv = iac === 1 ? 0 : (sig2E * iac) / ((1 - iac) * m);
  if (iac === 0) {
    sig2E = totalvar - sig2CP;
    sig2 = sig2E / m;
    sigindiv = 0;
  }
  if (iac === 1) {
    sig2 = 0;
    sigindiv = (totalvar - sig2CP) / m;
  }

  const X = designMatrix(design, periods);
  const T = X[0].length;
  const K = X.length;

  // Within-cluster covariance across periods, correlation decaying as r^|i-j|.
  const Vi = Array.from({ length: T }, (_, i) =>
    Array.from({ length: T }, (_, j) => sigindiv + (i === j ? sig2 : 0) + sig2CP * r ** Math.abs(i - j))
  );
  const Vinv = inv(Vi);

  // kron(I_K, Vinv) is block-diagonal, so x' kron(I,Vinv) x collapses to a
  // per-sequence sum. Same math as the mathjs original, minus the giant
  // matrix nobody needed to materialize.
  let part2 = 0;
  for (let k = 0; k < K; k++) part2 += dot(X[k], matvec(Vinv, X[k]));

  const colSums = X[0].map((_, j) => X.reduce((acc, row) => acc + row[j], 0));
  const part4 = dot(colSums, matvec(Vinv, colSums));

  const vartheta = 1 / (part2 - part4 / K);
  return vartheta / clustersPerSequence;
}

// identity is re-exported for design-matrix demos in docs
export { identity };
