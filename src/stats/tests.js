/**
 * tests.js — hypothesis tests and regression, DataFrame-aware.
 *
 * Every function accepts either a DataFrame or an array of row objects,
 * because half your data will arrive as one and half as the other, and
 * a statistics library that makes you convert first isn't a library,
 * it's a chore. All p-values are computed natively (see special.js) —
 * no CDN round trip stands between you and your disappointment.
 */

import { studentTCDF, fCDF, tTwoSidedP } from "./special.js";

/** Normalize DataFrame-or-rows to rows. The universal adapter plug. */
function toRows(data) {
  return typeof data?.toRows === "function" ? data.toRows() : data;
}

function columnValues(data, column) {
  if (typeof data?.getColumn === "function") {
    // DataFrame fast path: skip row objects entirely. The raw typed array
    // is only safe for genuinely numeric dtypes; "str" holds dictionary
    // codes and gets refused, "date" is converted to epoch ms explicitly.
    const c = data.getColumn(column);
    if (c.dtype === "str") {
      throw new Error(`Column "${column}" is "str"; these tests want numbers.`);
    }
    if (c.dtype === "date") {
      return c.values({ validOnly: true }).map((d) => d.getTime());
    }
    const out = [];
    for (let i = 0; i < c.length; i++) if (c.isValid(i)) out.push(c.data[i]);
    return out;
  }
  return toRows(data)
    .map((r) => r[column])
    .filter((v) => v != null && !Number.isNaN(v));
}

function meanOf(xs) {
  return xs.reduce((a, b) => a + b, 0) / xs.length;
}

function varOf(xs, mean) {
  return xs.reduce((a, b) => a + (b - mean) ** 2, 0) / (xs.length - 1);
}

/**
 * One-sample t-test: is the mean of `column` different from `mu`?
 *
 * @param {DataFrame|Array<object>} data
 * @param {string} column
 * @param {number} mu the null hypothesis mean you'd like to reject
 * @returns {{mean: number, stdDev: number, tValue: number, pValue: number, df: number, n: number}}
 */
export function oneSampleTTest(data, column, mu) {
  const xs = columnValues(data, column);
  const n = xs.length;
  if (n < 2) throw new Error(`One-sample t-test with n=${n}: bold, but no.`);
  const mean = meanOf(xs);
  const sd = Math.sqrt(varOf(xs, mean));
  const t = (mean - mu) / (sd / Math.sqrt(n));
  return { mean, stdDev: sd, tValue: t, pValue: tTwoSidedP(t, n - 1), df: n - 1, n };
}

/**
 * Two-sample pooled t-test comparing `column` across two levels of
 * `groupColumn`. Pooled variance, so it assumes homoscedasticity —
 * an assumption with a long history of being made and a short history
 * of being checked.
 *
 * @param {DataFrame|Array<object>} data
 * @param {string} column value column
 * @param {string} groupColumn grouping column
 * @param {*} group1 first level
 * @param {*} group2 second level
 * @returns {{mean1: number, mean2: number, tValue: number, pValue: number, df: number, n1: number, n2: number}}
 */
export function twoSampleTTest(data, column, groupColumn, group1, group2) {
  const rows = toRows(data);
  const g1 = rows.filter((r) => r[groupColumn] === group1).map((r) => r[column]);
  const g2 = rows.filter((r) => r[groupColumn] === group2).map((r) => r[column]);
  if (g1.length < 2 || g2.length < 2) {
    throw new Error(`Both groups need at least 2 observations (got ${g1.length} and ${g2.length}).`);
  }
  const mean1 = meanOf(g1);
  const mean2 = meanOf(g2);
  const var1 = varOf(g1, mean1);
  const var2 = varOf(g2, mean2);
  const pooled = ((g1.length - 1) * var1 + (g2.length - 1) * var2) / (g1.length + g2.length - 2);
  const t = (mean1 - mean2) / Math.sqrt(pooled * (1 / g1.length + 1 / g2.length));
  const df = g1.length + g2.length - 2;
  return { mean1, mean2, tValue: t, pValue: tTwoSidedP(t, df), df, n1: g1.length, n2: g2.length };
}

/**
 * Welch's t-test — the two-sample test you should probably use instead,
 * since it doesn't assume the variances match. Included so choosing the
 * pooled version above is a decision, not a default.
 */
export function welchTTest(data, column, groupColumn, group1, group2) {
  const rows = toRows(data);
  const g1 = rows.filter((r) => r[groupColumn] === group1).map((r) => r[column]);
  const g2 = rows.filter((r) => r[groupColumn] === group2).map((r) => r[column]);
  const mean1 = meanOf(g1);
  const mean2 = meanOf(g2);
  const v1 = varOf(g1, mean1) / g1.length;
  const v2 = varOf(g2, mean2) / g2.length;
  const t = (mean1 - mean2) / Math.sqrt(v1 + v2);
  const df = (v1 + v2) ** 2 / (v1 ** 2 / (g1.length - 1) + v2 ** 2 / (g2.length - 1));
  return { mean1, mean2, tValue: t, pValue: tTwoSidedP(t, df), df, n1: g1.length, n2: g2.length };
}

// ---------------------------------------------------------------------------
// OLS + ANCOVA
// ---------------------------------------------------------------------------

function fitOLS(X, y) {
  const n = X.length;
  const p = X[0].length;

  const XtX = Array.from({ length: p }, () => new Array(p).fill(0));
  const XtY = new Array(p).fill(0);
  for (let i = 0; i < n; i++) {
    for (let j = 0; j < p; j++) {
      XtY[j] += X[i][j] * y[i];
      for (let k = j; k < p; k++) XtX[j][k] += X[i][j] * X[i][k];
    }
  }
  for (let j = 0; j < p; j++) for (let k = 0; k < j; k++) XtX[j][k] = XtX[k][j];

  // Gauss-Jordan inverse, small p. If your design matrix is singular, the
  // model was overdetermined by wishful thinking; we throw accordingly.
  const aug = XtX.map((row, i) => [
    ...row,
    ...Array.from({ length: p }, (_, j) => (i === j ? 1 : 0)),
  ]);
  for (let colIdx = 0; colIdx < p; colIdx++) {
    let pivot = colIdx;
    for (let r = colIdx + 1; r < p; r++) {
      if (Math.abs(aug[r][colIdx]) > Math.abs(aug[pivot][colIdx])) pivot = r;
    }
    if (Math.abs(aug[pivot][colIdx]) < 1e-10) {
      throw new Error(`Design matrix is singular — some predictors are telling the same story twice.`);
    }
    [aug[colIdx], aug[pivot]] = [aug[pivot], aug[colIdx]];
    const pv = aug[colIdx][colIdx];
    for (let j = 0; j < 2 * p; j++) aug[colIdx][j] /= pv;
    for (let r = 0; r < p; r++) {
      if (r === colIdx) continue;
      const f = aug[r][colIdx];
      for (let j = 0; j < 2 * p; j++) aug[r][j] -= f * aug[colIdx][j];
    }
  }
  const XtXinv = aug.map((row) => row.slice(p));

  const beta = XtXinv.map((row) => row.reduce((acc, v, idx) => acc + v * XtY[idx], 0));
  const yhat = X.map((row) => row.reduce((acc, v, idx) => acc + v * beta[idx], 0));
  const residuals = y.map((v, i) => v - yhat[i]);
  const rss = residuals.reduce((acc, r) => acc + r * r, 0);
  const df = n - p;
  const s2 = rss / df;
  const se = Array.from({ length: p }, (_, i) => Math.sqrt(s2 * XtXinv[i][i]));
  const tStats = beta.map((b, i) => b / se[i]);
  const pValues = tStats.map((t) => tTwoSidedP(t, df));
  return { beta, se, tStats, pValues, yhat, residuals, rss, df, s2 };
}

/**
 * Ordinary least squares of `dependentVar` on `predictors` (plus intercept).
 *
 * @param {DataFrame|Array<object>} data
 * @param {{dependentVar: string, predictors: string[]}} options
 * @returns {{beta: number[], se: number[], tStats: number[], pValues: number[],
 *            yhat: number[], residuals: number[], rss: number, df: number, s2: number,
 *            terms: string[]}}
 */
export function ols(data, { dependentVar, predictors }) {
  const rows = toRows(data);
  const X = rows.map((r) => [1, ...predictors.map((p) => Number(r[p]))]);
  const y = rows.map((r) => Number(r[dependentVar]));
  return { ...fitOLS(X, y), terms: ["(intercept)", ...predictors] };
}

/**
 * ANCOVA: does `groupVar` explain `dependentVar` beyond the covariates?
 * Fits reduced (covariates only) and full (covariates + group dummies)
 * models and reports the partial F — the polite statistical phrasing of
 * "did the intervention do anything, adjusting for who got it".
 *
 * @param {DataFrame|Array<object>} data
 * @param {{dependentVar: string, covariates: string[], groupVar: string}} options
 * @returns {{reducedModel: object, fullModel: object,
 *            partialF: {fStatistic: number, pValue: number}, levels: string[]}}
 */
export function ancova(data, { dependentVar, covariates, groupVar }) {
  const rows = toRows(data);
  const levels = [...new Set(rows.map((r) => String(r[groupVar])))].sort();
  const dummyLevels = levels.slice(1); // first level is the reference; someone has to be

  const X_reduced = rows.map((r) => [1, ...covariates.map((c) => Number(r[c]))]);
  const X_full = rows.map((r) => [
    1,
    ...covariates.map((c) => Number(r[c])),
    ...dummyLevels.map((lvl) => (String(r[groupVar]) === lvl ? 1 : 0)),
  ]);
  const y = rows.map((r) => Number(r[dependentVar]));

  const reducedModel = fitOLS(X_reduced, y);
  const fullModel = fitOLS(X_full, y);

  const nAdded = dummyLevels.length;
  const fStatistic =
    (reducedModel.rss - fullModel.rss) / nAdded / (fullModel.rss / fullModel.df);
  const pValue = 1 - fCDF(fStatistic, nAdded, fullModel.df);

  return { reducedModel, fullModel, partialF: { fStatistic, pValue }, levels };
}

export { studentTCDF, fCDF };
