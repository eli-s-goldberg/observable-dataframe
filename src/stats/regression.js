/**
 * regression.js — OLS with the standard errors people actually defend.
 *
 * The estimation engine under the DiD module: ordinary least squares with
 * classical, heteroskedasticity-robust (HC1), and cluster-robust (CR1)
 * covariance. Your point estimate is only as credible as your standard
 * error, and "we clustered at the unit level" is the sentence that keeps
 * referee two seated.
 */

import { tTwoSidedP, probit } from "./special.js";

/**
 * Fit OLS of y on X (X includes whatever columns you want — add your own
 * intercept; we don't guess).
 *
 * @param {number[][]} X design matrix, row-major
 * @param {number[]} y outcomes
 * @param {object} [options]
 * @param {"classical"|"hc1"|"cluster"} [options.vcov="hc1"] covariance flavor
 * @param {Array} [options.clusters] cluster id per row (required for "cluster")
 * @param {string[]} [options.terms] coefficient names, for the summary
 * @returns {{beta, se, tStats, pValues, ci, residuals, yhat, rss, df, n, k,
 *            vcov, vcovType, nClusters, terms}}
 */
export function fitOLS(X, y, { vcov = "hc1", clusters = null, terms = null } = {}) {
  const n = X.length;
  const k = X[0].length;
  if (n <= k) {
    throw new Error(`OLS with n=${n} rows and k=${k} parameters: the residuals would like a word.`);
  }

  // X'X and X'y in one pass.
  const XtX = Array.from({ length: k }, () => new Float64Array(k));
  const Xty = new Float64Array(k);
  for (let i = 0; i < n; i++) {
    const xi = X[i];
    for (let a = 0; a < k; a++) {
      Xty[a] += xi[a] * y[i];
      for (let b = a; b < k; b++) XtX[a][b] += xi[a] * xi[b];
    }
  }
  for (let a = 0; a < k; a++) for (let b = 0; b < a; b++) XtX[a][b] = XtX[b][a];

  const XtXinv = invSym(XtX, k);

  const beta = new Float64Array(k);
  for (let a = 0; a < k; a++) {
    let s = 0;
    for (let b = 0; b < k; b++) s += XtXinv[a][b] * Xty[b];
    beta[a] = s;
  }

  const yhat = new Float64Array(n);
  const residuals = new Float64Array(n);
  let rss = 0;
  for (let i = 0; i < n; i++) {
    let s = 0;
    const xi = X[i];
    for (let a = 0; a < k; a++) s += xi[a] * beta[a];
    yhat[i] = s;
    residuals[i] = y[i] - s;
    rss += residuals[i] * residuals[i];
  }

  // --- the sandwich ---------------------------------------------------------
  // vcov = (X'X)^-1 · meat · (X'X)^-1, where the meat depends on how honest
  // we're being about the error structure.
  let V;
  let vcovType = vcov;
  let nClusters = null;
  let df = n - k;

  if (vcov === "classical") {
    const s2 = rss / df;
    V = XtXinv.map((row) => row.map((v) => v * s2));
  } else if (vcov === "cluster") {
    if (!clusters || clusters.length !== n) {
      throw new Error(`vcov: "cluster" needs a cluster id per row. Clustering on vibes is not identified.`);
    }
    // CR1: sum of per-cluster score outer products, small-sample scaled.
    const byCluster = new Map();
    for (let i = 0; i < n; i++) {
      const key = clusters[i];
      let idx = byCluster.get(key);
      if (!idx) byCluster.set(key, (idx = []));
      idx.push(i);
    }
    const G = byCluster.size;
    nClusters = G;
    if (G < 2) throw new Error(`Cluster-robust SEs with ${G} cluster: that's just one standard error in a trench coat.`);
    const meat = Array.from({ length: k }, () => new Float64Array(k));
    for (const idx of byCluster.values()) {
      const score = new Float64Array(k);
      for (const i of idx) {
        const xi = X[i];
        for (let a = 0; a < k; a++) score[a] += xi[a] * residuals[i];
      }
      for (let a = 0; a < k; a++) for (let b = 0; b < k; b++) meat[a][b] += score[a] * score[b];
    }
    const scale = (G / (G - 1)) * ((n - 1) / (n - k));
    V = sandwich(XtXinv, meat, k, scale);
    df = G - 1; // t with G-1 df, per the cluster-robust liturgy
  } else {
    // HC1: per-observation score outer products with the n/(n-k) fix-up.
    const meat = Array.from({ length: k }, () => new Float64Array(k));
    for (let i = 0; i < n; i++) {
      const xi = X[i];
      const e2 = residuals[i] * residuals[i];
      for (let a = 0; a < k; a++) for (let b = 0; b < k; b++) meat[a][b] += xi[a] * xi[b] * e2;
    }
    V = sandwich(XtXinv, meat, k, n / (n - k));
    vcovType = "hc1";
  }

  const se = beta.map ? new Float64Array(k) : [];
  const tStats = new Float64Array(k);
  const pValues = new Float64Array(k);
  const z = probit(0.975);
  const ci = [];
  for (let a = 0; a < k; a++) {
    se[a] = Math.sqrt(Math.max(0, V[a][a]));
    tStats[a] = se[a] > 0 ? beta[a] / se[a] : 0;
    pValues[a] = se[a] > 0 ? tTwoSidedP(tStats[a], df) : 1;
    ci.push([beta[a] - z * se[a], beta[a] + z * se[a]]);
  }

  return {
    beta: Array.from(beta),
    se: Array.from(se),
    tStats: Array.from(tStats),
    pValues: Array.from(pValues),
    ci,
    residuals: Array.from(residuals),
    yhat: Array.from(yhat),
    rss,
    df,
    n,
    k,
    vcov: V,
    vcovType,
    nClusters,
    terms: terms ?? Array.from({ length: k }, (_, i) => `x${i}`),
  };
}

function sandwich(bread, meat, k, scale) {
  // bread · meat · bread, all k×k. k is small; O(k³) is a rounding error.
  const tmp = Array.from({ length: k }, () => new Float64Array(k));
  for (let a = 0; a < k; a++) {
    for (let b = 0; b < k; b++) {
      let s = 0;
      for (let c = 0; c < k; c++) s += bread[a][c] * meat[c][b];
      tmp[a][b] = s;
    }
  }
  const out = Array.from({ length: k }, () => new Float64Array(k));
  for (let a = 0; a < k; a++) {
    for (let b = 0; b < k; b++) {
      let s = 0;
      for (let c = 0; c < k; c++) s += tmp[a][c] * bread[c][b];
      out[a][b] = s * scale;
    }
  }
  return out;
}

function invSym(A, k) {
  // Gauss-Jordan with partial pivoting; k is coefficient-count sized.
  const aug = Array.from({ length: k }, (_, i) => {
    const row = new Float64Array(2 * k);
    for (let j = 0; j < k; j++) row[j] = A[i][j];
    row[k + i] = 1;
    return row;
  });
  for (let col = 0; col < k; col++) {
    let pivot = col;
    for (let r = col + 1; r < k; r++) if (Math.abs(aug[r][col]) > Math.abs(aug[pivot][col])) pivot = r;
    if (Math.abs(aug[pivot][col]) < 1e-11) {
      throw new Error(
        `Design matrix is singular at column ${col} — a regressor is a linear combination of the others. Collinearity: found.`
      );
    }
    [aug[col], aug[pivot]] = [aug[pivot], aug[col]];
    const p = aug[col][col];
    for (let j = 0; j < 2 * k; j++) aug[col][j] /= p;
    for (let r = 0; r < k; r++) {
      if (r === col) continue;
      const f = aug[r][col];
      if (f === 0) continue;
      for (let j = 0; j < 2 * k; j++) aug[r][j] -= f * aug[col][j];
    }
  }
  return aug.map((row) => Array.from(row.slice(k)));
}

/**
 * Two-way within transformation for panel data: demean by unit and by time
 * (iterated to convergence, which for balanced panels is one pass and for
 * unbalanced panels is a few). This is how TWFE avoids materializing ten
 * thousand fixed-effect dummies nobody was going to read.
 *
 * @param {number[]} values
 * @param {Array} unitIds unit id per row
 * @param {Array} timeIds time id per row
 * @param {number} [maxIter=50]
 * @returns {number[]} demeaned values
 */
export function withinTransform(values, unitIds, timeIds, maxIter = 50) {
  const v = Float64Array.from(values);
  const n = v.length;
  for (let iter = 0; iter < maxIter; iter++) {
    let shifted = 0;
    for (const ids of [unitIds, timeIds]) {
      const sums = new Map();
      for (let i = 0; i < n; i++) {
        const g = ids[i];
        const s = sums.get(g) ?? [0, 0];
        s[0] += v[i];
        s[1]++;
        sums.set(g, s);
      }
      const means = new Map();
      for (const [g, [sum, count]] of sums) means.set(g, sum / count);
      for (let i = 0; i < n; i++) {
        const m = means.get(ids[i]);
        v[i] -= m;
        shifted = Math.max(shifted, Math.abs(m));
      }
    }
    if (shifted < 1e-10) break;
  }
  return Array.from(v);
}
