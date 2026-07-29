/**
 * special.js — the special functions underneath everything else.
 *
 * Every p-value you've ever trusted was computed by code like this:
 * polynomial approximations with magic constants copied carefully from
 * people who derived them decades ago. We cite our sources and keep the
 * magic numbers exactly as found, because "improving" them is how you get
 * p-values that are confidently wrong.
 */

/**
 * Inverse standard normal CDF (a.k.a. probit), via Acklam's rational
 * approximation. Turns "95% confidence" into the 1.6449 you actually
 * plug into a sample-size formula. Accurate to ~1.15e-9, which is
 * substantially more accurate than the effect-size guess you're about
 * to multiply it against.
 *
 * @param {number} p probability in (0, 1)
 * @returns {number} z such that Φ(z) = p
 */
export function probit(p) {
  if (p <= 0 || p >= 1) {
    throw new Error(`probit(${p}): p must be strictly between 0 and 1. Infinity is not a z-score.`);
  }
  const a = [-3.969683028665376e1, 2.209460984245205e2, -2.759285104469687e2, 1.38357751867269e2, -3.066479806614716e1, 2.506628277459239];
  const b = [-5.447609879822406e1, 1.615858368580409e2, -1.556989798598866e2, 6.680131188771972e1, -1.328068155288572e1];
  const c = [-7.784894002430293e-3, -3.223964580411365e-1, -2.400758277161838, -2.549732539343734, 4.374664141464968, 2.938163982698783];
  const d = [7.784695709041462e-3, 3.224671290700398e-1, 2.445134137142996, 3.754408661907416];
  const plow = 0.02425;
  const phigh = 1 - plow;
  let q, r;
  if (p < plow) {
    q = Math.sqrt(-2 * Math.log(p));
    return (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1);
  }
  if (p <= phigh) {
    q = p - 0.5;
    r = q * q;
    return ((((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q) / (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1);
  }
  q = Math.sqrt(-2 * Math.log(1 - p));
  return -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1);
}

/**
 * Standard normal CDF Φ(z), via Abramowitz & Stegun 7.1.26. Good to about
 * 1.5e-7 — seven digits of certainty about your uncertainty.
 */
export function normalCDF(z) {
  const sign = z < 0 ? -1 : 1;
  const x = Math.abs(z) / Math.SQRT2;
  const t = 1 / (1 + 0.3275911 * x);
  const y =
    1 -
    ((((1.061405429 * t - 1.453152027) * t + 1.421413741) * t - 0.284496736) * t + 0.254829592) *
      t *
      Math.exp(-x * x);
  return 0.5 * (1 + sign * y);
}

/**
 * ln Γ(x), Lanczos approximation. The gateway drug to every distribution
 * function below.
 */
export function gammaln(x) {
  const g = [
    76.18009172947146, -86.50532032941677, 24.01409824083091, -1.231739572450155,
    0.1208650973866179e-2, -0.5395239384953e-5,
  ];
  let y = x;
  let tmp = x + 5.5;
  tmp -= (x + 0.5) * Math.log(tmp);
  let ser = 1.000000000190015;
  for (let j = 0; j < 6; j++) ser += g[j] / ++y;
  return -tmp + Math.log((2.5066282746310005 * ser) / x);
}

/**
 * Regularized incomplete beta function I_x(a, b), continued-fraction
 * evaluation per Numerical Recipes. This one function powers the Student-t
 * and F CDFs, which is to say: most of frequentist statistics is a wrapper
 * around it. No pressure.
 */
export function ibeta(x, a, b) {
  if (x <= 0) return 0;
  if (x >= 1) return 1;
  const bt = Math.exp(gammaln(a + b) - gammaln(a) - gammaln(b) + a * Math.log(x) + b * Math.log(1 - x));
  if (x < (a + 1) / (a + b + 2)) return (bt * betacf(x, a, b)) / a;
  return 1 - (bt * betacf(1 - x, b, a)) / b;
}

function betacf(x, a, b) {
  const MAXIT = 200;
  const EPS = 3e-12;
  const FPMIN = 1e-300;
  const qab = a + b;
  const qap = a + 1;
  const qam = a - 1;
  let c = 1;
  let d = 1 - (qab * x) / qap;
  if (Math.abs(d) < FPMIN) d = FPMIN;
  d = 1 / d;
  let h = d;
  for (let m = 1; m <= MAXIT; m++) {
    const m2 = 2 * m;
    let aa = (m * (b - m) * x) / ((qam + m2) * (a + m2));
    d = 1 + aa * d;
    if (Math.abs(d) < FPMIN) d = FPMIN;
    c = 1 + aa / c;
    if (Math.abs(c) < FPMIN) c = FPMIN;
    d = 1 / d;
    h *= d * c;
    aa = (-(a + m) * (qab + m) * x) / ((a + m2) * (qap + m2));
    d = 1 + aa * d;
    if (Math.abs(d) < FPMIN) d = FPMIN;
    c = 1 + aa / c;
    if (Math.abs(c) < FPMIN) c = FPMIN;
    d = 1 / d;
    const del = d * c;
    h *= del;
    if (Math.abs(del - 1) < EPS) break;
  }
  return h;
}

/**
 * Regularized lower incomplete gamma P(a, x), series + continued fraction
 * per Numerical Recipes. Exists here because the chi-square CDF is a thin
 * wrapper around it, and Table 1 wants chi-square p-values.
 */
export function gammainc(a, x) {
  if (x < 0 || a <= 0) return NaN;
  if (x === 0) return 0;
  if (x < a + 1) {
    // series representation converges fast here
    let sum = 1 / a;
    let term = sum;
    for (let n = 1; n < 300; n++) {
      term *= x / (a + n);
      sum += term;
      if (Math.abs(term) < Math.abs(sum) * 1e-14) break;
    }
    return sum * Math.exp(-x + a * Math.log(x) - gammaln(a));
  }
  // continued fraction for the upper tail, complemented
  let b = x + 1 - a;
  let c = 1e300;
  let d = 1 / b;
  let h = d;
  for (let i = 1; i < 300; i++) {
    const an = -i * (i - a);
    b += 2;
    d = an * d + b;
    if (Math.abs(d) < 1e-300) d = 1e-300;
    c = b + an / c;
    if (Math.abs(c) < 1e-300) c = 1e-300;
    d = 1 / d;
    const del = d * c;
    h *= del;
    if (Math.abs(del - 1) < 1e-14) break;
  }
  return 1 - Math.exp(-x + a * Math.log(x) - gammaln(a)) * h;
}

/** Chi-square CDF with k degrees of freedom. */
export function chi2CDF(x, k) {
  return gammainc(k / 2, x / 2);
}

/** Student-t CDF with df degrees of freedom. */
export function studentTCDF(t, df) {
  const x = df / (df + t * t);
  const p = 0.5 * ibeta(x, df / 2, 0.5);
  return t > 0 ? 1 - p : p;
}

/** F-distribution CDF with (d1, d2) degrees of freedom. */
export function fCDF(f, d1, d2) {
  if (f <= 0) return 0;
  return ibeta((d1 * f) / (d1 * f + d2), d1 / 2, d2 / 2);
}

/** Two-sided p-value from a t statistic. The number people skip to. */
export function tTwoSidedP(t, df) {
  return 2 * (1 - studentTCDF(Math.abs(t), df));
}
