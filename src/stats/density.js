/**
 * density.js — kernel density estimation, the histogram's smoother cousin.
 *
 * Gaussian kernel, Silverman's rule-of-thumb bandwidth by default, and a
 * seaborn-compatible `cut` parameter controlling how many bandwidths the
 * curve extends past the data extremes. A KDE is an opinion about your
 * data's shape; the bandwidth is how strongly you hold it.
 */

/**
 * Silverman's rule-of-thumb bandwidth: 0.9 · min(σ, IQR/1.34) · n^(−1/5).
 * Optimal if your data is Gaussian, serviceable if it isn't, and famous
 * enough that nobody will ask why you chose it.
 *
 * @param {number[]} samples
 * @returns {number}
 */
export function silvermanBandwidth(samples) {
  const n = samples.length;
  if (n < 2) return 1;
  const mean = samples.reduce((a, b) => a + b, 0) / n;
  const sd = Math.sqrt(samples.reduce((a, b) => a + (b - mean) ** 2, 0) / (n - 1));
  const sorted = samples.slice().sort((a, b) => a - b);
  const q = (p) => {
    const idx = (n - 1) * p;
    const lo = Math.floor(idx);
    const hi = Math.ceil(idx);
    return lo === hi ? sorted[lo] : sorted[lo] + (sorted[hi] - sorted[lo]) * (idx - lo);
  };
  const iqr = q(0.75) - q(0.25);
  const scale = iqr > 0 ? Math.min(sd, iqr / 1.34) : sd;
  return 0.9 * (scale || 1) * Math.pow(n, -0.2);
}

/**
 * Gaussian KDE evaluated on a regular grid.
 *
 * @param {number[]} samples
 * @param {object} [options]
 * @param {number} [options.bandwidth] kernel bandwidth; Silverman if omitted
 * @param {number} [options.cut=3] extend the grid this many bandwidths past
 *   min/max (the seaborn convention: 0 clips at the data, 3 shows the tails)
 * @param {number} [options.n=200] grid resolution
 * @returns {{points: Array<{x: number, density: number}>, bandwidth: number}}
 *   densities integrate to ~1 over the grid, as densities are contractually obliged to
 */
export function kde(samples, { bandwidth, cut = 3, n = 200 } = {}) {
  if (!samples.length) {
    throw new Error(`kde() of an empty sample is a philosophical question, not a statistical one.`);
  }
  const h = bandwidth ?? silvermanBandwidth(samples);
  const [dataLo, dataHi] = extent(samples);
  const lo = dataLo - cut * h;
  const hi = dataHi + cut * h;
  const step = (hi - lo) / (n - 1);
  const inv = 1 / (samples.length * h * Math.sqrt(2 * Math.PI));

  const points = new Array(n);
  for (let i = 0; i < n; i++) {
    const x = lo + i * step;
    let density = 0;
    for (let j = 0; j < samples.length; j++) {
      const z = (x - samples[j]) / h;
      density += Math.exp(-0.5 * z * z);
    }
    points[i] = { x, density: density * inv };
  }
  return { points, bandwidth: h };
}

function extent(samples) {
  // Math.min(...arr) with 100k+ elements blows the stack; loop like adults.
  let lo = Infinity;
  let hi = -Infinity;
  for (const v of samples) {
    if (v < lo) lo = v;
    if (v > hi) hi = v;
  }
  return [lo, hi];
}
