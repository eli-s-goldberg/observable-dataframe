/**
 * distributions.js — random samplers and the DistP Monte Carlo class.
 *
 * Native implementations (Box-Muller, Marsaglia-Tsang, inverse transforms),
 * so nothing here phones a CDN for jStat. Each sampler takes a params
 * object and returns one draw; DistP turns samplers into distributions you
 * can bound, chain, and reason about — uncertainty algebra for business
 * cases, where every input is a guess but the guesses have shapes.
 */

import { DataFrame } from "../core/DataFrame.js";
import { random } from "../core/random.js";

/** One standard normal draw via Box-Muller. Two uniforms enter, one gaussian leaves. */
function randn(rand = Math.random) {
  let u = 0;
  let v = 0;
  while (u === 0) u = rand();
  while (v === 0) v = rand();
  return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v);
}

/** Gamma(shape, scale=1) via Marsaglia & Tsang, with the alpha<1 boost. */
function randGamma(shape, scale = 1, rand = Math.random) {
  if (shape < 1) {
    // Boost: sample Gamma(shape+1), then scale by U^(1/shape).
    return randGamma(shape + 1, scale, rand) * Math.pow(rand(), 1 / shape);
  }
  const d = shape - 1 / 3;
  const c = 1 / Math.sqrt(9 * d);
  for (;;) {
    let x;
    let v;
    do {
      x = randn(rand);
      v = 1 + c * x;
    } while (v <= 0);
    v = v * v * v;
    const u = rand();
    if (u < 1 - 0.0331 * x * x * x * x) return d * v * scale;
    if (Math.log(u) < 0.5 * x * x + d * (1 - v + Math.log(v))) return d * v * scale;
  }
}

/**
 * The sampler registry. Every entry: (params, rand?) => one draw, where
 * `rand` is a Math.random-compatible uniform source (defaulting to
 * Math.random itself). Pass these to DistP, or call them directly if you
 * enjoy loops.
 */
export const distributions = {
  /** Normal(mean, std). The one everyone assumes anyway. */
  normal: ({ mean = 0, std = 1 } = {}, rand = Math.random) => mean + std * randn(rand),

  /** Uniform(min, max). Maximum entropy, minimum thought. Sometimes correct. */
  uniform: ({ min = 0, max = 1 } = {}, rand = Math.random) => min + (max - min) * rand(),

  /** Exponential(rate). Waiting times, decay, and the gaps between incidents. */
  exponential: ({ rate = 1 } = {}, rand = Math.random) => -Math.log(1 - rand()) / rate,

  /**
   * Lognormal parameterized by real-space mean and shape (log-space sigma),
   * matching the original DistP convention: logMu = ln(mean) − shape²/2.
   */
  lognormal: ({ mean = 0.2, shape = 0.1 } = {}, rand = Math.random) => {
    const logMu = Math.log(mean) - shape ** 2 / 2;
    return Math.exp(logMu + shape * randn(rand));
  },

  /** Triangular(min, max, mode). The distribution of "ask three stakeholders". */
  triangular: ({ min = 0, max = 1, mode = (min + max) / 2 } = {}, rand = Math.random) => {
    const u = rand();
    const f = (mode - min) / (max - min);
    return u < f
      ? min + Math.sqrt(u * (max - min) * (mode - min))
      : max - Math.sqrt((1 - u) * (max - min) * (max - mode));
  },

  /** Beta(alpha, beta) via two gammas. Rates, proportions, priors. */
  beta: ({ alpha = 2, beta = 2 } = {}, rand = Math.random) => {
    const x = randGamma(alpha, 1, rand);
    const y = randGamma(beta, 1, rand);
    return x / (x + y);
  },

  /** Gamma(shape, scale). */
  gamma: ({ shape = 1, scale = 1 } = {}, rand = Math.random) => randGamma(shape, scale, rand),

  /** Weibull(scale, shape) via inverse transform. Time-to-failure's favorite. */
  weibull: ({ scale = 1, shape = 1 } = {}, rand = Math.random) =>
    scale * Math.pow(-Math.log(1 - rand()), 1 / shape),

  /** Resample from an empirical array. The distribution of "we have data, actually". */
  custom: ({ samples = [0, 1, 2, 3, 4, 5] } = {}, rand = Math.random) =>
    samples[Math.floor(rand() * samples.length)],
};

/**
 * A Monte Carlo distribution: draw samples, apply bounds, track stats,
 * and combine with other DistPs element-wise. Chain a few of these and
 * you have a business case with honest error bars instead of a single
 * number someone will later call "the estimate".
 *
 *   const conversion = new DistP({ name: "conversion", distfunc: distributions.beta,
 *                                  params: { alpha: 20, beta: 180 }, size: 10000 });
 *   const value = new DistP({ name: "value/member", distfunc: distributions.lognormal,
 *                             params: { mean: 120, shape: 0.4 }, size: 10000 });
 *   const impact = conversion.copy().chainMult(value).multConst(members);
 *   impact.confInt();  // → [p2.5, p97.5], the range you should have quoted
 */
export class DistP {
  /**
   * @param {object} config
   * @param {string} [config.name="default"] label, used by plot helpers
   * @param {string} [config.lever="default lever"] value lever this parameter moves
   * @param {string} [config.segment="default segment"] population segment
   * @param {Function|"fitted"} [config.distfunc] sampler from `distributions`, or "fitted" to bootstrap from provided samples
   * @param {object} [config.params] parameters handed to the sampler
   * @param {[number, number]} [config.bounds] [lo, hi] plausibility bounds
   * @param {"drop_recursive"|"stack"} [config.boundMethod="drop_recursive"] drop-and-resample, or clamp to bounds
   * @param {number} [config.size=5000] number of samples
   * @param {number[]} [config.samples] pre-existing samples (required for "fitted")
   * @param {number} [config.seed] integer seed for a reproducible draw sequence
   * @param {() => number} [config.rng] Math.random-compatible uniform source; wins over seed
   */
  constructor({
    name = "default",
    lever = "default lever",
    segment = "default segment",
    distfunc = null,
    params = {},
    bounds = [0, 1e6],
    boundMethod = "drop_recursive",
    size = 5000,
    samples = null,
    seed = null,
    rng = null,
  } = {}) {
    this.name = name;
    this.lever = lever;
    this.segment = segment;
    this.distfunc = distfunc;
    this.params = params;
    this.bounds = bounds;
    this.boundMethod = boundMethod;
    this.size = size;
    this.samples = samples;
    this.stats = {};
    this._rand = rng ?? (seed != null ? random(seed) : Math.random);

    if (!distfunc && !samples) {
      throw new Error(`DistP needs a distfunc or samples. An empty distribution is just philosophy.`);
    }
    if (distfunc === "fitted") {
      if (!samples) throw new Error(`"fitted" needs samples to fit to. That's the whole bit.`);
      this.samples = this._resampleWithin(samples, size);
    } else if (typeof distfunc === "function") {
      this.samples = Array.from({ length: size }, () => distfunc(params, this._rand));
      this._applyBounds();
    } else {
      this.samples = samples.slice();
      this.size = this.samples.length;
    }
    this._updateStats();
  }

  /** Independent copy — because the chain ops mutate, and sharing is how bugs breed. */
  copy() {
    return new DistP({
      name: this.name,
      lever: this.lever,
      segment: this.segment,
      samples: this.samples.slice(),
      bounds: this.bounds,
    });
  }

  _applyBounds() {
    const [lo, hi] = this.bounds;
    if (this.boundMethod === "stack") {
      this.samples = this.samples.map((x) => Math.min(Math.max(x, lo), hi));
    } else {
      this.samples = this._resampleWithin(this.samples, this.size);
    }
  }

  _resampleWithin(samples, size) {
    const [lo, hi] = this.bounds;
    const kept = samples.filter((x) => x >= lo && x <= hi);
    if (kept.length === 0) {
      throw new Error(`Bounds [${lo}, ${hi}] rejected every sample. The bounds and the distribution should talk.`);
    }
    return Array.from({ length: size }, () => kept[(this._rand() * kept.length) | 0]);
  }

  _updateStats() {
    const s = this.samples.slice().sort((a, b) => a - b);
    const n = s.length;
    const mean = s.reduce((a, b) => a + b, 0) / n;
    const variance = n > 1 ? s.reduce((a, b) => a + (b - mean) ** 2, 0) / (n - 1) : 0;
    const q = (p) => {
      const idx = (n - 1) * p;
      const lo = Math.floor(idx);
      const hi = Math.ceil(idx);
      return lo === hi ? s[lo] : s[lo] + (s[hi] - s[lo]) * (idx - lo);
    };
    this.stats = {
      mean,
      median: q(0.5),
      std: Math.sqrt(variance),
      size: n,
      percentiles: [0, 0.25, 0.5, 0.75, 1].map(q),
    };
    this._sorted = s;
  }

  /** Scale every sample by a constant. Mutates and returns this, chainably. */
  multConst(k) {
    this.samples = this.samples.map((x) => x * k);
    this._updateStats();
    return this;
  }

  /** Element-wise multiply with another DistP of equal size. */
  chainMult(other) {
    return this._chain(other, (a, b) => a * b);
  }
  /** Element-wise divide. */
  chainDivide(other) {
    return this._chain(other, (a, b) => a / b);
  }
  /** Element-wise add. */
  chainAdd(other) {
    return this._chain(other, (a, b) => a + b);
  }
  /** Element-wise subtract. */
  chainSub(other) {
    return this._chain(other, (a, b) => a - b);
  }

  _chain(other, fn) {
    if (this.samples.length !== other.samples.length) {
      throw new Error(
        `Chaining needs equal sizes: ${this.samples.length} vs ${other.samples.length}. Uncertainty doesn't broadcast.`
      );
    }
    this.samples = this.samples.map((x, i) => fn(x, other.samples[i]));
    this._updateStats();
    return this;
  }

  /**
   * Confidence interval from sample quantiles.
   * @param {[number, number]} [ci=[2.5, 97.5]] percentile bounds
   * @returns {[number, number]}
   */
  confInt(ci = [2.5, 97.5]) {
    const s = this._sorted;
    const n = s.length;
    return ci.map((p) => {
      const idx = (n - 1) * (p / 100);
      const lo = Math.floor(idx);
      const hi = Math.ceil(idx);
      return lo === hi ? s[lo] : s[lo] + (s[hi] - s[lo]) * (idx - lo);
    });
  }

  /**
   * Complement distribution: 1 − x for each sample. Useful for non-participation
   * fractions when the input is a participation rate.
   *
   * @param {string} [newName] optional rename
   * @returns {DistP} new distribution (does not mutate this)
   */
  createComplement(newName = null) {
    const samples = this.samples.map((x) => 1 - x);
    return new DistP({
      name: newName ?? `${this.name} (complement)`,
      lever: this.lever,
      segment: this.segment,
      distfunc: "fitted",
      samples,
      bounds: this.bounds,
      size: samples.length,
    });
  }

  /**
   * Export one row per MC draw for DataFrame workflows.
   *
   * @param {string} [valueColumn="value"] sample column name
   * @returns {import("../core/DataFrame.js").DataFrame}
   */
  toDataFrame(valueColumn = "value") {
    return DataFrame.fromRows(
      this.samples.map((v, i) => ({
        draw: i,
        name: this.name,
        lever: this.lever,
        segment: this.segment,
        [valueColumn]: v,
      }))
    );
  }

  /** Flat summary for assumptions tables and parameter registries. */
  toRecord() {
    return {
      name: this.name,
      lever: this.lever,
      segment: this.segment,
      mean: this.stats.mean,
      median: this.stats.median,
      std: this.stats.std,
      size: this.stats.size,
      p05: this.confInt([5, 5])[0],
      p95: this.confInt([95, 95])[0],
    };
  }
}
