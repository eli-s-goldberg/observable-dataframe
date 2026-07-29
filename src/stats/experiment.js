/**
 * experiment.js — the experimental design system, unified.
 *
 * Stratification, arm construction, power, and sample sizing behind one
 * builder, with the DataFrame as the substrate so everything speaks to
 * everything:
 *
 *   ExperimentDesign.fromPanel(df, {...})  — strata rates, population sizes,
 *                                            and the CUPED correlation come
 *                                            FROM YOUR DATA, not from a
 *                                            hopeful constant in a notebook
 *     .stratify({...})                     — risk strata by score quantile or column
 *     .arms([...])                         — treatment arms; lifts given directly
 *                                            or derived from channel cascades
 *                                            (reach × open × efficacy)
 *     .power({...})                        — alpha, power, and the method:
 *                                            standard | cuped | bayesian | simulation
 *     .design("rct" | "three-arm" | "factorial")
 *     .build()                             — an Experiment that renders itself
 *
 * The built Experiment hands configs to the plots (experimentDesignTree,
 * measurementTimeline, consortDiagram), returns summary DataFrames, prices
 * itself (businessCase), and validates its own arithmetic by Monte Carlo
 * (simulatePower). One object, one set of numbers, no transcription step
 * between the statistics and the slide.
 *
 * Conventions baked in on purpose:
 *   - CUPED reduces required n by (1 − ρ²). ρ = 0.5 buys you 25%.
 *   - Multi-arm trials pay the k/(k−1)-family tax (1.5× for three arms).
 *   - Channel cascades compound multiplicatively; nobody double-counts a
 *     member who ignored the email AND the mailer.
 */

import { probit, normalCDF } from "./special.js";
import { col } from "../core/expr.js";
import { DataFrame } from "../core/DataFrame.js";

// ---------------------------------------------------------------------------
// power calculators — the four methods, one signature
// ---------------------------------------------------------------------------

/**
 * Sample size per arm for two proportions.
 *
 * @param {object} options
 * @param {number} options.p1 control event rate
 * @param {number} options.p2 treatment event rate
 * @param {number} [options.alpha=0.05] / @param {number} [options.power=0.8]
 * @param {"pooled"|"arcsine"} [options.effectSize="arcsine"] pooled z-formula,
 *   or Cohen's h via arcsine transform (the default; more honest at small
 *   rates, where proportions get non-normal in a hurry)
 * @param {"standard"|"cuped"|"bayesian"} [options.method="standard"]
 * @param {number} [options.correlation=0.5] CUPED ρ (pre/post outcome correlation)
 * @param {number} [options.priorWeight=0.1] Bayesian effective prior fraction
 * @returns {{nPerArm: number, totalN: number, effectSize: number, varianceReduction: number}}
 */
export function sampleSizePerArm({
  p1,
  p2,
  alpha = 0.05,
  power = 0.8,
  effectSize = "arcsine",
  method = "standard",
  correlation = 0.5,
  priorWeight = 0.1,
} = {}) {
  const zA = probit(1 - alpha / 2);
  const zB = probit(power);

  let n;
  let h;
  if (effectSize === "arcsine") {
    h = 2 * (Math.asin(Math.sqrt(p1)) - Math.asin(Math.sqrt(p2)));
    if (Math.abs(h) < 1e-4) return { nPerArm: Infinity, totalN: Infinity, effectSize: h, varianceReduction: 0 };
    n = 2 * ((zA + zB) / h) ** 2;
  } else {
    const delta = Math.abs(p2 - p1);
    if (delta < 1e-9) return { nPerArm: Infinity, totalN: Infinity, effectSize: 0, varianceReduction: 0 };
    const pbar = (p1 + p2) / 2;
    const num = zA * Math.sqrt(2 * pbar * (1 - pbar)) + zB * Math.sqrt(p1 * (1 - p1) + p2 * (1 - p2));
    n = (num / delta) ** 2;
    h = delta;
  }

  let varianceReduction = 0;
  if (method === "cuped") {
    varianceReduction = correlation ** 2;
    n *= 1 - varianceReduction; // CUPED's entire pitch, one line
  } else if (method === "bayesian") {
    varianceReduction = priorWeight;
    n *= 1 - priorWeight;
  }

  const nPerArm = Math.ceil(n);
  return { nPerArm, totalN: nPerArm * 2, effectSize: h, varianceReduction };
}

/**
 * The k/(k−1)-family multi-arm tax, ported verbatim from the Python
 * framework: splitting one sample across more arms dilutes each pairwise
 * comparison, and this factor is what it costs to keep the power.
 */
export function multiArmAdjustment(nArms) {
  if (nArms <= 2) return 1;
  if (nArms === 3) return 1.5;
  if (nArms === 4) return 4 / 3;
  if (nArms === 5) return 1.25;
  return Math.min(nArms / 2, 2);
}

/**
 * Channel cascade → relative effect. Each channel converts
 * reach × open × efficacy of the not-yet-converted; the compounding is
 * multiplicative because a member persuaded by the mailer cannot be
 * re-persuaded by the email, however good the subject line.
 *
 * @param {Array<{reach: number, open: number, efficacy: number, cost?: number}>} channels
 * @returns {{relativeEffect: number, costPerMember: number}}
 */
export function channelCascade(channels) {
  let keep = 1;
  let costPerMember = 0;
  for (const ch of channels) {
    keep *= 1 - (ch.reach ?? 1) * (ch.open ?? 1) * (ch.efficacy ?? 0);
    costPerMember += ch.cost ?? 0;
  }
  return { relativeEffect: 1 - keep, costPerMember };
}

// ---------------------------------------------------------------------------
// the designer
// ---------------------------------------------------------------------------

export class ExperimentDesign {
  constructor() {
    this._strata = [];
    this._arms = [];
    this._power = { alpha: 0.05, power: 0.8, method: "standard", correlation: 0.5, priorWeight: 0.1, effectSize: "arcsine" };
    this._design = "rct";
    this._panelMeta = null;
  }

  /**
   * Start from a member panel: one row per unit-period, with an outcome and
   * (optionally) a risk score. Baseline rates, available population, and
   * the CUPED correlation are measured from these rows.
   *
   * @param {DataFrame|Array<object>} data
   * @param {object} options
   * @param {string} options.unit unit id column
   * @param {string} options.outcome binary outcome column (0/1 per period, or per unit if no period given)
   * @param {string} [options.period] period column; when present with `prePeriods`,
   *   the CUPED ρ is estimated as the unit-level correlation between pre- and
   *   post-window outcome rates
   * @param {string} [options.riskScore] score column for quantile stratification
   * @param {number[]} [options.prePeriods] periods treated as "pre" for CUPED estimation
   */
  static fromPanel(data, { unit, outcome, period, riskScore, prePeriods } = {}) {
    const df = data instanceof DataFrame ? data : DataFrame.fromRows(typeof data?.toRows === "function" ? data.toRows() : data);
    const design = new ExperimentDesign();

    // Collapse to unit level: event rate per unit, mean risk score.
    const aggs = [col(outcome).mean().alias("__rate")];
    if (riskScore) aggs.push(col(riskScore).mean().alias("__score"));
    const perUnit = df.groupBy(unit).agg(...aggs);

    // CUPED ρ from the data, when the panel can support it: correlation of
    // unit-level pre-window vs post-window outcome rates. The number every
    // notebook assumes is 0.5; yours is whatever it is.
    let estimatedCorrelation = null;
    if (period && prePeriods?.length) {
      const preSet = new Set(prePeriods);
      const withWindow = df.withColumns(
        col(period).map((p) => (preSet.has(p) ? 1 : 0)).alias("__isPre")
      );
      const pre = withWindow.filter(col("__isPre").eq(1)).groupBy(unit).agg(col(outcome).mean().alias("__pre"));
      const post = withWindow.filter(col("__isPre").eq(0)).groupBy(unit).agg(col(outcome).mean().alias("__post"));
      const joined = pre.join(post, { on: unit, how: "inner" });
      if (joined.height > 10) estimatedCorrelation = joined.corr("__pre", "__post");
    }

    design._panelMeta = { perUnit, unit, outcome, riskScore: riskScore ?? null, estimatedCorrelation };
    if (estimatedCorrelation != null) design._power.correlation = estimatedCorrelation;
    return design;
  }

  /** Start without data: strata declared by hand, rates from your slides. */
  static fromAssumptions() {
    return new ExperimentDesign();
  }

  /**
   * Define risk strata. With panel data and a risk score:
   *   .stratify({ high: [0.7, 1], low: [0.2, 0.7] })  // score quantile bands
   * Without panel data, declare everything:
   *   .stratify([{ name: "high", baseRate: 0.18, available: 40000 }])
   */
  stratify(spec) {
    if (Array.isArray(spec)) {
      this._strata = spec.map((s) => ({ ...s }));
      return this;
    }
    if (!this._panelMeta?.riskScore) {
      throw new Error(`Quantile stratification needs fromPanel(..., { riskScore }). Otherwise pass explicit strata.`);
    }
    const { perUnit } = this._panelMeta;
    const scores = perUnit.getColumn("__score").toArray().filter((v) => v != null).sort((a, b) => a - b);
    const q = (p) => scores[Math.min(scores.length - 1, Math.floor(p * scores.length))];
    const rows = perUnit.toRows();
    this._strata = Object.entries(spec).map(([name, [lo, hi]]) => {
      const loV = q(lo);
      const hiV = q(hi === 1 ? 0.999999 : hi);
      const members = rows.filter((r) => r.__score >= loV && (hi === 1 ? true : r.__score < hiV) && r.__score != null);
      const baseRate = members.reduce((a, r) => a + r.__rate, 0) / members.length;
      return { name, baseRate, available: members.length, quantiles: [lo, hi] };
    });
    return this;
  }

  /**
   * Define treatment arms. Each arm carries either a `relativeEffect`
   * directly (−0.10 = 10% relative reduction) or a `channels` cascade from
   * which the effect and per-member cost are derived.
   *
   * @param {Array<{name: string, relativeEffect?: number,
   *   channels?: Array<{reach, open, efficacy, cost}>, costPerMember?: number}>} arms
   */
  arms(arms) {
    this._arms = arms.map((a) => {
      if (a.channels) {
        const cascade = channelCascade(a.channels);
        return {
          name: a.name,
          relativeEffect: a.relativeEffect ?? -cascade.relativeEffect, // outreach prevents events
          costPerMember: a.costPerMember ?? cascade.costPerMember,
          channels: a.channels,
        };
      }
      if (a.relativeEffect == null) {
        throw new Error(`Arm "${a.name}" needs relativeEffect or channels. An arm with no effect is a control, and you get one free.`);
      }
      return { name: a.name, relativeEffect: a.relativeEffect, costPerMember: a.costPerMember ?? 0, channels: null };
    });
    return this;
  }

  /**
   * Statistical settings. `method`: "standard" | "cuped" | "bayesian" |
   * "simulation". If fromPanel estimated a CUPED ρ, it is already the
   * default correlation; override here to be more pessimistic (recommended)
   * or more optimistic (**this is extremely optimistic**, but it's your study).
   */
  power(settings) {
    this._power = { ...this._power, ...settings };
    return this;
  }

  /** "rct" (one treatment vs control), "three-arm", or "factorial" (2×2 via two factors). */
  design(type) {
    this._design = type;
    return this;
  }

  /**
   * Compute everything: per-stratum × per-arm sample sizes with the
   * multi-arm adjustment, feasibility against available population, and an
   * Experiment object wired into the plots and the business case.
   */
  build() {
    if (!this._strata.length) throw new Error(`No strata. stratify() first; an unstratified experiment is a coin flip with a budget.`);
    if (!this._arms.length) throw new Error(`No arms. arms() first.`);

    const nArms = this._arms.length + 1; // + control
    const adjustment = multiArmAdjustment(nArms);

    const cells = [];
    for (const stratum of this._strata) {
      for (const arm of this._arms) {
        const p1 = stratum.baseRate;
        const p2 = p1 * (1 + arm.relativeEffect);
        const base = sampleSizePerArm({
          p1,
          p2,
          alpha: this._power.alpha,
          power: this._power.power,
          effectSize: this._power.effectSize,
          method: this._power.method === "simulation" ? "standard" : this._power.method,
          correlation: this._power.correlation,
          priorWeight: this._power.priorWeight,
        });
        const nPerArm = Number.isFinite(base.nPerArm) ? Math.ceil(base.nPerArm * adjustment) : Infinity;
        cells.push({
          stratum: stratum.name,
          arm: arm.name,
          p1,
          p2,
          relativeEffect: arm.relativeEffect,
          nPerArm,
          varianceReduction: base.varianceReduction,
          costPerMember: arm.costPerMember,
        });
      }
    }

    return new Experiment({
      strata: this._strata,
      arms: this._arms,
      cells,
      nArms,
      adjustment,
      designType: this._design,
      power: { ...this._power },
      panelMeta: this._panelMeta,
    });
  }
}

// ---------------------------------------------------------------------------
// the built experiment — one object, every output
// ---------------------------------------------------------------------------

export class Experiment {
  constructor(fields) {
    Object.assign(this, fields);
  }

  /** Required n in a stratum: nPerArm × total arms (shared control included). */
  requiredInStratum(stratumName) {
    const stratumCells = this.cells.filter((c) => c.stratum === stratumName);
    const maxPerArm = Math.max(...stratumCells.map((c) => c.nPerArm));
    return Number.isFinite(maxPerArm) ? maxPerArm * this.nArms : Infinity;
  }

  /** Per-stratum feasibility: does the population you have cover the n you need? */
  feasibility() {
    return this.strata.map((s) => {
      const required = this.requiredInStratum(s.name);
      return {
        stratum: s.name,
        required,
        available: s.available ?? null,
        feasible: s.available != null ? s.available >= required : null,
      };
    });
  }

  /** The design as a DataFrame: one row per stratum × arm. groupBy away. */
  toDataFrame() {
    return DataFrame.fromRows(
      this.cells.map((c) => ({
        stratum: c.stratum,
        arm: c.arm,
        base_rate: c.p1,
        target_rate: c.p2,
        relative_effect: c.relativeEffect,
        n_per_arm: c.nPerArm,
        variance_reduction: c.varianceReduction,
        cost_per_member: c.costPerMember,
      }))
    );
  }

  /**
   * Config for plots' experimentDesignTree: one cohort per stratum, using
   * the first arm's effect (the tree is a two-arm picture; multi-arm detail
   * lives in toDataFrame and the ratio table).
   */
  toDesignTree({ months = {} } = {}) {
    const arm = this.arms[0];
    return {
      cohorts: this.strata.map((s) => ({
        label: s.name,
        baseRate: s.baseRate,
        behaviorChange: s.baseRate * Math.abs(arm.relativeEffect),
        channels: arm.channels ? arm.channels.map((c) => c.name ?? "touch").join(" + ") : arm.name,
        months: months[s.name],
      })),
      inputs: {
        alpha: this.power.alpha,
        power: this.power.power,
        attributable: 1,
        design: "two-sided-proportions",
      },
    };
  }

  /**
   * Config for plots' consortDiagram: screening spine from panel data when
   * available, then one allocation arm per treatment arm plus control,
   * sized at the computed n.
   */
  toConsort({ screened } = {}) {
    const totalRequired = this.strata.reduce((a, s) => a + this.requiredInStratum(s.name), 0);
    const available = this.strata.reduce((a, s) => a + (s.available ?? 0), 0);
    const assessed = screened ?? (this.panelMeta ? this.panelMeta.perUnit.height : available);
    const perArm = Math.round(totalRequired / this.nArms);

    return {
      title: "Planned participant flow",
      steps: [
        { label: "Assessed for eligibility", n: assessed },
        {
          label: "Randomized",
          n: totalRequired,
          excluded: {
            label: "Not randomized",
            n: Math.max(0, assessed - totalRequired),
            reasons: [{ label: "Outside strata or beyond required n", n: Math.max(0, assessed - totalRequired) }],
          },
        },
      ],
      arms: [
        ...this.arms.map((a) => ({ label: a.name, n: perArm, steps: [{ label: "Analyzed (planned)", n: perArm }] })),
        { label: "Control", n: perArm, steps: [{ label: "Analyzed (planned)", n: perArm }] },
      ],
    };
  }

  /**
   * Config for plots' measurementTimeline: identification, outreach across
   * `outreachMonths`, observation until `horizonMonths`, claims lag after.
   */
  toTimeline({ startMonthLabels, outreachMonths = 2, horizonMonths = 12, lagMonths = 1 } = {}) {
    const months =
      startMonthLabels ??
      Array.from({ length: horizonMonths + lagMonths + 2 }, (_, i) => `M${i}`);
    return {
      months,
      rows: this.strata.map((s, i) => ({
        group: s.name,
        risk: "",
        segments: [
          { start: 0, len: 1, type: "identify", label: "Identify" },
          { start: 1, len: outreachMonths, type: "outreach", label: this.arms[0].name },
          { start: 1 + outreachMonths, len: Math.max(1, horizonMonths - outreachMonths - 1), type: "observe", label: "Obs period" },
          { start: horizonMonths, len: lagMonths, type: "lag", label: "Claims lag" },
        ],
        opsMonths: Array.from({ length: outreachMonths + 1 }, (_, k) => 1 + k),
        clinicalMonth: horizonMonths + lagMonths,
        clinicalLabel: `${s.name} endpoint observable`,
      })),
      financialStartMonth: horizonMonths + lagMonths,
    };
  }

  /**
   * The business case: events prevented, program cost, value, ROI.
   * ROI is defined as (value − cost) / cost. Pro forma, not measured
   * savings; the DiD page is where measurement lives.
   *
   * @param {{eventValue: number, horizonMonths?: number}} options value per prevented event
   */
  businessCase({ eventValue, horizonMonths = 12 } = {}) {
    if (!eventValue) throw new Error(`businessCase needs eventValue: a prevented event is worth something, name it.`);
    const rows = this.cells.map((c) => {
      const treatedN = c.nPerArm;
      const eventsPrevented = treatedN * c.p1 * Math.abs(c.relativeEffect) * (horizonMonths / 12);
      const cost = treatedN * c.costPerMember;
      const value = eventsPrevented * eventValue;
      return {
        stratum: c.stratum,
        arm: c.arm,
        treated_n: treatedN,
        events_prevented: +eventsPrevented.toFixed(1),
        program_cost: +cost.toFixed(0),
        value: +value.toFixed(0),
        net: +(value - cost).toFixed(0),
        roi: cost > 0 ? +((value - cost) / cost).toFixed(2) : null,
      };
    });
    const totals = rows.reduce(
      (acc, r) => ({
        events_prevented: acc.events_prevented + r.events_prevented,
        program_cost: acc.program_cost + r.program_cost,
        value: acc.value + r.value,
      }),
      { events_prevented: 0, program_cost: 0, value: 0 }
    );
    return {
      byCell: DataFrame.fromRows(rows),
      totals: {
        ...totals,
        net: totals.value - totals.program_cost,
        roi: totals.program_cost > 0 ? (totals.value - totals.program_cost) / totals.program_cost : null,
      },
    };
  }

  /**
   * Monte Carlo validation: simulate the trial at the computed n and count
   * how often the two-proportion z-test rejects. Empirical power should
   * land near the design power; when it does not, the arithmetic and the
   * assumptions are having a disagreement worth attending.
   *
   * @param {{nSimulations?: number, random?: () => number}} [options]
   * @returns {Array<{stratum, arm, nPerArm, targetPower, empiricalPower}>}
   */
  simulatePower({ nSimulations = 500, random = Math.random } = {}) {
    const zA = probit(1 - this.power.alpha / 2);
    // CUPED/Bayesian designs simulate at the effective n their variance
    // reduction promises; the simulation validates the base arithmetic.
    const inflate = (n, vr) => Math.round(n / Math.max(1e-9, 1 - vr));

    return this.cells.map((c) => {
      if (!Number.isFinite(c.nPerArm)) {
        return { stratum: c.stratum, arm: c.arm, nPerArm: c.nPerArm, targetPower: this.power.power, empiricalPower: 0 };
      }
      const n = inflate(c.nPerArm, c.varianceReduction);
      let rejected = 0;
      for (let s = 0; s < nSimulations; s++) {
        let x1 = 0;
        let x2 = 0;
        for (let i = 0; i < n; i++) {
          if (random() < c.p1) x1++;
          if (random() < c.p2) x2++;
        }
        const ph1 = x1 / n;
        const ph2 = x2 / n;
        const pPool = (x1 + x2) / (2 * n);
        const se = Math.sqrt(pPool * (1 - pPool) * (2 / n));
        if (se > 0 && Math.abs(ph2 - ph1) / se > zA) rejected++;
      }
      return {
        stratum: c.stratum,
        arm: c.arm,
        nPerArm: c.nPerArm,
        targetPower: this.power.power,
        empiricalPower: rejected / nSimulations,
      };
    });
  }

  /** Human-readable summary, for the cell before the figures. */
  describe() {
    const lines = [
      `Experiment: ${this.designType} (${this.nArms} arms, multi-arm adjustment ×${this.adjustment})`,
      `Power: ${(this.power.power * 100).toFixed(0)}% at α=${this.power.alpha}, method=${this.power.method}` +
        (this.power.method === "cuped" ? ` (ρ=${this.power.correlation.toFixed(2)}, n reduced ${(this.power.correlation ** 2 * 100).toFixed(0)}%)` : ""),
    ];
    for (const f of this.feasibility()) {
      lines.push(
        `  ${f.stratum}: need ${Number.isFinite(f.required) ? f.required.toLocaleString() : "∞"}` +
          (f.available != null ? ` of ${f.available.toLocaleString()} available → ${f.feasible ? "feasible" : "NOT feasible"}` : "")
      );
    }
    return lines.join("\n");
  }
}
