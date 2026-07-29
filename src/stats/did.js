/**
 * did.js — difference-in-differences, the fundamental-statistics section.
 *
 * A JavaScript take on the diff-diff library's core estimators, built on
 * our DataFrame and regression engine:
 *
 *   did()                — the classic 2×2: treated × post, robust or
 *                          cluster-robust SEs, optional covariates
 *   twfe()               — two-way fixed effects for panels, via within
 *                          transformation (no dummy explosion)
 *   eventStudy()         — period-specific effects around adoption
 *                          (MultiPeriodDiD), the pre-trend picture
 *   callawaySantAnna()   — group-time ATT(g,t) for staggered adoption,
 *                          with overall / group / event-time aggregation
 *                          and optional unit-level bootstrap SEs
 *   checkParallelTrends()— pre-period trend interaction test
 *   placeboTest()        — fake the treatment date, hope for nothing
 *
 * House rules, stated once: every estimator accepts a DataFrame or rows;
 * "never treated" is encoded as a null/undefined/0/Infinity group; and an
 * insignificant pre-trend test does not prove parallel trends, it merely
 * fails to disprove them — a distinction that has ended friendships.
 */

import { fitOLS, withinTransform } from "./regression.js";
import { tTwoSidedP, probit } from "./special.js";

/** DataFrame-or-rows → rows. The usual adapter. */
function toRows(data) {
  return typeof data?.toRows === "function" ? data.toRows() : data;
}

const NEVER = (g) => g == null || g === 0 || g === Infinity;

// ---------------------------------------------------------------------------
// 2×2 DiD
// ---------------------------------------------------------------------------

/**
 * The classic 2×2 difference-in-differences: outcome ~ treated + post +
 * treated×post (+ covariates), where the interaction coefficient is the
 * ATT. Two groups, two periods, one assumption doing all the work.
 *
 * @param {DataFrame|Array<object>} data
 * @param {object} options
 * @param {string} options.outcome
 * @param {string} options.treatment 0/1 (or truthy) treated-group indicator
 * @param {string} options.time 0/1 (or truthy) post-period indicator
 * @param {string[]} [options.covariates=[]]
 * @param {string} [options.cluster] cluster id column → CR1 SEs; otherwise HC1
 * @param {"hc1"|"classical"|"cluster"} [options.vcov] override the SE flavor;
 *   "classical" reproduces textbook (and diff-diff default) output exactly
 * @returns {{att, se, tStat, pValue, ci, n, nTreated, nControl, nClusters,
 *            model, terms, summary: () => string}}
 */
export function did(data, { outcome, treatment, time, covariates = [], cluster = null, vcov } = {}) {
  const rows = toRows(data);
  const terms = ["(intercept)", "treated", "post", "treated:post", ...covariates];
  const X = rows.map((r) => [
    1,
    r[treatment] ? 1 : 0,
    r[time] ? 1 : 0,
    (r[treatment] ? 1 : 0) * (r[time] ? 1 : 0),
    ...covariates.map((c) => Number(r[c])),
  ]);
  const y = rows.map((r) => Number(r[outcome]));
  const model = fitOLS(X, y, {
    vcov: vcov ?? (cluster ? "cluster" : "hc1"),
    clusters: cluster ? rows.map((r) => r[cluster]) : null,
    terms,
  });
  const i = 3; // treated:post — the coefficient the meeting is about
  return didResult({
    att: model.beta[i],
    se: model.se[i],
    tStat: model.tStats[i],
    pValue: model.pValues[i],
    ci: model.ci[i],
    n: model.n,
    nTreated: rows.filter((r) => r[treatment]).length,
    nControl: rows.filter((r) => !r[treatment]).length,
    nClusters: model.nClusters,
    model,
    terms,
    estimator: "DifferenceInDifferences",
  });
}

// ---------------------------------------------------------------------------
// Two-way fixed effects
// ---------------------------------------------------------------------------

/**
 * Two-way fixed effects DiD for panel data: outcome on a treatment
 * indicator with unit and time fixed effects, absorbed by iterated
 * demeaning instead of a dummy for every member and month. SEs cluster on
 * the unit by default, per standing orders.
 *
 * The obligatory caveat: under staggered adoption with heterogeneous
 * effects, TWFE is a weighted average with weights nobody agreed to
 * (Goodman-Bacon 2021). That's what callawaySantAnna() is for.
 *
 * @param {DataFrame|Array<object>} data
 * @param {object} options
 * @param {string} options.outcome
 * @param {string} options.treatment 0/1 treated-in-this-period indicator
 * @param {string} options.unit / @param {string} options.time
 * @param {string[]} [options.covariates=[]] (time-varying; FE eat the rest)
 * @param {string} [options.cluster] defaults to the unit column
 */
export function twfe(data, { outcome, treatment, unit, time, covariates = [], cluster } = {}) {
  const rows = toRows(data);
  const unitIds = rows.map((r) => r[unit]);
  const timeIds = rows.map((r) => r[time]);

  const yTilde = withinTransform(rows.map((r) => Number(r[outcome])), unitIds, timeIds);
  const dTilde = withinTransform(rows.map((r) => (r[treatment] ? 1 : 0)), unitIds, timeIds);
  const covTilde = covariates.map((c) =>
    withinTransform(rows.map((r) => Number(r[c])), unitIds, timeIds)
  );

  const terms = ["treatment", ...covariates];
  const X = rows.map((_, i) => [dTilde[i], ...covTilde.map((col) => col[i])]);
  const model = fitOLS(X, yTilde, {
    vcov: "cluster",
    clusters: rows.map((r) => r[cluster ?? unit]),
    terms,
  });

  return didResult({
    att: model.beta[0],
    se: model.se[0],
    tStat: model.tStats[0],
    pValue: model.pValues[0],
    ci: model.ci[0],
    n: model.n,
    nTreated: rows.filter((r) => r[treatment]).length,
    nControl: rows.filter((r) => !r[treatment]).length,
    nClusters: model.nClusters,
    model,
    terms,
    estimator: "TwoWayFixedEffects",
  });
}

// ---------------------------------------------------------------------------
// Event study (MultiPeriodDiD)
// ---------------------------------------------------------------------------

/**
 * Event study: period-specific treatment effects relative to adoption,
 * with unit and time fixed effects. Coefficients at e = −2, −3, … are the
 * pre-trends everyone will scrutinize; e = 0, 1, 2, … are the dynamics
 * you actually wanted. e = −1 is the omitted reference, as is law.
 *
 * @param {DataFrame|Array<object>} data
 * @param {object} options
 * @param {string} options.outcome / unit / time (numeric or sortable periods)
 * @param {string} options.group column with each unit's adoption period
 *   (null/0/Infinity = never treated)
 * @param {number} [options.reference=-1] omitted event time
 * @param {string} [options.cluster] defaults to unit
 * @param {[number, number]} [options.window] clamp event times, e.g. [-6, 8];
 *   effects beyond the window bin into its endpoints
 * @returns {{effects: [{eventTime, estimate, se, ci, pValue, n}], att, model}}
 */
export function eventStudy(
  data,
  { outcome, unit, time, group, reference = -1, cluster, window = null } = {}
) {
  const rows = toRows(data);
  const unitIds = rows.map((r) => r[unit]);
  const timeIds = rows.map((r) => r[time]);

  // Event time per row; never-treated rows get no dummies (pure controls).
  const clamp = (e) => {
    if (!window) return e;
    return Math.max(window[0], Math.min(window[1], e));
  };
  const eventTimes = rows.map((r) => (NEVER(r[group]) ? null : clamp(Number(r[time]) - Number(r[group]))));

  const levels = [...new Set(eventTimes.filter((e) => e !== null && e !== reference))].sort((a, b) => a - b);
  if (levels.length === 0) {
    throw new Error(`eventStudy found no event times besides the reference. One period is a before-after, not a study.`);
  }

  // Dummies for each event time, within-transformed alongside the outcome
  // (Frisch–Waugh–Lovell says this is the same as running the dummies).
  const yTilde = withinTransform(rows.map((r) => Number(r[outcome])), unitIds, timeIds);
  const dummiesTilde = levels.map((e) =>
    withinTransform(rows.map((_, i) => (eventTimes[i] === e ? 1 : 0)), unitIds, timeIds)
  );

  const terms = levels.map((e) => `e=${e}`);
  const X = rows.map((_, i) => dummiesTilde.map((col) => col[i]));
  const model = fitOLS(X, yTilde, {
    vcov: "cluster",
    clusters: rows.map((r) => r[cluster ?? unit]),
    terms,
  });

  const counts = new Map();
  for (const e of eventTimes) if (e !== null) counts.set(e, (counts.get(e) ?? 0) + 1);

  const effects = levels.map((e, i) => ({
    eventTime: e,
    estimate: model.beta[i],
    se: model.se[i],
    ci: model.ci[i],
    pValue: model.pValues[i],
    n: counts.get(e) ?? 0,
  }));
  // Reference period pinned at zero by construction; included for plotting.
  effects.push({ eventTime: reference, estimate: 0, se: 0, ci: [0, 0], pValue: 1, n: counts.get(reference) ?? 0, reference: true });
  effects.sort((a, b) => a.eventTime - b.eventTime);

  const post = effects.filter((d) => d.eventTime >= 0 && !d.reference);
  const att = post.length ? post.reduce((acc, d) => acc + d.estimate * d.n, 0) / post.reduce((acc, d) => acc + d.n, 0) : null;

  return { effects, att, model, estimator: "EventStudy" };
}

// ---------------------------------------------------------------------------
// Callaway & Sant'Anna (2021)
// ---------------------------------------------------------------------------

/**
 * Callaway–Sant'Anna group-time ATTs for staggered adoption, unconditional
 * parallel trends flavor. For each adoption cohort g and period t, the
 * ATT(g,t) is a clean 2×2 on unit-level long differences: cohort g's
 * change from its base period versus the comparison units' change over
 * the same window. No forbidden comparisons, no already-treated units
 * moonlighting as controls — the whole reason this estimator exists.
 *
 * Base periods follow the R `did` defaults: g−1 (the period before
 * adoption) for post periods; the immediately preceding period for pre
 * periods, so pre-estimates read as short-run placebo checks.
 *
 * SEs: analytic per ATT(g,t) (two-sample, unit-level). Aggregations
 * default to the independence approximation across (g,t) cells; pass
 * `bootstrap: 999` for unit-level bootstrap SEs when the number matters.
 *
 * @param {DataFrame|Array<object>} data long panel
 * @param {object} options
 * @param {string} options.outcome / unit / time
 * @param {string} options.group adoption period per unit (null/0/Infinity = never)
 * @param {"never"|"notyet"} [options.control="never"] comparison group
 * @param {number} [options.bootstrap=0] unit-bootstrap iterations for aggregate SEs
 * @param {() => number} [options.random=Math.random] RNG, injectable for reproducibility
 * @returns {{attgt: Array<{group, time, eventTime, att, se, nTreated, nControl}>,
 *            overall: {att, se, ci, pValue}, byGroup: [...], byEventTime: [...]}}
 */
export function callawaySantAnna(
  data,
  { outcome, unit, time, group, control = "never", bootstrap = 0, random = Math.random } = {}
) {
  const rows = toRows(data);

  // Wide layout: unit → {group, outcomes: Map(time → y)}.
  const units = new Map();
  for (const r of rows) {
    const u = r[unit];
    let entry = units.get(u);
    if (!entry) units.set(u, (entry = { group: r[group], outcomes: new Map() }));
    entry.outcomes.set(Number(r[time]), Number(r[outcome]));
  }
  const times = [...new Set(rows.map((r) => Number(r[time])))].sort((a, b) => a - b);
  const groups = [...new Set([...units.values()].map((e) => e.group))]
    .filter((g) => !NEVER(g))
    .map(Number)
    .sort((a, b) => a - b);
  if (!groups.length) throw new Error(`No treated cohorts found in "${group}". Staggered adoption needs adopters.`);

  const unitList = [...units.entries()].map(([id, e]) => ({ id, group: e.group, outcomes: e.outcomes }));

  const attgtOn = (list) => {
    const cells = [];
    for (const g of groups) {
      const baseFor = (t) => (t >= g ? prevTime(times, g) : prevTime(times, t));
      for (const t of times) {
        if (t === prevTime(times, g) && t < g) continue; // base of itself
        const base = baseFor(t);
        if (base == null) continue;
        const deltas = { treat: [], ctrl: [] };
        for (const u of list) {
          const y1 = u.outcomes.get(t);
          const y0 = u.outcomes.get(base);
          if (y1 == null || y0 == null) continue;
          const d = y1 - y0;
          if (Number(u.group) === g) deltas.treat.push(d);
          else if (NEVER(u.group)) deltas.ctrl.push(d);
          else if (control === "notyet" && Number(u.group) > Math.max(t, g)) deltas.ctrl.push(d);
        }
        if (deltas.treat.length < 2 || deltas.ctrl.length < 2) continue;
        const mt = mean(deltas.treat);
        const mc = mean(deltas.ctrl);
        cells.push({
          group: g,
          time: t,
          eventTime: t - g,
          att: mt - mc,
          se: Math.sqrt(variance(deltas.treat) / deltas.treat.length + variance(deltas.ctrl) / deltas.ctrl.length),
          nTreated: deltas.treat.length,
          nControl: deltas.ctrl.length,
        });
      }
    }
    return cells;
  };

  const attgt = attgtOn(unitList);
  if (!attgt.length) {
    throw new Error(
      `No estimable ATT(g,t) cells — check that cohorts and ${control === "never" ? "never-treated" : "not-yet-treated"} comparisons overlap in time.`
    );
  }

  // Aggregations: weights are cohort sizes among post cells.
  const aggregate = (cells, keyFn) => {
    const buckets = new Map();
    for (const c of cells) {
      const key = keyFn(c);
      if (key === undefined) continue;
      if (!buckets.has(key)) buckets.set(key, []);
      buckets.get(key).push(c);
    }
    return [...buckets.entries()]
      .map(([key, cs]) => {
        const w = cs.map((c) => c.nTreated);
        const totalW = w.reduce((a, b) => a + b, 0);
        const att = cs.reduce((acc, c, i) => acc + c.att * w[i], 0) / totalW;
        // Independence approximation across cells; bootstrap upgrades this.
        const se = Math.sqrt(cs.reduce((acc, c, i) => acc + (w[i] / totalW) ** 2 * c.se ** 2, 0));
        return { key, att, se, nCells: cs.length };
      })
      .sort((a, b) => a.key - b.key);
  };

  const postOnly = (c) => (c.eventTime >= 0 ? 0 : undefined);
  let overallAgg = aggregate(attgt, postOnly)[0];
  let byGroup = aggregate(attgt, (c) => (c.eventTime >= 0 ? c.group : undefined));
  let byEventTime = aggregate(attgt, (c) => c.eventTime);

  if (bootstrap > 0) {
    // Unit bootstrap: resample units, recompute every aggregate, take the
    // empirical SD. Slower, honester about cross-cell covariance.
    const draws = { overall: [], byGroup: new Map(), byEventTime: new Map() };
    for (let b = 0; b < bootstrap; b++) {
      const resampled = Array.from({ length: unitList.length }, () => unitList[(random() * unitList.length) | 0]);
      let cells;
      try {
        cells = attgtOn(resampled);
      } catch {
        continue;
      }
      if (!cells.length) continue;
      const o = aggregate(cells, postOnly)[0];
      if (o) draws.overall.push(o.att);
      for (const r of aggregate(cells, (c) => (c.eventTime >= 0 ? c.group : undefined))) {
        (draws.byGroup.get(r.key) ?? draws.byGroup.set(r.key, []).get(r.key)).push(r.att);
      }
      for (const r of aggregate(cells, (c) => c.eventTime)) {
        (draws.byEventTime.get(r.key) ?? draws.byEventTime.set(r.key, []).get(r.key)).push(r.att);
      }
    }
    const sd = (xs) => (xs.length > 1 ? Math.sqrt(variance(xs)) : null);
    if (draws.overall.length > 1) overallAgg = { ...overallAgg, se: sd(draws.overall), bootstrapped: true };
    byGroup = byGroup.map((r) => ({ ...r, se: sd(draws.byGroup.get(r.key)) ?? r.se, bootstrapped: true }));
    byEventTime = byEventTime.map((r) => ({ ...r, se: sd(draws.byEventTime.get(r.key)) ?? r.se, bootstrapped: true }));
  }

  const z = probit(0.975);
  const dress = (r) => ({
    ...r,
    ci: [r.att - z * r.se, r.att + z * r.se],
    pValue: r.se > 0 ? 2 * (1 - normCDFApprox(Math.abs(r.att / r.se))) : 1,
  });

  return {
    attgt,
    overall: dress({ ...overallAgg }),
    byGroup: byGroup.map((r) => dress({ group: r.key, ...r })),
    byEventTime: byEventTime.map((r) => dress({ eventTime: r.key, estimate: r.att, ...r })),
    estimator: "CallawaySantAnna",
    control,
  };
}

// ---------------------------------------------------------------------------
// Diagnostics
// ---------------------------------------------------------------------------

/**
 * Pre-period parallel trends check: on pre-treatment data only, regress
 * the outcome on time, treated, and time×treated. The interaction slope
 * is the differential pre-trend; small and insignificant is the best you
 * can hope for, and it is evidence of absence of evidence, not the other
 * thing. For staggered designs, read the event-study pre-coefficients
 * instead — this test assumes one treatment date.
 *
 * @param {DataFrame|Array<object>} data
 * @param {{outcome, treatment, time, treatmentStart, cluster?}} options
 *   time: numeric period column; treatmentStart: first treated period
 * @returns {{slope, se, tStat, pValue, ci, n, passed}}
 */
export function checkParallelTrends(data, { outcome, treatment, time, treatmentStart, cluster = null } = {}) {
  const rows = toRows(data).filter((r) => Number(r[time]) < treatmentStart);
  if (rows.length < 8) {
    throw new Error(`Only ${rows.length} pre-period rows: a trend test needs a trend to test.`);
  }
  const X = rows.map((r) => {
    const t = Number(r[time]);
    const d = r[treatment] ? 1 : 0;
    return [1, t, d, t * d];
  });
  const y = rows.map((r) => Number(r[outcome]));
  const model = fitOLS(X, y, {
    vcov: cluster ? "cluster" : "hc1",
    clusters: cluster ? rows.map((r) => r[cluster]) : null,
    terms: ["(intercept)", "time", "treated", "time:treated"],
  });
  return {
    slope: model.beta[3],
    se: model.se[3],
    tStat: model.tStats[3],
    pValue: model.pValues[3],
    ci: model.ci[3],
    n: model.n,
    passed: model.pValues[3] > 0.05,
    note: "Insignificant ≠ proven parallel. It never did.",
  };
}

/**
 * Placebo timing test: rerun the 2×2 entirely inside the pre-period with
 * an invented treatment date. A "significant" placebo effect means your
 * design finds treatment effects where none existed, which is worth
 * knowing before the real estimate goes in a deck.
 *
 * @param {DataFrame|Array<object>} data
 * @param {{outcome, treatment, time, treatmentStart, placeboStart, cluster?}} options
 *   time is numeric; placeboStart defaults to the midpoint of the pre-period
 */
export function placeboTest(data, { outcome, treatment, time, treatmentStart, placeboStart, cluster = null } = {}) {
  const pre = toRows(data).filter((r) => Number(r[time]) < treatmentStart);
  const periods = [...new Set(pre.map((r) => Number(r[time])))].sort((a, b) => a - b);
  if (periods.length < 2) throw new Error(`Placebo timing needs at least 2 pre-periods to fake a treatment between.`);
  const cut = placeboStart ?? periods[Math.floor(periods.length / 2)];
  const rows = pre.map((r) => ({ ...r, __placeboPost: Number(r[time]) >= cut ? 1 : 0 }));
  const result = did(rows, { outcome, treatment, time: "__placeboPost", cluster });
  return { ...result, placeboStart: cut, passed: result.pValue > 0.05 };
}

// ---------------------------------------------------------------------------
// shared plumbing
// ---------------------------------------------------------------------------

function didResult(fields) {
  return {
    ...fields,
    summary() {
      const { estimator, att, se, tStat, pValue, ci, n, nTreated, nControl, nClusters } = fields;
      return [
        `${estimator}`,
        `ATT: ${att.toFixed(4)}  SE: ${se.toFixed(4)}  t: ${tStat.toFixed(3)}  p: ${pValue < 0.001 ? "<0.001" : pValue.toFixed(4)}`,
        `95% CI: [${ci[0].toFixed(4)}, ${ci[1].toFixed(4)}]`,
        `n: ${n} (treated: ${nTreated}, control: ${nControl})${nClusters ? `  clusters: ${nClusters}` : ""}`,
      ].join("\n");
    },
  };
}

function prevTime(times, t) {
  const i = times.indexOf(t);
  return i > 0 ? times[i - 1] : null;
}

function mean(xs) {
  return xs.reduce((a, b) => a + b, 0) / xs.length;
}

function variance(xs) {
  const m = mean(xs);
  return xs.reduce((a, b) => a + (b - m) ** 2, 0) / (xs.length - 1);
}

// Local light normal CDF for aggregate p-values (large-sample z).
function normCDFApprox(z) {
  const t = 1 / (1 + 0.3275911 * (z / Math.SQRT2));
  const y =
    1 -
    ((((1.061405429 * t - 1.453152027) * t + 1.421413741) * t - 0.284496736) * t + 0.254829592) *
      t *
      Math.exp(-(z * z) / 2);
  return 0.5 * (1 + y);
}
