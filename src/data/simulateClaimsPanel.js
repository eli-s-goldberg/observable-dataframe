/**
 * simulateClaimsPanel.js: a seeded, entirely synthetic member-month panel.
 *
 * NOTHING IN THIS FILE IS REAL, AND NOTHING IT PRODUCES IS REAL. Every number
 * below is an illustrative round value, chosen by hand so that the output has
 * the *shape* a payer claims extract has, not the values of one. No parameter
 * here is a measurement of any population, and the output contains no real
 * members, claims, dates, or amounts. Treat it as a fixture, never as evidence.
 *
 * The shapes it reproduces, and the mechanism that produces each:
 *
 *   overdispersed counts        member intensity ~ Gamma, month shock ~ Gamma,
 *                              count ~ Poisson(intensity x shock), so the
 *                              variance-to-mean ratio lands far above 1
 *   a mass at zero             a share of member-months are drawn inactive
 *                              outright, on top of the Poisson zeros
 *   persistent high utilizers  the member intensity is drawn once and reused
 *                              every month, so utilization autocorrelates
 *   right-skewed spend         paid amount per fill is lognormal
 *   enrollment churn           a share of members get a late start, an early
 *                              end, or a mid-window gap, and carry
 *                              enrolled_flag = 0 with no utilization there
 *   mild seasonality           a cosine on the calendar month, peaking in
 *                              winter
 *
 * The design also plants a treatment effect. Cohorts adopt at staggered
 * periods, a share of members never adopt, and on treated member-months each
 * would-be encounter is averted independently with probability
 * (1 - rateRatio), phased in over rampMonths. Because the averting is a
 * thinning of a draw the simulator already has in hand, the untreated
 * counterfactual is known for every treated cell, so the true average
 * treatment effect on the treated is returned exactly in `truth` rather than
 * assumed. Cohort assignment is independent of member intensity, so parallel
 * trends holds by construction and the pre-trend and placebo diagnostics have
 * nothing to find.
 */

import { random } from "../core/random.js";

/** Published column order. The loader, the CSV, and the dtypes agree here. */
export const CLAIMS_PANEL_COLUMNS = [
  "person_id",
  "month",
  "period",
  "cohort",
  "treated_now",
  "medical_claims",
  "pharmacy_fills",
  "pharmacy_paid",
  "enrolled_flag",
];

/**
 * Illustrative defaults. Round numbers on purpose; see the file header.
 */
export const CLAIMS_PANEL_DEFAULTS = {
  /**
   * 1,500 members over 24 months is a deliberate floor, not a round number
   * pulled from the air. Each Callaway-Sant'Anna ATT(g,t) for a cohort shares
   * one base period, so the noise in that single base mean shifts every cell
   * for that cohort together and never averages out across periods. Cohort
   * precision therefore scales with members per cohort and not at all with
   * months, and 300 members per cohort against counts this dispersed puts the
   * cohort-level standard error near a third of the planted effect. Cutting the
   * panel to a few hundred members makes the cohort estimates unusable while
   * leaving two-way fixed effects, which pools everything, looking fine.
   */
  members: 1500,
  months: 24,
  startMonth: "2024-01",
  seed: 42,

  /** Mean encounters per enrolled member-month, before zero inflation. */
  baselineIntensity: 6,
  /** Gamma shape for member intensity. 1 gives a coefficient of variation of 1. */
  intensityShape: 1,
  /** Gamma shape for the month-to-month multiplicative shock (bursts of care). */
  burstShape: 2,
  /** Share of member-months with no utilization at all. */
  zeroInflation: 0.25,
  /** Peak-to-mean seasonal swing, cosine peaking in January. */
  seasonalAmplitude: 0.15,

  /** Expected fills per enrolled month for a member of average intensity. */
  fillIntensity: 0.15,
  /** Lognormal paid amount per fill: exp(4) is roughly 55 dollars at the median. */
  paidLogMean: 4,
  paidLogSd: 1.5,

  /** Share of members enrolled for the whole window; the rest churn. */
  fullEnrollmentShare: 0.75,

  /** Adoption periods, and the share of members in each cohort. */
  cohortPeriods: [8, 12, 16],
  cohortShare: 0.2,
  /**
   * Multiplicative effect on encounter intensity once fully phased in.
   * 0.6 is a 40 percent reduction: large for a real program, and set large
   * enough on purpose that a panel small enough to ship as a static file can
   * still resolve it cohort by cohort.
   */
  rateRatio: 0.6,
  /** Periods over which the effect phases in, starting at adoption. */
  rampMonths: 3,
};

/**
 * Generate a synthetic member-month claims panel with a known treatment effect.
 *
 * @param {Partial<typeof CLAIMS_PANEL_DEFAULTS>} [options]
 * @returns {{rows: Array<object>, truth: object, options: object}}
 *   rows: one object per member-month, columns per CLAIMS_PANEL_COLUMNS.
 *   truth: the planted effect, measured exactly against the simulator's own
 *     untreated counterfactual over enrolled treated member-months.
 */
export function simulateClaimsPanel(options = {}) {
  const opt = { ...CLAIMS_PANEL_DEFAULTS, ...options };
  const rand = random(opt.seed);

  const monthLabels = monthSequence(opt.startMonth, opt.months);
  const calendarMonths = monthLabels.map((m) => Number(m.slice(5, 7)) - 1);
  const seasonal = calendarMonths.map(
    (m) => 1 + opt.seasonalAmplitude * Math.cos((2 * Math.PI * m) / 12)
  );

  const members = Array.from({ length: opt.members }, (_, i) => ({
    person_id: `sim-${String(i + 1).padStart(6, "0")}`,
    // Drawn once, reused every month: this is what makes high utilizers stay high.
    intensity: gamma(rand, opt.intensityShape) * (opt.baselineIntensity / opt.intensityShape),
    enrollment: drawEnrollment(rand, opt),
  }));

  assignCohorts(rand, members, opt);

  const rows = [];
  const deltas = [];
  for (const m of members) {
    for (let t = 0; t < opt.months; t++) {
      const enrolled = m.enrollment[t];
      const eventTime = m.cohort > 0 ? t - m.cohort : null;
      const treatedNow = eventTime !== null && eventTime >= 0 ? 1 : 0;

      let untreated = 0;
      let claims = 0;
      let fills = 0;
      let paid = 0;

      // An unenrolled month generates nothing: no coverage, no claims.
      if (enrolled) {
        const active = rand() >= opt.zeroInflation;
        if (active) {
          const lambda = m.intensity * seasonal[t] * gamma(rand, opt.burstShape) / opt.burstShape;
          untreated = poisson(rand, lambda);
          const avertProbability = treatedNow
            ? (1 - opt.rateRatio) * Math.min(1, (eventTime + 1) / opt.rampMonths)
            : 0;
          claims = avertProbability > 0 ? untreated - binomial(rand, untreated, avertProbability) : untreated;

          fills = poisson(rand, opt.fillIntensity * (m.intensity / opt.baselineIntensity));
          for (let k = 0; k < fills; k++) {
            paid += Math.exp(opt.paidLogMean + opt.paidLogSd * gaussian(rand));
          }
        }
      }

      if (treatedNow && enrolled) {
        deltas.push({ cohort: m.cohort, eventTime, delta: claims - untreated });
      }

      rows.push({
        person_id: m.person_id,
        month: monthLabels[t],
        period: t,
        cohort: m.cohort,
        treated_now: treatedNow,
        medical_claims: claims,
        pharmacy_fills: fills,
        pharmacy_paid: Math.round(paid * 100) / 100,
        enrolled_flag: enrolled ? 1 : 0,
      });
    }
  }

  return { rows, truth: summarizeTruth(deltas, opt), options: opt };
}

/**
 * The same panel as CSV text, ready for a data loader to write to stdout.
 *
 * @param {Partial<typeof CLAIMS_PANEL_DEFAULTS>} [options]
 * @returns {{csv: string, truth: object, options: object}}
 */
export function claimsPanelCsv(options = {}) {
  const { rows, truth, options: opt } = simulateClaimsPanel(options);
  const lines = [CLAIMS_PANEL_COLUMNS.join(",")];
  for (const r of rows) lines.push(CLAIMS_PANEL_COLUMNS.map((c) => r[c]).join(","));
  return { csv: lines.join("\n") + "\n", truth, options: opt };
}

// ---------------------------------------------------------------------------
// the planted truth
// ---------------------------------------------------------------------------

/**
 * The average treatment effect on the treated, measured rather than assumed:
 * the mean of (observed - untreated counterfactual) over enrolled treated
 * member-months, which is the sample the estimators see.
 */
function summarizeTruth(deltas, opt) {
  const group = (keyOf) => {
    const buckets = new Map();
    for (const d of deltas) {
      const k = keyOf(d);
      if (!buckets.has(k)) buckets.set(k, []);
      buckets.get(k).push(d.delta);
    }
    return [...buckets.entries()].sort((a, b) => a[0] - b[0]);
  };
  const phasedIn = deltas.filter((d) => d.eventTime >= opt.rampMonths - 1).map((d) => d.delta);
  return {
    att: mean(deltas.map((d) => d.delta)),
    attFullyPhasedIn: phasedIn.length ? mean(phasedIn) : null,
    attByEventTime: group((d) => d.eventTime).map(([eventTime, ds]) => ({
      eventTime,
      att: mean(ds),
      n: ds.length,
    })),
    attByCohort: group((d) => d.cohort).map(([cohort, ds]) => ({
      cohort,
      att: mean(ds),
      n: ds.length,
    })),
    rateRatio: opt.rateRatio,
    rampMonths: opt.rampMonths,
    treatedMemberMonths: deltas.length,
  };
}

// ---------------------------------------------------------------------------
// panel structure
// ---------------------------------------------------------------------------

/** YYYY-MM labels, `count` of them, starting at `start`. */
function monthSequence(start, count) {
  const [y0, m0] = start.split("-").map(Number);
  return Array.from({ length: count }, (_, i) => {
    const total = (y0 * 12 + (m0 - 1)) + i;
    return `${Math.floor(total / 12)}-${String((total % 12) + 1).padStart(2, "0")}`;
  });
}

/**
 * Coverage pattern per member: mostly full-window, otherwise a late start, an
 * early end, or a gap in the middle. Returns a boolean per period.
 */
function drawEnrollment(rand, opt) {
  const covered = new Array(opt.months).fill(true);
  if (rand() < opt.fullEnrollmentShare) return covered;
  const flavor = rand();
  const quarter = Math.max(1, Math.floor(opt.months / 4));
  if (flavor < 1 / 3) {
    const start = 1 + Math.floor(rand() * quarter);
    for (let t = 0; t < start; t++) covered[t] = false;
  } else if (flavor < 2 / 3) {
    const end = opt.months - 1 - Math.floor(rand() * quarter);
    for (let t = end; t < opt.months; t++) covered[t] = false;
  } else {
    const gapStart = quarter + Math.floor(rand() * quarter * 2);
    for (let t = gapStart; t < Math.min(opt.months, gapStart + 2); t++) covered[t] = false;
  }
  return covered;
}

/**
 * Cohort assignment, independent of member intensity: shuffle, then hand out
 * adoption periods in blocks. 0 means never treated, which is what the
 * estimators read as a clean comparison group.
 */
function assignCohorts(rand, members, opt) {
  const order = members.map((_, i) => i);
  for (let i = order.length - 1; i > 0; i--) {
    const j = Math.floor(rand() * (i + 1));
    [order[i], order[j]] = [order[j], order[i]];
  }
  const perCohort = Math.round(opt.members * opt.cohortShare);
  for (const m of members) m.cohort = 0;
  opt.cohortPeriods.forEach((period, k) => {
    for (let i = k * perCohort; i < (k + 1) * perCohort && i < order.length; i++) {
      members[order[i]].cohort = period;
    }
  });
}

// ---------------------------------------------------------------------------
// draws
// ---------------------------------------------------------------------------

/** Box-Muller, one draw per call. */
function gaussian(rand) {
  return Math.sqrt(-2 * Math.log(1 - rand())) * Math.cos(2 * Math.PI * rand());
}

/** Gamma(shape, 1), Marsaglia-Tsang with the shape < 1 boost. */
function gamma(rand, shape) {
  if (shape < 1) return gamma(rand, shape + 1) * Math.pow(1 - rand(), 1 / shape);
  const d = shape - 1 / 3;
  const c = 1 / Math.sqrt(9 * d);
  for (;;) {
    const z = gaussian(rand);
    const v = 1 + c * z;
    if (v <= 0) continue;
    const v3 = v * v * v;
    const u = 1 - rand();
    if (Math.log(u) < 0.5 * z * z + d - d * v3 + d * Math.log(v3)) return d * v3;
  }
}

/** Poisson by exponential inter-arrival times, summed in log space so large means are safe. */
function poisson(rand, lambda) {
  if (!(lambda > 0)) return 0;
  let k = 0;
  let sum = -Math.log(1 - rand());
  while (sum <= lambda) {
    k++;
    sum += -Math.log(1 - rand());
  }
  return k;
}

/** Binomial by direct trials. Counts here are small enough that this is cheap. */
function binomial(rand, n, p) {
  let k = 0;
  for (let i = 0; i < n; i++) if (rand() < p) k++;
  return k;
}

function mean(xs) {
  return xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : null;
}
