/**
 * basic.js — the everyday plot primitives.
 *
 * Every function here is (data, options) => Plot figure, where data is a
 * DataFrame or plain rows. They return real Observable Plot outputs, so
 * anything you can do to a Plot — override marks, change schemes — you can
 * do here through `options`, which is merged over our defaults.
 */

import * as Plot from "@observablehq/plot";
import { asRows } from "./util.js";
import { colors, plotDefaults, typography, tufteAxis } from "./theme.js";
import { resolveTip } from "./options.js";
import { kde as kdeEstimate } from "../stats/density.js";

/**
 * Correlation heatmap. Feeds df.corrMatrix() (or precomputed long-format
 * rows {a, b, corr}) to Plot.cell, with the coefficient printed in each
 * cell because "vibes-based color reading" is not a methodology.
 *
 * Cells are square by default (aspectRatio: 1 — truly square, margins
 * accounted for). Pass an explicit `height` if you want rectangles; the
 * matrix is symmetric either way, but at least one of us will know.
 *
 * @param {DataFrame|Array<{a: string, b: string, corr: number}>} data a DataFrame (corrMatrix is called for you) or long-format rows
 * @param {object} [options] Plot options; `scheme` picks the diverging color scheme
 */
export function corrPlot(data, { scheme = "RdBu", width = 480, legend = false, tip = false, ...options } = {}) {
  const rows =
    typeof data?.corrMatrix === "function" ? data.corrMatrix().toRows() : asRows(data);
  // Square by default: the height is sized so the plot area (net of
  // margins) is square, which makes the cells square, which is what
  // "square" should have meant all along. An explicit `height` opts out.
  const marginLeft = options.marginLeft ?? 90;
  const marginRight = options.marginRight ?? 20;
  const marginTop = options.marginTop ?? 30;
  const marginBottom = options.marginBottom ?? 30;
  const height = options.height ?? width - marginLeft - marginRight + marginTop + marginBottom;
  return Plot.plot({
    ...plotDefaults,
    width,
    height,
    marginLeft,
    marginRight,
    marginTop,
    marginBottom,
    padding: 0.05,
    x: { label: null, domain: [...new Set(rows.map((r) => r.a))] },
    y: { label: null, domain: [...new Set(rows.map((r) => r.b))] },
    // legend defaults off: the coefficients are printed in the cells, and
    // ramp legends need canvas, which test DOMs pretend not to own.
    color: { scheme, domain: [-1, 1], legend },
    marks: [
      Plot.cell(rows, {
        x: "a",
        y: "b",
        fill: "corr",
        inset: 0.5,
        ...resolveTip(tip, (d) => `${d.a} × ${d.b}\nr = ${d.corr?.toFixed(3)}`),
      }),
      Plot.text(rows, {
        x: "a",
        y: "b",
        text: (d) => (d.corr == null ? "" : d.corr.toFixed(2)),
        fill: (d) => (Math.abs(d.corr ?? 0) > 0.6 ? "white" : "black"),
        fontSize: typography.tick,
      }),
    ],
    ...options,
  });
}

/**
 * Distribution plot, Tufte edition — a thin binned outline with faint
 * fill, no grid, and statistical markers drawn as ticks rising from the
 * baseline. Maximum data, minimum ink, one red line for the statistic
 * that matters.
 *
 * Accepts a DistP, a raw sample array, or a DataFrame column (pass
 * `column`).
 *
 * @param {import("../stats/distributions.js").DistP|number[]|DataFrame} data
 * @param {object} [options] Plot options, plus:
 * @param {string} [options.column] column name, when data is a DataFrame
 * @param {"hist"|"kde"} [options.kind="hist"] binned counts, or a Gaussian
 *   kernel density estimate for the smooth-tails crowd
 * @param {number} [options.bandwidth] KDE bandwidth; Silverman's rule if omitted
 * @param {number} [options.cut=3] KDE only — how many bandwidths the curve
 *   extends past the data extremes (the seaborn convention: 0 clips at the
 *   data, 3 shows the tails easing to zero)
 * @param {number} [options.thresholds=30] histogram bin count
 * @param {string} [options.label] x-axis label; inferred from DistP name / column otherwise
 * @param {Array<"mean"|"median"|number>} [options.markers=["mean"]] statistics
 *   to mark: "mean", "median", or any percentile as a number in (0, 1) —
 *   e.g. ["mean", 0.25, 0.5, 0.75] for mean plus quartiles. The mean gets
 *   the full-height clinical-red rule; everything else gets a baseline tick.
 * @param {"top"|"bottom"|"none"} [options.labelPosition="top"] marker labels:
 *   centered above the plot ("top"), at each tick's baseline ("bottom"), or
 *   "none" for unlabeled ticks and an air of mystery
 * @param {number} [options.labelDigits=2] decimals on marker value labels
 * @param {"stack"|"justify"|"none"} [options.labelCollision="stack"] what to do
 *   when marker labels crowd each other (mean and median are usually the
 *   culprits, sitting three pixels apart like they planned it):
 *   "stack" bumps colliding labels up a line so they rest above one another;
 *   "justify" anchors near-miss neighbors apart (left label right-justified,
 *   right label left-justified) so they lean away instead of kissing;
 *   "none" lets them overlap, if chaos is the aesthetic
 * @param {boolean} [options.fill=true] faint area fill under the outline; false for outline only
 */
export function distPlot(
  data,
  {
    column,
    kind = "hist",
    bandwidth,
    cut = 3,
    thresholds = 30,
    label,
    markers = ["mean"],
    labelPosition = "top",
    labelDigits = 2,
    labelCollision = "stack",
    fill = true,
    width = 600,
    height = 220,
    ...options
  } = {}
) {
  let samples;
  let name = label;
  if (Array.isArray(data)) samples = data;
  else if (Array.isArray(data?.samples)) {
    samples = data.samples;
    name = name ?? data.name;
  } else if (typeof data?.getColumn === "function") {
    if (!column) throw new Error(`distPlot(df) needs a column name. It cannot histogram everything at once.`);
    samples = data.getColumn(column).toArray().filter((v) => v != null);
    name = name ?? column;
  } else {
    throw new Error(`distPlot wants a DistP, a sample array, or a DataFrame + column.`);
  }

  // Resolve markers to {name, value} once; sorting once beats re-sorting
  // per percentile, and your quartiles agree on which data they describe.
  const sorted = samples.slice().sort((a, b) => a - b);
  const quantile = (p) => {
    const idx = (sorted.length - 1) * p;
    const lo = Math.floor(idx);
    const hi = Math.ceil(idx);
    return lo === hi ? sorted[lo] : sorted[lo] + (sorted[hi] - sorted[lo]) * (idx - lo);
  };
  const mean = samples.reduce((a, b) => a + b, 0) / samples.length;

  const resolved = markers.map((m) => {
    if (m === "mean") return { name: "mean", value: mean, isMean: true };
    if (m === "median") return { name: "median", value: quantile(0.5), isMean: false };
    if (typeof m === "number" && m > 0 && m < 1) {
      return { name: `p${Math.round(m * 100)}`, value: quantile(m), isMean: false };
    }
    throw new Error(`Unknown marker "${m}". The menu: "mean", "median", or a percentile in (0, 1).`);
  });
  const meanMarkers = resolved.filter((m) => m.isMean);
  const tickMarkers = resolved.filter((m) => !m.isMean);

  // The curve: binned counts, or a KDE evaluated on a grid. Either way we
  // need the peak height so marker ticks can size themselves as furniture.
  const isKde = kind === "kde";
  let curveMarks;
  let peak;
  if (isKde) {
    const { points } = kdeEstimate(samples, { bandwidth, cut });
    peak = Math.max(...points.map((p) => p.density));
    curveMarks = [
      ...(fill
        ? [Plot.areaY(points, { x: "x", y: "density", fill: colors.navy, fillOpacity: 0.08 })]
        : []),
      Plot.lineY(points, { x: "x", y: "density", stroke: colors.navy, strokeWidth: 1.25 }),
    ];
  } else {
    const [lo, hi] = [sorted[0], sorted[sorted.length - 1]];
    const binCounts = new Array(thresholds).fill(0);
    const span = hi - lo || 1;
    for (const v of samples) {
      binCounts[Math.min(thresholds - 1, Math.floor(((v - lo) / span) * thresholds))]++;
    }
    peak = Math.max(...binCounts);
    curveMarks = [
      ...(fill
        ? [
            Plot.areaY(
              samples,
              Plot.binX({ y: "count" }, { x: (d) => d, thresholds, curve: "natural", fill: colors.navy, fillOpacity: 0.08 })
            ),
          ]
        : []),
      Plot.lineY(
        samples,
        Plot.binX({ y: "count" }, { x: (d) => d, thresholds, curve: "natural", stroke: colors.navy, strokeWidth: 1.25 })
      ),
    ];
  }
  const tickHeight = peak * 0.07;

  const labelText = (m) => `${m.name} ${m.value.toFixed(labelDigits)}`;

  // --- label collision handling -------------------------------------------
  // Estimate each label's pixel span from the x-domain and the label length,
  // then resolve crowding per the labelCollision policy. Estimates, not
  // measurements — but labels are either clearly apart or clearly on top of
  // each other, and mean-vs-median is famously the second kind.
  const domainLo = sorted[0];
  const domainHi = sorted[sorted.length - 1];
  const domainSpan = domainHi - domainLo || 1;
  const plotWidth = width - 70; // approximate margins; precision optional at this scale
  const charWidth = typography.annotation * 0.58;

  const placed = resolved
    .map((m) => {
      const text = labelText(m);
      const cx = ((m.value - domainLo) / domainSpan) * plotWidth;
      return { ...m, text, cx, halfWidth: (text.length * charWidth) / 2, tier: 0, anchor: "middle" };
    })
    .sort((a, b) => a.cx - b.cx);

  if (labelCollision !== "none" && placed.length > 1) {
    const pad = 6; // breathing room; labels this close still read as crowded
    const overlaps = (a, b) => {
      const aLeft = a.anchor === "middle" ? a.cx - a.halfWidth : a.anchor === "end" ? a.cx - 2 * a.halfWidth : a.cx;
      const aRight = a.anchor === "middle" ? a.cx + a.halfWidth : a.anchor === "end" ? a.cx : a.cx + 2 * a.halfWidth;
      const bLeft = b.anchor === "middle" ? b.cx - b.halfWidth : b.anchor === "end" ? b.cx - 2 * b.halfWidth : b.cx;
      const bRight = b.anchor === "middle" ? b.cx + b.halfWidth : b.anchor === "end" ? b.cx : b.cx + 2 * b.halfWidth;
      return aLeft < bRight + pad && bLeft < aRight + pad;
    };

    if (labelCollision === "justify") {
      // Near neighbors lean apart: left one right-justifies against its
      // rule, right one left-justifies. Works until three markers pile up,
      // at which point the middle one stacks anyway — geometry is finite.
      for (let i = 1; i < placed.length; i++) {
        if (overlaps(placed[i - 1], placed[i])) {
          placed[i - 1].anchor = "end";
          placed[i].anchor = "start";
        }
      }
      // Anything still overlapping after the lean gets bumped a tier.
      for (let i = 1; i < placed.length; i++) {
        while (placed.slice(0, i).some((p) => p.tier === placed[i].tier && overlaps(p, placed[i]))) {
          placed[i].tier++;
        }
      }
    } else {
      // "stack": greedy tier assignment — each label takes the lowest shelf
      // where it fits, so mean rests directly above median instead of on it.
      for (let i = 1; i < placed.length; i++) {
        while (placed.slice(0, i).some((p) => p.tier === placed[i].tier && overlaps(p, placed[i]))) {
          placed[i].tier++;
        }
      }
    }
  }

  const lineHeight = typography.annotation + 3;
  const maxTier = Math.max(0, ...placed.map((p) => p.tier));

  // Plot.text takes dy and textAnchor as constants, not channels, so each
  // (tier, anchor) group becomes its own mark. There are at most a handful.
  const groups = new Map();
  for (const p of placed) {
    const key = `${p.tier}|${p.anchor}`;
    if (!groups.has(key)) groups.set(key, { tier: p.tier, anchor: p.anchor, items: [] });
    groups.get(key).items.push(p);
  }

  const labelMarks =
    labelPosition === "none"
      ? []
      : [...groups.values()].map(({ tier, anchor, items }) =>
          Plot.text(items, {
            x: "value",
            text: "text",
            ...(labelPosition === "top" ? { frameAnchor: "top" } : { y: tickHeight }),
            dy: -6 - tier * lineHeight,
            lineAnchor: "bottom",
            textAnchor: anchor,
            fontSize: typography.annotation,
            fill: (m) => (m.isMean ? colors.clinical : colors.muted),
            fontWeight: (m) => (m.isMean ? 700 : 500),
          })
        );

  return Plot.plot({
    ...plotDefaults,
    width,
    height,
    // stacked label tiers need headroom; one line per shelf
    marginTop: (labelPosition === "top" ? 24 : 20) + (labelPosition === "top" ? maxTier * lineHeight : 0),
    x: { label: name ?? "value" },
    y: { label: isKde ? "density" : "count", ticks: 3, tickSize: 4 }, // Tufte: three honest ticks, no grid cage
    marks: [
      ...curveMarks,
      Plot.ruleY([0]),
      // percentile/median markers: quiet ticks rising from the baseline
      ...(tickMarkers.length
        ? [
            Plot.ruleX(tickMarkers, {
              x: "value",
              y1: 0,
              y2: tickHeight,
              stroke: colors.muted,
              strokeWidth: 1.25,
            }),
          ]
        : []),
      // the mean keeps its full-height red rule; hierarchy is a feature
      ...(meanMarkers.length
        ? [Plot.ruleX(meanMarkers, { x: "value", stroke: colors.clinical, strokeWidth: 1.5 })]
        : []),
      ...labelMarks,
    ],
    ...options,
  });
}

/**
 * Forest plot: point estimates with confidence whiskers per category,
 * sorted so the eye can walk down the effect sizes. The publication
 * figure for "we ran the analysis eight ways and here they all are".
 *
 * @param {DataFrame|Array<object>} data
 * @param {object} options
 * @param {string} options.category label column
 * @param {string} options.value point estimate column
 * @param {string} options.lower CI lower bound column
 * @param {string} options.upper CI upper bound column
 * @param {string} [options.pValue] optional p-value column; significant rows get the clinical red
 * @param {number} [options.alpha=0.05] significance cutoff for coloring
 * @param {"ascending"|"descending"|null} [options.sort="ascending"]
 * @param {boolean|Function|object} [options.tip=true] tooltip on the point estimates
 * @param {object} [options.pLabel] styling for the p-value annotations:
 *   {format: (p) => string, fontSize, fill, fontWeight, dx}. The right margin
 *   is sized from the longest formatted label so nothing gets guillotined at
 *   the plot edge; explicit marginRight overrides if you know better.
 */
export function forestPlot(
  data,
  {
    category,
    value,
    lower,
    upper,
    pValue,
    alpha = 0.05,
    sort = "ascending",
    tip = true,
    pLabel = {},
    width = 640,
    ...options
  } = {}
) {
  let rows = asRows(data).slice();
  if (sort) rows.sort((a, b) => (sort === "ascending" ? a[value] - b[value] : b[value] - a[value]));
  const height = Math.max(160, rows.length * 28 + 60);
  const sig = (d) => (pValue != null && d[pValue] < alpha ? colors.clinical : colors.navy);

  const {
    format = (p) => `p=${p < 0.001 ? "<0.001" : p.toFixed(3)}`,
    fontSize = typography.annotation,
    fill = colors.muted,
    fontWeight = 500,
    dx = 8,
  } = pLabel;

  // The annotation lives to the right of the widest whisker, so the right
  // margin must fit the longest label — measured, not hoped for. The label
  // may hang off the widest CI, not the row with the longest text, so we
  // size for the worst case and accept a few donated pixels.
  const maxLabelPx =
    pValue != null
      ? Math.max(...rows.map((d) => format(d[pValue]).length)) * fontSize * 0.62 + dx + 6
      : 0;
  const marginRight = options.marginRight ?? Math.max(20, Math.ceil(maxLabelPx));

  return Plot.plot({
    ...plotDefaults,
    width,
    height,
    marginLeft: 160,
    marginRight,
    x: { label: value, ...tufteAxis },
    y: { label: null, domain: rows.map((r) => r[category]), tickSize: 0 },
    marks: [
      Plot.ruleX([0], { stroke: colors.faint, strokeDasharray: "3,3" }),
      Plot.link(rows, { x1: lower, x2: upper, y1: category, y2: category, stroke: sig, strokeWidth: 1.5 }),
      Plot.dot(rows, {
        x: value,
        y: category,
        fill: sig,
        r: 4,
        ...resolveTip(
          tip,
          (d) =>
            `${d[category]}\n${value}: ${d[value]}\nCI: [${d[lower]}, ${d[upper]}]` +
            (pValue != null ? `\np: ${d[pValue]}` : "")
        ),
      }),
      ...(pValue != null
        ? [
            Plot.text(rows, {
              x: upper,
              y: category,
              text: (d) => format(d[pValue]),
              dx,
              textAnchor: "start",
              fill,
              fontSize,
              fontWeight,
            }),
          ]
        : []),
    ],
    ...options,
  });
}

/**
 * Horizontal funnel: stages down the y-axis, magnitude across x, labels
 * inside the bars. Optionally prints stage-to-stage conversion, which is
 * the number the funnel exists to make awkward.
 *
 * Value labels live inside their bar when the bar is wide enough, and hop
 * outside it (with the rate folded in) when it isn't — the last funnel
 * stages are always the smallest bars and the numbers people most want
 * to read, a coincidence the layout should survive.
 *
 * @param {DataFrame|Array<object>} data
 * @param {object} options
 * @param {string} [options.group="group"] stage column
 * @param {string} [options.value="value"] size column
 * @param {boolean} [options.showRates=true] print conversion vs previous stage
 * @param {boolean|Function|object} [options.tip=false] tooltip on the bars
 */
export function funnelChart(data, { group = "group", value = "value", showRates = true, tip = false, width = 640, ...options } = {}) {
  const rows = asRows(data);
  const height = Math.max(140, rows.length * 44 + 40);
  const marginLeft = 140;

  // Decide, per row, whether the value label fits inside the bar with some
  // breathing room. ~6.5px per character at 11px sans is a crude estimate,
  // but bars are either comfortably wide or comically narrow; the crude
  // estimate only has to referee the middle ground.
  const maxValue = Math.max(...rows.map((r) => r[value]));
  const drawableWidth = width - marginLeft - 24;
  const annotated = rows.map((r, i) => {
    const valueText = r[value].toLocaleString();
    const rateText =
      showRates && i > 0 ? `${((r[value] / rows[i - 1][value]) * 100).toFixed(0)}% of prior` : "";
    const barPx = (r[value] / maxValue) * drawableWidth;
    const fitsInside = barPx >= valueText.length * 6.5 + 16;
    return { ...r, valueText, rateText, fitsInside };
  });

  const inside = annotated.filter((r) => r.fitsInside);
  const outside = annotated.filter((r) => !r.fitsInside);

  return Plot.plot({
    ...plotDefaults,
    width,
    height,
    marginLeft,
    x: { label: null, axis: null },
    y: { label: null, domain: rows.map((r) => r[group]) },
    marks: [
      Plot.barX(annotated, {
        x: value,
        y: group,
        fill: colors.navy,
        fillOpacity: 0.9,
        rx: 3,
        ...resolveTip(tip, (d) => `${d[group]}\n${d.valueText}${d.rateText ? `\n${d.rateText}` : ""}`),
      }),
      // fits: value inside the bar, rate just after it
      Plot.text(inside, {
        x: value,
        y: group,
        text: "valueText",
        dx: -6,
        textAnchor: "end",
        fill: "white",
        fontWeight: 700,
      }),
      Plot.text(inside, {
        x: value,
        y: group,
        text: "rateText",
        dx: 8,
        textAnchor: "start",
        fill: colors.muted,
        fontSize: typography.annotation,
      }),
      // doesn't fit: value moves outside in bold; the rate follows in the
      // same muted annotation style it wears everywhere else. Two marks per
      // row because dx is a constant, not a channel — and there are only
      // ever a couple of bars this small.
      Plot.text(outside, {
        x: value,
        y: group,
        text: "valueText",
        dx: 8,
        textAnchor: "start",
        fill: colors.ink,
        fontWeight: 700,
      }),
      ...outside
        .filter((d) => d.rateText)
        .map((d) =>
          Plot.text([d], {
            x: value,
            y: group,
            text: "rateText",
            // offset past the bold value text (~6.8px/char at base size)
            dx: 8 + Math.ceil(d.valueText.length * 6.8) + 8,
            textAnchor: "start",
            fill: colors.muted,
            fontSize: typography.annotation,
          })
        ),
    ],
    ...options,
  });
}

/**
 * Tufte box plot: the box plot with the box removed, which turns out to be
 * most of the ink. Per category: a thin whisker from min to Q1, open space
 * where the interquartile range lives, a dot at the median, and a whisker
 * from Q3 to max. Same five-number summary, a fraction of the pigment.
 * Points beyond 1.5×IQR are drawn as faint outlier dots, because hiding
 * outliers is how they end up in production.
 *
 * @param {DataFrame|Array<object>} data
 * @param {object} options
 * @param {string} options.x category column
 * @param {string} options.y value column
 * @param {"tufte"|"box"} [options.variant="tufte"] "box" restores the classic Plot.boxY, for the traditionalists
 * @param {boolean} [options.showOutliers=true] dots beyond 1.5×IQR (tufte variant)
 * @param {boolean|Function|object} [options.tip=true] tooltip with the five-number summary;
 *   a function receives {category, n, min, q1, median, q3, max}
 */
export function boxPlot(
  data,
  { x, y, variant = "tufte", showOutliers = true, tip = true, width = 640, height = 320, ...options } = {}
) {
  const rows = asRows(data);

  if (variant === "box") {
    return Plot.plot({
      ...plotDefaults,
      width,
      height,
      x: { label: null, tickSize: 0 },
      y: { label: y, ...tufteAxis },
      marks: [Plot.boxY(rows, { x, y, fill: colors.panel, stroke: colors.navy })],
      ...options,
    });
  }

  // Five-number summary per category, computed once.
  const byCategory = new Map();
  for (const r of rows) {
    const key = r[x];
    if (!byCategory.has(key)) byCategory.set(key, []);
    const v = r[y];
    if (v != null && !Number.isNaN(v)) byCategory.get(key).push(v);
  }
  const q = (sorted, p) => {
    const idx = (sorted.length - 1) * p;
    const lo = Math.floor(idx);
    const hi = Math.ceil(idx);
    return lo === hi ? sorted[lo] : sorted[lo] + (sorted[hi] - sorted[lo]) * (idx - lo);
  };
  const summaries = [];
  const outliers = [];
  for (const [category, values] of byCategory) {
    values.sort((a, b) => a - b);
    const q1 = q(values, 0.25);
    const median = q(values, 0.5);
    const q3 = q(values, 0.75);
    const iqr = q3 - q1;
    const loFence = q1 - 1.5 * iqr;
    const hiFence = q3 + 1.5 * iqr;
    const inliers = values.filter((v) => v >= loFence && v <= hiFence);
    summaries.push({
      category,
      n: values.length,
      min: inliers[0] ?? values[0],
      q1,
      median,
      q3,
      max: inliers[inliers.length - 1] ?? values[values.length - 1],
    });
    if (showOutliers) {
      for (const v of values) {
        if (v < loFence || v > hiFence) outliers.push({ category, value: v });
      }
    }
  }

  const defaultTip = (d) =>
    `${d.category}\nn: ${d.n}\nmax: ${fmtStat(d.max)}\nq3: ${fmtStat(d.q3)}\nmedian: ${fmtStat(d.median)}\nq1: ${fmtStat(d.q1)}\nmin: ${fmtStat(d.min)}`;

  return Plot.plot({
    ...plotDefaults,
    width,
    height,
    x: { label: null, tickSize: 0, domain: summaries.map((d) => d.category) },
    y: { label: y, ...tufteAxis },
    marks: [
      // lower whisker: min → Q1
      Plot.ruleX(summaries, { x: "category", y1: "min", y2: "q1", stroke: colors.navy, strokeWidth: 1 }),
      // the IQR is the gap — that's the whole Tufte joke, and it lands
      // upper whisker: Q3 → max
      Plot.ruleX(summaries, { x: "category", y1: "q3", y2: "max", stroke: colors.navy, strokeWidth: 1 }),
      // median dot, carrying the tooltip for the whole category
      Plot.dot(summaries, {
        x: "category",
        y: "median",
        r: 3.5,
        fill: colors.navy,
        ...resolveTip(tip, defaultTip),
      }),
      ...(outliers.length
        ? [Plot.dot(outliers, { x: "category", y: "value", r: 1.5, fill: colors.faint })]
        : []),
    ],
    ...options,
  });
}

function fmtStat(v) {
  return Math.abs(v) >= 1000
    ? v.toLocaleString(undefined, { maximumFractionDigits: 0 })
    : +v.toFixed(2);
}

/**
 * Minimal event timeline: {year (or x), y, name} rows become labeled rules.
 * Sometimes a figure should just say when things happened and sit down.
 */
export function timeline(data, { x = "year", y = "y", label = "name", tip = false, width = 700, height = 180, ...options } = {}) {
  const rows = asRows(data);
  return Plot.plot({
    ...plotDefaults,
    width,
    height,
    x: { label: null, tickFormat: "" },
    y: { axis: null },
    marks: [
      Plot.ruleX(rows, { x, y1: 0, y2: y, stroke: colors.faint }),
      Plot.dot(rows, { x, y, fill: colors.navy, ...resolveTip(tip, (d) => `${d[label]}\n${d[x]}`) }),
      Plot.text(rows, { x, y, text: label, dy: -10, fontSize: typography.tick }),
      Plot.ruleY([0]),
    ],
    ...options,
  });
}

/**
 * Design-matrix staircase for trial layouts: sequences × periods, treated
 * cells filled. Pass the output of stats' designMatrixData(), or any rows
 * shaped {sequence, period, treated}.
 */
export function designMatrixPlot(data, { width = 480, tip = false, ...options } = {}) {
  const rows = asRows(data).map((r) => ({ ...r, arm: r.treated ? "Treatment" : "Control" }));
  const sequences = [...new Set(rows.map((r) => r.sequence))];
  return Plot.plot({
    ...plotDefaults,
    width,
    height: Math.max(120, sequences.length * 44 + 70),
    padding: 0.06,
    x: { label: "Period" },
    y: { label: null, domain: sequences },
    color: {
      domain: ["Control", "Treatment"],
      range: [colors.control, colors.operational],
      legend: true, // categorical → swatches, no canvas required
    },
    marks: [
      Plot.cell(rows, {
        x: "period",
        y: "sequence",
        fill: "arm",
        inset: 1,
        ...resolveTip(tip, (d) => `${d.sequence}, period ${d.period}\n${d.arm}`),
      }),
      Plot.text(rows, {
        x: "period",
        y: "sequence",
        text: (d) => (d.treated ? "T" : "C"),
        fill: (d) => (d.treated ? "white" : colors.controlText),
        fontWeight: 700,
      }),
    ],
    ...options,
  });
}
