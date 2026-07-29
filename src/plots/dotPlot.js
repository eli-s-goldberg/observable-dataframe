/**
 * dotPlot.js — stacked dot plots for dense event-level data.
 *
 * Each observation is a dot; Plot.stackY2 piles them when they share an
 * x position so overcrowded timelines stay readable. Optional right-margin
 * count labels summarize category totals (the categorized variant from the
 * original dot_plots helper).
 */

import * as Plot from "@observablehq/plot";
import { asRows } from "./util.js";
import { plotDefaults, tufteAxis, typography } from "./theme.js";
import { resolveTip } from "./options.js";

function subsample(rows, interval) {
  if (!interval || interval <= 1) return rows;
  return rows.filter((_, i) => i % interval === 0);
}

function accessor(field) {
  return typeof field === "function" ? field : (d) => d[field];
}

/** Stacking is arithmetic; it needs numbers, not category names. */
function isQuantitative(rows, acc) {
  let seen = false;
  for (const row of rows) {
    const v = acc(row);
    if (v == null) continue;
    if (typeof v !== "number" || Number.isNaN(v)) return false;
    seen = true;
  }
  return seen;
}

/**
 * Stacked dot plot. Dots at the same x stack vertically; subsample with
 * `interval` when the series is too dense for the pixel width.
 *
 * Stacking needs a numeric y (`y: () => 1` is the usual "count the events"
 * case). Give it a categorical y instead and the dots are positioned
 * directly on an ordinal axis, which is what a category asked for anyway.
 *
 * @param {import("../core/DataFrame.js").DataFrame|Array<object>} data
 * @param {object} [options]
 * @param {string|Function} [options.x="x"] x field or accessor
 * @param {string|Function} [options.y="y"] y field or accessor (stacked when numeric)
 * @param {string|Function} [options.fill] fill field or accessor
 * @param {string|Function} [options.title] tooltip field or accessor
 * @param {number} [options.r=2.5] dot radius
 * @param {number} [options.interval=1] keep every nth row (1 = all)
 * @param {string} [options.scheme="Observable10"] color scheme when fill is set
 * @param {string[]} [options.countValues] category values to annotate on the right
 * @param {string} [options.countField] field matched against countValues
 * @param {number} [options.width=640]
 * @param {number[]} [options.xDomain] / @param {number[]} [options.yDomain] explicit
 *   scale domains; omitted, Plot infers them from the data it actually draws
 * @param {"stack"|"mirror"} [options.layout="stack"] stack at x vs fixed y bands (balance plots)
 */
export function dotPlot(
  data,
  {
    x = "x",
    y = "y",
    fill = null,
    title = null,
    r = 2.5,
    interval = 1,
    scheme = "Observable10",
    countValues = null,
    countField = null,
    width = 640,
    height = 360,
    marginBottom = 36,
    marginLeft = 56,
    marginRight = countValues?.length ? 100 : 24,
    marginTop = 24,
    xLabel = null,
    yLabel = null,
    xDomain = null,
    yDomain = null,
    layout = "stack",
    tip = true,
    ...options
  } = {}
) {
  const rows = subsample(asRows(data), interval);
  const xAcc = accessor(x);
  const yAcc = accessor(y);
  const fillAcc = fill != null ? accessor(fill) : null;
  const titleAcc = title != null ? accessor(title) : null;

  // Stacking a category name has no meaning, and Plot's stack transform
  // quietly collapses every dot onto one row when asked to try.
  const numericY = isQuantitative(rows, yAcc);
  const stacked = layout === "stack" && numericY;

  const dotOpts = {
    x: xAcc,
    y: yAcc,
    r,
    ...(fillAcc ? { fill: fillAcc } : { fill: "currentColor" }),
    ...(titleAcc ? resolveTip(tip, (d) => String(titleAcc(d))) : {}),
  };

  const marks = [
    stacked ? Plot.dot(rows, Plot.stackY2(dotOpts)) : Plot.dot(rows, dotOpts),
    // A zero baseline belongs under a count axis; on a categorical y it
    // would just add "0" to the list of categories.
    ...(numericY ? [Plot.ruleY([0])] : []),
  ];

  if (countValues?.length && countField) {
    // Anchored to the frame rather than to data coordinates: the labels
    // live in the right margin whatever kind of scale the axes ended up.
    marks.push(
      ...countValues.map((value, index) =>
        Plot.text([`${value}: ${rows.filter((d) => d[countField] === value).length * interval}`], {
          frameAnchor: "top-right",
          dx: marginRight - 8,
          dy: index * (typography.base + 4),
          text: (d) => d,
          fill: "currentColor",
          textAnchor: "end",
          fontSize: typography.base,
        })
      )
    );
  }

  return Plot.plot({
    ...plotDefaults,
    width,
    height,
    marginBottom,
    marginLeft,
    marginRight,
    marginTop,
    x: {
      label: xLabel ?? (typeof x === "string" ? x : null),
      ...(xDomain ? { domain: xDomain } : {}),
      ...tufteAxis,
    },
    y: {
      label: yLabel ?? (typeof y === "string" ? y : null),
      ...(yDomain ? { domain: yDomain } : {}),
      // Subsampled dots each stand for `interval` observations, so the
      // stacked count axis has to be multiplied back up to real counts.
      tickFormat:
        interval > 1 && stacked
          ? (d) => String(interval * Math.abs(+d.toFixed(1)))
          : undefined,
      ...tufteAxis,
    },
    ...(fillAcc
      ? {
          color: {
            scheme,
            domain: [...new Set(rows.map((d) => fillAcc(d)))],
            legend: true,
          },
        }
      : {}),
    marks,
    ...options,
  });
}
