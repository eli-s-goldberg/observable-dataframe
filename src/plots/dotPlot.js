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
import { plotDefaults, tufteAxis } from "./theme.js";
import { resolveTip } from "./options.js";

function subsample(rows, interval) {
  if (!interval || interval <= 1) return rows;
  return rows.filter((_, i) => i % interval === 0);
}

function accessor(field) {
  return typeof field === "function" ? field : (d) => d[field];
}

function resolveDomain(rows, field, explicit, numeric = true) {
  if (explicit) return explicit;
  const vals = rows.map(accessor(field)).filter((v) => v != null && !Number.isNaN(v));
  if (vals.length === 0) return numeric ? [0, 1] : vals;
  if (!numeric) return [...new Set(vals)];
  return [Math.min(...vals), Math.max(...vals)];
}

/**
 * Stacked dot plot. Dots at the same x stack vertically; subsample with
 * `interval` when the series is too dense for the pixel width.
 *
 * @param {import("../core/DataFrame.js").DataFrame|Array<object>} data
 * @param {object} [options]
 * @param {string|Function} [options.x="x"] x field or accessor
 * @param {string|Function} [options.y="y"] y field or accessor (stacked)
 * @param {string|Function} [options.fill] fill field or accessor
 * @param {string|Function} [options.title] tooltip field or accessor
 * @param {number} [options.r=2.5] dot radius
 * @param {number} [options.interval=1] keep every nth row (1 = all)
 * @param {string} [options.scheme="Observable10"] color scheme when fill is set
 * @param {string[]} [options.countValues] category values to annotate on the right
 * @param {string} [options.countField] field matched against countValues
 * @param {number} [options.width=640]
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

  const xDom = resolveDomain(rows, xAcc, xDomain, true);
  const yDom = resolveDomain(rows, yAcc, yDomain, true);

  const dotOpts = {
    x: xAcc,
    y: yAcc,
    r,
    ...(fillAcc ? { fill: fillAcc } : { fill: "currentColor" }),
    ...(titleAcc ? resolveTip(tip, (d) => String(titleAcc(d))) : {}),
  };

  const marks = [
    layout === "mirror"
      ? Plot.dot(rows, dotOpts)
      : Plot.dot(rows, Plot.stackY2(dotOpts)),
    Plot.ruleY([0]),
  ];

  if (countValues?.length && countField) {
    const counts = Object.fromEntries(
      countValues.map((value) => [
        value,
        rows.filter((d) => d[countField] === value).length * interval,
      ])
    );
    const span = yDom[1] - yDom[0] || 1;
    marks.push(
      ...countValues.map((value, index) =>
        Plot.text(
          [
            {
              x: xDom[1],
              y: yDom[0] + index * 0.12 * span,
              label: `${value}: ${counts[value] ?? 0}`,
            },
          ],
          {
            x: "x",
            y: "y",
            text: (d) => d.label,
            fill: "currentColor",
            dx: "0.75em",
            textAnchor: "start",
            fontSize: 11,
          }
        )
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
    x: { label: xLabel ?? (typeof x === "string" ? x : null), domain: xDom, ...tufteAxis },
    y: {
      label: yLabel ?? (typeof y === "string" ? y : null),
      domain: yDom,
      tickFormat: interval > 1 ? (d) => String(interval * Math.abs(+d.toFixed(1))) : undefined,
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
