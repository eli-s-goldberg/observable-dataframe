/**
 * tufteLine.js — line charts with the quiet Tufte gap-dot treatment.
 *
 * Each data point gets a white halo that visually breaks the line, so the
 * observations read as observations instead of decoration along a curve.
 * The original implementation subclassed Plot's internal Mark machinery;
 * this one composes three public marks and gets the same picture without
 * depending on anyone's src/ directory staying put.
 */

import * as Plot from "@observablehq/plot";
import { asRows } from "./util.js";
import { colors, plotDefaults } from "./theme.js";
import { resolveTip } from "./options.js";

/**
 * The marks only — compose into your own Plot.plot() alongside axes,
 * annotations, and whatever else the figure needs.
 *
 * @param {DataFrame|Array<object>} data
 * @param {object} options
 * @param {string} options.x
 * @param {string} options.y
 * @param {string} [options.stroke] series/color column (also used as z)
 * @param {number} [options.haloRadius=7] radius of the white gap around each point
 * @param {number} [options.dotRadius=2.5] radius of the visible point
 * @param {string} [options.curve="monotone-x"]
 * @param {boolean|Function|object} [options.tip=false] tooltip on the data points
 */
export function tufteLineMarks(
  data,
  { x, y, stroke, haloRadius = 7, dotRadius = 2.5, curve = "monotone-x", tip = false, ...options } = {}
) {
  const rows = asRows(data);
  const strokeChannel = stroke ?? (() => colors.ink);
  return Plot.marks(
    Plot.line(rows, { x, y, z: stroke, stroke: strokeChannel, curve, strokeWidth: 1.25, ...options }),
    // The gap: white dots slightly larger than the data dots, drawn over the
    // line. Cheaper than clipping, and it survives curve changes untouched.
    Plot.dot(rows, { x, y, r: haloRadius, fill: "white", stroke: null }),
    Plot.dot(rows, {
      x,
      y,
      r: dotRadius,
      fill: strokeChannel,
      stroke: null,
      ...resolveTip(tip, (d) => `${stroke ? `${d[stroke]}\n` : ""}${x}: ${d[x]}\n${y}: ${d[y]}`),
    })
  );
}

/**
 * The convenience figure: tufteLineMarks with sensible axes around it.
 *
 * @param {DataFrame|Array<object>} data
 * @param {object} options same as tufteLineMarks, plus standard Plot options
 */
export function tufteLine(data, { x, y, stroke, width = 640, height = 300, ...options } = {}) {
  return Plot.plot({
    ...plotDefaults,
    width,
    height,
    x: { label: x },
    y: { label: y, grid: true },
    ...(stroke ? { color: { legend: true } } : {}),
    marks: [tufteLineMarks(data, { x, y, stroke, ...options })],
  });
}
