/** Draw supplied quantiles and interval endpoints without recomputing statistics. */
import * as Plot from "@observablehq/plot";
import { asRows } from "./util.js";
import { colors, plotDefaults, tufteAxis } from "./theme.js";
import { resolveTip } from "./options.js";

/**
 * Numeric positions allow adjacent groups within a shared continuous x axis.
 * A box spans q1–q3, a line marks median, and whiskers use exactly lower/upper.
 * Optional point estimates are independent of the median and can lie outside
 * the interval. The caller supplies the interval's meaning and coverage label.
 * No sample size, IQR fence, outlier classification or inference is invented.
 *
 * @param {Array<object>|{toRows: Function}} data precomputed summaries
 * @param {object} [options]
 * @param {string} [options.position="position"] finite numeric x-position column
 * @param {string} [options.lower="lower"] lower endpoint column
 * @param {string} [options.q1="q1"] lower quartile column
 * @param {string} [options.median="median"] median column
 * @param {string} [options.q3="q3"] upper quartile column
 * @param {string} [options.upper="upper"] upper endpoint column
 * @param {string|null} [options.estimate=null] optional point-estimate column
 * @param {string|Function} [options.stroke] color or Observable Plot color channel
 * @param {number} [options.boxWidth=0.3] box width in x-axis data units
 * @param {boolean|Function|object} [options.tip=true]
 * @param {number} [options.width=640]
 * @param {number} [options.height=320]
 * @param {object} [options.x] Observable Plot x-scale options
 * @param {object} [options.y] Observable Plot y-scale options
 * @returns {SVGSVGElement|HTMLElement} Observable Plot figure
 */
export function summaryBoxPlot(data, {
  position = "position", lower = "lower", q1 = "q1", median = "median",
  q3 = "q3", upper = "upper", estimate = null, stroke = colors.navy,
  boxWidth = 0.3, tip = true, width = 640, height = 320, ...options
} = {}) {
  const rows = asRows(data);
  if (!Number.isFinite(boxWidth) || boxWidth <= 0) throw new RangeError("boxWidth must be positive and finite");
  rows.forEach((d, i) => {
    const keys = [position, lower, q1, median, q3, upper, ...(estimate == null ? [] : [estimate])];
    for (const key of keys) {
      if (typeof d[key] !== "number" || !Number.isFinite(d[key]))
        throw new TypeError(`summaryBoxPlot row ${i}: ${key} must be a finite number`);
    }
    if (!(d[lower] <= d[q1] && d[q1] <= d[median] && d[median] <= d[q3] && d[q3] <= d[upper]))
      throw new RangeError(`summaryBoxPlot row ${i}: require lower <= q1 <= median <= q3 <= upper`);
  });
  const left = d => d[position] - boxWidth / 2;
  const right = d => d[position] + boxWidth / 2;
  const defaultTip = d => `Position: ${d[position]}\nSupplied interval: [${d[lower]}, ${d[upper]}]\nQ1: ${d[q1]}\nMedian: ${d[median]}\nQ3: ${d[q3]}` +
    (estimate == null ? "" : `\nEstimate: ${d[estimate]}`);
  return Plot.plot({
    ...plotDefaults, width, height,
    x: { label: position, ...tufteAxis }, y: { label: null, ...tufteAxis },
    marks: [
      Plot.ruleX(rows, { x: position, y1: lower, y2: upper, stroke }),
      Plot.rect(rows, { x1: left, x2: right, y1: q1, y2: q3,
        fill: stroke, fillOpacity: 0.18, stroke, ...resolveTip(tip, defaultTip) }),
      ...[lower, median, upper].map(y => Plot.link(rows, {
        x1: left, x2: right, y1: y, y2: y, stroke, strokeWidth: y === median ? 1.7 : 1,
      })),
      ...(estimate == null ? [] : [Plot.dot(rows, {
        x: position, y: estimate, fill: stroke, stroke: "white", r: 3.5,
        ...resolveTip(tip, defaultTip),
      })]),
    ],
    ...options,
  });
}
