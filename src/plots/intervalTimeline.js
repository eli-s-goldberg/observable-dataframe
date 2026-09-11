/** Timelines of actual intervals, including negative time and zero-length milestones. */
import * as Plot from "@observablehq/plot";
import { asRows } from "./util.js";
import { colors, plotDefaults, tufteAxis } from "./theme.js";
import { resolveTip } from "./options.js";

/**
 * Draw numeric or Date intervals at categorical rows. Start/end remain exactly
 * as supplied; endpoint inclusion/exclusion is the caller's study contract.
 * Zero-length intervals render as diamonds. All endpoints must use one type.
 * Labels are opt-in to avoid silently clipping prose into short intervals.
 *
 * @param {Array<object>|{toRows: Function}} data
 * @param {object} [options]
 * @param {string} [options.start="start"] numeric or Date start column
 * @param {string} [options.end="end"] numeric or Date end column
 * @param {string} [options.row="row"] categorical row column
 * @param {string|null} [options.label=null] optional interval label column
 * @param {string|Function} [options.fill] color or Plot color channel
 * @param {string} [options.labelFill="white"] label color
 * @param {boolean|Function|object} [options.tip=true]
 * @param {number} [options.width=700]
 * @param {number} [options.height=220]
 * @param {object} [options.x] Observable Plot x-scale options
 * @param {object} [options.y] Observable Plot y-scale options
 * @returns {SVGSVGElement|HTMLElement} Observable Plot figure
 */
export function intervalTimeline(data, {
  start = "start", end = "end", row = "row", label = null,
  fill = colors.operational, labelFill = "white", tip = true,
  width = 700, height = 220, ...options
} = {}) {
  const rows = asRows(data);
  let kind;
  rows.forEach((d, i) => {
    if (d[row] == null) throw new TypeError(`intervalTimeline row ${i}: ${row} is required`);
    for (const key of [start, end]) {
      const value = d[key];
      const type = value instanceof Date ? "date" : typeof value === "number" ? "number" : null;
      if (!type || !Number.isFinite(+value)) throw new TypeError(`intervalTimeline row ${i}: ${key} must be a finite number or valid Date`);
      kind ??= type;
      if (kind !== type) throw new TypeError("intervalTimeline endpoints must all be numbers or all be Dates");
    }
    if (+d[end] < +d[start]) throw new RangeError(`intervalTimeline row ${i}: end precedes start`);
  });
  const intervals = rows.filter(d => +d[end] > +d[start]);
  const milestones = rows.filter(d => +d[end] === +d[start]);
  const midpoint = d => kind === "date" ? new Date((+d[start] + +d[end]) / 2) : (d[start] + d[end]) / 2;
  const format = value => value instanceof Date ? value.toISOString() : String(value);
  const tipOptions = resolveTip(tip, d => `${d[row]}\n${format(d[start])} → ${format(d[end])}`);
  return Plot.plot({
    ...plotDefaults, width, height, marginLeft: 150,
    x: { type: kind === "date" ? "utc" : "linear", label: null, ...tufteAxis },
    y: { domain: [...new Set(rows.map(d => d[row]))], label: null, tickSize: 0 },
    marks: [
      Plot.barX(intervals, { x1: start, x2: end, y: row, fill, insetTop: 6, insetBottom: 6, ...tipOptions }),
      Plot.dot(milestones, { x: start, y: row, symbol: "diamond", fill, r: 5, ...tipOptions }),
      ...(label == null ? [] : [Plot.text(intervals, {
        x: midpoint, y: row, text: label, fill: labelFill,
      })]),
    ],
    ...options,
  });
}
