/**
 * eventStudyPlot.js — the figure every DiD analysis ends up drawing.
 *
 * Period-specific effects around adoption: dots with CI whiskers, a zero
 * line for the null, a vertical rule at the reference period. The pre
 * side is the credibility exhibit (flat means believable); the post side
 * is the finding. One figure, both burdens.
 *
 * Feed it eventStudy() output, callawaySantAnna().byEventTime, or any
 * rows shaped {eventTime, estimate, ci: [lo, hi]}.
 */

import * as Plot from "@observablehq/plot";
import { colors, plotDefaults, typography, tufteAxis } from "./theme.js";
import { resolveTip } from "./options.js";

/**
 * @param {object|Array} results eventStudy()/CS byEventTime output, or bare rows
 * @param {object} [options]
 * @param {string} [options.yLabel="effect"]
 * @param {boolean|Function|object} [options.tip=true]
 * @param {boolean} [options.shadePost=true] faint band over the post period
 */
export function eventStudyPlot(
  results,
  { yLabel = "effect", tip = true, shadePost = true, width = 640, height = 300, ...options } = {}
) {
  const rows = (Array.isArray(results) ? results : results.effects ?? results.byEventTime ?? [])
    .map((d) => ({
      eventTime: d.eventTime,
      estimate: d.estimate ?? d.att,
      lo: d.ci?.[0] ?? d.estimate ?? d.att,
      hi: d.ci?.[1] ?? d.estimate ?? d.att,
      pValue: d.pValue,
      reference: d.reference ?? false,
      n: d.n ?? d.nCells,
    }))
    .sort((a, b) => a.eventTime - b.eventTime);
  if (!rows.length) throw new Error(`eventStudyPlot got no effects. Estimate first, admire second.`);

  const maxPost = Math.max(...rows.map((r) => r.eventTime));

  return Plot.plot({
    ...plotDefaults,
    width,
    height,
    x: { label: "event time (periods since adoption)", tickFormat: (d) => (d > 0 ? `+${d}` : `${d}`) },
    y: { label: yLabel, ...tufteAxis },
    marks: [
      ...(shadePost && maxPost >= 0
        ? [Plot.rectX([{ x1: -0.5, x2: maxPost + 0.5 }], { x1: "x1", x2: "x2", fill: colors.operational, fillOpacity: 0.05 })]
        : []),
      Plot.ruleY([0], { stroke: colors.faint, strokeDasharray: "3,3" }),
      Plot.ruleX([-0.5], { stroke: colors.border }),
      Plot.ruleX(rows.filter((r) => !r.reference), {
        x: "eventTime",
        y1: "lo",
        y2: "hi",
        stroke: (d) => (d.eventTime < 0 ? colors.muted : colors.navy),
        strokeWidth: 1.25,
      }),
      Plot.dot(rows, {
        x: "eventTime",
        y: "estimate",
        r: 3.5,
        fill: (d) => (d.reference ? colors.faint : d.eventTime < 0 ? colors.muted : colors.navy),
        ...resolveTip(tip, (d) =>
          d.reference
            ? `e=${d.eventTime} (reference)`
            : `e=${d.eventTime}\nestimate: ${d.estimate.toFixed(3)}\nCI: [${d.lo.toFixed(3)}, ${d.hi.toFixed(3)}]` +
              (d.pValue != null ? `\np: ${d.pValue < 0.001 ? "<0.001" : d.pValue.toFixed(3)}` : "")
        ),
      }),
      Plot.text([{ x: -0.5 }], {
        x: "x",
        text: ["adoption"],
        frameAnchor: "top",
        dy: -6,
        dx: 4,
        textAnchor: "start",
        fontSize: typography.annotation,
        fill: colors.muted,
      }),
    ],
    ...options,
  });
}
