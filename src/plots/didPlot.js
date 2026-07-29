/**
 * didPlot.js — the four-means picture every DiD explanation draws on a
 * whiteboard, rendered properly for once.
 *
 * Control group line, treated group line, and the dashed counterfactual —
 * the treated group's pre level walking forward along the control group's
 * slope. The vertical gap between "what happened" and "what would have"
 * IS the ATT; this figure is the argument, made visible. Pair with did()
 * so the number on the bracket and the number in the table cannot drift.
 */

import * as Plot from "@observablehq/plot";
import { asRows } from "./util.js";
import { colors, plotDefaults, typography, tufteAxis } from "./theme.js";
import { resolveTip } from "./options.js";

/**
 * @param {DataFrame|Array<object>} data the same rows you handed did()
 * @param {object} options
 * @param {string} options.outcome / treatment / time — same lexicon as did()
 * @param {string} [options.yLabel] defaults to the outcome column name
 * @param {[string, string]} [options.periodLabels=["pre", "post"]]
 * @param {number} [options.labelDigits=2]
 * @param {boolean|Function|object} [options.tip=true] tooltips on the group means
 * @returns Plot figure with the ATT bracketed at the post period
 */
export function didPlot(
  data,
  {
    outcome,
    treatment,
    time,
    yLabel,
    periodLabels = ["pre", "post"],
    labelDigits = 2,
    tip = true,
    width = 560,
    height = 320,
    ...options
  } = {}
) {
  const rows = asRows(data);

  // The four means. All of difference-in-differences lives in these cells;
  // the regression is just these means wearing standard errors.
  const cell = (t, p) => {
    const subset = rows.filter((r) => (r[treatment] ? 1 : 0) === t && (r[time] ? 1 : 0) === p);
    if (!subset.length) throw new Error(`didPlot: no rows with ${treatment}=${t}, ${time}=${p}. A 2×2 needs all four cells.`);
    return subset.reduce((a, r) => a + Number(r[outcome]), 0) / subset.length;
  };
  const m = { t0: cell(1, 0), t1: cell(1, 1), c0: cell(0, 0), c1: cell(0, 1) };
  const counterfactual = m.t0 + (m.c1 - m.c0); // treated pre + control's slope
  const att = m.t1 - counterfactual;

  const series = [
    { group: "treated", period: periodLabels[0], x: 0, y: m.t0 },
    { group: "treated", period: periodLabels[1], x: 1, y: m.t1 },
    { group: "control", period: periodLabels[0], x: 0, y: m.c0 },
    { group: "control", period: periodLabels[1], x: 1, y: m.c1 },
  ];
  const cfSeries = [
    { x: 0, y: m.t0 },
    { x: 1, y: counterfactual },
  ];

  const groupColor = (d) => (d.group === "treated" ? colors.operational : colors.navy);
  const bracketX = 1.06;

  return Plot.plot({
    ...plotDefaults,
    width,
    height,
    marginRight: 110,
    x: {
      domain: [-0.15, 1.3],
      ticks: [0, 1],
      tickFormat: (d) => periodLabels[d],
      label: null,
      tickSize: 0,
    },
    y: { label: yLabel ?? outcome, ...tufteAxis },
    marks: [
      // counterfactual first, so the real lines draw over it
      Plot.line(cfSeries, { x: "x", y: "y", stroke: colors.operational, strokeWidth: 1.25, strokeDasharray: "5,4", strokeOpacity: 0.7 }),
      Plot.dot(cfSeries.slice(1), { x: "x", y: "y", r: 3, fill: "white", stroke: colors.operational, strokeDasharray: "2,2" }),
      Plot.text(cfSeries.slice(1), {
        x: "x",
        y: "y",
        text: ["counterfactual"],
        dx: 8,
        dy: 10,
        textAnchor: "start",
        fontSize: typography.annotation,
        fill: colors.muted,
        fontStyle: "italic",
      }),

      Plot.line(series, { x: "x", y: "y", z: "group", stroke: groupColor, strokeWidth: 2 }),
      Plot.dot(series, {
        x: "x",
        y: "y",
        fill: groupColor,
        r: 4,
        ...resolveTip(tip, (d) => `${d.group}, ${d.period}\nmean ${outcome}: ${d.y.toFixed(labelDigits)}`),
      }),
      Plot.text(
        series.filter((d) => d.x === 0),
        { x: "x", y: "y", text: "group", dx: -10, textAnchor: "end", fontSize: typography.tick, fill: groupColor, fontWeight: 600 }
      ),

      // the ATT bracket: the whole point, in clinical red. Serifs drawn as
      // tiny ruleY segments — Plot.tickY would hijack x into a band scale
      // and take the rest of the figure down with it. Ask us how we know.
      Plot.ruleX([bracketX], { x: () => bracketX, y1: Math.min(m.t1, counterfactual), y2: Math.max(m.t1, counterfactual), stroke: colors.clinical, strokeWidth: 1.5 }),
      Plot.ruleY([m.t1, counterfactual].map((y) => ({ y })), {
        y: "y",
        x1: () => bracketX - 0.025,
        x2: () => bracketX + 0.025,
        stroke: colors.clinical,
        strokeWidth: 1.5,
      }),
      Plot.text([{ y: (m.t1 + counterfactual) / 2 }], {
        x: () => bracketX,
        y: "y",
        text: [`ATT ${att >= 0 ? "+" : ""}${att.toFixed(labelDigits)}`],
        dx: 10,
        textAnchor: "start",
        fontSize: typography.tick,
        fontWeight: 700,
        fill: colors.clinical,
      }),
    ],
    ...options,
  });
}
