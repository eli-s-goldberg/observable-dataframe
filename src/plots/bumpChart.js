/**
 * bumpChart.js — rank trajectories over time.
 *
 * The chart for "what mattered most, when, and what overtook it": each
 * series gets a rank per period (1 = biggest value) and you watch the
 * lines cross. Values are for tables; rank crossings are for narratives.
 */

import * as Plot from "@observablehq/plot";
import { asRows } from "./util.js";
import { colors, plotDefaults, typography } from "./theme.js";
import { resolveTip } from "./options.js";

/**
 * @param {DataFrame|Array<object>} data long-format rows: one row per (series, period)
 * @param {object} options
 * @param {string} [options.x="period"] time/period column
 * @param {string} [options.y="value"] value column (ranked within each period, descending)
 * @param {string} [options.z="series"] series column
 * @param {number} [options.topN] show only series that ever reach the top N ranks
 * @param {boolean} [options.labelEnds=true] print series names at both ends of each line
 * @param {boolean|Function|object} [options.tip=true] tooltip on the dots; defaults to series/period/value/rank
 */
export function bumpChart(
  data,
  { x = "period", y = "value", z = "series", topN, labelEnds = true, tip = true, width = 700, ...options } = {}
) {
  const rows = asRows(data);

  // Rank within each period: biggest value gets rank 1. Ties broken by
  // series name so reruns don't reshuffle the chart under your narration.
  const periods = [...new Set(rows.map((r) => r[x]))];
  const ranked = periods.flatMap((p) => {
    return rows
      .filter((r) => r[x] === p)
      .sort((a, b) => b[y] - a[y] || String(a[z]).localeCompare(String(b[z])))
      .map((r, i) => ({ ...r, rank: i + 1 }));
  });

  let kept = ranked;
  let maxRank = Math.max(...ranked.map((r) => r.rank));
  if (topN != null) {
    const keepSeries = new Set(ranked.filter((r) => r.rank <= topN).map((r) => r[z]));
    kept = ranked.filter((r) => keepSeries.has(r[z]));
    maxRank = Math.min(maxRank, Math.max(topN, Math.max(...kept.map((r) => r.rank))));
  }

  const first = periods[0];
  const last = periods[periods.length - 1];
  const height = Math.max(200, maxRank * 44 + 60);

  // End labels carry the rank ("#2 Acute Kidney Failure"), which makes a
  // separate rank axis redundant — and removing it is what stops the axis
  // ticks and the start labels from typesetting on top of each other.
  const endLabel = (d) => `#${d.rank} ${d[z]}`;
  const charWidth = typography.tick * 0.6;
  const maxLabelPx = labelEnds
    ? Math.max(...kept.map((r) => endLabel(r).length)) * charWidth
    : 0;
  // Margins sized to the longest label plus the dot and its offset, so
  // "Acute Kidney Failure" fits at any rank without a manual width hunt.
  const marginLeft = options.marginLeft ?? Math.max(40, Math.ceil(maxLabelPx + 22));
  const marginRight = options.marginRight ?? Math.max(40, Math.ceil(maxLabelPx + 22));

  return Plot.plot({
    ...plotDefaults,
    width,
    height,
    marginLeft,
    marginRight,
    x: { domain: periods, label: null },
    y: { domain: [maxRank + 0.5, 0.5], axis: null },
    color: { legend: false },
    marks: [
      Plot.ruleY(Array.from({ length: maxRank }, (_, i) => i + 1), {
        stroke: colors.border,
        strokeDasharray: "3,3",
      }),
      Plot.line(kept, { x, y: "rank", z, stroke: z, curve: "monotone-x", strokeWidth: 2 }),
      Plot.dot(kept, {
        x,
        y: "rank",
        fill: z,
        r: 4,
        ...resolveTip(tip, (d) => `${d[z]}\n${x}: ${d[x]}\n${y}: ${d[y]}\nrank: #${d.rank}`),
      }),
      ...(labelEnds
        ? [
            Plot.text(
              kept.filter((r) => r[x] === first),
              { x, y: "rank", text: endLabel, dx: -10, textAnchor: "end", fontSize: typography.tick, fill: colors.ink }
            ),
            Plot.text(
              kept.filter((r) => r[x] === last),
              { x, y: "rank", text: endLabel, dx: 10, textAnchor: "start", fontSize: typography.tick, fill: colors.ink }
            ),
          ]
        : []),
    ],
    ...options,
  });
}
