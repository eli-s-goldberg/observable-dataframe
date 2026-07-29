/**
 * waterfallPlot.js — value bridge / waterfall chart.
 *
 * Floating bars from start→end with dashed connectors between steps, for
 * decomposing a total into the contributions that build it: each bar is one
 * contribution, positive or negative, and the connectors carry the running
 * balance forward so the final bar reconciles against the first.
 */

import * as Plot from "@observablehq/plot";
import { asRows } from "./util.js";
import { plotDefaults, tufteAxis } from "./theme.js";

/**
 * @param {import("../core/DataFrame.js").DataFrame|Array<object>} data
 * rows: { step, step_num?, start, end, color? }
 */
export function waterfallPlot(
  data,
  {
    x = "step_num",
    start = "start",
    end = "end",
    fill = "color",
    label = "step",
    width = 640,
    height = 280,
    marginLeft = 72,
    marginBottom = 40,
    yLabel = "Benefit",
    yFormat = null,
    showConnectors = true,
    connectorStroke = "var(--theme-foreground-muted, #c44)",
    showValueLabels = false,
    ...options
  } = {}
) {
  const rows = asRows(data);
  const marks = [Plot.ruleY([0]), Plot.barY(rows, { x, y1: start, y2: end, fill })];

  if (showConnectors) {
    for (let i = 1; i < rows.length; i++) {
      const prev = rows[i - 1];
      const cur = rows[i];
      marks.push(
        Plot.link([{ length: 1 }], {
          x1: prev[x],
          y1: prev[end],
          x2: cur[x],
          y2: cur[start],
          curve: "linear",
          stroke: connectorStroke,
          strokeDasharray: "5,5",
        })
      );
    }
  }

  marks.push(
    Plot.text(rows.slice(0, 1), {
      x,
      y: end,
      text: label,
      dy: 10,
      textAnchor: "middle",
      fontSize: 11,
    }),
    Plot.text(rows.slice(1), {
      x,
      y: end,
      text: label,
      dy: -10,
      textAnchor: "middle",
      fontSize: 11,
    })
  );

  if (showValueLabels) {
    marks.push(
      Plot.text(rows, {
        x,
        y: end,
        text: (d) => d[end]?.toLocaleString?.() ?? d[end],
        dy: 18,
        textAnchor: "middle",
        fontSize: 10,
      })
    );
  }

  return Plot.plot({
    ...plotDefaults,
    width,
    height,
    marginLeft,
    marginBottom,
    x: { padding: 0.4, grid: true, ...tufteAxis },
    y: {
      label: yLabel,
      tickFormat: yFormat ?? ((d) => (Math.abs(d) >= 1e6 ? `$${(d / 1e6).toFixed(1)}M` : d.toLocaleString())),
      ...tufteAxis,
    },
    marks,
    ...options,
  });
}
