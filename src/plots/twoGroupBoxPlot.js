/**
 * twoGroupBoxPlot.js — side-by-side box plots comparing two datasets.
 *
 * Both datasets share a categorical x (e.g. month) and a numeric y; each is
 * offset with dx so the pair reads as one grouped comparison.
 */

import * as Plot from "@observablehq/plot";
import { asRows } from "./util.js";
import { colors, plotDefaults, tufteAxis } from "./theme.js";

export function twoGroupBoxPlot(
  data1,
  data2,
  {
    x = "date_category",
    y = "effect",
    label1 = "Group A",
    label2 = "Group B",
    fill1 = colors.navy,
    fill2 = "#81D4FA",
    dx1 = -6,
    dx2 = 6,
    width = 720,
    height = 320,
    marginLeft = 80,
    yLabel = null,
    yDomain = null,
    yTickFormat = (d) => `$${(d / 1000).toFixed(1)}k`,
    xPadding = 0.8,
    ...options
  } = {}
) {
  const rows1 = asRows(data1).map((d) => ({ ...d, _group: label1 }));
  const rows2 = asRows(data2).map((d) => ({ ...d, _group: label2 }));

  return Plot.plot({
    ...plotDefaults,
    width,
    height,
    marginLeft,
    x: { label: null, padding: xPadding, grid: true, ...tufteAxis },
    y: {
      label: yLabel,
      ...(yDomain != null ? { domain: yDomain } : {}),
      grid: true,
      tickFormat: yTickFormat,
      ...tufteAxis,
    },
    color: { domain: [label1, label2], range: [fill1, fill2], legend: true },
    marks: [
      Plot.ruleY([0]),
      Plot.boxY(rows1, { x, y, fill: "_group", dx: dx1 }),
      Plot.boxY(rows2, { x, y, fill: "_group", dx: dx2 }),
    ],
    ...options,
  });
}
