/**
 * facetedDensityPlot.js — KDE ridges faceted by dose or category.
 *
 * One smoothed distribution per level of a grouping variable, stacked so
 * the levels are comparable at a glance: each facet is a dose or category,
 * x is the numeric effect, y is proportion within facet rather than count.
 * Facets are normalized independently, so uneven group sizes don't flatten
 * the small groups into nothing.
 */

import * as Plot from "@observablehq/plot";
import { kde } from "../stats/density.js";
import { asRows } from "./util.js";
import { plotDefaults, tufteAxis } from "./theme.js";

export function facetedDensityPlot(
  data,
  {
    x = "effect",
    facet = "treatment_dose",
    width = 640,
    height = 360,
    marginLeft = 72,
    marginBottom = 40,
    xLabel = null,
    yLabel = null,
    yDomain = null,
    fillOpacity = 0.15,
    strokeWidth = 1.5,
    bandwidth = null,
    ...options
  } = {}
) {
  const rows = asRows(data);
  const facets = [...new Set(rows.map((d) => d[facet]))].sort((a, b) => a - b);
  const curves = [];

  for (const f of facets) {
    const samples = rows.filter((d) => d[facet] === f).map((d) => +d[x]).filter(Number.isFinite);
    if (samples.length < 2) continue;
    const { points } = kde(samples, { bandwidth });
    const maxD = Math.max(...points.map((p) => p.density), 1e-9);
    for (const p of points) {
      curves.push({
        [facet]: f,
        [x]: p.x,
        proportion: p.density / maxD,
      });
    }
  }

  return Plot.plot({
    ...plotDefaults,
    width,
    height,
    marginLeft,
    marginBottom,
    facet: { data: curves, y: facet },
    x: { label: xLabel ?? x, grid: true, ...tufteAxis },
    y: { label: yLabel ?? facet, ...(yDomain != null ? { domain: yDomain } : {}), axis: false },
    marks: [
      Plot.ruleX([0], { stroke: "var(--theme-foreground-faint)" }),
      Plot.areaY(curves, {
        x,
        y1: () => 0,
        y2: "proportion",
        fill: "currentColor",
        fillOpacity,
        fy: facet,
      }),
      Plot.line(curves, {
        x,
        y: "proportion",
        strokeWidth,
        fy: facet,
      }),
    ],
    ...options,
  });
}
