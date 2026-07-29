/**
 * pictogram.js — fill an arbitrary silhouette to a percentage.
 *
 * The generalization of the beloved soldier chart: any SVG path becomes a
 * proportion gauge by drawing the shape, masking the top (100 − fill)% with
 * white, and re-stroking the outline. "1.5 of every 3.9 soldiers" lands
 * differently than a bar at 38%, and communication is the job.
 *
 * @param {object} options
 * @param {string} options.pathData the silhouette's SVG path (d attribute)
 * @param {number} [options.fillLevel=50] percent filled, 0–100, from the bottom
 * @param {string} [options.fillColor] fill color (defaults to theme navy)
 * @param {string} [options.outlineColor="#333"]
 * @param {string} [options.width="169px"] / @param {string} [options.height="320px"]
 * @param {[number, number]} [options.viewBox=[677, 1280]] the path's native dimensions
 * @param {number} [options.outlineWidth=50] stroke width in path units
 * @returns {{node: HTMLDivElement, setFillLevel: (percent: number) => void}}
 */

import * as d3 from "d3";
import { colors } from "./theme.js";

export function pictogramFill({
  pathData,
  fillLevel = 50,
  fillColor = colors.navy,
  outlineColor = "#333",
  width = "169px",
  height = "320px",
  viewBox = [677, 1280],
  outlineWidth = 50,
  paddingLeft = 20,
} = {}) {
  if (!pathData) {
    throw new Error(`pictogramFill needs pathData. A silhouette of nothing is 100% full of it, granted.`);
  }
  const [vw, vh] = viewBox;

  const div = d3.create("div").style("overflow-x", "auto");
  const svg = div
    .append("svg")
    .attr("viewBox", `0 0 ${vw + paddingLeft} ${vh}`)
    .style("width", width)
    .style("height", height);

  const g = svg.append("g");
  const transform = `translate(${paddingLeft},${vh}) scale(0.1,-0.1)`;

  // Layer 1: the filled silhouette.
  g.append("path").attr("d", pathData).attr("transform", transform).attr("fill", fillColor).attr("stroke", "none");

  // Layer 2: the white mask descending from the top; its height is the empty share.
  const mask = g
    .append("rect")
    .attr("x", paddingLeft)
    .attr("y", 0)
    .attr("width", vw)
    .attr("height", 0)
    .attr("fill", "white");

  // Layer 3: the outline, so the empty portion still reads as the shape.
  g.append("path")
    .attr("d", pathData)
    .attr("transform", transform)
    .attr("fill", "none")
    .attr("stroke", outlineColor)
    .attr("stroke-width", outlineWidth);

  function setFillLevel(percent) {
    const clamped = Math.max(0, Math.min(100, percent));
    mask.attr("height", (vh * (100 - clamped)) / 100);
  }
  setFillLevel(fillLevel);

  return { node: div.node(), setFillLevel };
}
