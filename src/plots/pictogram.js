/**
 * pictogram.js — fill an arbitrary silhouette to a percentage.
 *
 * The generalization of the beloved soldier chart: any SVG path becomes a
 * proportion gauge by drawing the shape, masking the top (100 − fill)% with
 * white, and re-stroking the outline. "1.5 of every 3.9 soldiers" lands
 * differently than a bar at 38%, and communication is the job.
 *
 * The path is measured and fitted into the box rather than assumed to
 * arrive pre-scaled, so the mask spans exactly the silhouette and
 * `fillLevel` is a true percentage of the shape's height.
 *
 * @param {object} options
 * @param {string} options.pathData the silhouette's SVG path (d attribute)
 * @param {number} [options.fillLevel=50] percent filled, 0–100, from the bottom
 * @param {string} [options.fillColor] fill color (defaults to theme navy)
 * @param {string} [options.outlineColor="#333"]
 * @param {string} [options.width="169px"] / @param {string} [options.height="320px"]
 * @param {[number, number]} [options.viewBox=[677, 1280]] the drawing box the shape is fitted into
 * @param {number} [options.outlineWidth=5] outline weight in viewBox units, held
 *   steady whatever scale the path had to be fitted at
 * @param {boolean} [options.flipY=false] set for path data authored y-up (traced
 *   glyph outlines, potrace output) rather than in SVG's y-down convention
 * @returns {{node: HTMLDivElement, setFillLevel: (percent: number) => void}}
 */

import * as d3 from "d3";
import { colors } from "./theme.js";

const PARAMS = { m: 2, l: 2, t: 2, h: 1, v: 1, c: 6, s: 4, q: 4, a: 7, z: 0 };

/**
 * The bounding box of a path's coordinates. Bezier control points count,
 * which can overstate a curve's extent slightly; erring toward a smaller
 * fitted shape is the right way to be wrong here.
 */
function pathExtent(d) {
  const tokens = String(d).match(/[a-zA-Z]|-?\d*\.?\d+(?:e[-+]?\d+)?/gi) ?? [];
  const box = { minX: Infinity, minY: Infinity, maxX: -Infinity, maxY: -Infinity };
  const see = (x, y) => {
    if (x < box.minX) box.minX = x;
    if (x > box.maxX) box.maxX = x;
    if (y < box.minY) box.minY = y;
    if (y > box.maxY) box.maxY = y;
  };

  let cmd = "m";
  let cx = 0;
  let cy = 0;
  let startX = 0;
  let startY = 0;
  let i = 0;
  while (i < tokens.length) {
    if (/[a-z]/i.test(tokens[i])) cmd = tokens[i++];
    const key = cmd.toLowerCase();
    const n = PARAMS[key];
    if (n == null) break;
    if (n === 0) {
      cx = startX;
      cy = startY;
      continue;
    }
    const rel = cmd === key;
    const args = tokens.slice(i, i + n).map(Number);
    if (args.length < n || args.some(Number.isNaN)) break;
    i += n;

    if (key === "h") cx = rel ? cx + args[0] : args[0];
    else if (key === "v") cy = rel ? cy + args[0] : args[0];
    else {
      // Every intermediate pair is a coordinate except the arc's radii and
      // flags, whose endpoint is the last pair either way.
      const pairs = key === "a" ? [[args[5], args[6]]] : [];
      if (key !== "a") for (let p = 0; p + 1 < n; p += 2) pairs.push([args[p], args[p + 1]]);
      for (const [px, py] of pairs) see(rel ? cx + px : px, rel ? cy + py : py);
      const [ex, ey] = pairs[pairs.length - 1];
      cx = rel ? cx + ex : ex;
      cy = rel ? cy + ey : ey;
    }
    see(cx, cy);
    if (key === "m") {
      startX = cx;
      startY = cy;
      cmd = rel ? "l" : "L";
    }
  }

  if (!Number.isFinite(box.minX) || box.maxX <= box.minX || box.maxY <= box.minY) return null;
  return box;
}

export function pictogramFill({
  pathData,
  fillLevel = 50,
  fillColor = colors.navy,
  outlineColor = "#333",
  width = "169px",
  height = "320px",
  viewBox = [677, 1280],
  outlineWidth = 5,
  paddingLeft = 20,
  flipY = false,
} = {}) {
  if (!pathData) {
    throw new Error(`pictogramFill needs pathData. A silhouette of nothing is 100% full of it, granted.`);
  }
  const [vw, vh] = viewBox;
  const totalWidth = vw + paddingLeft;

  // Fit the measured shape into the box, aspect preserved and centered, so
  // the drawing lands where the mask expects it whatever units it arrived in.
  const box = pathExtent(pathData) ?? { minX: 0, minY: 0, maxX: vw, maxY: vh };
  const k = Math.min(vw / (box.maxX - box.minX), vh / (box.maxY - box.minY));
  const shapeW = (box.maxX - box.minX) * k;
  const shapeX = paddingLeft + (vw - shapeW) / 2;
  const shapeH = (box.maxY - box.minY) * k;
  const shapeY = (vh - shapeH) / 2;

  const transform = flipY
    ? `translate(${shapeX - box.minX * k},${shapeY + shapeH + box.minY * k}) scale(${k},${-k})`
    : `translate(${shapeX - box.minX * k},${shapeY - box.minY * k}) scale(${k})`;

  const div = d3.create("div").style("overflow-x", "auto");
  const svg = div
    .append("svg")
    .attr("viewBox", `0 0 ${totalWidth} ${vh}`)
    .style("width", width)
    .style("height", height);

  const g = svg.append("g");

  // Layer 1: the filled silhouette.
  g.append("path").attr("d", pathData).attr("transform", transform).attr("fill", fillColor).attr("stroke", "none");

  // Layer 2: the white mask descending from the top of the shape; its
  // height is the empty share of the silhouette, not of the canvas.
  const mask = g
    .append("rect")
    .attr("x", shapeX)
    .attr("y", shapeY)
    .attr("width", shapeW)
    .attr("height", 0)
    .attr("fill", "white");

  // Layer 3: the outline, so the empty portion still reads as the shape.
  g.append("path")
    .attr("d", pathData)
    .attr("transform", transform)
    .attr("fill", "none")
    .attr("stroke", outlineColor)
    .attr("stroke-width", outlineWidth / k);

  function setFillLevel(percent) {
    const clamped = Math.max(0, Math.min(100, Number(percent) || 0));
    mask.attr("height", (shapeH * (100 - clamped)) / 100);
  }
  setFillLevel(fillLevel);

  return { node: div.node(), setFillLevel };
}
