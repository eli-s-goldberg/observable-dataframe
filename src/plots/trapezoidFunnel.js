/**
 * trapezoidFunnel.js — the classic centered funnel, in actual trapezoids.
 *
 * The bar-chart funnel (funnelChart) is more honest about magnitudes; this
 * one is what stakeholders picture when they say "funnel", and there are
 * meetings where drawing the picture people expect is the whole job.
 *
 * Drawn the quiet way: one hue darkening down the taper so the ink
 * collects on the few who made it through, hairline whitespace instead of
 * borders between stages, stage names in a fixed left column rather than
 * chasing the funnel's edge, and every number printed where it is read.
 * No gradients, no shadows, no legend.
 */

import * as d3 from "d3";
import { asRows } from "./util.js";
import { colors, fonts, typography } from "./theme.js";
import { createTooltip, tipHTML } from "./options.js";

/** The default ramp: a pale wash at the top, full navy at the last stage. */
const RAMP = d3.interpolateRgb("#CBD5DF", colors.navy);

/**
 * @param {DataFrame|Array<object>} data one row per stage, ordered top to bottom
 * @param {object} options
 * @param {string} [options.group="group"] stage name column
 * @param {string} [options.value="value"] stage size column (absolute; percentages computed off the first stage)
 * @param {string} [options.altLabel] optional column shown in place of the count inside the band
 * @param {boolean} [options.showRates=true] print stage-to-stage conversion beside each band edge
 * @param {string[]} [options.palette] band colors, top to bottom; omitted, a
 *   single-hue ramp is used so color carries progression rather than category
 * @param {boolean|Function} [options.tip=false] hover tooltip per band; a function
 *   receives the row and returns HTML (see options.tipHTML for easy formatting)
 * @param {number} [options.width=720]
 * @param {number} [options.height=440]
 * @returns {SVGSVGElement}
 */
export function trapezoidFunnel(
  data,
  {
    group = "group",
    value = "value",
    altLabel,
    showRates = true,
    tip = false,
    palette = null,
    width = 720,
    height = 440,
    margin = { top: 24, right: 96, bottom: 24, left: 176 },
  } = {}
) {
  const rows = asRows(data);
  if (!rows.length) throw new Error(`trapezoidFunnel needs stages. An empty funnel converts beautifully.`);

  const chartWidth = width - margin.left - margin.right;
  const chartHeight = height - margin.top - margin.bottom;
  const base = rows[0][value] || 1;
  const bandGap = 3;

  const colorFor = palette
    ? d3.scaleOrdinal(rows.map((r) => r[group]), palette)
    : (_, i) => RAMP(rows.length > 1 ? i / (rows.length - 1) : 1);

  const svg = d3
    .create("svg")
    .attr("width", width)
    .attr("height", height)
    .attr("viewBox", [0, 0, width, height])
    .attr("style", `max-width: 100%; height: auto; font-family: ${fonts.sans};`);

  const g = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

  const sectionHeight = chartHeight / rows.length;
  // The gap is taken out of the band, not out of the taper: the silhouette
  // stays a straight line through the whitespace.
  const taper = (sectionHeight - bandGap) / sectionHeight;

  const sections = rows.map((r, i) => {
    const pct = (r[value] / base) * 100;
    const nextPct = rows[i + 1] ? (rows[i + 1][value] / base) * 100 : pct;
    const endPct = pct + (nextPct - pct) * taper;
    const y = i * sectionHeight;
    const bandHeight = sectionHeight - bandGap;
    const topWidth = (pct / 100) * chartWidth;
    const endWidth = (endPct / 100) * chartWidth;
    const topLeft = (chartWidth - topWidth) / 2;
    const endLeft = (chartWidth - endWidth) / 2;
    const fill = colorFor(r[group], i);
    return {
      row: r,
      pct,
      y,
      fill,
      textColor: contrastingText(fill),
      path: `M${topLeft},${y} L${topLeft + topWidth},${y} L${endLeft + endWidth},${y + bandHeight} L${endLeft},${y + bandHeight}Z`,
      labelY: y + bandHeight / 2,
      bandHeight,
      countText: altLabel ? String(r[altLabel]) : Number(r[value]).toLocaleString("en-US"),
      narrowest: Math.min(topWidth, endWidth),
      rightEdge: Math.max(topLeft + topWidth, endLeft + endWidth),
      rate: i < rows.length - 1 ? rows[i + 1][value] / r[value] : null,
      rateY: y + sectionHeight,
    };
  });

  // The last stages are the narrowest bands and carry the numbers people
  // most want to read: when the count no longer fits, it steps outside.
  const legible = sections.filter((d) => d.bandHeight >= typography.base * 1.6);
  const fits = (d) => d.narrowest >= d.countText.length * typography.base * 0.6 + 12;

  const bands = g
    .selectAll("path.band")
    .data(sections)
    .join("path")
    .attr("class", "band")
    .attr("d", (d) => d.path)
    .attr("fill", (d) => d.fill);

  if (tip) {
    const tooltip = createTooltip();
    const contentFor =
      typeof tip === "function"
        ? tip
        : (row, section) =>
            tipHTML({
              [group]: row[group],
              [value]: row[value].toLocaleString(),
              "share of top": `${section.pct.toFixed(1)}%`,
              ...(section.rate != null ? { "converts down": `${(section.rate * 100).toFixed(0)}%` } : {}),
            });
    bands
      .style("cursor", "default")
      .on("mouseenter", (event, d) => tooltip.show(event, contentFor(d.row, d)))
      .on("mousemove", (event) => tooltip.move(event))
      .on("mouseleave", () => tooltip.hide());
  }

  // Stage names in a fixed column, so their right edges line up instead of
  // following the taper down and to the right.
  const stage = g
    .selectAll("g.stage")
    .data(sections)
    .join("g")
    .attr("class", "stage")
    .attr("transform", (d) => `translate(-16,${d.labelY})`);

  stage
    .append("text")
    .attr("text-anchor", "end")
    .attr("dy", (d, i) => (i > 0 ? "-0.15em" : "0.35em"))
    .style("font-size", `${typography.base}px`)
    .style("font-weight", "600")
    .style("fill", colors.ink)
    .text((d) => d.row[group]);

  // The top stage is trivially 100% of itself, so it keeps its name alone.
  stage
    .filter((d, i) => i > 0)
    .append("text")
    .attr("text-anchor", "end")
    .attr("dy", "1.05em")
    .style("font-size", `${typography.annotation}px`)
    .style("fill", colors.muted)
    .text((d) => `${d.pct.toFixed(d.pct >= 10 ? 0 : 1)}% of top`);

  // The count itself, because that is the number people came for.
  g.selectAll("text.count")
    .data(legible)
    .join("text")
    .attr("class", "count")
    .attr("x", (d) => (fits(d) ? chartWidth / 2 : d.rightEdge + 8))
    .attr("y", (d) => d.labelY)
    .attr("dy", "0.35em")
    .attr("text-anchor", (d) => (fits(d) ? "middle" : "start"))
    .style("font-size", `${typography.base}px`)
    .style("font-weight", "700")
    .style("fill", (d) => (fits(d) ? d.textColor : colors.ink))
    .text((d) => d.countText);

  // Conversions sit in their own right-hand column, level with the edge
  // they describe, so nothing overprints the shape.
  if (showRates) {
    g.selectAll("text.rate")
      .data(sections.filter((d) => d.rate != null))
      .join("text")
      .attr("class", "rate")
      .attr("x", chartWidth + 16)
      .attr("y", (d) => d.rateY)
      .attr("dy", "0.35em")
      .attr("text-anchor", "start")
      .style("font-size", `${typography.annotation}px`)
      .style("fill", colors.muted)
      .text((d) => `↓ ${(d.rate * 100).toFixed(0)}%`);
  }

  return svg.node();
}

/** WCAG-contrast text color for any CSS background: white or black, whichever reads. */
function contrastingText(background) {
  const rgb = d3.color(background)?.rgb();
  if (!rgb) return "#000000";
  const lum = (c) => {
    c /= 255;
    return c <= 0.03928 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4;
  };
  const bg = 0.2126 * lum(rgb.r) + 0.7152 * lum(rgb.g) + 0.0722 * lum(rgb.b);
  const contrast = (l1, l2) => (Math.max(l1, l2) + 0.05) / (Math.min(l1, l2) + 0.05);
  return contrast(1, bg) > contrast(0, bg) ? "#FFFFFF" : "#000000";
}
