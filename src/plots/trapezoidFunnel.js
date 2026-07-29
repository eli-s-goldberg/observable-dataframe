/**
 * trapezoidFunnel.js — the classic centered funnel, in actual trapezoids.
 *
 * The bar-chart funnel (funnelChart) is more honest about magnitudes; this
 * one is what stakeholders picture when they say "funnel", and there are
 * meetings where drawing the picture people expect is the whole job.
 * Label colors are chosen by WCAG contrast against each band, so the text
 * survives whatever palette the brand team sends over.
 */

import * as d3 from "d3";
import { asRows } from "./util.js";
import { colors, fonts, typography } from "./theme.js";
import { createTooltip, tipHTML } from "./options.js";

/**
 * @param {DataFrame|Array<object>} data one row per stage, ordered top to bottom
 * @param {object} options
 * @param {string} [options.group="group"] stage name column
 * @param {string} [options.value="value"] stage size column (absolute; percentages computed off the first stage)
 * @param {string} [options.altLabel] optional column shown in place of the percentage (e.g. "12.4K members")
 * @param {boolean} [options.showRates=true] print stage-to-stage conversion under each band edge
 * @param {string[]} [options.palette] band colors, top to bottom
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
    palette = ["#20B2AA", "#48D1CC", "#90EE90", "#BBF7D0", "#FFEC8B", "#FFA07A"],
    width = 720,
    height = 440,
    margin = { top: 24, right: 40, bottom: 24, left: 200 },
  } = {}
) {
  const rows = asRows(data);
  const chartWidth = width - margin.left - margin.right;
  const chartHeight = height - margin.top - margin.bottom;
  const base = rows[0][value];

  const colorScale = d3.scaleOrdinal(rows.map((r) => r[group]), palette);

  const svg = d3
    .create("svg")
    .attr("width", width)
    .attr("height", height)
    .attr("viewBox", [0, 0, width, height])
    .attr("style", `max-width: 100%; height: auto; font-family: ${fonts.sans};`);

  const g = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

  const sectionHeight = chartHeight / rows.length;
  const sections = rows.map((r, i) => {
    const pct = (r[value] / base) * 100;
    const nextPct = rows[i + 1] ? (rows[i + 1][value] / base) * 100 : pct;
    const y = i * sectionHeight;
    const topWidth = (pct / 100) * chartWidth;
    const bottomWidth = (nextPct / 100) * chartWidth;
    const topLeft = (chartWidth - topWidth) / 2;
    const bottomLeft = (chartWidth - bottomWidth) / 2;
    const fill = colorScale(r[group]);
    return {
      row: r,
      pct,
      y,
      fill,
      textColor: contrastingText(fill),
      path: `M${topLeft},${y} L${topLeft + topWidth},${y} L${bottomLeft + bottomWidth},${y + sectionHeight} L${bottomLeft},${y + sectionHeight}Z`,
      labelX: (topLeft + bottomLeft) / 2,
      labelY: y + sectionHeight / 2,
      rate: i < rows.length - 1 ? rows[i + 1][value] / r[value] : null,
      rateX: bottomLeft,
      rateY: y + sectionHeight,
    };
  });

  const bands = g
    .selectAll("path")
    .data(sections)
    .join("path")
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

  // Stage names, right-aligned into the left margin so long labels have room.
  g.selectAll("text.stage")
    .data(sections)
    .join("text")
    .attr("class", "stage")
    .attr("x", (d) => d.labelX - 10)
    .attr("y", (d) => d.labelY)
    .attr("dy", "0.35em")
    .attr("text-anchor", "end")
    .style("font-size", `${typography.base}px`)
    .style("fill", colors.ink)
    .text((d) => d.row[group]);

  // Center value: percentage of the top stage, or the altLabel column.
  g.selectAll("text.center")
    .data(sections)
    .join("text")
    .attr("class", "center")
    .attr("x", chartWidth / 2)
    .attr("y", (d) => d.labelY)
    .attr("dy", "0.35em")
    .attr("text-anchor", "middle")
    .style("font-size", `${typography.base}px`)
    .style("font-weight", "700")
    .style("fill", (d) => d.textColor)
    .text((d) => (altLabel ? d.row[altLabel] : `${d.pct.toFixed(d.pct >= 10 ? 0 : 1)}%`));

  if (showRates) {
    g.selectAll("text.rate")
      .data(sections.filter((d) => d.rate != null))
      .join("text")
      .attr("class", "rate")
      .attr("x", (d) => d.rateX - 10)
      .attr("y", (d) => d.rateY)
      .attr("dy", "-0.35em")
      .attr("text-anchor", "end")
      .style("font-size", `${typography.annotation}px`)
      .style("fill", colors.muted)
      .text((d) => `${(d.rate * 100).toFixed(0)}% convert ↓`);
  }

  return svg.node();
}

/** WCAG-contrast text color for a hex background: white or black, whichever reads. */
function contrastingText(hex) {
  const r = parseInt(hex.slice(1, 3), 16);
  const g = parseInt(hex.slice(3, 5), 16);
  const b = parseInt(hex.slice(5, 7), 16);
  const lum = (c) => {
    c /= 255;
    return c <= 0.03928 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4;
  };
  const bg = 0.2126 * lum(r) + 0.7152 * lum(g) + 0.0722 * lum(b);
  const contrast = (l1, l2) => (Math.max(l1, l2) + 0.05) / (Math.min(l1, l2) + 0.05);
  return contrast(1, bg) > contrast(0, bg) ? "#FFFFFF" : "#000000";
}
