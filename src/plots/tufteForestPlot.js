/**
 * tufteForestPlot.js — the publication forest plot, with optional axis break.
 *
 * Dot-and-whisker confidence intervals per category, zebra striping,
 * p-value annotations, and — the party trick — a broken x-axis for when
 * most effects huddle near zero and one outlier sits a postal code away.
 * Without the break you'd compress the interesting cluster into 40 pixels
 * to accommodate one show-off; with it, everyone gets read.
 *
 * This is the D3 sibling of plots/forestPlot (Plot-based, no break). Use
 * that one until the outlier shows up; then come back here.
 */

import * as d3 from "d3";
import { asRows } from "./util.js";
import { fonts, typography } from "./theme.js";
import { createTooltip, tipHTML } from "./options.js";

/**
 * @param {DataFrame|Array<object>} data
 * @param {object} options
 * @param {string} [options.category="category"] label column
 * @param {string} [options.value="value"] point estimate column
 * @param {string} [options.lower="lower"] CI lower bound column
 * @param {string} [options.upper="upper"] CI upper bound column
 * @param {string} [options.pValue] p-value column (annotated when present)
 * @param {string} [options.relative] relative-effect column, shown next to p as "(+12.4%)"
 * @param {string[]} [options.leftLabelCategories=[]] categories whose annotation goes left of the whisker
 * @param {"ascending"|"descending"|null} [options.sort=null]
 * @param {string} [options.title] / @param {string} [options.subtitle="95% Confidence Intervals"]
 * @param {"percent"|"number"|"currency"} [options.tickFormat="percent"]
 * @param {boolean} [options.break=false] enable the axis break
 * @param {[number, number]} [options.xScaleLeftRange] left-panel domain (break mode)
 * @param {[number, number]} [options.xScaleRightRange] right-panel domain (break mode)
 * @param {[number, number]} [options.xScaleRange] domain (continuous mode; auto from data if omitted)
 * @param {number} [options.leftSectionWidth=0.85] fraction of width given to the left panel
 * @param {boolean|Function} [options.tip=false] hover tooltip on the point estimates;
 *   a function receives {category, value, lowerBound, upperBound, pValue, relative} and returns HTML
 * @returns {SVGSVGElement}
 */
export function tufteForestPlot(
  data,
  {
    category = "category",
    value = "value",
    lower = "lower",
    upper = "upper",
    pValue = null,
    relative = null,
    leftLabelCategories = [],
    sort = null,
    title = "",
    subtitle = "95% Confidence Intervals",
    tickFormat = "percent",
    break: useBreak = false,
    xScaleRange = null,
    xScaleLeftRange = [-0.1, 0.025],
    xScaleRightRange = [0.3, 0.4],
    leftSectionWidth = 0.85,
    breakGap = 20,
    tip = false,
    width = 900,
    marginTop = 24,
    marginRight = 110,
    marginBottom = 30,
    marginLeft = 200,
    // The house sans, same as every other figure. Serif was this plot's
    // academic heritage; pass fonts.serif back in if the journal insists.
    fontFamily = fonts.sans,
    r = 3,
  } = {}
) {
  let rows = asRows(data).map((d) => ({
    category: d[category],
    value: d[value],
    lowerBound: d[lower],
    upperBound: d[upper],
    pValue: pValue ? d[pValue] : null,
    relative: relative ? d[relative] : null,
  }));
  if (sort) {
    rows.sort((a, b) => (sort === "ascending" ? a.value - b.value : b.value - a.value));
  }

  const height = rows.length * 25 + marginTop + marginBottom;

  const fmt = {
    percent: d3.format(".0%"),
    currency: d3.format("$,.2f"),
    number: d3.format(",.2f"),
  }[tickFormat] ?? d3.format(".0%");

  const formatP = (p) => {
    if (p == null || Number.isNaN(Number(p))) return "N/A";
    if (p < 0.001) return "p < 0.001";
    if (p < 0.05) return "p < 0.05";
    return "p = " + Number(Number(p).toExponential(1)).toString();
  };

  // Scales: one continuous, or two panels with a gap the data isn't using.
  let getX;
  let xScaleLeft;
  let xScaleRight;
  let xScale;
  if (useBreak) {
    const usable = width - marginLeft - marginRight - breakGap;
    const leftWidth = usable * leftSectionWidth;
    xScaleLeft = d3.scaleLinear().domain(xScaleLeftRange).range([marginLeft, marginLeft + leftWidth]);
    xScaleRight = d3
      .scaleLinear()
      .domain(xScaleRightRange)
      .range([marginLeft + leftWidth + breakGap, marginLeft + leftWidth + breakGap + usable * (1 - leftSectionWidth)]);
    getX = (v) => {
      if (v <= xScaleLeftRange[1]) return xScaleLeft(v);
      if (v >= xScaleRightRange[0]) return xScaleRight(v);
      const t = (v - xScaleLeftRange[1]) / (xScaleRightRange[0] - xScaleLeftRange[1]);
      return marginLeft + leftWidth + breakGap * t; // no-man's-land: interpolate across the gap
    };
  } else {
    const domain =
      xScaleRange ??
      (() => {
        const lo = d3.min(rows, (d) => d.lowerBound);
        const hi = d3.max(rows, (d) => d.upperBound);
        const pad = (hi - lo) * 0.1 || 0.01;
        return [Math.min(lo - pad, 0), hi + pad];
      })();
    xScale = d3.scaleLinear().domain(domain).range([marginLeft, width - marginRight]);
    getX = (v) => xScale(v);
  }

  const yScale = d3
    .scaleBand()
    .domain(rows.map((d) => d.category))
    .range([marginTop, height - marginBottom])
    .padding(0.5);

  const svg = d3
    .create("svg")
    .attr("width", width)
    .attr("height", height)
    .attr("viewBox", [0, 0, width, height])
    .attr("style", `max-width: 100%; height: auto; font: ${typography.base}px ${fontFamily};`);

  // Zebra striping: the least technology that keeps eyes on their row.
  svg
    .append("g")
    .selectAll("rect")
    .data(rows)
    .join("rect")
    .attr("x", marginLeft)
    .attr("y", (d) => yScale(d.category))
    .attr("width", width - marginLeft - marginRight)
    .attr("height", yScale.bandwidth())
    .attr("fill", (d, i) => (i % 2 ? "#f8f8f8" : "white"));

  if (title) {
    svg
      .append("text")
      .attr("x", marginLeft)
      .attr("y", marginTop - 8)
      .attr("font-weight", "bold")
      .attr("font-size", `${typography.title}px`)
      .text(title);
  }
  if (subtitle) {
    svg.append("text").attr("x", 0).attr("y", marginTop).attr("font-size", `${typography.tick}px`).text(subtitle);
  }

  // y axis: category labels, no domain line — Tufte would approve of the absence.
  const yAxis = svg.append("g").attr("transform", `translate(${marginLeft},0)`).call(d3.axisLeft(yScale).tickSize(0));
  yAxis.select(".domain").attr("stroke-width", 0);
  yAxis.selectAll("text").attr("x", -10).style("text-anchor", "end").style("font-size", `${typography.tick}px`).attr("font-weight", "bold");

  const addAxis = (scale, ticks) => {
    const axisGroup = svg
      .append("g")
      .attr("transform", `translate(0,${height - marginBottom})`)
      .call(d3.axisBottom(scale).tickSize(4).tickFormat(fmt).ticks(ticks));
    axisGroup.select(".domain").attr("stroke-width", 0.5);
    axisGroup.selectAll(".tick line").attr("stroke-width", 0.5);
    axisGroup.selectAll("text").style("font-size", `${typography.micro}px`);
  };

  if (useBreak) {
    addAxis(xScaleLeft, 6);
    addAxis(xScaleRight, 4);
    const breakX = marginLeft + (width - marginLeft - marginRight) * leftSectionWidth + breakGap / 2;
    for (const offset of [-1, 1]) {
      svg
        .append("line")
        .attr("x1", breakX + offset * 2)
        .attr("x2", breakX + offset * 2)
        .attr("y1", marginTop)
        .attr("y2", height - marginBottom)
        .attr("stroke", "#e0e0e0")
        .attr("stroke-width", 1);
    }
  } else {
    addAxis(xScale, 5);
  }

  // Zero reference, wherever zero happens to live.
  const zeroScale = useBreak
    ? xScaleLeftRange[0] <= 0 && xScaleLeftRange[1] >= 0
      ? xScaleLeft
      : xScaleRightRange[0] <= 0 && xScaleRightRange[1] >= 0
        ? xScaleRight
        : null
    : xScale.domain()[0] <= 0 && xScale.domain()[1] >= 0
      ? xScale
      : null;
  if (zeroScale) {
    svg
      .append("line")
      .attr("x1", zeroScale(0))
      .attr("x2", zeroScale(0))
      .attr("y1", marginTop)
      .attr("y2", height - marginBottom)
      .attr("stroke", "#777")
      .attr("stroke-dasharray", "2,2")
      .attr("stroke-opacity", 0.5);
  }

  // The point of the exercise: whiskers, dots, annotations.
  const groups = svg.append("g").selectAll("g").data(rows).join("g");
  const midY = (d) => yScale(d.category) + yScale.bandwidth() / 2;

  groups
    .append("line")
    .attr("x1", (d) => getX(d.lowerBound))
    .attr("x2", (d) => getX(d.upperBound))
    .attr("y1", midY)
    .attr("y2", midY)
    .attr("stroke", "currentColor")
    .attr("stroke-width", 1);

  const dots = groups
    .append("circle")
    .attr("cx", (d) => getX(d.value))
    .attr("cy", midY)
    .attr("r", r)
    .attr("fill", "black")
    .attr("stroke", "white")
    .attr("stroke-width", 1);

  if (tip) {
    const tooltip = createTooltip();
    const contentFor =
      typeof tip === "function"
        ? tip
        : (d) =>
            tipHTML({
              category: d.category,
              estimate: fmt(d.value),
              "95% CI": `[${fmt(d.lowerBound)}, ${fmt(d.upperBound)}]`,
              ...(d.pValue != null ? { p: formatP(d.pValue) } : {}),
              ...(d.relative != null ? { relative: `${(d.relative * 100).toFixed(1)}%` } : {}),
            });
    dots
      .style("cursor", "default")
      .attr("r", r + 1) // a touch more target; hovering a 3px dot is a carnival game
      .on("mouseenter", (event, d) => tooltip.show(event, contentFor(d)))
      .on("mousemove", (event) => tooltip.move(event))
      .on("mouseleave", () => tooltip.hide());
  }

  if (pValue || relative) {
    groups
      .append("text")
      .attr("x", (d) =>
        leftLabelCategories.includes(d.category) ? getX(d.lowerBound) - 5 : getX(d.upperBound) + 5
      )
      .attr("y", midY)
      .attr("dy", "0.35em")
      .attr("text-anchor", (d) => (leftLabelCategories.includes(d.category) ? "end" : "start"))
      .attr("font-size", `${typography.annotation}px`)
      .text((d) => {
        const parts = [];
        if (pValue) parts.push(formatP(d.pValue));
        if (relative && d.relative != null) {
          parts.push(`(${d.relative < 0 ? "" : "+"}${(d.relative * 100).toFixed(1)}%)`);
        }
        return parts.join(" ");
      });
  }

  return svg.node();
}

/**
 * Wrap any SVG-producing plot function with SVG/PNG download buttons —
 * for the collaborators who consume figures exclusively via PowerPoint.
 * (The old version also did PDF via a CDN-loaded jsPDF; if you need PDF,
 * print the page. You were going to anyway.)
 *
 * @param {Function} plotFunction (data, options) => SVGElement
 * @param {*} data forwarded to plotFunction
 * @param {object} [options] forwarded to plotFunction
 * @param {{filename?: string, formats?: Array<"svg"|"png">, scale?: number}} [downloadOptions]
 * @returns {HTMLDivElement}
 */
export function withDownloadButtons(
  plotFunction,
  data,
  options = {},
  { filename = "figure", formats = ["svg", "png"], scale = 2 } = {}
) {
  const container = document.createElement("div");
  container.style.cssText = "display: flex; flex-direction: column; gap: 10px;";
  const plot = plotFunction(data, options);
  container.appendChild(plot);

  const buttonRow = document.createElement("div");
  buttonRow.style.cssText = "display: flex; gap: 8px;";

  const trigger = (href, name) => {
    const link = document.createElement("a");
    link.href = href;
    link.download = name;
    document.body.appendChild(link);
    link.click();
    link.remove();
  };

  const handlers = {
    svg: () => {
      const blob = new Blob([new XMLSerializer().serializeToString(plot)], { type: "image/svg+xml" });
      const url = URL.createObjectURL(blob);
      trigger(url, `${filename}.svg`);
      URL.revokeObjectURL(url);
    },
    png: () => {
      const svgStr = new XMLSerializer().serializeToString(plot);
      const canvas = document.createElement("canvas");
      canvas.width = plot.width.baseVal.value * scale;
      canvas.height = plot.height.baseVal.value * scale;
      const ctx = canvas.getContext("2d");
      ctx.scale(scale, scale);
      const img = new Image();
      const url = URL.createObjectURL(new Blob([svgStr], { type: "image/svg+xml" }));
      img.onload = () => {
        ctx.drawImage(img, 0, 0);
        trigger(canvas.toDataURL("image/png"), `${filename}.png`);
        URL.revokeObjectURL(url);
      };
      img.src = url;
    },
  };

  for (const format of formats) {
    if (!handlers[format]) continue;
    const button = document.createElement("button");
    button.textContent = `Download ${format.toUpperCase()}`;
    button.style.cssText =
      "padding: 6px 12px; background: #f3f4f6; border: 1px solid #d1d5db; border-radius: 4px; cursor: pointer; font-size: 12px;";
    button.onclick = handlers[format];
    buttonRow.appendChild(button);
  }
  container.appendChild(buttonRow);
  return container;
}
