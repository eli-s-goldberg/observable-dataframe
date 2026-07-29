/**
 * serpentineTimeline.js — a program timeline that snakes across the page.
 *
 * Milestones sit along an S-curve path scaled by date, with floating
 * labels, hover tooltips, a "Now" marker, and per-phase stroke styling.
 * Linear timelines waste a wall of horizontal space to say "time passed";
 * the serpentine says it in a rectangle that actually fits on a slide,
 * which is where this figure inevitably ends up.
 */

import * as d3 from "d3";
import { asRows } from "./util.js";
import { colors, fonts, typography } from "./theme.js";

const TOOLTIP_CSS = `
  .odf-timeline-tooltip {
    background: rgba(255, 255, 255, 0.98);
    border-radius: 8px;
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.1);
    padding: 12px 16px;
    max-width: 280px;
    pointer-events: none;
    position: absolute;
    z-index: 1000;
    font-family: ${fonts.sans};
  }
  .odf-timeline-tooltip .label {
    color: #666; font-size: 12px; font-weight: 500; text-transform: uppercase; margin-bottom: 2px;
  }
  .odf-timeline-tooltip .value {
    color: #333; font-size: 14px; margin-bottom: 8px; line-height: 1.4;
  }
`;

function generateCurve({ width, height, margin, turns }) {
  const effectiveHeight = height - 2 * margin;
  const segmentHeight = effectiveHeight / turns;
  const m = margin;
  const r = segmentHeight / 2;
  const t = m + r;
  let curve = `M ${m} ${m} L ${width - t} ${m}`;
  for (let i = 1; i <= turns; i++) {
    const dx = i === turns ? m : t;
    const y = m + i * segmentHeight;
    if (i % 2 === 0) curve += `A ${r} ${r} 0 0 0 ${t} ${y} L ${width - dx} ${y}`;
    else curve += `A ${r} ${r} 0 0 1 ${width - t} ${y} L ${dx} ${y}`;
  }
  return curve;
}

function wrapText(text, width) {
  const words = String(text ?? "").split(/\s+/);
  const lines = [];
  let line = [];
  let currentWidth = 0;
  const charWidth = 6;
  for (const word of words) {
    const wordWidth = word.length * charWidth;
    if (currentWidth + wordWidth > width && line.length) {
      lines.push(line.join(" "));
      line = [word];
      currentWidth = wordWidth;
    } else {
      line.push(word);
      currentWidth += wordWidth + charWidth;
    }
  }
  if (line.length) lines.push(line.join(" "));
  return lines;
}

/**
 * @param {DataFrame|Array<object>} data one row per milestone, chronological. Columns
 *   (rename via the column options): date, phase, activity, description, plus optional
 *   strokeColor / strokeThickness / dashing per row for segment styling.
 * @param {object} options
 * @param {string} [options.dateColumn="date"] / phaseColumn / activityColumn / descriptionColumn
 * @param {number} [options.width=1200] / @param {number} [options.height=800]
 * @param {number} [options.turns=2] how many times the snake bends
 * @param {number} [options.margin=120]
 * @param {Date} [options.now] draws the Now marker (and a dashed pre-phase lead-in when now precedes the first milestone)
 * @param {string[]} [options.displayLevels=["phase","activity"]] which text lines float at each point: "date" | "phase" | "activity" | "description"
 * @param {string[]} [options.visiblePhases] filter to these phases
 * @param {number} [options.wrapLength=150] description wrap width in px
 * @returns {{node: HTMLDivElement, cleanup: () => void}} call cleanup when removing — it owns a body-level tooltip and a ResizeObserver
 */
export function serpentineTimeline(
  data,
  {
    dateColumn = "date",
    phaseColumn = "phase",
    activityColumn = "activity",
    descriptionColumn = "description",
    width = 1200,
    height = 800,
    turns = 2,
    margin = 120,
    now = null,
    displayLevels = ["phase", "activity"],
    visiblePhases = null,
    wrapLength = 150,
    backgroundColor = "#ffffff",
    prePhaseColor = "#E0E0E0",
    nowLabel = "Now",
    nowPointColor = colors.clinical,
  } = {}
) {
  const rows = asRows(data)
    .map((r) => ({
      date: r[dateColumn] instanceof Date ? r[dateColumn] : new Date(r[dateColumn]),
      phase: r[phaseColumn] ?? "",
      activity: r[activityColumn] ?? "",
      description: r[descriptionColumn] ?? "",
      strokeColor: r.strokeColor ?? colors.navy,
      strokeThickness: r.strokeThickness ?? 3,
      dashing: r.dashing ?? null,
    }))
    .sort((a, b) => a.date - b.date);

  const adjustedMargin = Math.max(margin, height * 0.15);
  const dateFormat = { year: "numeric", month: "numeric", day: "numeric" };

  // Fluid down to the column, but never larger than nominal: an S-curve
  // upscaled past its design size is just a big S-curve.
  const div = d3.create("div").style("width", "100%").style("max-width", `${width}px`);
  const svg = div
    .append("svg")
    .attr("viewBox", [0, 0, width, height])
    .style("width", "100%")
    .style("height", "auto")
    .style("display", "block")
    .attr("preserveAspectRatio", "xMidYMid meet");

  const tooltip = d3
    .select("body")
    .append("div")
    .attr("class", "odf-timeline-tooltip")
    .style("opacity", 0);

  const style = document.createElement("style");
  style.textContent = TOOLTIP_CSS;
  document.head.appendChild(style);

  function drawTimeline(scale = 1) {
    svg.selectAll("*").remove();
    svg.append("rect").attr("width", width).attr("height", height).attr("fill", backgroundColor);

    const filtered = visiblePhases ? rows.filter((d) => visiblePhases.includes(d.phase)) : rows;
    if (!filtered.length) return;

    const curve = generateCurve({ width, height, margin: adjustedMargin, turns });
    const fullPath = svg.append("path").attr("fill", "none").attr("stroke", "none").attr("d", curve);
    const pathNode = fullPath.node();
    // jsdom has no path geometry; bail to an empty (but valid) rendering there.
    if (typeof pathNode.getTotalLength !== "function") return;
    let pathLength;
    try {
      pathLength = pathNode.getTotalLength();
    } catch {
      return;
    }

    const firstDate = d3.min(filtered, (d) => d.date);
    const timeScale = d3
      .scaleTime()
      .domain([now && now < firstDate ? now : firstDate, d3.max(filtered, (d) => d.date)])
      .range([0, pathLength]);

    const pointsAlong = (startLength, endLength) =>
      d3.range(startLength, endLength, pathLength / 200).map((l) => {
        const p = pathNode.getPointAtLength(l);
        return [p.x, p.y];
      });
    const line = d3.line().curve(d3.curveBasis);

    // Dashed lead-in from "now" to the first milestone, for programs that
    // exist so far only as intention.
    if (now && now < firstDate) {
      svg
        .append("path")
        .attr("fill", "none")
        .attr("stroke", prePhaseColor)
        .attr("stroke-width", 2 * scale)
        .attr("stroke-dasharray", "5,5")
        .attr("d", line(pointsAlong(timeScale(now), timeScale(firstDate))));
    }

    // One styled segment per inter-milestone stretch.
    filtered.forEach((item, i) => {
      const startDate = i === 0 ? (now && now < item.date ? now : item.date) : filtered[i - 1].date;
      const segment = svg
        .append("path")
        .attr("fill", "none")
        .attr("stroke", item.strokeColor)
        .attr("stroke-width", item.strokeThickness * scale)
        .attr("d", line(pointsAlong(timeScale(startDate), timeScale(item.date))));
      if (item.dashing) segment.attr("stroke-dasharray", item.dashing);
    });

    // Floating labels, sized from the shared type scale; `scale` already
    // carries the viewBox compensation, so these land at their nominal px.
    const displayFactor = scale;
    const pointGroups = svg.append("g").selectAll("g").data(filtered).join("g");
    const lineHeight = {
      date: typography.title * displayFactor,
      phase: typography.title * displayFactor,
      activity: typography.base * displayFactor,
      description: typography.tick * displayFactor,
    };
    const lineSpacing = 4 * scale;

    pointGroups.append("g").each(function (d) {
      const point = pathNode.getPointAtLength(timeScale(d.date));
      const group = d3.select(this);

      const textLines = [];
      for (const level of displayLevels) {
        if (level === "date") textLines.push({ text: d.date.toLocaleString("en", dateFormat), size: lineHeight.date, weight: 700 });
        else if (level === "phase") textLines.push({ text: d.phase, size: lineHeight.phase, weight: 700 });
        else if (level === "activity") textLines.push({ text: d.activity, size: lineHeight.activity, weight: 500 });
        else if (level === "description") {
          for (const l of wrapText(d.description, wrapLength)) {
            textLines.push({ text: l, size: lineHeight.description, weight: 400 });
          }
        }
      }

      const totalHeight = textLines.reduce((acc, l) => acc + l.size + lineSpacing, 0);
      const maxWidth = Math.max(...textLines.map((l) => l.text.length * l.size * 0.6), 0);
      const padding = 10 * scale;

      // Labels sit at their milestone but never past the frame; the ones
      // on the right-hand bend would otherwise be drawn off the canvas.
      const x = Math.min(Math.max(point.x, padding), Math.max(padding, width - maxWidth - padding));
      group.attr("transform", `translate(${x}, ${point.y - 50 * scale})`);

      group
        .append("rect")
        .attr("x", -padding)
        .attr("y", -padding * 2)
        .attr("width", maxWidth + padding)
        .attr("height", totalHeight + padding)
        .attr("fill", "white")
        .attr("opacity", 0.9)
        .attr("rx", 5 * scale)
        .attr("filter", "drop-shadow(0 2px 4px rgba(0,0,0,0.1))");

      let y = 0;
      for (const l of textLines) {
        group
          .append("text")
          .style("font-family", fonts.sans)
          .style("font-size", `${l.size}px`)
          .style("font-weight", l.weight)
          .attr("dy", y)
          .text(l.text);
        y += l.size + lineSpacing;
      }
    });

    // Interactive points with tooltips.
    pointGroups
      .append("circle")
      .attr("r", 6 * scale)
      .attr("cx", (d) => pathNode.getPointAtLength(timeScale(d.date)).x)
      .attr("cy", (d) => pathNode.getPointAtLength(timeScale(d.date)).y)
      .attr("fill", "white")
      .attr("stroke", (d) => d.strokeColor)
      .attr("stroke-width", 2 * scale)
      .style("cursor", "pointer")
      .on("mouseenter", (event, d) => {
        d3.select(event.currentTarget).transition().duration(200).attr("r", 8 * scale);
        tooltip.transition().duration(200).style("opacity", 1);
        tooltip
          .html(
            `<div class="label">Date</div><div class="value">${d.date.toLocaleString("en", dateFormat)}</div>` +
              `<div class="label">Phase</div><div class="value">${d.phase}</div>` +
              (d.activity ? `<div class="label">Activity</div><div class="value">${d.activity}</div>` : "") +
              (d.description ? `<div class="label">Description</div><div class="value">${d.description}</div>` : "")
          )
          .style("left", `${event.pageX + 15}px`)
          .style("top", `${event.pageY - 10}px`);
      })
      .on("mousemove", (event) => {
        tooltip.style("left", `${event.pageX + 15}px`).style("top", `${event.pageY - 10}px`);
      })
      .on("mouseleave", (event) => {
        d3.select(event.currentTarget).transition().duration(200).attr("r", 6 * scale);
        tooltip.transition().duration(200).style("opacity", 0);
      });

    // Now marker.
    if (now) {
      const nowPoint = pathNode.getPointAtLength(timeScale(now));
      const nowGroup = svg.append("g").attr("transform", `translate(${nowPoint.x}, ${nowPoint.y})`);
      nowGroup
        .append("rect")
        .attr("x", -30 * scale)
        .attr("y", -30 * scale)
        .attr("width", 60 * scale)
        .attr("height", 20 * scale)
        .attr("fill", "white")
        .attr("opacity", 0.9)
        .attr("rx", 5 * scale);
      nowGroup
        .append("text")
        .attr("y", -15 * scale)
        .attr("text-anchor", "middle")
        .style("font-family", fonts.sans)
        .style("font-size", `${typography.title * 1.25 * scale}px`)
        .style("font-weight", 700)
        .text(nowLabel);
      nowGroup
        .append("circle")
        .attr("r", 8 * scale)
        .attr("fill", nowPointColor)
        .attr("stroke", "white")
        .attr("stroke-width", 2 * scale);
    }
  }

  // Text and markers are drawn in viewBox units, which the browser then
  // scales by renderedWidth/width. Pre-dividing by that ratio is what keeps
  // a label the same number of screen pixels at any column width.
  function redraw() {
    const container = div.node().parentElement;
    if (!container) return;
    const rendered = container.getBoundingClientRect().width;
    if (!rendered) return;
    drawTimeline(Math.min(Math.max(width / rendered, 1), 2.5));
  }

  const resizeObserver = typeof ResizeObserver !== "undefined" ? new ResizeObserver(redraw) : null;
  if (typeof requestAnimationFrame === "function") {
    requestAnimationFrame(() => {
      const container = div.node().parentElement;
      if (container && resizeObserver) {
        resizeObserver.observe(container);
        redraw();
      }
    });
  }
  drawTimeline(1);

  function cleanup() {
    resizeObserver?.disconnect();
    tooltip.remove();
    style.remove();
  }

  return { node: div.node(), cleanup };
}
