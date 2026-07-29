/**
 * treeExplore.js — a collapsible drill-down tree with a stats panel.
 *
 * Rows get folded into a hierarchy down your chosen level columns; click a
 * node to expand it and see its metrics; hover to trace the branch. It's
 * the "let me show you where the members actually are" figure, and unlike
 * its ancestor, the metrics are configuration instead of a hardcoded
 * imaging use case (we've all been there; some of us wrote it).
 */

import * as d3 from "d3";
import { asRows } from "./util.js";
import { colors, fonts, typography } from "./theme.js";

/**
 * @param {DataFrame|Array<object>} data long-format rows
 * @param {object} options
 * @param {string[]} options.levels columns to nest by, outermost first
 * @param {Array<object>} [options.metrics] stats shown on node click. Each:
 *   {label, type: "count"} — row count
 *   {label, type: "sum", column} — sum of a column
 *   {label, type: "rate", numerator, denominator, format?: "percent"} — ratio of two sums
 *   {label, compute: (rows) => value} — the escape hatch
 * @param {number} [options.width=800]
 * @param {number} [options.strokeWidthDivisor=100] link thickness = node count / this
 * @returns {HTMLDivElement}
 */
export function treeExplore(
  data,
  { levels, metrics = [{ label: "Rows", type: "count" }], width = 800, strokeWidthDivisor = 100 } = {}
) {
  const rows = asRows(data);
  if (!levels?.length) {
    throw new Error(`treeExplore needs { levels: [...] } — a tree with no levels is a dot.`);
  }

  const hierarchyData = buildHierarchy(rows, levels, metrics);

  const mainContainer = document.createElement("div");
  mainContainer.style.cssText = `display: flex; flex-direction: column; gap: 16px; width: 100%; font-family: ${fonts.sans};`;

  const statsContainer = document.createElement("div");
  statsContainer.style.cssText = `padding: 10px 12px; background: ${colors.panel}; border-radius: 5px; display: none;`;

  const updateStats = (d) => {
    statsContainer.style.display = "block";
    const rowsHtml = metricRows(d.data, metrics)
      .map(
        ([label, value]) =>
          `<tr><td style="padding: 6px 8px; border: 1px solid ${colors.border};">${label}</td>
           <td style="padding: 6px 8px; text-align: right; border: 1px solid ${colors.border}; font-weight: 600;">${value}</td></tr>`
      )
      .join("");
    statsContainer.innerHTML = `<table style="width: 100%; border-collapse: collapse; font-size: ${typography.base + 1}px;">
      <tr style="background: #e0e0e0;">
        <th style="padding: 6px 8px; text-align: left; border: 1px solid ${colors.border};">Metric</th>
        <th style="padding: 6px 8px; text-align: right; border: 1px solid ${colors.border};">Value</th>
      </tr>
      <tr><td style="padding: 6px 8px; border: 1px solid ${colors.border};">Node</td>
          <td style="padding: 6px 8px; text-align: right; border: 1px solid ${colors.border}; font-weight: 600;">${d.data.name || "(root)"}</td></tr>
      ${rowsHtml}
    </table>`;
  };
  const clearStats = () => (statsContainer.style.display = "none");

  const tree = collapsibleTree(hierarchyData, width, updateStats, clearStats, strokeWidthDivisor);
  mainContainer.appendChild(tree);
  mainContainer.appendChild(statsContainer);
  return mainContainer;
}

function metricRows(nodeData, metrics) {
  return metrics.map((m) => {
    const v = nodeData.metrics[m.label];
    const formatted =
      m.format === "percent"
        ? `${(v * 100).toFixed(1)}%`
        : typeof v === "number"
          ? v.toLocaleString(undefined, { maximumFractionDigits: 2 })
          : String(v);
    return [m.label, formatted];
  });
}

function computeMetrics(rows, metrics) {
  const out = {};
  for (const m of metrics) {
    if (m.compute) out[m.label] = m.compute(rows);
    else if (m.type === "count") out[m.label] = rows.length;
    else if (m.type === "sum") out[m.label] = d3.sum(rows, (r) => r[m.column] || 0);
    else if (m.type === "rate") {
      const num = d3.sum(rows, (r) => r[m.numerator] || 0);
      const den = d3.sum(rows, (r) => r[m.denominator] || 0);
      out[m.label] = den > 0 ? num / den : 0;
    } else throw new Error(`Unknown metric type "${m.type}" for "${m.label}".`);
  }
  return out;
}

function buildHierarchy(rows, levels, metrics) {
  const root = { name: "", children: [], _rows: rows };
  const add = (node, row, depth) => {
    if (depth >= levels.length) return;
    const value = row[levels[depth]];
    if (value == null) return;
    let child = node.children.find((c) => c.name === String(value));
    if (!child) {
      child = { name: String(value), children: [], _rows: [] };
      node.children.push(child);
    }
    child._rows.push(row);
    add(child, row, depth + 1);
  };
  for (const row of rows) add(root, row, 0);

  (function finalize(node) {
    node.value = node._rows.length;
    node.metrics = computeMetrics(node._rows, metrics);
    delete node._rows;
    if (node.children.length === 0) delete node.children;
    else node.children.forEach(finalize);
  })(root);
  return root;
}

function collapsibleTree(data, width, updateStats, clearStats, strokeWidthDivisor) {
  const treeContainer = document.createElement("div");
  treeContainer.style.overflow = "auto";

  let selectedNode = null;
  const margin = { top: 10, right: 80, bottom: 10, left: 80 };
  const dx = 30;
  const dy = width / 6;

  const tree = d3.tree().nodeSize([dx, dy]).separation((a, b) => (a.parent === b.parent ? 1 : 1.5));
  const diagonal = d3.linkHorizontal().x((d) => d.y).y((d) => d.x);

  const svg = d3
    .create("svg")
    .attr("viewBox", [-margin.left, -margin.top, width, dx])
    .style("max-width", `${width}px`)
    .style("width", "100%")
    .style("height", "auto")
    .style("user-select", "none");

  const gLink = svg.append("g").attr("fill", "none").attr("stroke-opacity", 0.4);
  const gNode = svg.append("g").attr("cursor", "pointer").attr("pointer-events", "all");

  const root = d3.hierarchy(data);
  root.x0 = 0;
  root.y0 = 0;
  root.descendants().forEach((d, i) => {
    d.id = i;
    d._children = d.children;
    if (d.depth > 0) d.children = null; // start folded; curiosity is the UI
  });

  const branchIdsOf = (node) => {
    const ids = new Set();
    for (let cur = node; cur; cur = cur.parent) ids.add(cur.id);
    return ids;
  };

  function highlightBranch(d, color = colors.clinical) {
    const ids = branchIdsOf(d);
    gNode.selectAll("g").filter((n) => ids.has(n.id)).select("circle").attr("fill", color).attr("r", 6);
    gLink
      .selectAll("path")
      .filter((l) => ids.has(l.source.id) && ids.has(l.target.id))
      .attr("stroke", color)
      .attr("stroke-opacity", 1);
  }

  function resetHighlights() {
    gNode
      .selectAll("circle")
      .attr("fill", (d) =>
        selectedNode && d.id === selectedNode.id ? colors.clinical : d._children ? "#555" : "#999"
      )
      .attr("r", (d) => (selectedNode && d.id === selectedNode.id ? 6 : 4));
    gLink.selectAll("path").attr("stroke", "#999").attr("stroke-opacity", 0.4);
    if (selectedNode) highlightBranch(selectedNode);
  }

  // jsdom and friends don't implement SVG transform interpolation, and a
  // tree that snaps instead of gliding is still a tree. Feature-detect.
  const canAnimate = (() => {
    try {
      const probe = document.createElementNS("http://www.w3.org/2000/svg", "g");
      return probe.transform?.baseVal != null && typeof requestAnimationFrame === "function";
    } catch {
      return false;
    }
  })();

  function update(source) {
    const duration = 250;
    const nodes = root.descendants().reverse();
    const links = root.links();
    tree(root);

    let left = root;
    let right = root;
    root.eachBefore((n) => {
      if (n.x < left.x) left = n;
      if (n.x > right.x) right = n;
    });
    const height = right.x - left.x + margin.top + margin.bottom;

    const viewBox = [-margin.left, left.x - margin.top, width, height];
    const transition = canAnimate
      ? svg.transition().duration(duration).attr("viewBox", viewBox)
      : null;
    if (!canAnimate) svg.attr("viewBox", viewBox);
    // Apply attrs through the transition when we can, directly when we can't.
    const settle = (selection) => (canAnimate ? selection.transition(transition) : selection);

    const node = gNode.selectAll("g").data(nodes, (d) => d.id);

    const nodeEnter = node
      .enter()
      .append("g")
      .attr("transform", () => `translate(${source.y0},${source.x0})`)
      .attr("fill-opacity", 0)
      .attr("stroke-opacity", 0)
      .on("click", (event, d) => {
        if (d._children || d.children) {
          d.children = d.children ? null : d._children;
          update(d);
        }
        selectedNode = selectedNode && selectedNode.id === d.id ? null : d;
        resetHighlights();
        if (selectedNode) updateStats(d);
        else clearStats();
      })
      .on("mouseover", (event, d) => {
        if (!selectedNode || selectedNode.id !== d.id) highlightBranch(d, "orange");
      })
      .on("mouseout", resetHighlights);

    nodeEnter
      .append("circle")
      .attr("r", 4)
      .attr("fill", (d) => (d._children ? "#555" : "#999"))
      .attr("stroke-width", 10);

    nodeEnter
      .append("text")
      .attr("dy", "0.31em")
      .style("font-size", `${typography.base}px`)
      .attr("x", (d) => (d._children ? -6 : 6))
      .attr("text-anchor", (d) => (d._children ? "end" : "start"))
      .text((d) => (d.data.name ? `${d.data.name} (${d.data.value.toLocaleString()})` : ""))
      .clone(true)
      .lower()
      .attr("stroke-linejoin", "round")
      .attr("stroke-width", 3)
      .attr("stroke", "white");

    settle(node.merge(nodeEnter))
      .attr("transform", (d) => `translate(${d.y},${d.x})`)
      .attr("fill-opacity", 1)
      .attr("stroke-opacity", 1);

    settle(node.exit())
      .remove()
      .attr("transform", () => `translate(${source.y},${source.x})`);

    const link = gLink.selectAll("path").data(links, (d) => d.target.id);
    const linkEnter = link
      .enter()
      .append("path")
      .attr("stroke", "#999")
      .attr("d", () => {
        const o = { x: source.x0, y: source.y0 };
        return diagonal({ source: o, target: o });
      });

    settle(link.merge(linkEnter))
      .attr("d", diagonal)
      .attr("stroke-width", (d) => Math.max(1, d.target.data.value / strokeWidthDivisor));

    settle(link.exit())
      .remove()
      .attr("d", () => {
        const o = { x: source.x, y: source.y };
        return diagonal({ source: o, target: o });
      });

    root.eachBefore((d) => {
      d.x0 = d.x;
      d.y0 = d.y;
    });
  }

  update(root);
  treeContainer.appendChild(svg.node());
  return treeContainer;
}
