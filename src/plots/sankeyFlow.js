/**
 * sankeyFlow.js — an animated Sankey where the flow actually flows.
 *
 * Particles spawn at the root, ride the links, and land in terminal
 * buckets while live counters tick up. Objectively slower to read than a
 * static Sankey; subjectively the reason the room looks up from their
 * laptops. Use accordingly.
 *
 * Input is either the nested tree format ({root: {...}}) or, for
 * DataFrame people, `nestFromFrame(df, {levels, buckets})` builds that
 * tree from long-format rows.
 */

import * as d3 from "d3";
import { sankey, sankeyLinkHorizontal } from "d3-sankey";
import { asRows } from "./util.js";
import { colors, fonts, typography } from "./theme.js";

/**
 * Build the nested tree sankeyFlow eats, from a DataFrame or rows.
 * Rows are grouped down the `levels` columns; the `buckets` columns are
 * summed at the leaves and become the terminal counts.
 *
 *   nestFromFrame(df, { levels: ["region", "specialty"], buckets: ["preferred", "non_preferred"] })
 *   // → { root: { East: { Cardiology: { preferred: 120, non_preferred: 44 }, ... }, ... } }
 *
 * @param {DataFrame|Array<object>} data
 * @param {{levels: string[], buckets: string[]}} options
 * @returns {{root: object}}
 */
export function nestFromFrame(data, { levels, buckets }) {
  const rows = asRows(data);
  const root = {};
  for (const row of rows) {
    let node = root;
    for (const level of levels) {
      const key = String(row[level]);
      node = node[key] ?? (node[key] = {});
    }
    for (const bucket of buckets) {
      node[bucket] = (node[bucket] ?? 0) + (Number(row[bucket]) || 0);
    }
  }
  return { root };
}

/**
 * @param {{root: object}} rawData nested tree; leaves carry the terminal bucket counts
 * @param {object} [config]
 * @param {string[]} [config.terminalBuckets=["preferred","non_preferred"]] leaf keys treated as outcomes
 * @param {Record<string,string>} [config.bucketLabels] display names per bucket
 * @param {Record<string,string>} [config.bucketColors] colors per bucket
 * @param {number} [config.width=800]
 * @param {number} [config.height=300]
 * @param {boolean} [config.animate=true] set false for static rendering (tests, screenshots, low-whimsy environments)
 * @param {Function} [onReplay] callback after a replay is triggered
 * @returns {{node: SVGSVGElement, replay: () => void, stop: () => void}}
 */
export function sankeyFlow(rawData, config = {}, onReplay = null) {
  const cfg = {
    width: 800,
    height: 300,
    margin: { top: 20, right: 250, bottom: 20, left: 40 },
    nodePadding: 30,
    nodeWidth: 15,
    particleSize: 6,
    particleSpeed: 5,
    particleSpawnRate: 0.4,
    particleVerticalSpread: 0.8,
    showLabels: true,
    hideRootLabel: true,
    counterSpacing: 100,
    animate: true,
    terminalBuckets: ["preferred", "non_preferred"],
    bucketLabels: { preferred: "Preferred", non_preferred: "Non-Preferred" },
    bucketColors: { preferred: colors.highlight, non_preferred: "#D04A02" },
    ...config,
  };

  const cache = {};
  let particles = [];
  let animationFrame = null;

  const isTerminal = (node) => node[cfg.terminalBuckets[0]] !== undefined;
  const nodeTotal = (node) =>
    isTerminal(node)
      ? cfg.terminalBuckets.reduce((sum, b) => sum + (node[b] || 0), 0)
      : Object.values(node).reduce((sum, child) => sum + nodeTotal(child), 0);
  const bucketSum = (counts) => cfg.terminalBuckets.reduce((s, b) => s + (counts[b] || 0), 0);

  // Flatten the tree into sankey nodes + links.
  const nodeSet = new Set(["root"]);
  const nodeValues = new Map([["root", nodeTotal(rawData.root)]]);
  const links = [];
  (function walk(node, source) {
    for (const [name, child] of Object.entries(node)) {
      nodeSet.add(name);
      nodeValues.set(name, nodeTotal(child));
      const link = { source, target: name, value: nodeTotal(child) };
      if (isTerminal(child)) {
        for (const b of cfg.terminalBuckets) link[b] = child[b] || 0;
      }
      links.push(link);
      if (!isTerminal(child)) walk(child, name);
    }
  })(rawData.root, "root");

  const graph = sankey()
    .nodeId((d) => d.id)
    .nodeWidth(cfg.nodeWidth)
    .nodePadding(cfg.nodePadding)
    .extent([
      [cfg.margin.left, cfg.margin.top],
      [cfg.width - cfg.margin.right, cfg.height - cfg.margin.bottom],
    ])({
    nodes: [...nodeSet].map((id) => ({ id, value: nodeValues.get(id) })),
    links,
  });

  const targetCounts = {};
  const particleCounts = {};
  for (const node of graph.nodes) {
    const terminalLink = graph.links.find(
      (l) => l.target === node && l[cfg.terminalBuckets[0]] !== undefined
    );
    if (terminalLink) {
      targetCounts[node.id] = {};
      particleCounts[node.id] = {};
      for (const b of cfg.terminalBuckets) {
        targetCounts[node.id][b] = terminalLink[b] || 0;
        particleCounts[node.id][b] = 0;
      }
    }
  }

  const svg = d3
    .create("svg")
    .attr("viewBox", [0, 0, cfg.width, cfg.height])
    .style("background", "white")
    // Explicit house font: an SVG without one inherits whatever the page
    // is wearing, which in Observable Framework is a serif. Surprise serifs
    // are how figures stop matching.
    .style("font-family", fonts.sans)
    .style("max-width", `${cfg.width}px`)
    .style("width", "100%")
    .style("height", "auto");

  svg
    .append("g")
    .selectAll("path")
    .data(graph.links)
    .join("path")
    .attr("d", sankeyLinkHorizontal())
    .attr("fill", "none")
    .attr("stroke", "#f0f0f0")
    .attr("stroke-opacity", 0.5)
    .attr("stroke-width", (d) => Math.max(1, d.width))
    .each(function (d) {
      // Pre-walk each path once so particles can index into points instead
      // of calling getPointAtLength sixty times a second. jsdom also lacks
      // getTotalLength, so tests take the graceful zero-length branch.
      const pathKey = `${d.source.id}-${d.target.id}`;
      const points = [];
      let length = 0;
      if (typeof this.getTotalLength === "function") {
        try {
          length = this.getTotalLength();
          for (let i = 0; i <= length; i++) {
            const p = this.getPointAtLength(i);
            points.push({ x: p.x, y: p.y });
          }
        } catch {
          length = 0;
        }
      }
      cache[pathKey] = { points, length, width: d.width };
    });

  const particlesContainer = svg.append("g").attr("class", "particles");

  if (cfg.showLabels) {
    svg
      .append("g")
      .selectAll("text")
      .data(graph.nodes)
      .join("text")
      .filter((d) => !(cfg.hideRootLabel && d.id === "root"))
      .attr("x", (d) => (d.x0 < cfg.width / 2 ? d.x1 + 6 : d.x0 - 6))
      .attr("y", (d) => (d.y1 + d.y0) / 2)
      .attr("dy", "0.35em")
      .attr("text-anchor", (d) => (d.x0 < cfg.width / 2 ? "start" : "end"))
      .attr("fill", "#000")
      .style("font-size", `${typography.base}px`)
      .text((d) => d.id);
  }

  const counters = svg
    .append("g")
    .attr("class", "counters")
    .selectAll("g")
    .data(graph.nodes.filter((n) => targetCounts[n.id]))
    .join("g")
    .attr("transform", (d) => `translate(${cfg.width - cfg.margin.right + 50}, ${(d.y0 + d.y1) / 2})`);

  const counterGroups = counters
    .selectAll(".group")
    .data((d) =>
      cfg.terminalBuckets.map((bucket) => ({
        type: bucket,
        node: d,
        label: cfg.bucketLabels[bucket] ?? bucket,
        total: bucketSum(targetCounts[d.id]),
      }))
    )
    .join("g")
    .attr("class", "group")
    .attr("transform", (d, i) => `translate(${i * cfg.counterSpacing}, 0)`);

  counterGroups
    .append("text")
    .attr("class", "label")
    .attr("y", -15)
    .attr("fill", "#000")
    .style("font-size", `${typography.tick}px`)
    .text((d) => d.label);

  counterGroups
    .append("text")
    .attr("class", "count")
    .attr("fill", (d) => cfg.bucketColors[d.type] ?? colors.navy)
    .style("font-size", `${typography.title}px`)
    .text("0");

  counterGroups
    .append("text")
    .attr("class", "percentage")
    .attr("y", 20)
    .attr("fill", (d) => cfg.bucketColors[d.type] ?? colors.navy)
    .style("font-family", "monospace")
    .style("font-size", `${typography.tick}px`)
    .text("0%");

  function setFinalCounts() {
    counterGroups
      .select(".count")
      .text((d) => targetCounts[d.node.id][d.type]);
    counterGroups
      .select(".percentage")
      .text((d) => `${Math.round((targetCounts[d.node.id][d.type] / d.total) * 100)}%`);
  }

  function startAnimation() {
    const nodeTargets = new Map(
      Object.entries(targetCounts).map(([nodeId, counts]) => [
        nodeId,
        { remaining: { ...counts }, total: bucketSum(counts) },
      ])
    );
    const grandTotal = [...nodeTargets.values()].reduce((s, n) => s + n.total, 0);
    let created = 0;

    function animate() {
      if (created < grandTotal && Math.random() < cfg.particleSpawnRate) {
        spawnParticle(nodeTargets, () => created++);
      }

      particlesContainer
        .selectAll(".particle")
        .data(particles, (d) => d.id)
        .join(
          (enter) =>
            enter
              .append("rect")
              .attr("class", "particle")
              .attr("width", cfg.particleSize)
              .attr("height", cfg.particleSize)
              .attr("fill", (d) => cfg.bucketColors[d.type] ?? colors.navy)
              .attr("opacity", 0.6),
          (update) => update,
          (exit) => exit.remove()
        )
        .attr("transform", (d) => {
          const pathData = cache[d.pathKey];
          const index = Math.floor(d.pos);
          if (pathData && index >= 0 && index < pathData.points.length) {
            const point = pathData.points[index];
            return `translate(${point.x - cfg.particleSize / 2},${point.y + d.offset - cfg.particleSize / 2})`;
          }
          return null;
        });

      particles = particles.filter((d) => {
        d.pos += d.speed;
        const pathData = cache[d.pathKey];
        if (d.pos < pathData.length) return true;
        if (d.currentPathIndex < d.completePath.length - 1) {
          d.currentPathIndex++;
          const next = d.completePath[d.currentPathIndex];
          d.pathKey = `${next.source.id}-${next.target.id}`;
          d.pos = 0;
          d.offset = (Math.random() - 0.5) * cache[d.pathKey].width * cfg.particleVerticalSpread;
          return true;
        }
        // Arrived: tick the counters.
        if (particleCounts[d.target][d.type] < targetCounts[d.target][d.type]) {
          particleCounts[d.target][d.type]++;
          const total = bucketSum(targetCounts[d.target]);
          const current = particleCounts[d.target][d.type];
          const match = counterGroups.filter((g) => g.node.id === d.target && g.type === d.type);
          match.select(".count").text(current);
          match.select(".percentage").text(`${Math.round((current / total) * 100)}%`);
        }
        return false;
      });

      const allDone = [...nodeTargets.values()].every((n) =>
        Object.values(n.remaining).every((c) => c <= 0)
      );
      if (!allDone || particles.length > 0) {
        animationFrame = requestAnimationFrame(animate);
      }
    }
    animate();
  }

  function spawnParticle(nodeTargets, onCreate) {
    // Choose a terminal weighted by how much of its quota is unfilled,
    // then a bucket the same way — so the visual mix converges on the data
    // instead of on Math.random's mood.
    const terminals = graph.links.filter((l) => l[cfg.terminalBuckets[0]] !== undefined);
    const available = [];
    const weights = [];
    for (const link of terminals) {
      const target = nodeTargets.get(link.target.id);
      if (!target) continue;
      const remaining = bucketSum(target.remaining);
      if (remaining > 0) {
        available.push(link);
        weights.push(remaining / target.total);
      }
    }
    if (!available.length) return;

    const pick = (items, w) => {
      const total = w.reduce((a, b) => a + b, 0);
      let r = Math.random() * total;
      for (let i = 0; i < items.length; i++) {
        r -= w[i];
        if (r <= 0) return i;
      }
      return items.length - 1;
    };

    const terminal = available[pick(available, weights)];
    const nodeTarget = nodeTargets.get(terminal.target.id);

    const bucketChoices = cfg.terminalBuckets.filter((b) => nodeTarget.remaining[b] > 0);
    if (!bucketChoices.length) return;
    const bucket = bucketChoices[pick(bucketChoices, bucketChoices.map((b) => nodeTarget.remaining[b] / nodeTarget.total))];

    const completePath = [];
    let current = terminal;
    while (current) {
      completePath.unshift(current);
      current = graph.links.find((l) => l.target === current.source);
    }
    const first = completePath[0];
    const pathKey = `${first.source.id}-${first.target.id}`;
    const pathData = cache[pathKey];
    if (!pathData) return;

    onCreate();
    nodeTarget.remaining[bucket]--;
    particles.push({
      id: Date.now() + Math.random(),
      completePath,
      currentPathIndex: 0,
      pathKey,
      pos: 0,
      speed: cfg.particleSpeed,
      offset: (Math.random() - 0.5) * pathData.width * cfg.particleVerticalSpread,
      type: bucket,
      target: terminal.target.id,
    });
  }

  function stop() {
    if (animationFrame) cancelAnimationFrame(animationFrame);
    particles = [];
  }

  function replay() {
    stop();
    for (const nodeId of Object.keys(particleCounts)) {
      for (const b of cfg.terminalBuckets) particleCounts[nodeId][b] = 0;
    }
    counterGroups.select(".count").text("0");
    counterGroups.select(".percentage").text("0%");
    if (cfg.animate) startAnimation();
    else setFinalCounts();
    if (onReplay) onReplay();
  }

  if (cfg.animate && typeof requestAnimationFrame === "function") startAnimation();
  else setFinalCounts();

  return { node: svg.node(), replay, stop };
}
