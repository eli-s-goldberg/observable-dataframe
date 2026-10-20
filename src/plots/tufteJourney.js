/** Branching encounter history, ordered by first recorded appearance. */
import * as d3 from "d3";
import { asRows } from "./util.js";

/**
 * Render a single member's history. Lines span recorded contacts, not continuous
 * disease activity. Branches are inferred associations, not clinical progression.
 * Parent selection prefers the most recently introduced earlier code in the same
 * block, then chapter, then sharing an onset encounter. Tied onsets are not linked.
 * Duplicate code/encounter rows use their earliest date and preserve acute status.
 *
 * @param {Array<object>|import("../core/DataFrame.js").DataFrame} data
 * @param {object} [options]
 * @param {string} [options.code="code"] code column
 * @param {string} [options.date="date"] date column; UTC display
 * @param {string} [options.encounter="enc"] encounter identifier column
 * @param {string} [options.setting="setting"] setting column; ER, ED, emergency and inpatient are acute (case insensitive)
 * @param {Map<string, {label?: string, block?: string, chapter?: string}>} [options.meta] code labels and grouping metadata
 * @param {number} [options.width=1100] width in pixels, minimum 760
 * @param {number} [options.rowH=46] row height, minimum 40
 * @param {string} [options.title="One member’s history"] title
 * @param {string} [options.accent="#963d36"] acute mark color
 * @param {string} [options.ink="#302e2a"] primary text color
 * @param {string} [options.faint="#77736b"] supporting text color
 * @returns {SVGSVGElement} wrap in an overflow-x:auto container on small screens
 */
export function tufteJourney(data, {
  code = "code", date = "date", encounter = "enc", setting = "setting",
  meta = new Map(), ...options
} = {}) {
  for (const key of ["width", "rowH"]) {
    if (options[key] != null && (!Number.isFinite(options[key]) || options[key] <= 0)) {
      throw new Error(`${key} must be a positive finite number.`);
    }
  }
  const rows = asRows(data).map(row => ({code: row[code], date: row[date], enc: row[encounter], setting: row[setting]}));
  if (rows.some(row => row.date == null || row.date === "")) throw new Error("Each event needs a valid date.");
  return renderJourney(rows, meta, options);
}

function renderJourney(events, meta = new Map(), {
  width = 1100, rowH = 46, accent = "#963d36", ink = "#302e2a",
  faint = "#77736b", title = "One member’s history"
} = {}) {
  width = Math.max(760, width);
  rowH = Math.max(40, rowH);
  const rows = events.map(e => ({...e, date: new Date(e.date)}));
  if (rows.some(e => !Number.isFinite(+e.date) || !e.code || e.enc == null)) {
    throw new Error("Each event needs a valid date, code, and encounter identifier.");
  }
  const acute = e => /^(er|ed|emergency|inpatient)$/i.test(e.setting || "");
  // Collapse duplicate code/encounter records without losing acute contacts.
  const points = d3.flatRollup(rows, v => ({
    ...v.slice().sort((a, b) => +a.date - +b.date)[0],
    setting: v.find(acute)?.setting ?? v[0].setting
  }), e => e.code, e => e.enc).map(d => d[2]);
  const byCode = d3.group(points, e => e.code);
  const first = new Map([...byCode].map(([c, v]) => [c, d3.min(v, e => e.date)]));
  const last = new Map([...byCode].map(([c, v]) => [c, d3.max(v, e => e.date)]));
  const order = [...byCode.keys()].sort((a, b) => +first.get(a) - +first.get(b) || String(a).localeCompare(String(b)));
  const m = {t: 116, r: Math.min(350, width * .34), b: 64, l: 24};
  const height = m.t + Math.max(0, order.length - 1) * rowH + m.b;
  const svg = d3.create("svg").attr("xmlns", "http://www.w3.org/2000/svg")
    .attr("viewBox", `0 0 ${width} ${height}`).attr("width", width)
    .attr("role", "img").attr("aria-label", `${title}. Codes ordered by first recorded appearance, earliest at bottom. Branches indicate inferred relationships, not clinical progression.`)
    .style("max-width", "100%").style("min-width", "760px").style("height", "auto")
    .style("font-family", "Georgia, 'Times New Roman', serif").style("font-size", "12px")
    .style("fill", ink);
  svg.append("title").text(title);
  svg.append("desc").text("Ticks mark other contacts; red circles mark ER or inpatient contacts. A line connects the first and last recorded contacts; it does not establish continuous disease activity. Branches are heuristic associations.");
  svg.append("text").attr("x", m.l).attr("y", 30).attr("font-size", 25).text(title);
  const note = svg.append("text").attr("x", m.l).attr("y", 54).attr("fill", faint);
  note.append("tspan").text("Recorded contacts, ordered by first appearance");
  const legend = svg.append("g").attr("transform", `translate(${m.l},77)`);
  legend.append("circle").attr("r", 3).attr("cx", 3).attr("fill", accent);
  legend.append("text").attr("x", 13).attr("dy", ".32em").attr("fill", faint).text("ER or inpatient");
  legend.append("line").attr("x1", 120).attr("x2", 120).attr("y1", -3).attr("y2", 3).attr("stroke", ink);
  legend.append("text").attr("x", 130).attr("dy", ".32em").attr("fill", faint).text("Other contact");
  if (!points.length) {
    svg.append("text").attr("x", m.l).attr("y", m.t).attr("fill", faint).text("No recorded contacts");
    return svg.node();
  }
  const extent = d3.extent(points, e => e.date);
  const domain = +extent[0] === +extent[1]
    ? [d3.utcDay.offset(extent[0], -1), d3.utcDay.offset(extent[1], 1)] : extent;
  const endX = width - m.r;
  const x = d3.scaleUtc(domain, [m.l, endX]);
  const y = new Map(order.map((c, i) => [c, m.t + (order.length - 1 - i) * rowH]));
  const fmt = d3.utcFormat("%b %d, %Y");

  // Only link genuinely earlier codes with a shared block/chapter/encounter.
  // Tied onsets and unrelated codes remain independent. Missing metadata never matches.
  const links = [];
  for (const c of order) {
    const earlier = order.filter(p => +first.get(p) < +first.get(c)).reverse();
    const cm = meta.get(c) || {};
    const onsetEncounters = new Set(byCode.get(c).filter(e => +e.date === +first.get(c)).map(e => e.enc));
    const p = earlier.find(p => cm.block && cm.block === meta.get(p)?.block)
      ?? earlier.find(p => cm.chapter && cm.chapter === meta.get(p)?.chapter)
      ?? earlier.find(p => byCode.get(p).some(e => onsetEncounters.has(e.enc) && +e.date <= +first.get(c)));
    if (p == null) continue;
    // Attach within the parent's observed span, never to empty space after its last contact.
    const start = Math.min(x(last.get(p)), Math.max(x(first.get(p)), x(first.get(c)) - 28));
    links.push({c, p, start});
  }
  svg.append("g").attr("fill", "none").attr("stroke", "#c9c5bc").attr("stroke-width", .8)
    .selectAll("path").data(links).join("path")
    .attr("data-parent", d => d.p).attr("data-child", d => d.c)
    .attr("d", ({c, p, start}) => {
      const finish = x(first.get(c)), bend = start + (finish - start) * .55;
      return `M${start},${y.get(p)}C${bend},${y.get(p)} ${bend},${y.get(c)} ${finish},${y.get(c)}`;
    }).append("title").text(({c, p}) => `${p} → ${c}: inferred association, not clinical progression`);
  svg.append("g").attr("stroke", "#827d73").attr("stroke-width", .75)
    .selectAll("line").data(order).join("line")
    .attr("data-lane", c => c)
    .attr("x1", c => x(first.get(c))).attr("x2", c => x(last.get(c)))
    .attr("y1", c => y.get(c)).attr("y2", c => y.get(c));
  // Acute marks are drawn last so coincident routine contacts cannot cover them.
  svg.append("g").selectAll("g").data(points.slice().sort((a, b) => acute(a) - acute(b))).join("g")
    .attr("data-code", e => e.code).attr("data-encounter", e => e.enc)
    .attr("transform", e => `translate(${x(e.date)},${y.get(e.code)})`)
    .each(function(e) {
      const mark = d3.select(this);
      if (acute(e)) mark.append("circle").attr("r", 3.1).attr("fill", accent);
      else mark.append("line").attr("y1", -2.8).attr("y2", 2.8).attr("stroke", ink).attr("stroke-width", .85);
      mark.append("title").text(`${e.code} · ${e.setting || "Other contact"} · ${fmt(e.date)}`);
    });
  const labelX = endX + 26;
  const maxChars = Math.floor((width - labelX - 16) / 6.1);
  const short = text => text.length > maxChars ? text.slice(0, maxChars - 1).trimEnd() + "…" : text;
  svg.append("g").selectAll("g").data(order).join("g")
    .attr("transform", c => `translate(${labelX},${y.get(c)})`)
    .each(function(c) {
      const g = d3.select(this), info = meta.get(c) || {};
      const description = String(info.label || info.block || "").replace(/^[A-Z]\d[^ ]*\s+/, "");
      g.append("text").attr("dy", ".3em").attr("font-size", 13).text(c);
      g.append("text").attr("y", 17).attr("fill", faint).attr("font-size", 11.5).text(short(description));
      g.append("title").text(`${c}${description ? " · " + description : ""}\nFirst recorded: ${fmt(first.get(c))}\nLast recorded: ${fmt(last.get(c))}`);
    });
  // Quiet temporal reference: no enclosing axis or vertical grid.
  const ay = height - 30;
  const endpointFormat = d3.utcFormat("%b %Y");
  let ticks = x.ticks(Math.max(2, Math.floor((endX - m.l) / 140)))
    .filter(d => x(d) > m.l + 65 && x(d) < endX - 65);
  ticks = [{date: extent[0], anchor: "start"}, ...(+extent[0] === +extent[1] ? [] : ticks.map(date => ({date, anchor: "middle"}))),
    ...(+extent[0] !== +extent[1] ? [{date: extent[1], anchor: "end"}] : [])];
  const tick = svg.append("g").attr("fill", faint).selectAll("g").data(ticks).join("g")
    .attr("transform", d => `translate(${x(d.date)},${ay})`);
  tick.append("line").attr("y1", -10).attr("y2", -6).attr("stroke", "#b5b0a5").attr("stroke-width", .7);
  tick.append("text").attr("text-anchor", d => d.anchor).attr("font-size", 11)
    .text(d => (+extent[1] - +extent[0]) < 60 * 864e5 ? d3.utcFormat("%d %b %Y")(d.date) : endpointFormat(d.date));
  return svg.node();
}
