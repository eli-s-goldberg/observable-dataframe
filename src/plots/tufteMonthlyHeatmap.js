/**
 * Monthly recurrence in recorded encounters. Uses UTC calendar months and the
 * branching history's first-appearance order (earliest at bottom).
 */
import * as d3 from "d3";
import { asRows } from "./util.js";

/**
 * Each cell counts distinct encounters carrying a code. Percentage mode divides
 * that count by all distinct encounters in the supplied rows for that month.
 * Codes can co-occur, so percentages across rows need not sum to 100%.
 * Filter to one member before calling; encounter IDs must identify encounters
 * uniquely within that input. Duplicate code/encounter rows use their earliest
 * date. Missing months describe the input, not enrollment or absence of disease.
 *
 * @param {Array<object>|import("../core/DataFrame.js").DataFrame} data
 * @param {object} [options]
 * @param {string} [options.code="code"] code column
 * @param {string} [options.date="date"] date column, parsed as a Date
 * @param {string} [options.encounter="enc"] encounter identifier column
 * @param {Map<string, {label?: string, block?: string}>} [options.meta] direct label metadata
 * @param {"count"|"percent"} [options.mode="count"] initial measure
 * @param {boolean} [options.toggle=true] show native measure buttons; false returns SVG only
 * @param {number} [options.width=1100] minimum 760; long histories expand for readable cells
 * @param {number} [options.rowH=46] row height, minimum 40
 * @param {string} [options.ink="#302e2a"] text color
 * @param {string} [options.faint="#77736b"] supporting text color
 * @param {string} [options.title="The rhythm of recorded care"] chart title
 * @returns {HTMLDivElement|SVGSVGElement} interactive figure or static SVG
 */
export function tufteMonthlyHeatmap(data, {
  code = "code", date = "date", encounter = "enc", meta = new Map(),
  mode = "count", toggle = true, ...options
} = {}) {
  if (!["count", "percent"].includes(mode)) throw new Error('mode must be "count" or "percent".');
  for (const key of ["width", "rowH"]) {
    if (options[key] != null && (!Number.isFinite(options[key]) || options[key] <= 0)) {
      throw new Error(`${key} must be a positive finite number.`);
    }
  }
  const rows = asRows(data).map(row => ({code: row[code], date: row[date], enc: row[encounter]}));
  if (rows.some(row => row.date == null || row.date === "")) throw new Error("Each event needs a valid date.");
  const draw = () => renderMonthlyHeatmap(rows, meta, {...options, mode});
  if (!toggle) return draw();
  const root = document.createElement("div");
  root.style.cssText = "max-width:100%;font-family:Georgia,'Times New Roman',serif;";
  const controls = document.createElement("div");
  controls.setAttribute("role", "group");
  controls.setAttribute("aria-label", "Monthly heatmap measure");
  controls.style.cssText = "display:flex;gap:16px;margin:0 24px 14px;";
  const viewport = document.createElement("div");
  viewport.style.overflowX = "auto";
  const buttons = [ ["count", "Encounter counts"], ["percent", "Percentage of encounters"] ].map(([value, label]) => {
    const button = document.createElement("button");
    button.type = "button";
    button.textContent = label;
    button.style.cssText = "font:13px Georgia,serif;background:transparent;border:0;border-bottom:1px solid transparent;padding:5px 0;cursor:pointer;";
    button.addEventListener("click", () => { mode = value; update(); });
    controls.append(button);
    return {button, value};
  });
  function update() {
    for (const {button, value} of buttons) {
      button.setAttribute("aria-pressed", String(mode === value));
      button.style.color = mode === value ? (options.ink || "#302e2a") : (options.faint || "#77736b");
      button.style.borderBottomColor = mode === value ? "currentColor" : "transparent";
    }
    const scrollLeft = viewport.scrollLeft;
    viewport.replaceChildren(draw());
    viewport.scrollLeft = scrollLeft;
  }
  update();
  root.append(controls, viewport);
  return root;
}

function renderMonthlyHeatmap(events, meta, {
  mode = "count",
  width = 1100, rowH = 46, ink = "#302e2a", faint = "#77736b",
  title = "The rhythm of recorded care"
} = {}) {
  width = Math.max(760, width);
  rowH = Math.max(40, rowH);
  const rows = events.map(e => ({...e, date: new Date(e.date)}));
  if (rows.some(e => !Number.isFinite(+e.date) || !e.code || e.enc == null)) {
    throw new Error("Each event needs a valid date, code, and encounter identifier.");
  }
  // Match the journey's code/encounter deduplication and earliest-date rule.
  const points = d3.flatRollup(rows,
    v => v.slice().sort((a, b) => +a.date - +b.date)[0],
    e => e.code, e => e.enc).map(d => d[2]);
  const first = d3.rollup(points, v => d3.min(v, e => e.date), e => e.code);
  const order = [...first.keys()].sort((a, b) => +first.get(a) - +first.get(b)
    || String(a).localeCompare(String(b)));
  const m = {t: 116, r: Math.min(350, width * .34), b: 94, l: 24};
  // Preserve legible monthly cells for long histories via horizontal scrolling.
  const extent = d3.extent(points, e => e.date);
  const months = points.length ? d3.utcMonths(d3.utcMonth.floor(extent[0]),
    d3.utcMonth.offset(d3.utcMonth.floor(extent[1]), 1)) : [];
  width = Math.max(width, m.l + m.r + months.length * 9);
  const height = m.t + Math.max(0, order.length - 1) * rowH + m.b;
  const svg = d3.create("svg").attr("xmlns", "http://www.w3.org/2000/svg")
    .attr("viewBox", `0 0 ${width} ${height}`).attr("width", width)
    .attr("role", "img").attr("aria-label", `${title}. ${mode === "count" ? "Monthly distinct encounter counts" : "Monthly encounter percentages"} by code; earliest codes at bottom. Dots indicate months without encounters in the supplied data.`)
    .style("max-width", "100%").style("min-width", `${width}px`).style("height", "auto")
    .style("font-family", "Georgia, 'Times New Roman', serif")
    .style("font-size", "12px").style("fill", ink);
  svg.append("title").text(title);
  svg.append("desc").text(`One cell per calendar month and code. Darker cells mean ${mode === "count" ? "more distinct encounters" : "a larger share of the month’s distinct encounters"}. Pale cells mean zero occurrences in a month with other encounters. Dots mean no encounters in the supplied data; they do not establish absence of care. All rows share ${mode === "count" ? "one linear count scale" : "a fixed 0–100% scale"}.`);
  svg.append("text").attr("x", m.l).attr("y", 30).attr("font-size", 25).text(title);
  svg.append("text").attr("x", m.l).attr("y", 54).attr("fill", faint)
    .text(`${mode === "count" ? "Distinct encounters per month" : "Share of each month’s distinct encounters"} · ordered by first appearance`);
  if (!points.length) {
    svg.append("text").attr("x", m.l).attr("y", m.t).attr("fill", faint).text("No recorded contacts");
    return svg.node();
  }
  const monthOf = e => +d3.utcMonth.floor(e.date);
  const coverage = d3.rollup(points, v => new Set(v.map(e => e.enc)).size, monthOf);
  const counts = d3.rollup(points, v => new Set(v.map(e => e.enc)).size, e => e.code, monthOf);
  const max = d3.max([...counts.values()].flatMap(v => [...v.values()]));
  const zero = "#eeeae3";
  const shade = n => mode === "percent" ? d3.interpolateRgb(zero, "#403d37")(n) : n === 0 ? zero : d3.interpolateRgb("#c1baae", "#403d37")(
    max === 1 ? 1 : (n - 1) / (max - 1));
  const endX = width - m.r;
  const x = d3.scaleBand(months.map(Number), [m.l, endX]).paddingInner(.15);
  const y = new Map(order.map((c, i) => [c, m.t + (order.length - 1 - i) * rowH]));
  const monthFmt = d3.utcFormat("%B %Y");
  const legendValues = mode === "percent" ? [0, .25, .5, 1] : [...new Set([0, 1, Math.ceil(max / 2), max])].sort((a, b) => a - b);
  const legend = svg.append("g").attr("transform", `translate(${m.l},77)`);
  legend.append("text").attr("dy", ".32em").attr("fill", faint).text(mode === "count" ? "Encounters" : "Share");
  legendValues.forEach((n, i) => {
    const g = legend.append("g").attr("transform", `translate(${80 + i * 58},0)`);
    g.append("rect").attr("y", -6).attr("width", 12).attr("height", 12).attr("fill", shade(n));
    g.append("text").attr("x", 18).attr("dy", ".32em").attr("fill", faint).text(mode === "percent" ? d3.format(".0%")(n).trim() : n);
  });
  const noDataX = 88 + legendValues.length * 58;
  legend.append("circle").attr("cx", noDataX).attr("r", 1).attr("fill", "#b5b0a5");
  legend.append("text").attr("x", noDataX + 10).attr("dy", ".32em").attr("fill", faint)
    .text("No encounters in supplied data");
  const cells = order.flatMap(code => months.map(date => ({code, date,
    count: counts.get(code)?.get(+date) || 0, observed: coverage.has(+date)})));
  svg.append("g").selectAll("g").data(cells).join("g")
    .attr("data-code", d => d.code).attr("data-month", d => d3.utcFormat("%Y-%m")(d.date))
    .attr("data-count", d => d.count).attr("data-total", d => coverage.get(+d.date) || 0)
    .attr("transform", d => `translate(${x(+d.date)},${y.get(d.code)})`)
    .each(function(d) {
      const g = d3.select(this);
      if (d.observed) g.append("rect").attr("y", -9).attr("width", x.bandwidth())
        .attr("height", 18).attr("fill", shade(mode === "percent" ? d.count / coverage.get(+d.date) : d.count));
      else g.append("circle").attr("cx", x.bandwidth() / 2).attr("r", 1).attr("fill", "#b5b0a5");
      // Full-cell hover target also makes the tiny no-encounter marks inspectable.
      g.append("rect").attr("y", -12).attr("width", x.step()).attr("height", 24)
        .attr("fill", "transparent").append("title")
        .text(`${d.code} · ${monthFmt(d.date)}\n${d.observed
          ? `${d.count} of ${coverage.get(+d.date)} distinct encounters (${d3.format(".1%")(d.count / coverage.get(+d.date))}) carrying this code`
          : "No encounters in supplied data; code status unknown"}`);
    });
  const labelX = endX + 26;
  const maxChars = Math.floor((width - labelX - 16) / 6.1);
  svg.append("g").selectAll("g").data(order).join("g")
    .attr("transform", c => `translate(${labelX},${y.get(c)})`)
    .each(function(c) {
      const g = d3.select(this), info = meta.get(c) || {};
      const description = String(info.label || info.block || "").replace(/^[A-Z]\d[^ ]*\s+/, "");
      g.append("text").attr("dy", ".3em").attr("font-size", 13).text(c);
      g.append("text").attr("y", 17).attr("fill", faint).attr("font-size", 11.5)
        .text(description.length > maxChars ? description.slice(0, maxChars - 1).trimEnd() + "…" : description);
      g.append("title").text(`${c} · ${description}`);
    });
  const ay = height - 60;
  const labelEvery = Math.max(1, Math.ceil(months.length * 65 / (endX - m.l) / 12));
  const years = [...new Set(months.map(d => d.getUTCFullYear()))];
  years.filter((_, i) => i % labelEvery === 0).forEach(year => {
    const firstMonth = months.find(d => d.getUTCFullYear() === year);
    const tx = x(+firstMonth);
    svg.append("line").attr("x1", tx).attr("x2", tx).attr("y1", ay - 10)
      .attr("y2", ay - 6).attr("stroke", "#b5b0a5").attr("stroke-width", .7);
    svg.append("text").attr("x", tx).attr("y", ay).attr("fill", faint).attr("font-size", 11).text(year);
  });
  svg.append("text").attr("x", m.l).attr("y", height - 22).attr("fill", faint).attr("font-size", 11)
    .text(`${d3.utcFormat("%b %Y")(months[0])}–${d3.utcFormat("%b %Y")(months.at(-1))} · One cell = one month · ${mode === "count" ? "Shared linear count scale" : "Fixed 0–100% scale"} across all codes`);
  return svg.node();
}
