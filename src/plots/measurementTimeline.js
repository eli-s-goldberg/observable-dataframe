/**
 * measurementTimeline.js — when will we actually know anything?
 *
 * A Gantt-style timeline for staggered intervention cohorts that marks,
 * explicitly, when each flavor of impact becomes measurable:
 *
 *   Operational (teal ✓)  — reach, engagement, visits: visible during outreach
 *   Clinical    (red +)   — endpoints after observation window + claims lag
 *   Financial   (gold $)  — PMPM / TME savings, last to arrive, first to be asked about
 *
 * This is the figure that stops the "it's been three months, where are the
 * savings" meeting before it gets scheduled. Phases per row (identify →
 * outreach → observe → claims lag) are config, not code, so it describes
 * your program rather than our example.
 */

import { html } from "htl";
import { colors, fonts, cardStyle, exhibitLabelStyle } from "./theme.js";

const PHASE_STYLES = {
  identify: { bg: colors.identify, color: "#fff" },
  outreach: { bg: colors.outreach, color: "#fff" },
  observe: { bg: colors.observe, color: "#3A4048" },
  lag: { bg: colors.lag, color: "#5A3030" },
};

const PHASE_NAMES = {
  identify: "Identification",
  outreach: "Outreach",
  observe: "Observation",
  lag: "Claims lag",
};

/**
 * @param {object} config
 * @param {string[]} config.months column labels, e.g. ["Nov-25", "Dec-25", ...]
 * @param {Array<object>} config.rows one per cohort row:
 *   {group, risk, segments: [{start, len, type: "identify"|"outreach"|"observe"|"lag", label}],
 *    opsMonths?: number[], clinicalMonth?: number, clinicalLabel?: string, note?: string}
 * @param {number} [config.financialStartMonth] month index where financial impact becomes measurable
 * @param {string} [config.financialLabel] caption for the financial flag
 * @param {string} [config.title="Experimentation & measurement timeline"]
 * @param {number} [config.monthWidth=52] px per month column
 * @returns {HTMLElement}
 */
export function measurementTimeline({
  months,
  rows,
  financialStartMonth = null,
  financialLabel = "Financial impact measurable (PMPM / TME)",
  title = "Experimentation & measurement timeline",
  monthWidth = 52,
} = {}) {
  const nMonths = months.length;
  const labelWidth = 92;
  const gridWidth = labelWidth + nMonths * monthWidth;

  const cellFor = (row, m) => {
    for (const s of row.segments) {
      if (m >= s.start && m < s.start + s.len) return s;
    }
    return null;
  };

  const opsMarker = html`<span title="Operational KPIs measurable"
    style="width: 13px; height: 13px; border-radius: 50%; background: ${colors.navy};
           border: 1.5px solid ${colors.operational}; color: ${colors.operational};
           font-size: 7px; font-weight: 900; display: inline-flex; align-items: center; justify-content: center;">✓</span>`;

  const flag = (bg, glyph, label) => html`<div style="display: flex; flex-direction: column; align-items: center; max-width: ${monthWidth * 2.6}px; z-index: 2;">
    <div style="width: 0; height: 0; border-left: 7px solid transparent; border-right: 7px solid transparent; border-bottom: 10px solid ${bg};"></div>
    <div style="background: ${bg}; color: white; width: 18px; height: 18px; border-radius: 2px; display: flex;
                align-items: center; justify-content: center; font-size: 11px; font-weight: 900; margin-top: -1px;">${glyph}</div>
    <div style="font-size: 8.5px; color: #444; line-height: 1.3; text-align: center; margin-top: 3px; font-style: italic;">${label}</div>
  </div>`;

  const legendChip = (bg, name) => html`<span style="display: inline-flex; align-items: center; gap: 5px; font-size: 10px; font-weight: 600; color: #444;">
    <span style="width: 13px; height: 13px; border-radius: 2px; background: ${bg}; display: inline-block;"></span>${name}</span>`;

  const impactLegend = html`<div style="display: flex; flex-wrap: wrap; gap: 16px; margin: 10px 0 14px; padding: 8px 12px;
       background: rgba(28,43,58,0.04); border: 1px solid rgba(28,43,58,0.12); border-radius: 4px; font-size: 10px;">
    <span style="font-size: 9.5px; font-weight: 800; color: ${colors.navy}; text-transform: uppercase; letter-spacing: 0.06em;">Impact measurable when →</span>
    <span>${opsMarker} <strong style="color: ${colors.operational};">Operational</strong> — reach, engagement, visits</span>
    <span><strong style="color: ${colors.clinical};">+ Clinical</strong> — endpoints after observation + lag</span>
    <span><strong style="color: ${colors.financial};">$ Financial</strong> — PMPM / TME savings</span>
  </div>`;

  return html`<figure style="margin: 0; font-family: ${fonts.sans};">
    <div style="${exhibitLabelStyle} margin-bottom: 6px;">${title}</div>
    <div style="${cardStyle} overflow-x: auto;">
      <div style="min-width: ${gridWidth + 24}px;">
        <div style="display: flex; flex-wrap: wrap; gap: 14px; align-items: center;">
          ${Object.entries(PHASE_NAMES).map(([t, name]) => legendChip(PHASE_STYLES[t].bg, name))}
        </div>
        ${impactLegend}

        <!-- month header -->
        <div style="display: grid; grid-template-columns: ${labelWidth}px repeat(${nMonths}, ${monthWidth}px);">
          <div></div>
          ${months.map(
            (m, i) => html`<div style="font-size: 9px; font-weight: 700; color: ${colors.muted}; text-align: center; padding-bottom: 4px;
              ${financialStartMonth != null && i >= financialStartMonth ? `background: rgba(183,134,11,0.08);` : ""}">${m}</div>`
          )}
        </div>

        <!-- cohort rows -->
        ${rows.map((row) => {
          return html`<div style="display: grid; grid-template-columns: ${labelWidth}px repeat(${nMonths}, ${monthWidth}px); align-items: stretch; margin-bottom: 3px;">
            <div style="font-size: 10px; font-weight: 700; color: ${colors.navy}; display: flex; flex-direction: column; justify-content: center; padding-right: 6px;">
              ${row.group}
              <span style="font-weight: 500; color: ${colors.muted};">${row.risk}</span>
            </div>
            ${months.map((_, m) => {
              const seg = cellFor(row, m);
              const style = seg ? PHASE_STYLES[seg.type] : null;
              const isFirst = seg && m === seg.start;
              const hasOps = row.opsMonths?.includes(m);
              const financial = financialStartMonth != null && m >= financialStartMonth;
              return html`<div style="position: relative; min-height: 34px; display: flex; align-items: center; justify-content: center;
                ${financial ? "background: rgba(183,134,11,0.08);" : ""}">
                ${seg
                  ? html`<div title=${seg.label} style="position: absolute; inset: 3px 1px; background: ${style.bg}; color: ${style.color};
                       border-radius: ${isFirst ? "3px 0 0 3px" : m === seg.start + seg.len - 1 ? "0 3px 3px 0" : "0"};
                       font-size: 8px; font-weight: 700; display: flex; align-items: center; justify-content: center;
                       overflow: hidden; white-space: nowrap;">${isFirst ? seg.label : ""}</div>`
                  : ""}
                ${hasOps ? html`<div style="position: absolute; top: -5px; left: 50%; transform: translateX(-50%); z-index: 2;">${opsMarker}</div>` : ""}
              </div>`;
            })}
          </div>
          ${row.clinicalMonth != null
            ? html`<div style="display: grid; grid-template-columns: ${labelWidth}px repeat(${nMonths}, ${monthWidth}px); margin-bottom: 6px;">
                <div></div>
                ${months.map((_, m) =>
                  m === row.clinicalMonth
                    ? html`<div style="grid-column: ${m + 2} / span 3;">${flag(colors.clinical, "+", row.clinicalLabel ?? "Clinical endpoint measurable")}</div>`
                    : ""
                )}
              </div>`
            : ""}`;
        })}

        <!-- financial flag row -->
        ${financialStartMonth != null
          ? html`<div style="display: grid; grid-template-columns: ${labelWidth}px repeat(${nMonths}, ${monthWidth}px); margin-top: 4px;">
              <div></div>
              ${months.map((_, m) =>
                m === financialStartMonth
                  ? html`<div style="grid-column: ${m + 2} / span 4;">${flag(colors.financial, "$", financialLabel)}</div>`
                  : ""
              )}
            </div>`
          : ""}

        ${rows.some((r) => r.note)
          ? html`<div style="margin-top: 10px; font-size: 10px; color: ${colors.faint}; font-style: italic; line-height: 1.5;">
              ${rows.filter((r) => r.note).map((r) => html`<div>· ${r.group} ${r.risk}: ${r.note}</div>`)}
            </div>`
          : ""}
      </div>
    </div>
  </figure>`;
}
