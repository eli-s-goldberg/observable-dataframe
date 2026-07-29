/**
 * tabPanel.js — tabs, self-contained and reusable.
 *
 * Extracted from the pattern every dashboard page kept re-implementing
 * with querySelector and hope. Hand it labeled slots; it handles the
 * state, the styling, and the part where clicking a tab actually works.
 *
 *   view(tabPanel({
 *     tabs: [
 *       { label: "Historical", content: historicalCards },
 *       { label: "Predictions", content: predictionCharts },
 *     ],
 *   }))
 */

import { html } from "htl";
import { colors, fonts } from "../plots/theme.js";

/**
 * @param {object} config
 * @param {Array<{label: string, content: Node|string}>} config.tabs
 * @param {number} [config.active=0] initially selected tab index
 * @param {string} [config.accent] active-tab underline color (theme accent by default)
 * @returns {HTMLElement}
 */
export function tabPanel({ tabs, active = 0, accent = colors.accent } = {}) {
  if (!tabs?.length) {
    throw new Error(`tabPanel with no tabs is a div. You want a div.`);
  }

  const buttons = tabs.map(
    (t, i) => html`<button
      style="background: none; border: none; padding: 8px 2px; margin-right: 18px; cursor: pointer;
             font: 600 13px ${fonts.sans}; text-transform: uppercase; letter-spacing: 0.03em;
             color: ${colors.muted}; border-bottom: 2px solid transparent;"
    >${t.label}</button>`
  );
  const panes = tabs.map(
    (t, i) => html`<div style="display: ${i === active ? "block" : "none"}; padding-top: 12px;">${t.content}</div>`
  );

  const select = (index) => {
    buttons.forEach((b, i) => {
      b.style.color = i === index ? colors.ink : colors.muted;
      b.style.borderBottomColor = i === index ? accent : "transparent";
      b.style.fontWeight = i === index ? "700" : "600";
    });
    panes.forEach((p, i) => (p.style.display = i === index ? "block" : "none"));
  };
  buttons.forEach((b, i) => b.addEventListener("click", () => select(i)));
  select(active);

  return html`<div style="font-family: ${fonts.sans};">
    <div style="border-bottom: 1px solid ${colors.border};">${buttons}</div>
    ${panes}
  </div>`;
}
