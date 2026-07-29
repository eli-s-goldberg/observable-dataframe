/**
 * splitPanel.js — the declarative section layout.
 *
 * Title + subtitle, a two-column grid with a configurable split, and
 * content slots that take whatever you hand them — a Plot figure, an
 * Inputs.table, prose, KPI cards. Pages compose panels; panels handle
 * structure; nobody hand-writes the same grid HTML a ninth time.
 *
 *   view(splitPanel({
 *     title: "Where is the cohort?",
 *     gridSplit: [35, 65],
 *     left: prose(html`We identified <span class="pull-quote-highlight">8 categories</span>...`),
 *     right: myPlot,
 *   }))
 */

import { html } from "htl";
import { colors, fonts } from "../plots/theme.js";

/**
 * @param {object} config
 * @param {string} [config.title]
 * @param {string} [config.subtitle]
 * @param {[number, number]} [config.gridSplit=[50, 50]] left/right fr units
 * @param {Node|Node[]|string} config.left left slot content
 * @param {Node|Node[]|string} config.right right slot content
 * @param {"light"|"gray"} [config.theme="light"] gray gets the full-bleed panel background
 * @param {boolean} [config.cards=true] wrap slots in white cards
 * @returns {HTMLElement}
 */
export function splitPanel({
  title,
  subtitle,
  gridSplit = [50, 50],
  left,
  right,
  theme = "light",
  cards = true,
} = {}) {
  const wrap = (content) =>
    cards
      ? html`<div style="background: white; border: 1px solid ${colors.border}; border-radius: 8px; padding: 1.1rem 1.2rem; box-shadow: 0 1px 3px rgba(0,0,0,0.06);">${content}</div>`
      : html`<div>${content}</div>`;

  const section = html`<section style="position: relative; padding: 1.4rem 0; font-family: ${fonts.sans};">
    ${theme === "gray"
      ? html`<div style="position: absolute; top: 0; left: 50%; width: 100vw; height: 100%; background: ${colors.panel}; transform: translateX(-50%); z-index: -1;"></div>`
      : ""}
    ${title
      ? html`<h2 style="font-family: ${fonts.serif}; font-size: 24px; font-weight: 600; letter-spacing: -0.02em; margin: 0 0 2px 0;">${title}</h2>`
      : ""}
    ${subtitle
      ? html`<p style="font-size: 13px; color: ${colors.muted}; margin: 0 0 1rem 0;">${subtitle}</p>`
      : ""}
    <div style="display: grid; grid-template-columns: ${gridSplit[0]}fr ${gridSplit[1]}fr; gap: 18px; align-items: start;">
      ${wrap(left)} ${wrap(right)}
    </div>
  </section>`;
  return section;
}

/**
 * Vertical variant: cards row on top, wide content below. The executive
 * dashboard shape.
 */
export function stackPanel({ title, subtitle, top, bottom, theme = "light" } = {}) {
  return splitPanelBase({ title, subtitle, theme }, html`<div style="display: flex; flex-direction: column; gap: 18px;">
    <div>${top}</div>
    <div>${bottom}</div>
  </div>`);
}

function splitPanelBase({ title, subtitle, theme }, body) {
  return html`<section style="position: relative; padding: 1.4rem 0; font-family: ${fonts.sans};">
    ${theme === "gray"
      ? html`<div style="position: absolute; top: 0; left: 50%; width: 100vw; height: 100%; background: ${colors.panel}; transform: translateX(-50%); z-index: -1;"></div>`
      : ""}
    ${title
      ? html`<h2 style="font-family: ${fonts.serif}; font-size: 24px; font-weight: 600; letter-spacing: -0.02em; margin: 0 0 2px 0;">${title}</h2>`
      : ""}
    ${subtitle
      ? html`<p style="font-size: 13px; color: ${colors.muted}; margin: 0 0 1rem 0;">${subtitle}</p>`
      : ""}
    ${body}
  </section>`;
}

/**
 * Pull-quote prose block with a yellow-highlighted phrase — the signature
 * move of the house style. Compose with splitPanel's left slot.
 *
 * @param {Node|string} content htl content; use class "pull-quote-highlight" on the hot phrase
 */
export function prose(content) {
  return html`<div style="font: 400 17px/1.6 ${fonts.sans};">${content}</div>`;
}

/**
 * KPI card. flavor: "plain" | "keyTakeaway" (yellow) | "valueAtStake" (navy).
 *
 * @param {{label: string, value: string, note?: string, flavor?: string}} options
 */
export function kpiCard({ label, value, note, flavor = "plain" } = {}) {
  const styles = {
    plain: `background: white; color: #111; border: 1px solid ${colors.border};`,
    keyTakeaway: `background: ${colors.highlight}; color: #111827;`,
    valueAtStake: `background: ${colors.navy}; color: white;`,
  };
  return html`<div style="${styles[flavor] ?? styles.plain} border-radius: 12px; padding: 1.2rem 1.3rem; box-shadow: 0 2px 5px rgba(0,0,0,0.12); font-family: ${fonts.sans};">
    <div style="font-weight: 700; font-size: 12px; text-transform: uppercase; letter-spacing: 0.03em;">${label}</div>
    <div style="width: 34px; border-top: 2px solid currentColor; margin: 0.45rem 0;"></div>
    <div style="font-weight: 700; font-size: 30px;">${value}</div>
    ${note ? html`<div style="font-size: 13px; margin-top: 0.6rem; opacity: 0.85;">${note}</div>` : ""}
  </div>`;
}

/** A row of KPI cards, evenly spread. Feed it kpiCard() outputs. */
export function cardRow(...cards) {
  return html`<div style="display: grid; grid-template-columns: repeat(${cards.flat().length}, 1fr); gap: 14px;">${cards.flat()}</div>`;
}
