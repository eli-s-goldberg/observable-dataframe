/**
 * tableFormatters.js — cell-level polish for Inputs.table.
 *
 * Higher-order formatters you drop into `Inputs.table(df.toRows(), { format: {...} })`.
 * The difference between "a table" and "a table someone reads" is about
 * four of these functions.
 *
 * Text formatters take an options object and wrap via CSS (max-width in
 * em, word-break) rather than regex-inserted line breaks — the browser is
 * better at typesetting than a regular expression, a low bar it clears.
 *
 *   Inputs.table(df.toRows(), {
 *     format: {
 *       outreach: sparkbar(d3.max(rows, d => d.outreach)),
 *       status: formatStatus(),
 *       version: formatTwoLevel(),
 *       description: withRowHeight("45px", "top")(formatWrappedText({ charLimit: 40 })),
 *       pros: formatProsList(),
 *       cons: formatConsList(),
 *     },
 *   })
 */

import { html } from "htl";
import { colors, typography } from "../plots/theme.js";

/**
 * Inline bar chart in a cell, scaled against `max`. The cheapest chart
 * that still counts as one.
 * @param {number} max the value that fills 100% of the cell
 * @param {string} [color="lightblue"]
 */
export function sparkbar(max, color = "lightblue") {
  return (x) => html`<div
    style="background: ${color}; color: black; width: ${Math.max(0, (100 * x) / max)}%;
           float: left; padding: 0 3px; box-sizing: border-box; overflow: visible;
           display: flex; justify-content: start; white-space: nowrap;"
  >${x.toLocaleString("en-US")}</div>`;
}

/**
 * Status pill: green for good states, red for bad ones. Configure which is
 * which; by default anything in `bad` gets the red treatment.
 * @param {{bad?: string[]}} [options]
 */
export function formatStatus({ bad = ["Deprecated", "Failed", "Underpowered"] } = {}) {
  const badSet = new Set(bad);
  return (status) => html`<div
    style="display: inline-block; border-radius: 4px; padding: 1px 8px; font-size: 0.9em;
           background: ${badSet.has(status) ? "#fee2e2" : "#dcfce7"};
           color: ${badSet.has(status) ? "#991b1b" : "#166534"};"
  >${status}</div>`;
}

/**
 * Two-line cell: bold header over muted description. Feed it values shaped
 * `{header, description}`; anything else falls through as plain text.
 */
export function formatTwoLevel() {
  return (value) => {
    if (value && value.header !== undefined) {
      return html`<div style="margin: 0; padding: 0;">
        <div style="font-weight: bold; white-space: normal; margin-bottom: 4px;">${value.header}</div>
        <div style="font-size: 0.9em; color: ${colors.muted}; white-space: normal;">${value.description}</div>
      </div>`;
    }
    return String(value);
  };
}

/**
 * Wrapped text cell with full typographic control. `charLimit` sets an
 * em-based max-width so the browser wraps naturally at roughly that many
 * characters; 0 means "use the whole cell".
 *
 * @param {object} [options]
 * @param {number} [options.charLimit=50]
 * @param {string} [options.fontSize] px string; defaults to the theme tick size
 * @param {string} [options.color="black"] / fontWeight / lineHeight / textAlign / verticalAlign
 * @param {string} [options.padding="0"] / margin / minHeight / backgroundColor / borderRadius / border
 */
export function formatWrappedText(options = {}) {
  const {
    charLimit = 50,
    fontSize = `${typography.tick}px`,
    color = "black",
    fontWeight = "normal",
    textAlign = "left",
    verticalAlign = "top",
    padding = "0",
    margin = "0",
    lineHeight = "1.4",
    minHeight = "auto",
    backgroundColor = "transparent",
    borderRadius = "0",
    border = "none",
  } = options;

  const alignItems = { top: "flex-start", center: "center", bottom: "flex-end" }[verticalAlign] ?? verticalAlign;
  const justifyContent = { left: "flex-start", center: "center", right: "flex-end" }[textAlign] ?? textAlign;

  return (x) => html`<div
    style="color: ${color}; font-size: ${fontSize}; font-weight: ${fontWeight}; line-height: ${lineHeight};
           min-height: ${minHeight}; padding: ${padding}; margin: ${margin};
           background-color: ${backgroundColor}; border-radius: ${borderRadius}; border: ${border};
           box-sizing: border-box; white-space: normal; word-break: break-word; width: 100%;
           max-width: ${charLimit > 0 ? `${charLimit * 0.6}em` : "100%"};
           display: flex; align-items: ${alignItems}; justify-content: ${justifyContent};"
  >${String(x)}</div>`;
}

/**
 * Bold text cell; splits on `separator` into a bulleted list when the
 * value contains one, because someone upstream always packs a list into a
 * string, and now the table forgives them.
 *
 * @param {object} [options] same knobs as formatWrappedText, plus
 * @param {string} [options.separator="|"]
 * @param {string} [options.itemSpacing="2px"] / bulletSpacing
 */
export function formatTextBold(options = {}) {
  const { separator = "|", fontWeight = "800", ...rest } = options;
  return (x) => {
    const str = String(x);
    if (str.includes(separator)) {
      return formatBulletedList({ ...rest, fontWeight, separator })(str);
    }
    return formatWrappedText({ ...rest, fontWeight })(str);
  };
}

/** Bold header cell text, wrapped. For Inputs.table's `header` option. */
export function formatHeaderTextBold(headerText, charLimit = 50) {
  return html`<div
    style="color: black; font-weight: bold; font-size: ${typography.tick}px; box-sizing: border-box;
           white-space: normal; word-break: break-word; max-width: ${charLimit * 0.6}em;
           display: flex; align-items: flex-start; justify-content: flex-start;"
  >${String(headerText)}</div>`;
}

/**
 * Bulleted list in a cell, from an array or a delimited string. With
 * `autoDetect` on it recognizes values that already carry ✓/× markers or
 * bullets and preserves them instead of double-bulleting. This formatter
 * exists because packed-string list columns are eternal; we've made peace.
 *
 * @param {object} [options]
 * @param {string} [options.bulletStyle="•"] any glyph: "◦", "▪", "→", your call
 * @param {string} [options.bulletColor="inherit"] / bulletSize
 * @param {string} [options.fontSize] / color / fontWeight / lineHeight
 * @param {string} [options.itemSpacing="4px"] / bulletSpacing / padding / margin
 * @param {number} [options.charLimit=0] em-based wrap width per item; 0 = cell width
 * @param {string} [options.separator="|"] delimiter for packed strings
 * @param {boolean} [options.autoDetect=true] respect existing ✓ × • - * markers
 */
export function formatBulletedList(options = {}) {
  const {
    bulletStyle = "•",
    bulletColor = "inherit",
    bulletSize = "1em",
    fontSize = `${typography.tick}px`,
    color = "black",
    fontWeight = "normal",
    lineHeight = "1.4",
    itemSpacing = "4px",
    bulletSpacing = "8px",
    padding = "4px",
    margin = "0",
    charLimit = 0,
    backgroundColor = "transparent",
    borderRadius = "0",
    border = "none",
    minHeight = "auto",
    separator = "|",
    autoDetect = true,
  } = options;

  const baseStyle = `color: ${color}; font-size: ${fontSize}; font-weight: ${fontWeight};
    line-height: ${lineHeight}; padding: ${padding}; margin: ${margin};
    word-break: break-word; white-space: normal; box-sizing: border-box;`;

  return (data) => {
    let items;
    let raw = "";
    if (Array.isArray(data)) {
      items = data.map((item) => String(item).trim()).filter(Boolean);
    } else {
      raw = String(data ?? "").trim();
      if (raw.length < 2) return html`<div style="${baseStyle}">${raw}</div>`;
      if (autoDetect && /[✓×√✗]/.test(raw)) {
        items = raw.split(separator).map((s) => s.trim()).filter(Boolean);
      } else if (autoDetect && /[•]/.test(raw)) {
        items = raw.split(/•\s*/).map((s) => s.trim()).filter(Boolean);
      } else {
        items = raw.split(separator).map((s) => s.trim()).filter(Boolean);
      }
    }

    if (items.length <= 1) {
      return html`<div style="${baseStyle} width: 100%;">${items[0] ?? raw}</div>`;
    }

    const itemMaxWidth = charLimit > 0 ? `${charLimit * 0.6}em` : "100%";
    return html`<div
      style="${baseStyle} min-height: ${minHeight}; background-color: ${backgroundColor};
             border-radius: ${borderRadius}; border: ${border}; width: 100%;
             display: flex; flex-direction: column; align-items: flex-start;"
    >
      ${items.map((item, index) => {
        // Values arriving with their own ✓/× keep them; we don't bullet a checkmark.
        const hasOwnMarker = /^[✓×√✗]/.test(item);
        return html`<div style="display: flex; align-items: flex-start; width: 100%;
            margin-bottom: ${index < items.length - 1 ? itemSpacing : "0"};">
          ${hasOwnMarker
            ? ""
            : html`<span style="color: ${bulletColor}; font-size: ${bulletSize}; margin-right: ${bulletSpacing};
                flex-shrink: 0; line-height: ${lineHeight};">${bulletStyle}</span>`}
          <span style="word-break: break-word; white-space: normal; flex: 1; min-width: 0;
              line-height: ${lineHeight}; max-width: ${itemMaxWidth};
              ${hasOwnMarker ? `margin-left: 0;` : ""}">${item}</span>
        </div>`;
      })}
    </div>`;
  };
}

/** Pros column: green checks. The optimistic half of every comparison table. */
export function formatProsList(options = {}) {
  return formatBulletedList({ bulletStyle: "✓", bulletColor: "#16a34a", itemSpacing: "4px", bulletSpacing: "6px", ...options });
}

/** Cons column: red ×s. The half people actually read. */
export function formatConsList(options = {}) {
  return formatBulletedList({ bulletStyle: "×", bulletColor: "#dc2626", itemSpacing: "4px", bulletSpacing: "6px", ...options });
}

/** Neutral list: muted bullets, slightly larger text, no editorializing. */
export function formatNeutralList(options = {}) {
  return formatBulletedList({
    bulletStyle: "•",
    bulletColor: colors.muted,
    fontSize: `${typography.base + 2}px`,
    itemSpacing: "2px",
    bulletSpacing: "6px",
    ...options,
  });
}

/**
 * Wrap any formatter with a minimum row height and vertical alignment.
 * Composable: withRowHeight("45px", "top")(formatWrappedText()).
 * @param {string} [height="40px"]
 * @param {"top"|"center"|"bottom"} [verticalAlign="center"]
 */
export function withRowHeight(height = "40px", verticalAlign = "center") {
  const alignmentMap = { top: "flex-start", center: "center", bottom: "flex-end" };
  return (formatter) => (x) => {
    const content = (formatter ?? ((v) => html`<div>${String(v)}</div>`))(x);
    return html`<div style="min-height: ${height}; display: flex; align-items: ${alignmentMap[verticalAlign] ?? "center"}; width: 100%;">${content}</div>`;
  };
}

/** Currency cell. Negative values go clinical red, as is fiscally traditional. */
export function formatCurrency({ currency = "USD", digits = 0 } = {}) {
  return (x) => html`<div style="text-align: right; font-variant-numeric: tabular-nums; color: ${x < 0 ? colors.clinical : "black"};">
    ${Number(x).toLocaleString("en-US", { style: "currency", currency, maximumFractionDigits: digits })}</div>`;
}

/** Percent cell with fixed digits, right-aligned like numbers should be. */
export function formatPercent(digits = 1) {
  return (x) => html`<div style="text-align: right; font-variant-numeric: tabular-nums;">${(x * 100).toFixed(digits)}%</div>`;
}
