/**
 * options.js — the common lexicon every plot speaks.
 *
 * One vocabulary, learned once, honored everywhere. The Plot-based
 * primitives inherit Observable Plot's own option names (that's the
 * "observable-ish" part — we pass your options straight into Plot.plot,
 * so anything Plot understands, our figures understand). The D3-based
 * primitives implement the same names by hand so you can't tell from the
 * call site which rendering engine you got.
 *
 * The lexicon:
 *
 *   width, height          — pixels, like always
 *   margin / marginTop...  — Plot's margin names; D3 plots take {top,right,bottom,left}
 *   title, subtitle,       — figure-level text; Plot renders these natively,
 *   caption                  D3 plots draw equivalents
 *   label / labels         — axis labels ({x, y} where two axes exist)
 *   labelPosition          — "top" | "bottom" | "none", for value/marker labels
 *   labelDigits            — decimals on numeric labels
 *   markers                — statistical marks where distributions appear
 *   break / xScaleRange... — axis-break controls (tufteForestPlot, and counting)
 *   sort                   — "ascending" | "descending" | null
 *   tip                    — tooltips. false | true | (d) => string | Plot tip options.
 *                            true gives a sensible default; a function receives the
 *                            datum and returns the tooltip text; an object passes
 *                            through to Plot's tip channel for full control.
 *   ...rest                — spread into Plot.plot() (or applied where D3 allows),
 *                            so scale overrides like x: {domain: [...]} just work
 */

import { colors, fonts, typography } from "./theme.js";

/**
 * Resolve the universal `tip` option into Plot mark options.
 *
 * @param {boolean|Function|object} tip the lexicon's tip value
 * @param {(d: object) => string} [defaultTitle] what `tip: true` shows
 * @returns {object} spreadable mark options ({} when tips are off)
 */
export function resolveTip(tip, defaultTitle) {
  if (!tip) return {};
  if (tip === true) return { tip: true, ...(defaultTitle ? { title: defaultTitle } : {}) };
  if (typeof tip === "function") return { tip: true, title: tip };
  if (typeof tip === "object") return { tip }; // full Plot tip options: the power-user door
  return {};
}

/**
 * A shared floating tooltip for the D3-rendered primitives — one styled
 * div, positioned at the pointer, filled with whatever HTML you hand it.
 * Every D3 plot uses this instead of inventing its own, which is how the
 * tooltips all ended up looking the same on purpose.
 *
 * @returns {{show: (event: MouseEvent, html: string) => void,
 *            move: (event: MouseEvent) => void,
 *            hide: () => void,
 *            remove: () => void}}
 */
export function createTooltip() {
  if (typeof document === "undefined") {
    const noop = () => {};
    return { show: noop, move: noop, hide: noop, remove: noop };
  }
  const el = document.createElement("div");
  el.style.cssText = `
    position: absolute; z-index: 1000; pointer-events: none; opacity: 0;
    background: rgba(255, 255, 255, 0.98); border: 1px solid ${colors.border};
    border-radius: 6px; box-shadow: 0 4px 12px rgba(0,0,0,0.12);
    padding: 8px 10px; max-width: 280px;
    font: ${typography.tick}px/${1.45} ${fonts.sans}; color: ${colors.ink};
    transition: opacity 120ms;
  `;
  document.body.appendChild(el);

  const place = (event) => {
    el.style.left = `${event.pageX + 12}px`;
    el.style.top = `${event.pageY - 10}px`;
  };

  return {
    show(event, html) {
      el.innerHTML = html;
      place(event);
      el.style.opacity = "1";
    },
    move: place,
    hide() {
      el.style.opacity = "0";
    },
    remove() {
      el.remove();
    },
  };
}

/**
 * Default tooltip HTML: a label/value grid from an object. Hand it the
 * datum fields worth reading and it does the typesetting.
 *
 * @param {Record<string, *>} fields {label: value}
 * @returns {string}
 */
export function tipHTML(fields) {
  return Object.entries(fields)
    .map(
      ([label, value]) =>
        `<div style="color: ${colors.muted}; font-size: ${typography.micro}px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.04em;">${label}</div>` +
        `<div style="margin-bottom: 4px;">${value instanceof Date ? value.toLocaleDateString() : value}</div>`
    )
    .join("");
}
