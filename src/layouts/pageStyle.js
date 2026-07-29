/**
 * pageStyle.js — the site-wide look, injected once per page.
 *
 * Three layers, separable because taste is contextual:
 *
 *   1. `pageStyle` — typography, cards, sections, tables, prose polish.
 *   2. `siteChromeStyle` — the Observable Framework chrome skin (dark
 *      sidebar, accent hovers, dark search box).
 *   3. `numberedHeadingsStyle` — CSS-counter enumeration (1., 1.1, 1.1.1)
 *      for report-style pages where reviewers cite sections by number.
 *
 * Import once at the top of a page:
 *
 *   import { injectPageStyle } from "observable-dataframe/layouts";
 *   injectPageStyle();                              // page + chrome
 *   injectPageStyle({ numberedHeadings: true });    // report mode
 *   injectPageStyle({ chrome: false });             // keep Framework's default nav
 */

import { colors, fonts } from "../plots/theme.js";

/** Core page stylesheet: typography, cards, sections, prose, tables. */
export const pageStyle = `
  /* ---- typography ------------------------------------------------------ */
  .page-title {
    font-family: ${fonts.serif};
    font-weight: 700;
    font-size: 40px;
    line-height: 1.2;
    letter-spacing: -0.03em;
  }
  .page-subtitle {
    font-family: ${fonts.serif};
    font-weight: 600;
    font-size: 26px;
    letter-spacing: -0.03em;
    margin-bottom: 10px;
  }
  .page-title-header {
    font: 16px/1.5 ${fonts.sans};
    margin: 0 0 2.5rem 0;
    max-width: 100%;
    letter-spacing: -0.02em;
  }
  .text-header {
    font: 600 12px/1.4 ${fonts.sans};
    color: #000;
    letter-spacing: 0.02em;
    text-transform: uppercase;
    margin-bottom: 0.25rem;
  }
  .pull-quote {
    font: 400 18px/1.55 ${fonts.sans};
    margin-bottom: 10px;
  }
  .pull-quote-highlight {
    background-color: ${colors.highlight};
    color: black;
    font-weight: 500;
    padding: 0 2px;
  }

  /* ---- hero (landing pages earn one gradient; use it wisely) -------------- */
  .hero {
    display: flex;
    flex-direction: column;
    align-items: center;
    font-family: ${fonts.sans};
    margin: 3rem 0 5rem;
    text-wrap: balance;
    text-align: center;
  }
  .hero h1 {
    margin: 1rem 0;
    padding: 1rem 0;
    max-width: none;
    font-size: 14vw;
    font-weight: 900;
    line-height: 1;
    background: linear-gradient(30deg, var(--theme-foreground-focus, ${colors.accent}), currentColor);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
  }
  .hero h2 {
    margin: 0;
    max-width: 34em;
    font-size: 20px;
    font-style: initial;
    font-weight: 500;
    line-height: 1.5;
    color: var(--theme-foreground-muted, ${colors.muted});
  }
  @media (min-width: 640px) {
    .hero h1 { font-size: 90px; }
  }

  /* ---- prose polish ---------------------------------------------------------- */
  .katex { font-size: 1em; }
  h1, h2, h3, h4, h5, h6 {
    white-space: normal;
    overflow-wrap: break-word;
    max-width: 100%;
  }
  pre code {
    background-color: #f5f5f5;
    padding: 10px;
    display: block;
    overflow-x: auto;
    border-radius: 5px;
  }
  code {
    background-color: #f5f5f5;
    padding: 2px 4px;
    border-radius: 3px;
    font-size: 0.85em;
  }
  img {
    max-width: 90%;
    height: auto;
    display: block;
    margin: 10px 0;
  }
  .quote-box {
    border-left: 4px solid ${colors.operational};
    background-color: #f9f9f9;
    padding: 10px 20px;
    margin: 20px 0;
    font-family: Georgia, serif;
    font-style: italic;
    color: #333;
    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
  }
  .quote-box .quote-author {
    text-align: right;
    margin-top: 10px;
    color: #555;
  }
  .bibliography {
    margin-top: 20px;
    font: 0.9em ${fonts.sans};
    color: #333;
  }
  .bibliography p { margin: 5px 0; }

  /* ---- cards ------------------------------------------------------------- */
  .card {
    background: white;
    border-radius: 0.5rem;
    padding: 1.5rem;
    box-shadow: 0 1px 2px rgba(0,0,0,0.05);
    display: flex;
    flex-direction: column;
    justify-content: center;
    text-align: left;
  }
  .card-yellow { background: ${colors.highlight}; color: #111827; }
  .card-gray { background: ${colors.panel}; }
  .card-value {
    font-family: ${fonts.sans};
    font-weight: 700;
    font-size: 32px;
    margin: 0;
  }
  .card-label {
    font-family: ${fonts.sans};
    font-weight: 700;
    font-size: 13px;
    text-transform: uppercase;
    margin: 0 0 0.25rem 0;
  }
  .card-note {
    font-family: ${fonts.sans};
    font-weight: 400;
    font-size: 14px;
    color: #333;
    margin: 0.75rem 0 0 0;
  }

  /* ---- full-bleed gray sections ---------------------------------------------- */
  .full-width-section {
    position: relative;
    background: transparent;
    padding: 1.5rem 0;
  }
  .full-width-section::before {
    content: "";
    position: absolute;
    top: 0;
    left: 50%;
    width: 100vw;
    height: 100%;
    background: ${colors.panel};
    transform: translateX(-50%);
    z-index: -1;
  }

  /* ---- grids -------------------------------------------------------------------- */
  .layout-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 16px;
    align-items: start;
    width: 100%;
  }
  .layout-grid .card { width: 100%; box-sizing: border-box; }
  .layout-grid figcaption { grid-column: 1 / -1; width: 100%; box-sizing: border-box; }
  .divider {
    width: 40px;
    border: 1px solid black;
    margin: 0.5rem 0;
  }
  .horizontal-line {
    border-top: 0.5px solid #d3d3d3;
    width: 100%;
    margin: 10px 0;
  }

  /* ---- tables --------------------------------------------------------------------- */
  .odf-table table {
    background: white;
    width: 100%;
    border-collapse: collapse;
    font: 13px/1.35 ${fonts.sans};
  }
  .odf-table th {
    background: ${colors.ink};
    color: white;
    text-align: left;
  }

  /* ---- figures ----------------------------------------------------------------------- */
  figure { margin: 0 0 1.25rem 0; }
  figcaption {
    font: italic 11px/1.5 ${fonts.sans};
    color: ${colors.faint};
    margin-top: 6px;
  }
`;

/**
 * Observable Framework chrome: dark sidebar, accent hover states, dark
 * search input. This is site skin, not page content — inject it when you
 * want the whole site to feel like one publication instead of a default
 * Framework install (which is fine, but everyone can tell).
 */
export const siteChromeStyle = `
  #observablehq-sidebar {
    --observablehq-sidebar-padding-left: calc(max(0rem, (100vw - var(--observablehq-max-width)) / 2));
    background: black;
    color: white;
    font: 13px var(--sans-serif);
    font-weight: 500;
  }
  #observablehq-sidebar > ol:first-child::before { background: #2b2b2b; }
  #observablehq-sidebar summary {
    color: white;
    background: black;
    cursor: default;
  }
  #observablehq-sidebar details summary { background: black; font: 13px var(--sans-serif); }
  #observablehq-sidebar details summary:hover { background: ${colors.accent}; }
  #observablehq-sidebar > ol,
  #observablehq-sidebar > details,
  #observablehq-sidebar > section { border-bottom-color: #404040; }

  .observablehq-link a:hover { background: ${colors.highlight}; color: black; }
  .observablehq-link-active a { color: #ffffff; background: black; }
  .observablehq-link-active::before { background: #e64646; }
  .observablehq-link:not(.observablehq-link-active) a[href]:not(:hover) { color: #cccccc; }

  #observablehq-search input {
    background-color: #3d3d3d;
    color: #ffffff;
  }
  #observablehq-search input::placeholder { color: #808080; }
`;

/**
 * Automatic heading numbers via CSS counters: h1 → "1.", h2 → "1.1.", and
 * so on. Opt-in, for report-style pages where "see section 2.3" needs to
 * mean something. Do not enable on marketing pages; nobody cites those.
 */
export const numberedHeadingsStyle = `
  body { counter-reset: h1counter; }
  h1 { counter-reset: h2counter; counter-increment: h1counter; }
  h2 { counter-reset: h3counter; counter-increment: h2counter; }
  h3 { counter-reset: h4counter; counter-increment: h3counter; }
  h4 { counter-increment: h4counter; }
  h1::before { content: counter(h1counter) ". "; }
  h2::before { content: counter(h1counter) "." counter(h2counter) ". "; }
  h3::before { content: counter(h1counter) "." counter(h2counter) "." counter(h3counter) ". "; }
  h4::before { content: counter(h1counter) "." counter(h2counter) "." counter(h3counter) "." counter(h4counter) ". "; }
`;

/**
 * Append the stylesheet layers to document.head, once. Safe to call from
 * every page and every cell; the first call's options win and repeats are
 * no-ops, not barnacles.
 *
 * @param {object|string} [options] options object, or a raw CSS string for
 *   backwards compatibility with the old `injectPageStyle(extra)` call
 * @param {boolean} [options.chrome=true] include the dark sidebar/search skin
 * @param {boolean} [options.numberedHeadings=false] auto-number h1–h4
 * @param {string} [options.extra=""] additional CSS appended last, so it wins
 */
export function injectPageStyle(options = {}) {
  if (typeof document === "undefined") return; // SSR/tests: nothing to style, nobody to see it
  const opts = typeof options === "string" ? { extra: options } : options;
  const { chrome = true, numberedHeadings = false, extra = "" } = opts;

  const id = "odf-page-style";
  if (document.getElementById(id)) return;
  const el = document.createElement("style");
  el.id = id;
  el.textContent =
    pageStyle +
    (chrome ? siteChromeStyle : "") +
    (numberedHeadings ? numberedHeadingsStyle : "") +
    extra;
  document.head.appendChild(el);
}

/**
 * Runtime heading numberer for dynamically generated markdown, where CSS
 * counters can't reach. Returns a function you call per heading, in
 * document order, and it keeps the "1.2.3" bookkeeping for you.
 *
 *   const number = createNumberHeadings();
 *   number("Introduction", 1)  // → "1 Introduction"
 *   number("Methods", 1)       // → "2 Methods"
 *   number("Cohorts", 2)       // → "2.1 Cohorts"
 *
 * @param {number} [maxLevel=5] deepest heading level to track
 * @returns {(text: string, level: number) => string}
 */
export function createNumberHeadings(maxLevel = 5) {
  const counters = new Array(maxLevel + 1).fill(0);
  return function numberHeadings(text, level) {
    if (level < 1 || level > maxLevel) return text;
    counters[level]++;
    for (let l = level + 1; l <= maxLevel; l++) counters[l] = 0;
    return `${counters.slice(1, level + 1).join(".")} ${text}`;
  };
}
