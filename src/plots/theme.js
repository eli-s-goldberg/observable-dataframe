/**
 * theme.js — the design tokens, so every figure disagrees about nothing.
 *
 * The palette encodes meaning rather than decoration: teal means
 * operational, red means clinical, amber means financial, navy means
 * structure, and yellow means "look here, this is the number". Consistency
 * is the whole point — a reader who learns the colors once reads every
 * figure faster forever.
 */

export const colors = {
  // semantic
  operational: "#0D7377",
  clinical: "#A32020",
  financial: "#B7860B",
  navy: "#1C2B3A",
  highlight: "#FFB600",
  accent: "#D93954",

  // neutrals
  ink: "#1A1A1A",
  muted: "#666666",
  faint: "#888888",
  border: "#E0E0E0",
  panel: "#F2F2F2",
  white: "#FFFFFF",

  // timeline phases
  identify: "#1C2B3A",
  outreach: "#6A9BB5",
  observe: "#E4E8EC",
  lag: "#F2D6D6",

  // arm boxes
  control: "#EDEFF1",
  controlText: "#34404B",
  treatment: "rgba(13,115,119,0.10)",
  treatmentText: "#0A5457",
};

export const fonts = {
  sans: '"Helvetica Neue", Helvetica, Arial, sans-serif',
  serif: '"ITC Charter Com", Georgia, serif',
};

/**
 * The type scale, in px. Every plot draws text at one of these five sizes
 * — no more per-figure font-size archaeology. If a label doesn't fit its
 * size class, the label is too long, not the class too small.
 */
export const typography = {
  title: 13, // figure titles
  base: 11, // axis labels, default Plot text
  tick: 10, // tick labels, in-figure category text
  annotation: 9.5, // p-values, rates, marker labels, other marginalia
  micro: 8.5, // last resort; below this, use a tooltip
};

/** Shared card look: white, hairline border, quiet shadow. */
export const cardStyle = `background: white; border: 1px solid ${colors.border}; border-radius: 4px; padding: 1rem 1.05rem; box-shadow: 0 1px 4px rgba(0,0,0,0.07);`;

/** The small uppercase exhibit label that starts a figure. */
export const exhibitLabelStyle = `font: 700 10px ${fonts.sans}; color: ${colors.clinical}; text-transform: uppercase; letter-spacing: 0.08em;`;

/**
 * Default Plot options every figure starts from: the house font at base
 * size, and Tufte manners — no grid cage, short ticks, quantitative axes
 * held to a handful of honest labels. Individual plots opt *in* to more
 * ink, never inherit it.
 */
export const plotDefaults = {
  style: { fontFamily: fonts.sans, fontSize: `${typography.base}px` },
};

/**
 * Tufte scale options for a quantitative axis: a few ticks, short marks,
 * no gridlines. Spread into x/y scale definitions:
 *
 *   y: { label: "count", ...tufteAxis }
 */
export const tufteAxis = { ticks: 4, tickSize: 4, grid: false };
