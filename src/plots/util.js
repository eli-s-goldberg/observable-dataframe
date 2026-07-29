/**
 * util.js — small shared conveniences for the plot primitives.
 */

/**
 * Accept a DataFrame or an array of row objects and return rows. Every
 * plot primitive calls this first, so you never have to remember which
 * shape a helper wants. It wants both.
 */
export function asRows(data) {
  return typeof data?.toRows === "function" ? data.toRows() : data;
}

/** Format a count the way slides want it: 24000 → "24.0K". */
export function fmtK(n) {
  if (n == null || !Number.isFinite(n)) return "—";
  return n >= 1000 ? `${(n / 1000).toFixed(1)}K` : `${n}`;
}

/** Format a proportion as a percent with sensible digits. */
export function fmtPct(p, digits = 1) {
  if (p == null || !Number.isFinite(p)) return "—";
  return `${(p * 100).toFixed(digits)}%`;
}
