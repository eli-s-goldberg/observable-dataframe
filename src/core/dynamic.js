/**
 * dynamic.js — time-window grouping, the polars group_by_dynamic.
 *
 * Panels don't arrive in tidy months; they arrive as events with
 * timestamps, and somebody has to draw the window boundaries. This module
 * does the drawing: fixed windows ("7d", "1w", "12h"), calendar windows
 * ("1mo", "1q", "1y" — because months refuse to be a constant number of
 * milliseconds), stepped starts (`every`), window lengths (`period`,
 * which may overlap), boundary shifts (`offset`), and inclusive/exclusive
 * edges (`closed`). Numeric index columns work too, with plain numbers
 * for the durations.
 *
 * Calendar arithmetic is done in UTC. Local-time windows are a daylight
 * saving bug with extra steps, and we decline to take them.
 */

/**
 * Parse a duration: a number (numeric index columns), or a string like
 * "7d", "2w", "1mo", "1q", "1y", "12h", "30m", "45s", "500ms".
 *
 * @returns {{months: number}|{ms: number}}
 */
export function parseDuration(spec) {
  if (typeof spec === "number") return { ms: spec };
  const match = /^(\d+(?:\.\d+)?)(ms|s|m|h|d|w|mo|q|y)$/.exec(String(spec).trim());
  if (!match) {
    throw new Error(
      `Can't parse duration "${spec}". The menu: number, or "<n>" + ms|s|m|h|d|w|mo|q|y — e.g. "7d", "1mo".`
    );
  }
  const n = Number(match[1]);
  const unit = match[2];
  const MS = { ms: 1, s: 1000, m: 60_000, h: 3_600_000, d: 86_400_000, w: 604_800_000 };
  if (unit in MS) return { ms: n * MS[unit] };
  const MONTHS = { mo: 1, q: 3, y: 12 };
  if (!Number.isInteger(n)) throw new Error(`Calendar durations need whole numbers; "${spec}" is asking for fractional months.`);
  return { months: n * MONTHS[unit] };
}

const EPOCH_MONTH = 0; // Jan 1970, month 0 since epoch

function monthsSinceEpoch(ms) {
  const d = new Date(ms);
  return (d.getUTCFullYear() - 1970) * 12 + d.getUTCMonth();
}

function monthStartMs(monthIndex) {
  const year = 1970 + Math.floor(monthIndex / 12);
  const month = ((monthIndex % 12) + 12) % 12;
  return Date.UTC(year, month, 1);
}

/**
 * The window starts a value belongs to, given every/period/offset/closed.
 * Non-overlapping windows (period == every) yield exactly one start; a
 * period longer than its stride yields several, which is how you get
 * rolling aggregations out of a groupby.
 *
 * @param {number} value index value (epoch ms for dates, raw for numerics)
 * @param {object} spec {every, period, offset} as parsed durations
 * @param {"left"|"right"|"both"|"none"} closed
 * @returns {number[]} window start values (epoch ms / numeric)
 */
export function windowStartsFor(value, { every, period, offset }, closed) {
  const starts = [];
  const inWindow = (t, start, end) => {
    switch (closed) {
      case "left":
        return t >= start && t < end;
      case "right":
        return t > start && t <= end;
      case "both":
        return t >= start && t <= end;
      case "none":
        return t > start && t < end;
      default:
        throw new Error(`Unknown closed="${closed}".`);
    }
  };

  if (every.months != null) {
    // Calendar windows: index k counts windows of `every.months` since epoch.
    const offMonths = offset?.months ?? 0;
    const m = monthsSinceEpoch(value) - offMonths;
    const periodMonths = period.months ?? every.months;
    // candidate window indices: those whose [start, start+period) can contain value
    const kHigh = Math.floor(m / every.months);
    const kLow = Math.ceil((m - periodMonths + 1) / every.months) - 1;
    for (let k = kLow; k <= kHigh + 1; k++) {
      const startMonth = k * every.months + offMonths + EPOCH_MONTH;
      const start = monthStartMs(startMonth);
      const end = monthStartMs(startMonth + periodMonths);
      if (inWindow(value, start, end)) starts.push(start);
    }
    return starts;
  }

  const off = offset?.ms ?? 0;
  const everyMs = every.ms;
  const periodMs = period.ms ?? everyMs;
  const t = value - off;
  const kHigh = Math.floor(t / everyMs);
  const kLow = Math.ceil((t - periodMs) / everyMs);
  for (let k = kLow; k <= kHigh + 1; k++) {
    const start = k * everyMs + off;
    if (inWindow(value, start, start + periodMs)) starts.push(start);
  }
  return starts;
}

/**
 * Assign every row to its window(s).
 *
 * @param {import("./Column.js").Column} indexColumn "date" or numeric column
 * @param {object} options {every, period, offset, closed}
 * @returns {{rowIndices: Uint32Array, starts: Float64Array, overlapping: boolean, isDate: boolean}}
 *   parallel arrays of (source row, window start); rows may repeat when windows overlap
 */
export function assignWindows(indexColumn, { every, period, offset, closed = "left" } = {}) {
  const isDate = indexColumn.dtype === "date";
  if (!isDate && indexColumn.dtype === "str") {
    throw new Error(`groupByDynamic needs a "date" or numeric index column; "str" windows are a filing system, not a timeline.`);
  }
  const everyD = parseDuration(every);
  const periodD = period != null ? parseDuration(period) : everyD;
  const offsetD = offset != null ? parseDuration(offset) : null;
  if ((everyD.months != null) !== (periodD.months != null)) {
    throw new Error(`every and period must share a unit family: mixing "${every}" with "${period}" invites month-length philosophy.`);
  }
  if (!isDate && (everyD.months != null)) {
    throw new Error(`Calendar durations ("mo"/"q"/"y") need a date index column; this one is numeric.`);
  }

  const rowIdx = [];
  const starts = [];
  const n = indexColumn.length;
  for (let i = 0; i < n; i++) {
    if (!indexColumn.isValid(i)) continue; // undated rows join no window; time waits for data
    const value = indexColumn.data[i];
    for (const s of windowStartsFor(value, { every: everyD, period: periodD, offset: offsetD }, closed)) {
      rowIdx.push(i);
      starts.push(s);
    }
  }
  return {
    rowIndices: Uint32Array.from(rowIdx),
    starts: Float64Array.from(starts),
    overlapping: (periodD.months ?? periodD.ms) > (everyD.months ?? everyD.ms),
    isDate,
  };
}
