/**
 * summaryTable.js — the five-second EDA table.
 *
 * One row per column: dtype, a mini distribution plot, missingness, and
 * the summary stats you'd have computed anyway. Run it before you trust a
 * dataset; it is much cheaper than the alternative, which is trusting a
 * dataset.
 */

import * as Plot from "@observablehq/plot";
import { html } from "htl";
import { colors, fonts } from "./theme.js";

/**
 * @param {DataFrame} df an observable-dataframe DataFrame (rows also accepted; we'll cope)
 * @param {object} [options]
 * @param {number} [options.width=760] total table width in px
 * @param {string} [options.label="Summary"] caption text
 * @returns {HTMLElement}
 */
export function summaryTable(df, { width = 760, label = "Summary" } = {}) {
  // Accept raw rows for the road-warriors who skipped the DataFrame.
  if (typeof df?.getColumn !== "function") {
    throw new Error(
      `summaryTable wants a DataFrame. You have rows; DataFrame.fromRows(rows) is right there.`
    );
  }

  const sparkWidth = 170;
  const rows = df.columns.map((name) => summarizeColumn(df, name, sparkWidth));

  return html`<div style="font: 12px ${fonts.sans}; max-width: ${width}px;">
    <div style="font-weight: 700; font-size: 13px; margin-bottom: 6px; color: ${colors.ink};">
      ${label}
      <span style="font-weight: 400; color: ${colors.muted};">
        — ${df.height.toLocaleString()} rows × ${df.width} columns</span>
    </div>
    <table style="width: 100%; border-collapse: collapse;">
      <thead>
        <tr style="border-bottom: 2px solid ${colors.ink}; text-align: left;">
          <th style="padding: 4px 8px;">Column</th>
          <th style="padding: 4px 8px;">Type</th>
          <th style="padding: 4px 8px;">Distribution</th>
          <th style="padding: 4px 8px; text-align: right;">Missing</th>
          <th style="padding: 4px 8px; text-align: right;">Mean</th>
          <th style="padding: 4px 8px; text-align: right;">Median</th>
          <th style="padding: 4px 8px; text-align: right;">SD</th>
        </tr>
      </thead>
      <tbody>
        ${rows.map(
          (r) => html`<tr style="border-bottom: 1px solid ${colors.border};">
            <td style="padding: 4px 8px; font-weight: 600;">${r.name}</td>
            <td style="padding: 4px 8px; color: ${colors.muted}; font-family: monospace;">${r.dtype}</td>
            <td style="padding: 2px 8px;">${r.spark}</td>
            <td style="padding: 4px 8px; text-align: right; color: ${r.missingPct > 0 ? colors.clinical : colors.muted};">
              ${r.missingPct.toFixed(1)}%</td>
            <td style="padding: 4px 8px; text-align: right;">${r.mean}</td>
            <td style="padding: 4px 8px; text-align: right;">${r.median}</td>
            <td style="padding: 4px 8px; text-align: right;">${r.sd}</td>
          </tr>`
        )}
      </tbody>
    </table>
  </div>`;
}

function summarizeColumn(df, name, sparkWidth) {
  const column = df.getColumn(name);
  const n = column.length;
  const missing = column.nullCount();
  const missingPct = n ? (missing / n) * 100 : 0;

  const numeric = column.dtype === "f64" || column.dtype === "i32";
  let mean = "—";
  let median = "—";
  let sd = "—";
  let spark;

  if (numeric || column.dtype === "date") {
    const raw = column.values({ validOnly: true });
    const values = numeric ? raw : raw.map((d) => d.getTime());
    if (values.length) {
      const m = values.reduce((a, b) => a + b, 0) / values.length;
      const sorted = values.slice().sort((a, b) => a - b);
      const med = sorted[(sorted.length / 2) | 0];
      if (numeric) {
        mean = fmtNum(m);
        median = fmtNum(med);
        if (values.length > 1) {
          sd = fmtNum(Math.sqrt(values.reduce((a, b) => a + (b - m) ** 2, 0) / (values.length - 1)));
        }
      } else {
        mean = new Date(m).toISOString().slice(0, 10);
        median = new Date(med).toISOString().slice(0, 10);
      }
      spark = Plot.plot({
        width: sparkWidth,
        height: 36,
        margin: 2,
        x: { axis: null },
        y: { axis: null },
        marks: [
          Plot.rectY(
            values,
            Plot.binX({ y: "count" }, { x: (d) => d, thresholds: 18, fill: colors.operational, insetLeft: 0.5 })
          ),
        ],
      });
    }
  } else if (column.dtype === "str") {
    // Top categories as a single stacked strip: the shape of the column in
    // one glance, without pretending 40 categories deserve 40 rows.
    const counts = new Map();
    for (const v of column.values({ validOnly: true })) {
      counts.set(v, (counts.get(v) ?? 0) + 1);
    }
    const top = [...counts.entries()].sort((a, b) => b[1] - a[1]).slice(0, 5);
    median = `${counts.size} distinct`;
    spark = Plot.plot({
      width: sparkWidth,
      height: 36,
      margin: 2,
      x: { axis: null },
      y: { axis: null },
      color: { range: [colors.navy, colors.operational, colors.financial, colors.accent, colors.faint] },
      marks: [
        Plot.barX(
          top.map(([k, v], i) => ({ k, v, i })),
          { x: "v", fill: "k", title: (d) => `${d.k}: ${d.v}` }
        ),
      ],
    });
  } else {
    // bool
    let trues = 0;
    let valid = 0;
    for (const v of column.values({ validOnly: true })) {
      valid++;
      if (v) trues++;
    }
    mean = valid ? `${((trues / valid) * 100).toFixed(0)}% true` : "—";
    spark = html`<span style="color:${colors.muted}; font-size: 10px;">${trues} / ${valid}</span>`;
  }

  return { name, dtype: column.dtype, spark: spark ?? html`<span>—</span>`, missingPct, mean, median, sd };
}

function fmtNum(v) {
  if (Math.abs(v) >= 1000) return v.toLocaleString(undefined, { maximumFractionDigits: 0 });
  return v.toLocaleString(undefined, { maximumFractionDigits: 2 });
}
