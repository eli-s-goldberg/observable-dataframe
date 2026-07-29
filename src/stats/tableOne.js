/**
 * tableOne.js — the baseline characteristics table, Table 1 of every paper.
 *
 * Continuous variables report mean (SD) per arm with a two-sample test;
 * categorical variables report n (%) per level with a chi-square test.
 * The p-value column exists because reviewers expect it; under true
 * randomization those p-values test a hypothesis you already know is
 * false at rate alpha, a point the epidemiologists have been making
 * politely for decades. We compute them anyway. Convention is a force.
 *
 * @example
 *   const t1 = tableOne(df, {
 *     by: "arm",
 *     continuous: ["age", "baseline_pmpm"],
 *     categorical: ["sex", "region"],
 *   });
 *   Inputs.table(t1.rows)   // or plots' tableOnePlot(t1) for the styled version
 */

import { welchTTest } from "./tests.js";
import { chi2CDF } from "./special.js";

/** DataFrame-or-rows → rows. */
function toRows(data) {
  return typeof data?.toRows === "function" ? data.toRows() : data;
}

/**
 * @param {DataFrame|Array<object>} data one row per unit (not unit-period —
 *   collapse your panel to baseline first, or Table 1 becomes Table n)
 * @param {object} options
 * @param {string} options.by grouping column (the arm)
 * @param {string[]} [options.continuous=[]] numeric columns → mean (SD)
 * @param {string[]} [options.categorical=[]] discrete columns → n (%)
 * @param {number} [options.digits=1] decimals for means and percents
 * @param {Record<string,string>} [options.labels] pretty names per column
 * @returns {{rows: Array<object>, groups: Array<{value, label, n}>, by: string}}
 */
export function tableOne(
  data,
  { by, continuous = [], categorical = [], digits = 1, labels = {} } = {}
) {
  const rows = toRows(data);
  if (!by) throw new Error(`tableOne needs { by }: a one-armed Table 1 is a summary, not a comparison.`);

  const groupValues = [...new Set(rows.map((r) => r[by]))].sort();
  if (groupValues.length < 2) {
    throw new Error(`tableOne found ${groupValues.length} level(s) of "${by}"; comparisons need at least 2.`);
  }
  const groups = groupValues.map((value) => ({
    value,
    label: String(value),
    n: rows.filter((r) => r[by] === value).length,
  }));

  const out = [];

  // Header row: arm sizes. Every Table 1 starts with the denominators.
  out.push({
    characteristic: "n",
    ...Object.fromEntries(groups.map((g) => [g.label, String(g.n)])),
    p: "",
  });

  for (const col of continuous) {
    const cells = {};
    for (const g of groups) {
      const values = rows
        .filter((r) => r[by] === g.value && r[col] != null && !Number.isNaN(Number(r[col])))
        .map((r) => Number(r[col]));
      const m = values.reduce((a, b) => a + b, 0) / values.length;
      const sd = Math.sqrt(values.reduce((a, b) => a + (b - m) ** 2, 0) / (values.length - 1));
      cells[g.label] = `${m.toFixed(digits)} (${sd.toFixed(digits)})`;
    }
    // Welch across the first two arms (the common case); >2 arms report the
    // pairwise test and say so rather than silently pretending it's an ANOVA.
    let p = null;
    try {
      p = welchTTest(rows, col, by, groups[0].value, groups[1].value).pValue;
    } catch {
      p = null;
    }
    out.push({
      characteristic: `${labels[col] ?? col}, mean (SD)`,
      ...cells,
      p: formatP(p) + (groups.length > 2 ? "†" : ""),
    });
  }

  for (const col of categorical) {
    const levels = [...new Set(rows.map((r) => String(r[col])))].sort();
    // Chi-square over the full level × arm contingency table.
    const observed = levels.map((lvl) =>
      groups.map((g) => rows.filter((r) => r[by] === g.value && String(r[col]) === lvl).length)
    );
    const p = chiSquareP(observed);

    out.push({ characteristic: `${labels[col] ?? col}, n (%)`, ...Object.fromEntries(groups.map((g) => [g.label, ""])), p: formatP(p) });
    levels.forEach((lvl, li) => {
      const cells = {};
      groups.forEach((g, gi) => {
        const count = observed[li][gi];
        cells[g.label] = `${count} (${((count / g.n) * 100).toFixed(digits)}%)`;
      });
      out.push({ characteristic: `   ${lvl}`, ...cells, p: "" });
    });
  }

  return { rows: out, groups, by, note: groups.length > 2 ? "† p compares the first two arms only." : null };
}

function chiSquareP(observed) {
  const nRows = observed.length;
  const nCols = observed[0].length;
  const rowSums = observed.map((r) => r.reduce((a, b) => a + b, 0));
  const colSums = observed[0].map((_, j) => observed.reduce((a, r) => a + r[j], 0));
  const total = rowSums.reduce((a, b) => a + b, 0);
  if (total === 0 || nRows < 2) return null;
  let chi2 = 0;
  for (let i = 0; i < nRows; i++) {
    for (let j = 0; j < nCols; j++) {
      const expected = (rowSums[i] * colSums[j]) / total;
      if (expected > 0) chi2 += (observed[i][j] - expected) ** 2 / expected;
    }
  }
  const df = (nRows - 1) * (nCols - 1);
  return 1 - chi2CDF(chi2, df);
}

function formatP(p) {
  if (p == null || Number.isNaN(p)) return "";
  if (p < 0.001) return "<0.001";
  return p.toFixed(3);
}
