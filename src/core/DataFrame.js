/**
 * DataFrame.js — the part you actually import.
 *
 * A DataFrame is an ordered bag of named Columns, all the same length,
 * plus every method you'd expect from having read the polars docs once
 * and the pandas docs for years. Every operation returns a new DataFrame;
 * nothing mutates. If you catch a method mutating, file a bug and accept
 * our apology in advance.
 *
 * The polars-style expression API is primary:
 *
 *   df.filter(col("age").gt(25))
 *     .groupBy("city")
 *     .agg(col("income").mean().alias("avg_income"))
 *     .sort("avg_income", { descending: true })
 *

 * There are no pandas-style aliases. One API, one spelling, no
 * `sort_values` waiting to disappoint your autocomplete. If you're
 * migrating from v1: groupby → groupBy, sort_values → sort,
 * with_columns → withColumns, to_data → toRows. That's the whole map.
 */

import { Column } from "./Column.js";
import { Expr, col, outputName, wrap } from "./expr.js";
import { evalExpr, evalAggScalar, evalAggGrouped, isAggExpr, quantileOf } from "./eval.js";
import { filterIndices, argsort, groupIds, hashJoin, takeWithNulls } from "./kernels.js";
import { assignWindows } from "./dynamic.js";
import { random } from "./random.js";

export class DataFrame {
  /**
   * Build a DataFrame from rows (`[{a: 1, b: "x"}, ...]`) or columns
   * (`{a: [1, 2], b: ["x", "y"]}`). Types are inferred per column; pass
   * `options.dtypes` (`{a: "i32"}`) if inference guesses wrong and you'd
   * rather not live with its choices.
   *
   * @param {Array<object>|object} data rows or a column dictionary
   * @param {{dtypes?: Record<string, string>}} [options]
   */
  constructor(data = [], options = {}) {
    /** @type {Map<string, Column>} */
    this._columns = new Map();
    this._length = 0;

    if (data instanceof Map) {
      // Internal fast path: pre-built Columns, no inference, no copying.
      this._columns = data;
      this._length = data.size > 0 ? data.values().next().value.length : 0;
      return;
    }

    const dtypes = options.dtypes ?? {};

    if (Array.isArray(data)) {
      if (data.length > 0) {
        const names = Object.keys(data[0]);
        this._length = data.length;
        for (const name of names) {
          const values = new Array(data.length);
          for (let i = 0; i < data.length; i++) values[i] = data[i][name];
          this._columns.set(name, Column.from(values, dtypes[name]));
        }
      }
    } else if (data && typeof data === "object") {
      const names = Object.keys(data);
      for (const name of names) {
        const values = data[name];
        const column =
          values instanceof Column ? values : Column.from(Array.from(values), dtypes[name]);
        if (this._columns.size > 0 && column.length !== this._length) {
          throw new Error(
            `Column "${name}" has ${column.length} rows but earlier columns have ${this._length}. ` +
              `A DataFrame is rectangular. This is non-negotiable.`
          );
        }
        this._length = column.length;
        this._columns.set(name, column);
      }
    } else {
      throw new Error(`Can't build a DataFrame from ${typeof data}. Rows or columns, please.`);
    }
  }

  // ---- construction helpers ------------------------------------------------

  /**
   * Explicit rows constructor, for people who like their intent in the name.
   * @param {Array<object>} rows
   * @param {{dtypes?: Record<string, string>}} [options]
   */
  static fromRows(rows, options) {
    return new DataFrame(rows, options);
  }

  /**
   * Explicit columns constructor. Same deal.
   * @param {object} columns
   * @param {{dtypes?: Record<string, string>}} [options]
   */
  static fromColumns(columns, options) {
    return new DataFrame(columns, options);
  }

  /**
   * Stack DataFrames vertically. Columns are aligned by name; a column
   * missing from any input becomes null there, because guessing values
   * is above our pay grade.
   *
   * @param {DataFrame[]} frames
   * @returns {DataFrame}
   */
  static concat(frames) {
    if (frames.length === 0) return new DataFrame();
    if (frames.length === 1) return frames[0];
    const names = [];
    for (const f of frames) {
      for (const name of f.columns) if (!names.includes(name)) names.push(name);
    }
    const out = {};
    for (const name of names) {
      const values = [];
      for (const f of frames) {
        if (f._columns.has(name)) values.push(...f._columns.get(name).toArray());
        else values.push(...new Array(f.height).fill(null));
      }
      out[name] = values;
    }
    return new DataFrame(out);
  }

  // ---- shape & introspection -------------------------------------------------

  /** Number of rows. */
  get height() {
    return this._length;
  }

  /** Number of columns. */
  get width() {
    return this._columns.size;
  }

  /** [rows, cols], for the pandas muscle memory. */
  get shape() {
    return [this._length, this._columns.size];
  }

  /** Column names, in order. */
  get columns() {
    return [...this._columns.keys()];
  }

  /** Map of column name -> dtype. Good for arguments and debugging, in that order. */
  get dtypes() {
    const out = {};
    for (const [name, column] of this._columns) out[name] = column.dtype;
    return out;
  }

  /**
   * The underlying Column for a name. This is the low-level accessor —
   * plots and stats use it to skip row materialization entirely.
   */
  getColumn(name) {
    const column = this._columns.get(name);
    if (!column) {
      throw new Error(
        `No column "${name}". Available: ${this.columns.join(", ") || "(none, this frame is empty)"}.`
      );
    }
    return column;
  }

  /** One row as a plain object. Fine for peeking; criminal in a loop. */
  row(i) {
    const out = {};
    for (const [name, column] of this._columns) out[name] = column.get(i);
    return out;
  }

  /** Iterate rows as objects. Convenient, allocation-happy — edges only. */
  *[Symbol.iterator]() {
    for (let i = 0; i < this._length; i++) yield this.row(i);
  }

  // ---- core verbs ---------------------------------------------------------------

  /**
   * Choose and/or compute columns. Accepts names, exprs, or a mix.
   * If every expression aggregates, you get a one-row frame, just like
   * the polars you've read about.
   *
   * @param {...(string|Expr)} exprs
   * @returns {DataFrame}
   */
  select(...exprs) {
    const flat = exprs.flat();
    const allAgg = flat.length > 0 && flat.every((e) => e instanceof Expr && isAggExpr(e.node));
    if (allAgg) {
      const out = {};
      for (const e of flat) out[outputName(e)] = [evalAggScalar(e.node, this)];
      return new DataFrame(out);
    }
    const columns = new Map();
    for (const e of flat) {
      if (typeof e === "string") columns.set(e, this.getColumn(e));
      else columns.set(outputName(e), evalExpr(e.node, this));
    }
    return new DataFrame(columns);
  }

  /**
   * Add or replace columns; everything else rides along. Takes expressions
   * (`col("income").div(12).alias("monthly")`) or the named-expression
   * object form (`{monthly: col("income").div(12)}`), which is the closest
   * JavaScript gets to polars' keyword arguments. Literals are broadcast.
   *
   * @param {...(Expr|Record<string, Expr|*>)} exprs
   * @returns {DataFrame}
   */
  withColumns(...exprs) {
    const columns = new Map(this._columns);
    for (const e of exprs.flat()) {
      if (e instanceof Expr) {
        columns.set(outputName(e), evalExpr(e.node, this));
      } else if (e && typeof e === "object") {
        for (const [name, spec] of Object.entries(e)) {
          if (typeof spec === "function") {
            throw new Error(
              `withColumns no longer takes fn(row) — build an expression, or use col("x").map(fn) for arbitrary JS on one column.`
            );
          }
          columns.set(name, evalExpr(wrap(spec).node, this));
        }
      } else {
        throw new Error(`withColumns wants expressions or {name: expression} objects, not ${typeof e}.`);
      }
    }
    return new DataFrame(columns);
  }

  /**
   * Keep rows where the boolean expression is true. Nulls don't pass;
   * uncertainty is not a filter criterion.
   *
   * A row predicate (`df.filter(row => ...)`) is also accepted as the
   * documented escape hatch for logic the expression API can't express.
   * It materializes a row object per row, so treat it like the emergency
   * exit: good to have, embarrassing to use daily.
   *
   * @param {Expr|((row: object) => boolean)} expr boolean expression, e.g. col("age").gt(25)
   * @returns {DataFrame}
   */
  filter(expr) {
    if (typeof expr === "function") {
      const keep = [];
      for (let i = 0; i < this._length; i++) if (expr(this.row(i))) keep.push(i);
      return this._take(Uint32Array.from(keep));
    }
    const mask = evalExpr(expr.node, this);
    if (mask.dtype !== "bool") {
      throw new Error(`filter() needs a boolean expression; this one evaluates to "${mask.dtype}".`);
    }
    return this._take(filterIndices(mask));
  }

  /**
   * Sort by one or more columns. Stable, null-last, dtype-aware — strings
   * sort as strings, not as whatever `a - b` thinks of them.
   *
   * @param {string|string[]} by
   * @param {{descending?: boolean|boolean[]}} [options]
   * @returns {DataFrame}
   */
  sort(by, { descending = false } = {}) {
    const names = Array.isArray(by) ? by : [by];
    const desc = Array.isArray(descending) ? descending : names.map(() => descending);
    const keys = names.map((n) => this.getColumn(n));
    return this._take(argsort(keys, desc));
  }

  /**
   * Group by columns, then `.agg(...)`. Returns a GroupBy holding the group
   * ids — computed once, reused for every aggregation you throw at it.
   *
   * @param {...string} names
   * @returns {GroupBy}
   */
  groupBy(...names) {
    return new GroupBy(this, names.flat());
  }

  /**
   * Time-window grouping, in the polars group_by_dynamic mold: bucket a
   * date (or numeric) index column into windows and aggregate per window,
   * optionally alongside other group keys. The panel-builder's workhorse:
   * claims in, member-months out.
   *
   *   df.groupByDynamic({ indexColumn: "date", every: "1mo", by: ["person_id"] })
   *     .agg(col("paid").sum().alias("paid"), col("claim_id").count().alias("claims"))
   *
   * Windows step by `every` and span `period` (defaults to `every`; longer
   * periods overlap, which turns this into a rolling aggregation and
   * duplicates rows across windows on purpose). `offset` shifts window
   * starts; `closed` picks the inclusive edge ("left" default, like
   * polars). Fixed durations ("7d", "12h", "2w") are exact milliseconds;
   * calendar durations ("1mo", "1q", "1y") respect month boundaries in
   * UTC, because months never agreed to be a constant width.
   *
   * The output keeps the index column's name, holding each window's start,
   * sorted ascending. Rows with a null index join no window.
   *
   * @param {object} options
   * @param {string} options.indexColumn "date" or numeric column to window on
   * @param {string|number} options.every window stride
   * @param {string|number} [options.period] window span (default: every)
   * @param {string|number} [options.offset] shift applied to window starts
   * @param {"left"|"right"|"both"|"none"} [options.closed="left"]
   * @param {string[]} [options.by=[]] additional group keys
   * @returns {GroupBy} call .agg(...) as usual
   */
  groupByDynamic({ indexColumn, every, period, offset, closed = "left", by = [] } = {}) {
    if (!indexColumn) throw new Error(`groupByDynamic needs { indexColumn }.`);
    const index = this.getColumn(indexColumn);
    const { rowIndices, starts, isDate } = assignWindows(index, { every, period, offset, closed });

    // Expand to one row per (row, window) pair — a no-op copy when windows
    // don't overlap — and attach the window start as a column that inherits
    // the index column's name and dtype.
    const expanded = this._take(rowIndices);
    const windowColumn = new Column(isDate ? "date" : "f64", starts);
    const columns = new Map(expanded._columns);
    columns.set(indexColumn, windowColumn);
    const frame = new DataFrame(columns);

    const grouped = new GroupBy(frame, [indexColumn, ...by]);
    grouped._sortBy = indexColumn; // windows read best in order
    return grouped;
  }

  /**
   * Hash join. `on` for shared key names, or `leftOn`/`rightOn` when the
   * two frames couldn't agree on naming (relatable). Non-key column name
   * collisions on the right get `suffix` appended.
   *
   * @param {DataFrame} other
   * @param {{on?: string|string[], leftOn?: string|string[], rightOn?: string|string[],
   *          how?: "inner"|"left"|"right"|"outer", suffix?: string}} options
   * @returns {DataFrame}
   */
  join(other, { on, leftOn, rightOn, how = "inner", suffix = "_right" } = {}) {
    const leftNames = toList(leftOn ?? on);
    const rightNames = toList(rightOn ?? on);
    if (!leftNames.length || leftNames.length !== rightNames.length) {
      throw new Error(`join() needs matching key columns: on, or leftOn + rightOn of equal length.`);
    }
    const leftKeys = leftNames.map((n) => this.getColumn(n));
    const rightKeys = rightNames.map((n) => other.getColumn(n));
    const { leftIdx, rightIdx } = hashJoin(leftKeys, rightKeys, how);

    const columns = new Map();
    for (const [name, column] of this._columns) {
      columns.set(name, takeWithNulls(column, leftIdx));
    }
    for (const [name, column] of other._columns) {
      if (rightNames.includes(name) && leftNames.includes(name)) continue; // shared key: keep left's copy
      const outName = columns.has(name) ? name + suffix : name;
      columns.set(outName, takeWithNulls(column, rightIdx));
    }

    // For right/outer joins, left key columns have holes where only the
    // right side matched. Patch them from the right keys so the key is
    // always present. It's their whole job.
    if (how === "right" || how === "outer") {
      for (let k = 0; k < leftNames.length; k++) {
        const patched = columns.get(leftNames[k]).toArray();
        const rightKey = rightKeys[k];
        for (let i = 0; i < leftIdx.length; i++) {
          if (leftIdx[i] === -1 && rightIdx[i] !== -1) patched[i] = rightKey.get(rightIdx[i]);
        }
        columns.set(leftNames[k], Column.from(patched));
      }
    }
    return new DataFrame(columns);
  }

  /**
   * Distinct rows, optionally judged by a subset of columns (first
   * occurrence wins, as is first occurrences' custom).
   *
   * @param {{subset?: string[]}} [options]
   * @returns {DataFrame}
   */
  unique({ subset } = {}) {
    const names = subset ?? this.columns;
    const keys = names.map((n) => this.getColumn(n));
    const { ids, firstRow } = groupIds(keys);
    void ids;
    return this._take(firstRow);
  }

  /** First n rows. The classic. */
  head(n = 5) {
    return this.slice(0, Math.min(n, this._length));
  }

  /** Last n rows. The other classic. */
  tail(n = 5) {
    return this.slice(Math.max(0, this._length - n), this._length);
  }

  /**
   * Random sample of n rows. Seeded draws replay exactly; unseeded draws
   * use Math.random and replay never. Without replacement (the default)
   * each row appears at most once, via a partial Fisher-Yates over the
   * index vector; with replacement, rows may repeat.
   *
   * @param {number} n rows to draw
   * @param {{seed?: number, withReplacement?: boolean}} [options]
   * @returns {DataFrame}
   */
  sample(n, { seed, withReplacement = false } = {}) {
    if (!Number.isInteger(n) || n < 0) throw new Error(`sample() needs a nonnegative integer n.`);
    const rand = seed != null ? random(seed) : Math.random;
    const height = this._length;
    if (withReplacement) {
      const out = new Uint32Array(n);
      for (let i = 0; i < n; i++) out[i] = (rand() * height) | 0;
      return this._take(out);
    }
    if (n > height) {
      throw new Error(`sample(${n}) without replacement from ${height} rows: the rows do not exist twice.`);
    }
    // Partial Fisher-Yates: shuffle only the first n slots.
    const indices = new Uint32Array(height);
    for (let i = 0; i < height; i++) indices[i] = i;
    for (let i = 0; i < n; i++) {
      const j = i + ((rand() * (height - i)) | 0);
      const tmp = indices[i];
      indices[i] = indices[j];
      indices[j] = tmp;
    }
    return this._take(indices.subarray(0, n));
  }

  /** Rows [start, end). */
  slice(start, end = this._length) {
    const columns = new Map();
    for (const [name, column] of this._columns) columns.set(name, column.slice(start, end));
    return new DataFrame(columns);
  }

  /** Rename columns via `{oldName: newName}`. Unmentioned columns keep their names, unbothered. */
  rename(mapping) {
    const columns = new Map();
    for (const [name, column] of this._columns) {
      columns.set(mapping[name] ?? name, column);
    }
    return new DataFrame(columns);
  }

  /** Drop columns by name. Dropping a column that doesn't exist is silently forgiven, once. */
  drop(...names) {
    const gone = new Set(names.flat());
    const columns = new Map();
    for (const [name, column] of this._columns) {
      if (!gone.has(name)) columns.set(name, column);
    }
    return new DataFrame(columns);
  }

  /**
   * Drop rows containing nulls, in all columns or a subset. The nuclear
   * option for missing data; consider fillNull before reaching for it.
   *
   * @param {{subset?: string[]}} [options]
   */
  dropNulls({ subset } = {}) {
    const names = subset ?? this.columns;
    const cols = names.map((n) => this.getColumn(n));
    const keep = [];
    for (let i = 0; i < this._length; i++) {
      let ok = true;
      for (let k = 0; k < cols.length; k++) {
        if (!cols[k].isValid(i)) {
          ok = false;
          break;
        }
      }
      if (ok) keep.push(i);
    }
    return this._take(Uint32Array.from(keep));
  }

  // ---- reshape -------------------------------------------------------------------

  /**
   * Long to wide. One row per unique `index`, one column per unique value
   * of `columns`, cells filled with `values` (aggregated by `agg` when a
   * cell has multiple candidates — "first" by default, which is honest
   * about being arbitrary).
   *
   * @param {{index: string|string[], columns: string, values: string,
   *          agg?: "first"|"sum"|"mean"|"min"|"max"|"count"}} options
   * @returns {DataFrame}
   */
  pivot({ index, columns, values, agg = "first" }) {
    const indexNames = toList(index);
    const indexCols = indexNames.map((n) => this.getColumn(n));
    const { ids: rowIds, nGroups: nRows, firstRow } = groupIds(indexCols);
    const colColumn = this.getColumn(columns);
    const valColumn = this.getColumn(values);
    const { codes: colIds, nCodes } = colColumn.groupCodes();

    // Column labels in first-seen order, deduped.
    const labels = [];
    const labelOfCode = new Map();
    for (let i = 0; i < this._length; i++) {
      if (!colColumn.isValid(i)) continue;
      if (!labelOfCode.has(colIds[i])) {
        labelOfCode.set(colIds[i], labels.length);
        labels.push(String(colColumn.get(i)));
      }
    }
    void nCodes;

    // cell accumulation
    const cells = new Array(nRows * labels.length).fill(undefined);
    for (let i = 0; i < this._length; i++) {
      if (!colColumn.isValid(i)) continue;
      const r = rowIds[i];
      const c = labelOfCode.get(colIds[i]);
      const idx = r * labels.length + c;
      const v = valColumn.isValid(i) ? valColumn.get(i) : null;
      const cur = cells[idx];
      if (cur === undefined) {
        cells[idx] = agg === "count" ? (v === null ? 0 : 1) : v;
      } else {
        switch (agg) {
          case "first":
            break;
          case "sum":
          case "mean":
            cells[idx] = (cur ?? 0) + (v ?? 0);
            break;
          case "min":
            cells[idx] = v !== null && (cur === null || v < cur) ? v : cur;
            break;
          case "max":
            cells[idx] = v !== null && (cur === null || v > cur) ? v : cur;
            break;
          case "count":
            cells[idx] = cur + (v === null ? 0 : 1);
            break;
          default:
            throw new Error(`pivot() doesn't know the aggregation "${agg}".`);
        }
      }
    }
    if (agg === "mean") {
      const counts = new Float64Array(nRows * labels.length);
      for (let i = 0; i < this._length; i++) {
        if (!colColumn.isValid(i) || !valColumn.isValid(i)) continue;
        counts[rowIds[i] * labels.length + labelOfCode.get(colIds[i])]++;
      }
      for (let k = 0; k < cells.length; k++) {
        if (cells[k] !== undefined && counts[k] > 0) cells[k] = cells[k] / counts[k];
      }
    }

    const out = {};
    for (let k = 0; k < indexNames.length; k++) {
      out[indexNames[k]] = Array.from(firstRow, (i) => indexCols[k].get(i));
    }
    for (let c = 0; c < labels.length; c++) {
      const values = new Array(nRows);
      for (let r = 0; r < nRows; r++) {
        const v = cells[r * labels.length + c];
        values[r] = v === undefined ? null : v;
      }
      out[labels[c]] = values;
    }
    return new DataFrame(out);
  }

  /**
   * Wide to long: keep `idVars`, fold `valueVars` into (variable, value)
   * pairs. The inverse of pivot, and the shape Observable Plot usually
   * wants anyway.
   *
   * @param {{idVars?: string[], valueVars?: string[],
   *          variableName?: string, valueName?: string}} options
   * @returns {DataFrame}
   */
  melt({ idVars = [], valueVars, variableName = "variable", valueName = "value" } = {}) {
    const ids = toList(idVars);
    const vals = valueVars ?? this.columns.filter((c) => !ids.includes(c));
    const n = this._length;
    const out = {};
    for (const id of ids) {
      const src = this.getColumn(id).toArray();
      const rep = new Array(n * vals.length);
      for (let v = 0; v < vals.length; v++) {
        for (let i = 0; i < n; i++) rep[v * n + i] = src[i];
      }
      out[id] = rep;
    }
    const variable = new Array(n * vals.length);
    const value = new Array(n * vals.length);
    for (let v = 0; v < vals.length; v++) {
      const src = this.getColumn(vals[v]);
      for (let i = 0; i < n; i++) {
        variable[v * n + i] = vals[v];
        value[v * n + i] = src.get(i);
      }
    }
    out[variableName] = variable;
    out[valueName] = value;
    return new DataFrame(out);
  }

  // ---- statistics -------------------------------------------------------------------

  /**
   * Summary statistics per numeric column: count, null_count, mean, std,
   * min, quartiles, max. The five-second health check before you trust
   * anything else about the data.
   *
   * @returns {DataFrame} one row per statistic
   */
  describe() {
    const numeric = this.columns.filter((n) => {
      const dt = this.getColumn(n).dtype;
      return dt === "f64" || dt === "i32";
    });
    const stats = ["count", "null_count", "mean", "std", "min", "25%", "50%", "75%", "max"];
    const out = { statistic: stats };
    for (const name of numeric) {
      const column = this.getColumn(name);
      const values = [];
      for (let i = 0; i < column.length; i++) if (column.isValid(i)) values.push(column.data[i]);
      const count = values.length;
      const mean = count ? values.reduce((a, b) => a + b, 0) / count : null;
      let std = null;
      if (count > 1) {
        const m = mean;
        std = Math.sqrt(values.reduce((a, b) => a + (b - m) ** 2, 0) / (count - 1));
      }
      const sorted = values.slice();
      out[name] = [
        count,
        column.nullCount(),
        mean,
        std,
        count ? values.reduce((a, b) => (b < a ? b : a), Infinity) : null,
        quantileOf(sorted.slice(), 0.25),
        quantileOf(sorted.slice(), 0.5),
        quantileOf(sorted.slice(), 0.75),
        count ? values.reduce((a, b) => (b > a ? b : a), -Infinity) : null,
      ];
    }
    return new DataFrame(out);
  }

  /**
   * Counts per distinct value of a column, sorted descending, because
   * you were going to sort it descending.
   *
   * @param {string} name
   * @returns {DataFrame} columns: [name, "count"]
   */
  valueCounts(name) {
    return this.groupBy(name)
      .agg(col(name).count().alias("count"))
      .sort("count", { descending: true });
  }

  /** Pearson correlation between two numeric columns. */
  corr(a, b) {
    const ca = this.getColumn(a);
    const cb = this.getColumn(b);
    let n = 0;
    let sa = 0;
    let sb = 0;
    let saa = 0;
    let sbb = 0;
    let sab = 0;
    for (let i = 0; i < this._length; i++) {
      if (!ca.isValid(i) || !cb.isValid(i)) continue;
      const x = ca.data[i];
      const y = cb.data[i];
      n++;
      sa += x;
      sb += y;
      saa += x * x;
      sbb += y * y;
      sab += x * y;
    }
    if (n < 2) return null;
    const cov = sab - (sa * sb) / n;
    const va = saa - (sa * sa) / n;
    const vb = sbb - (sb * sb) / n;
    if (va === 0 || vb === 0) return null;
    return cov / Math.sqrt(va * vb);
  }

  /**
   * Pairwise Pearson correlations of all numeric columns, in long format
   * (`a`, `b`, `corr`) — which is what Plot.cell wants to eat, so that's
   * what we serve.
   *
   * @returns {DataFrame}
   */
  corrMatrix() {
    const numeric = this.columns.filter((n) => {
      const dt = this.getColumn(n).dtype;
      return dt === "f64" || dt === "i32";
    });
    const a = [];
    const b = [];
    const r = [];
    for (const x of numeric) {
      for (const y of numeric) {
        a.push(x);
        b.push(y);
        r.push(x === y ? 1 : this.corr(x, y));
      }
    }
    return new DataFrame({ a, b, corr: r });
  }

  // ---- export --------------------------------------------------------------------------

  /**
   * Materialize as an array of plain row objects — the lingua franca of
   * Observable Plot, Inputs.table, and everything else in that ecosystem.
   * This copies everything, so make it the last step, not the first.
   *
   * @returns {Array<object>}
   */
  toRows() {
    const names = this.columns;
    const cols = names.map((n) => this._columns.get(n));
    const out = new Array(this._length);
    for (let i = 0; i < this._length; i++) {
      const row = {};
      for (let k = 0; k < names.length; k++) row[names[k]] = cols[k].get(i);
      out[i] = row;
    }
    return out;
  }

  /** Materialize as `{name: values[]}`. */
  toColumns() {
    const out = {};
    for (const [name, column] of this._columns) out[name] = column.toArray();
    return out;
  }

  // ---- internals -------------------------------------------------------------------------

  /** Gather rows by index into a new frame. The one true row-shuffle. */
  _take(indices) {
    const columns = new Map();
    for (const [name, column] of this._columns) columns.set(name, column.take(indices));
    return new DataFrame(columns);
  }
}

/**
 * The object between groupBy() and agg(). Computes group ids exactly once,
 * then answers as many aggregations as you can type.
 */
export class GroupBy {
  constructor(frame, names) {
    this.frame = frame;
    this.names = names;
    if (names.length === 0) {
      throw new Error(`groupBy() with no columns is just the whole frame wearing a trench coat. Use select().`);
    }
    const keys = names.map((n) => frame.getColumn(n));
    const { ids, nGroups, firstRow } = groupIds(keys);
    this.ids = ids;
    this.nGroups = nGroups;
    this.firstRow = firstRow;
  }

  /**
   * Aggregate with expressions, and only expressions:
   *
   *   df.groupBy("city").agg(col("income").mean(), col("id").count().alias("n"))
   *
   * (v1's pandas object spec — `{income: ["mean", "max"]}` — is gone.
   * The expression version is longer by a few characters and better by
   * every other measure, including telling you the output column names.)
   *
   * @param {...Expr} exprs
   * @returns {DataFrame}
   */
  agg(...exprs) {
    const list = [];
    for (const e of exprs.flat()) {
      if (e instanceof Expr) list.push(e);
      else
        throw new Error(
          `agg() takes expressions like col("x").mean().alias("avg") — got ${typeof e}.`
        );
    }
    const out = new Map();
    // Group keys first: one representative row per group.
    for (const name of this.names) {
      out.set(name, this.frame.getColumn(name).take(this.firstRow));
    }
    const cache = {};
    for (const e of list) {
      const values = evalAggGrouped(e.node, this.frame, this.ids, this.nGroups, cache);
      out.set(outputName(e), Column.from(values));
    }
    const result = new DataFrame(out);
    // groupByDynamic asks for its windows in chronological order; humans
    // reading month rollups in hash order file complaints, correctly.
    return this._sortBy ? result.sort(this._sortBy) : result;
  }

  /** Row count per group. The aggregation everyone actually wanted. */
  count() {
    const counts = new Float64Array(this.nGroups);
    for (let i = 0; i < this.ids.length; i++) counts[this.ids[i]]++;
    const out = new Map();
    for (const name of this.names) {
      out.set(name, this.frame.getColumn(name).take(this.firstRow));
    }
    out.set("count", Column.from(Array.from(counts), "i32"));
    return new DataFrame(out);
  }

  /**
   * Iterate [keyObject, subframe] pairs, for when you truly need to see
   * each group as its own DataFrame. Costs more than agg(); worth it less
   * often than it feels.
   */
  *[Symbol.iterator]() {
    const rowsPerGroup = new Array(this.nGroups).fill(null).map(() => []);
    for (let i = 0; i < this.ids.length; i++) rowsPerGroup[this.ids[i]].push(i);
    for (let g = 0; g < this.nGroups; g++) {
      const key = {};
      for (const name of this.names) key[name] = this.frame.getColumn(name).get(this.firstRow[g]);
      yield [key, this.frame._take(Uint32Array.from(rowsPerGroup[g]))];
    }
  }
}

function toList(x) {
  return x == null ? [] : Array.isArray(x) ? x : [x];
}
