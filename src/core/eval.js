/**
 * eval.js — where expressions stop being descriptions and start being work.
 *
 * `evalExpr` turns an expression node into a Column, one vectorized pass at
 * a time. `evalAggScalar` and `evalAggGrouped` handle the aggregations.
 * The frame argument is anything with `getColumn(name)` and `height` —
 * which is to say, a DataFrame, but we're not clingy about it.
 */

import { Column } from "./Column.js";
import { groupIds, groupRowIndices } from "./kernels.js";

/**
 * Evaluate a non-aggregating expression to a Column of `frame.height` rows.
 * Aggregations that sneak in here get broadcast to full length, which is
 * occasionally what you want and always what you asked for.
 *
 * @param {object} node expression tree node (Expr.node)
 * @param {{getColumn: (name: string) => Column, height: number}} frame
 * @returns {Column}
 */
export function evalExpr(node, frame) {
  switch (node.kind) {
    case "col":
      return frame.getColumn(node.name);

    case "lit":
      return broadcastLiteral(node.value, frame.height);

    case "alias":
      return evalExpr(node.input, frame);

    case "cast":
      return evalExpr(node.input, frame).cast(node.dtype);

    case "unary":
      return evalUnary(node, frame);

    case "binary":
      return evalBinary(node, frame);

    case "isin": {
      const input = evalExpr(node.input, frame);
      return evalIsIn(input, node.values);
    }

    case "between": {
      const input = toNumeric(evalExpr(node.input, frame));
      const n = input.length;
      const out = new Uint8Array(n);
      const lo = node.lo instanceof Date ? node.lo.getTime() : node.lo;
      const hi = node.hi instanceof Date ? node.hi.getTime() : node.hi;
      for (let i = 0; i < n; i++) {
        const v = input.data[i];
        out[i] = v >= lo && v <= hi ? 1 : 0;
      }
      return new Column("bool", out, { validity: cloneValidity(input.validity) });
    }

    case "ternary": {
      const cond = evalExpr(node.cond, frame);
      const thenCol = evalExpr(node.then, frame);
      const elseCol = evalExpr(node.otherwise, frame);
      return evalTernary(cond, thenCol, elseCol);
    }

    case "str":
      return evalStr(node, frame);

    case "dt":
      return evalDt(node, frame);

    case "window":
      return evalWindow(node, frame);

    case "map": {
      const input = evalExpr(node.input, frame);
      const n = input.length;
      const out = new Array(n);
      for (let i = 0; i < n; i++) out[i] = node.fn(input.get(i));
      return Column.from(out, node.dtype ?? undefined);
    }

    case "agg": {
      // An aggregation outside groupBy: compute the scalar, broadcast it.
      const value = evalAggScalar(node, frame);
      return broadcastLiteral(value, frame.height);
    }

    default:
      throw new Error(`Cannot evaluate node kind "${node.kind}". It sounds made up.`);
  }
}

/** Does this expression tree bottom out in an aggregation at its root spine? */
export function isAggExpr(node) {
  if (node.kind === "agg") return true;
  if (node.kind === "alias" || node.kind === "cast") return isAggExpr(node.input);
  if (node.kind === "binary") return isAggExpr(node.left) || isAggExpr(node.right);
  if (node.kind === "unary") return isAggExpr(node.input);
  return false;
}

/**
 * Evaluate an aggregation over the entire frame to a single scalar.
 * Arithmetic on top of aggs (e.g. sum(a).div(sum(b))) is supported by
 * recursing with scalar semantics.
 */
export function evalAggScalar(node, frame) {
  switch (node.kind) {
    case "agg": {
      const input = evalExpr(node.input, frame);
      return aggregateColumn(node.op, input, node.param);
    }
    case "alias":
      return evalAggScalar(node.input, frame);
    case "cast":
      return evalAggScalar(node.input, frame);
    case "binary": {
      const l = isAggExpr(node.left) ? evalAggScalar(node.left, frame) : scalarOf(node.left, frame);
      const r = isAggExpr(node.right) ? evalAggScalar(node.right, frame) : scalarOf(node.right, frame);
      return scalarBinary(node.op, l, r);
    }
    case "unary": {
      const v = evalAggScalar(node.input, frame);
      return scalarUnary(node.op, v, node.param);
    }
    case "lit":
      return node.value;
    default:
      throw new Error(`Expression of kind "${node.kind}" is not an aggregation, no matter how hard we squint.`);
  }
}

function scalarOf(node, frame) {
  if (node.kind === "lit") return node.value;
  throw new Error(
    `Mixing aggregations with full-length columns in one expression needs a .mean()/.sum()/etc on both sides.`
  );
}

/**
 * Evaluate an aggregation expression per group. Returns a plain array of
 * one value per group, which the caller turns into a Column.
 *
 * @param {object} node expression node whose spine contains an agg
 * @param {{getColumn: Function, height: number}} frame
 * @param {Int32Array} ids group id per row
 * @param {number} nGroups
 * @param {{rows?: Uint32Array[]}} cache shared per-groupBy cache (row lists are built at most once)
 * @returns {Array}
 */
export function evalAggGrouped(node, frame, ids, nGroups, cache = {}) {
  switch (node.kind) {
    case "alias":
    case "cast": {
      const inner = evalAggGrouped(node.input, frame, ids, nGroups, cache);
      return node.kind === "cast" ? inner : inner;
    }
    case "binary": {
      const l = isAggExpr(node.left)
        ? evalAggGrouped(node.left, frame, ids, nGroups, cache)
        : constantPerGroup(node.left, nGroups);
      const r = isAggExpr(node.right)
        ? evalAggGrouped(node.right, frame, ids, nGroups, cache)
        : constantPerGroup(node.right, nGroups);
      const out = new Array(nGroups);
      for (let g = 0; g < nGroups; g++) out[g] = scalarBinary(node.op, l[g], r[g]);
      return out;
    }
    case "unary": {
      const inner = evalAggGrouped(node.input, frame, ids, nGroups, cache);
      const out = new Array(nGroups);
      for (let g = 0; g < nGroups; g++) out[g] = scalarUnary(node.op, inner[g], node.param);
      return out;
    }
    case "agg": {
      const input = evalExpr(node.input, frame);
      return aggregateGrouped(node.op, input, ids, nGroups, node.param, cache);
    }
    default:
      throw new Error(`groupBy().agg() expressions must aggregate. "${node.kind}" does not.`);
  }
}

function constantPerGroup(node, nGroups) {
  if (node.kind !== "lit") {
    throw new Error(`Non-aggregated columns inside agg() need their own aggregation. Try .first() if you're sure.`);
  }
  return new Array(nGroups).fill(node.value);
}

// --------------------------------------------------------------------------
// whole-column aggregation
// --------------------------------------------------------------------------

function aggregateColumn(op, column, param) {
  const n = column.length;
  switch (op) {
    case "count": {
      return n - column.nullCount();
    }
    case "sum":
    case "mean": {
      let sum = 0;
      let count = 0;
      const num = toNumeric(column);
      for (let i = 0; i < n; i++) {
        if (!num.isValid(i)) continue;
        sum += num.data[i];
        count++;
      }
      return op === "sum" ? sum : count > 0 ? sum / count : null;
    }
    case "min":
    case "max": {
      if (column.dtype === "str") {
        let best = null;
        for (let i = 0; i < n; i++) {
          if (!column.isValid(i)) continue;
          const s = column.dict[column.data[i]];
          if (best === null || (op === "min" ? s < best : s > best)) best = s;
        }
        return best;
      }
      // A loop, not Math.min(...arr): spread blows the stack around ~100k
      // elements, and we plan on having more elements than that.
      let best = op === "min" ? Infinity : -Infinity;
      let saw = false;
      for (let i = 0; i < n; i++) {
        if (!column.isValid(i)) continue;
        const v = column.data[i];
        if (op === "min" ? v < best : v > best) best = v;
        saw = true;
      }
      if (!saw) return null;
      return column.dtype === "date" ? new Date(best) : best;
    }
    case "first": {
      for (let i = 0; i < n; i++) if (column.isValid(i)) return column.get(i);
      return null;
    }
    case "last": {
      for (let i = n - 1; i >= 0; i--) if (column.isValid(i)) return column.get(i);
      return null;
    }
    case "nUnique": {
      const seen = new Set();
      for (let i = 0; i < n; i++) if (column.isValid(i)) seen.add(column.data[i]);
      return seen.size;
    }
    case "std":
    case "var": {
      const { variance } = sumStats(column);
      if (variance === null) return null;
      return op === "var" ? variance : Math.sqrt(variance);
    }
    case "median":
      return quantileOf(collectValid(column), 0.5);
    case "quantile":
      return quantileOf(collectValid(column), param);
    case "implode": {
      const out = [];
      for (let i = 0; i < n; i++) if (column.isValid(i)) out.push(column.get(i));
      return out;
    }
    default:
      throw new Error(`Unknown aggregation "${op}".`);
  }
}

// --------------------------------------------------------------------------
// grouped aggregation — single pass with typed accumulators where possible
// --------------------------------------------------------------------------

function aggregateGrouped(op, column, ids, nGroups, param, cache) {
  const n = column.length;

  switch (op) {
    case "count": {
      const counts = new Float64Array(nGroups);
      for (let i = 0; i < n; i++) if (column.isValid(i)) counts[ids[i]]++;
      return Array.from(counts);
    }
    case "sum":
    case "mean": {
      const num = toNumeric(column);
      const sums = new Float64Array(nGroups);
      const counts = new Float64Array(nGroups);
      for (let i = 0; i < n; i++) {
        if (!num.isValid(i)) continue;
        sums[ids[i]] += num.data[i];
        counts[ids[i]]++;
      }
      const out = new Array(nGroups);
      for (let g = 0; g < nGroups; g++) {
        out[g] = op === "sum" ? sums[g] : counts[g] > 0 ? sums[g] / counts[g] : null;
      }
      return out;
    }
    case "min":
    case "max": {
      if (column.dtype === "str") {
        const best = new Array(nGroups).fill(null);
        for (let i = 0; i < n; i++) {
          if (!column.isValid(i)) continue;
          const g = ids[i];
          const s = column.dict[column.data[i]];
          if (best[g] === null || (op === "min" ? s < best[g] : s > best[g])) best[g] = s;
        }
        return best;
      }
      const best = new Float64Array(nGroups).fill(op === "min" ? Infinity : -Infinity);
      const saw = new Uint8Array(nGroups);
      for (let i = 0; i < n; i++) {
        if (!column.isValid(i)) continue;
        const g = ids[i];
        const v = column.data[i];
        if (op === "min" ? v < best[g] : v > best[g]) best[g] = v;
        saw[g] = 1;
      }
      const out = new Array(nGroups);
      for (let g = 0; g < nGroups; g++) {
        out[g] = saw[g] ? (column.dtype === "date" ? new Date(best[g]) : best[g]) : null;
      }
      return out;
    }
    case "first":
    case "last": {
      const idx = new Int32Array(nGroups).fill(-1);
      if (op === "first") {
        for (let i = 0; i < n; i++) {
          if (column.isValid(i) && idx[ids[i]] === -1) idx[ids[i]] = i;
        }
      } else {
        for (let i = 0; i < n; i++) if (column.isValid(i)) idx[ids[i]] = i;
      }
      const out = new Array(nGroups);
      for (let g = 0; g < nGroups; g++) out[g] = idx[g] === -1 ? null : column.get(idx[g]);
      return out;
    }
    case "nUnique": {
      const sets = new Array(nGroups);
      for (let i = 0; i < n; i++) {
        if (!column.isValid(i)) continue;
        const g = ids[i];
        (sets[g] ?? (sets[g] = new Set())).add(column.data[i]);
      }
      const out = new Array(nGroups);
      for (let g = 0; g < nGroups; g++) out[g] = sets[g] ? sets[g].size : 0;
      return out;
    }
    case "std":
    case "var": {
      const num = toNumeric(column);
      const sums = new Float64Array(nGroups);
      const sumsq = new Float64Array(nGroups);
      const counts = new Float64Array(nGroups);
      for (let i = 0; i < n; i++) {
        if (!num.isValid(i)) continue;
        const g = ids[i];
        const v = num.data[i];
        sums[g] += v;
        sumsq[g] += v * v;
        counts[g]++;
      }
      const out = new Array(nGroups);
      for (let g = 0; g < nGroups; g++) {
        const c = counts[g];
        if (c < 2) {
          out[g] = null;
          continue;
        }
        const variance = (sumsq[g] - (sums[g] * sums[g]) / c) / (c - 1);
        out[g] = op === "var" ? variance : Math.sqrt(Math.max(0, variance));
      }
      return out;
    }
    case "median":
    case "quantile":
    case "implode": {
      // These need to see every group member, so build row lists once and
      // stash them in the cache for the next order-statistic in line.
      const rows = cache.rows ?? (cache.rows = groupRowIndices(ids, nGroups));
      const p = op === "median" ? 0.5 : param;
      const out = new Array(nGroups);
      for (let g = 0; g < nGroups; g++) {
        if (op === "implode") {
          const vals = [];
          const r = rows[g];
          for (let k = 0; k < r.length; k++) if (column.isValid(r[k])) vals.push(column.get(r[k]));
          out[g] = vals;
        } else {
          const vals = [];
          const r = rows[g];
          for (let k = 0; k < r.length; k++) if (column.isValid(r[k])) vals.push(column.data[r[k]]);
          out[g] = quantileOf(vals, p);
        }
      }
      return out;
    }
    default:
      throw new Error(`Unknown grouped aggregation "${op}".`);
  }
}

// --------------------------------------------------------------------------
// element-wise kernels
// --------------------------------------------------------------------------

const ARITH = {
  add: (a, b) => a + b,
  sub: (a, b) => a - b,
  mul: (a, b) => a * b,
  div: (a, b) => a / b,
  mod: (a, b) => a % b,
  pow: (a, b) => a ** b,
};

const COMPARE = {
  gt: (a, b) => a > b,
  gte: (a, b) => a >= b,
  lt: (a, b) => a < b,
  lte: (a, b) => a <= b,
  eq: (a, b) => a === b,
  neq: (a, b) => a !== b,
};

function evalBinary(node, frame) {
  const { op } = node;

  if (op === "fillNull") {
    const input = evalExpr(node.left, frame);
    const fill = evalExpr(node.right, frame);
    return evalFillNull(input, fill);
  }

  const left = evalExpr(node.left, frame);
  const right = evalExpr(node.right, frame);
  const n = frame.height;

  if (op === "and" || op === "or") {
    const out = new Uint8Array(n);
    const validity = mergeValidity(left.validity, right.validity, n);
    if (op === "and") for (let i = 0; i < n; i++) out[i] = left.data[i] & right.data[i];
    else for (let i = 0; i < n; i++) out[i] = left.data[i] | right.data[i];
    return new Column("bool", out, { validity });
  }

  if (op in COMPARE) {
    // String comparison goes through the dictionary; everything else is
    // straight typed-array numbers (dates included, since they're epoch ms).
    const cmp = COMPARE[op];
    const out = new Uint8Array(n);
    const validity = mergeValidity(left.validity, right.validity, n);
    if (left.dtype === "str" || right.dtype === "str") {
      if (left.dtype !== "str" || right.dtype !== "str") {
        throw new Error(`Comparing "${left.dtype}" to "${right.dtype}" is a category error, literally.`);
      }
      if ((op === "eq" || op === "neq") && left.dict === right.dict) {
        // Same dictionary: comparing codes is comparing strings. Free lunch.
        for (let i = 0; i < n; i++) out[i] = cmp(left.data[i], right.data[i]) ? 1 : 0;
      } else {
        for (let i = 0; i < n; i++) {
          out[i] = cmp(left.dict[left.data[i]], right.dict[right.data[i]]) ? 1 : 0;
        }
      }
      return new Column("bool", out, { validity });
    }
    const a = left.data;
    const b = right.data;
    for (let i = 0; i < n; i++) out[i] = cmp(a[i], b[i]) ? 1 : 0;
    return new Column("bool", out, { validity });
  }

  if (op in ARITH) {
    const a = toNumeric(left);
    const b = toNumeric(right);
    const fn = ARITH[op];
    const out = new Float64Array(n);
    const validity = mergeValidity(a.validity, b.validity, n);
    const da = a.data;
    const db = b.data;
    for (let i = 0; i < n; i++) out[i] = fn(da[i], db[i]);
    return new Column("f64", out, { validity });
  }

  throw new Error(`Unknown binary op "${op}".`);
}

function evalUnary(node, frame) {
  const input = evalExpr(node.input, frame);
  const n = input.length;

  switch (node.op) {
    case "not": {
      const out = new Uint8Array(n);
      for (let i = 0; i < n; i++) out[i] = input.data[i] ? 0 : 1;
      return new Column("bool", out, { validity: cloneValidity(input.validity) });
    }
    case "isNull": {
      const out = new Uint8Array(n);
      if (input.validity) for (let i = 0; i < n; i++) out[i] = input.validity[i] ? 0 : 1;
      return new Column("bool", out);
    }
    case "isNotNull": {
      const out = new Uint8Array(n).fill(1);
      if (input.validity) for (let i = 0; i < n; i++) out[i] = input.validity[i];
      return new Column("bool", out);
    }
    case "neg":
    case "abs":
    case "log":
    case "exp":
    case "sqrt":
    case "round": {
      const num = toNumeric(input);
      const out = new Float64Array(n);
      const d = num.data;
      switch (node.op) {
        case "neg":
          for (let i = 0; i < n; i++) out[i] = -d[i];
          break;
        case "abs":
          for (let i = 0; i < n; i++) out[i] = Math.abs(d[i]);
          break;
        case "log":
          for (let i = 0; i < n; i++) out[i] = Math.log(d[i]);
          break;
        case "exp":
          for (let i = 0; i < n; i++) out[i] = Math.exp(d[i]);
          break;
        case "sqrt":
          for (let i = 0; i < n; i++) out[i] = Math.sqrt(d[i]);
          break;
        case "round": {
          const factor = 10 ** (node.param ?? 0);
          for (let i = 0; i < n; i++) out[i] = Math.round(d[i] * factor) / factor;
          break;
        }
      }
      return new Column("f64", out, { validity: cloneValidity(num.validity) });
    }
    default:
      throw new Error(`Unknown unary op "${node.op}".`);
  }
}

function evalIsIn(input, values) {
  const n = input.length;
  const out = new Uint8Array(n);
  if (input.dtype === "str") {
    // Test the dictionary once, not every row. The dictionary is small;
    // your data, presumably, is not.
    const wanted = new Set(values.map(String));
    const dictHit = new Uint8Array(input.dict.length);
    for (let c = 0; c < input.dict.length; c++) dictHit[c] = wanted.has(input.dict[c]) ? 1 : 0;
    for (let i = 0; i < n; i++) out[i] = dictHit[input.data[i]];
  } else {
    const wanted = new Set(values.map((v) => (v instanceof Date ? v.getTime() : v)));
    for (let i = 0; i < n; i++) out[i] = wanted.has(input.data[i]) ? 1 : 0;
  }
  // Nulls are in nothing, not even {null}.
  if (input.validity) for (let i = 0; i < n; i++) out[i] &= input.validity[i];
  return new Column("bool", out);
}

function evalFillNull(input, fill) {
  if (input.validity === null) return input;
  const n = input.length;
  // Same dtype fast path: copy data, patch holes.
  if (fill.dtype === input.dtype || (isNumericDtype(fill.dtype) && isNumericDtype(input.dtype))) {
    const out = input.toArray();
    for (let i = 0; i < n; i++) if (!input.isValid(i)) out[i] = fill.get(i);
    return Column.from(out, fill.dtype === input.dtype ? input.dtype : undefined);
  }
  const out = new Array(n);
  for (let i = 0; i < n; i++) out[i] = input.isValid(i) ? input.get(i) : fill.get(i);
  return Column.from(out);
}

function evalTernary(cond, thenCol, elseCol) {
  const n = cond.length;
  // General path via JS values: correctness first; the branchy typed-array
  // version can come when a profiler asks for it by name.
  const out = new Array(n);
  for (let i = 0; i < n; i++) {
    const c = cond.isValid(i) && cond.data[i] === 1;
    out[i] = c ? thenCol.get(i) : elseCol.get(i);
  }
  return Column.from(out);
}

function evalStr(node, frame) {
  const input = evalExpr(node.input, frame);
  if (input.dtype !== "str") {
    throw new Error(`.str operations need a "str" column; got "${input.dtype}". Cast first, or reconsider.`);
  }
  const n = input.length;
  const dictSize = input.dict.length;
  const [a, b] = node.args ?? [];

  // Compute per-dictionary-entry, then gather by code. The dictionary has
  // one entry per unique string, so "lowercase a million rows" becomes
  // "lowercase forty strings and shuffle integers". You're welcome.
  const perDict = (fn) => {
    const mapped = new Array(dictSize);
    for (let c = 0; c < dictSize; c++) mapped[c] = fn(input.dict[c]);
    return mapped;
  };

  switch (node.op) {
    case "lower":
    case "upper":
    case "strip":
    case "slice":
    case "replace": {
      const fn =
        node.op === "lower"
          ? (s) => s.toLowerCase()
          : node.op === "upper"
            ? (s) => s.toUpperCase()
            : node.op === "strip"
              ? (s) => s.trim()
              : node.op === "slice"
                ? (s) => s.slice(a, b)
                : (s) => s.replaceAll(a, b);
      const mapped = perDict(fn);
      // Re-encode: transformed strings may collide (e.g. "A" and "a" → "a").
      const dict = [];
      const codes = new Map();
      const remap = new Uint32Array(dictSize);
      for (let c = 0; c < dictSize; c++) {
        const s = mapped[c];
        let code = codes.get(s);
        if (code === undefined) {
          code = dict.length;
          codes.set(s, code);
          dict.push(s);
        }
        remap[c] = code;
      }
      const data = new Uint32Array(n);
      for (let i = 0; i < n; i++) data[i] = remap[input.data[i]];
      return new Column("str", data, { validity: cloneValidity(input.validity), dict });
    }
    case "len": {
      const lens = perDict((s) => s.length);
      const out = new Float64Array(n);
      for (let i = 0; i < n; i++) out[i] = lens[input.data[i]];
      return new Column("f64", out, { validity: cloneValidity(input.validity) });
    }
    case "contains":
    case "startsWith":
    case "endsWith": {
      const test =
        node.op === "contains"
          ? a instanceof RegExp
            ? (s) => a.test(s)
            : (s) => s.includes(a)
          : node.op === "startsWith"
            ? (s) => s.startsWith(a)
            : (s) => s.endsWith(a);
      const hits = perDict((s) => (test(s) ? 1 : 0));
      const out = new Uint8Array(n);
      for (let i = 0; i < n; i++) out[i] = hits[input.data[i]];
      if (input.validity) for (let i = 0; i < n; i++) out[i] &= input.validity[i];
      return new Column("bool", out);
    }
    default:
      throw new Error(`Unknown string op "${node.op}".`);
  }
}

function evalDt(node, frame) {
  const input = evalExpr(node.input, frame);
  if (input.dtype !== "date") {
    throw new Error(`.dt operations need a "date" column; got "${input.dtype}".`);
  }
  const n = input.length;
  const out = new Float64Array(n);
  for (let i = 0; i < n; i++) {
    const d = new Date(input.data[i]);
    switch (node.op) {
      case "year":
        out[i] = d.getFullYear();
        break;
      case "month":
        out[i] = d.getMonth() + 1; // humans count from 1; we side with humans
        break;
      case "day":
        out[i] = d.getDate();
        break;
      case "weekday":
        out[i] = d.getDay();
        break;
      case "hour":
        out[i] = d.getHours();
        break;
      default:
        throw new Error(`Unknown date op "${node.op}".`);
    }
  }
  return new Column("f64", out, { validity: cloneValidity(input.validity) });
}

/**
 * Row lists per partition for a windowed .over(keys): one Uint32Array of
 * original row indices per distinct key tuple, each in row order. Null
 * keys group together, into a partition of their own.
 */
function partitionRows(keys, frame) {
  const columns = keys.map((k) => frame.getColumn(k));
  const { ids, nGroups } = groupIds(columns);
  return groupRowIndices(ids, nGroups);
}

function identityRows(n) {
  const rows = new Uint32Array(n);
  for (let i = 0; i < n; i++) rows[i] = i;
  return rows;
}

function evalWindow(node, frame) {
  const input = evalExpr(node.input, frame);
  const n = input.length;
  // Without .over(), the whole column is one partition; the per-partition
  // loops below then read straight through in row order, exactly as the
  // ungrouped implementation always did.
  const partitions = node.over ? partitionRows(node.over, frame) : [identityRows(n)];

  switch (node.op) {
    case "shift": {
      const k = node.param ?? 1;
      const data = new input.data.constructor(n);
      const validity = new Uint8Array(n);
      for (const rows of partitions) {
        const m = rows.length;
        for (let i = 0; i < m; i++) {
          const src = i - k;
          if (src >= 0 && src < m && input.isValid(rows[src])) {
            data[rows[i]] = input.data[rows[src]];
            validity[rows[i]] = 1;
          }
        }
      }
      return new Column(input.dtype, data, { validity, dict: input.dict });
    }
    case "cumSum": {
      const num = toNumeric(input);
      const out = new Float64Array(n);
      for (const rows of partitions) {
        let acc = 0;
        for (let i = 0; i < rows.length; i++) {
          const r = rows[i];
          if (num.isValid(r)) acc += num.data[r];
          out[r] = acc;
        }
      }
      return new Column("f64", out, { validity: cloneValidity(num.validity) });
    }
    case "diff": {
      const k = node.param ?? 1;
      const num = toNumeric(input);
      const out = new Float64Array(n);
      const validity = new Uint8Array(n);
      for (const rows of partitions) {
        for (let i = k; i < rows.length; i++) {
          const r = rows[i];
          const prev = rows[i - k];
          if (num.isValid(r) && num.isValid(prev)) {
            out[r] = num.data[r] - num.data[prev];
            validity[r] = 1;
          }
        }
      }
      return new Column("f64", out, { validity });
    }
    case "rollingMean": {
      const w = node.param;
      const num = toNumeric(input);
      const out = new Float64Array(n);
      const validity = new Uint8Array(n);
      for (const rows of partitions) {
        let sum = 0;
        let count = 0;
        for (let i = 0; i < rows.length; i++) {
          const r = rows[i];
          if (num.isValid(r)) {
            sum += num.data[r];
            count++;
          }
          if (i >= w) {
            const old = rows[i - w];
            if (num.isValid(old)) {
              sum -= num.data[old];
              count--;
            }
          }
          if (i >= w - 1 && count > 0) {
            out[r] = sum / count;
            validity[r] = 1;
          }
        }
      }
      return new Column("f64", out, { validity });
    }
    case "rank": {
      // Ascending rank by value, ties averaged, nulls null. Strings rank
      // by dictionary lookup; everything else by the physical number.
      const out = new Float64Array(n);
      const validity = new Uint8Array(n);
      const valueOf =
        input.dtype === "str" ? (r) => input.dict[input.data[r]] : (r) => input.data[r];
      for (const rows of partitions) {
        const valid = [];
        for (let i = 0; i < rows.length; i++) if (input.isValid(rows[i])) valid.push(rows[i]);
        valid.sort((a, b) => {
          const va = valueOf(a);
          const vb = valueOf(b);
          return va < vb ? -1 : va > vb ? 1 : 0;
        });
        for (let i = 0; i < valid.length; ) {
          let j = i;
          while (j + 1 < valid.length && valueOf(valid[j + 1]) === valueOf(valid[i])) j++;
          const avg = (i + j) / 2 + 1; // 1-based, ties share the mean rank
          for (let k = i; k <= j; k++) {
            out[valid[k]] = avg;
            validity[valid[k]] = 1;
          }
          i = j + 1;
        }
      }
      return new Column("f64", out, { validity });
    }
    default:
      throw new Error(`Unknown window op "${node.op}".`);
  }
}

// --------------------------------------------------------------------------
// helpers
// --------------------------------------------------------------------------

/** Binary op on two scalars, for arithmetic over aggregation results. */
function scalarBinary(op, a, b) {
  if (a == null || b == null) return null;
  if (op in ARITH) return ARITH[op](a, b);
  if (op in COMPARE) return COMPARE[op](a, b);
  if (op === "and") return Boolean(a && b);
  if (op === "or") return Boolean(a || b);
  throw new Error(`Unknown scalar op "${op}".`);
}

/** Unary op on a scalar. Same idea, half the operands. */
function scalarUnary(op, v, param) {
  if (v == null) return null;
  switch (op) {
    case "neg":
      return -v;
    case "abs":
      return Math.abs(v);
    case "log":
      return Math.log(v);
    case "exp":
      return Math.exp(v);
    case "sqrt":
      return Math.sqrt(v);
    case "round": {
      const f = 10 ** (param ?? 0);
      return Math.round(v * f) / f;
    }
    case "not":
      return !v;
    default:
      throw new Error(`Unknown scalar unary op "${op}".`);
  }
}

function broadcastLiteral(value, n) {
  if (value == null) {
    return new Column("f64", new Float64Array(n), { validity: new Uint8Array(n) });
  }
  if (typeof value === "number") {
    return new Column("f64", new Float64Array(n).fill(value));
  }
  if (typeof value === "boolean") {
    return new Column("bool", new Uint8Array(n).fill(value ? 1 : 0));
  }
  if (value instanceof Date) {
    return new Column("date", new Float64Array(n).fill(value.getTime()));
  }
  const s = String(value);
  return new Column("str", new Uint32Array(n), { dict: [s] });
}

/** Numeric view of a column: f64/i32/date/bool pass through; str refuses. */
function toNumeric(column) {
  if (column.dtype === "str") {
    throw new Error(`This operation wants numbers, and "str" is famously not one.`);
  }
  return column;
}

function isNumericDtype(dtype) {
  return dtype === "f64" || dtype === "i32" || dtype === "bool" || dtype === "date";
}

function mergeValidity(a, b, n) {
  if (a === null && b === null) return null;
  const out = new Uint8Array(n);
  if (a === null) out.set(b);
  else if (b === null) out.set(a);
  else for (let i = 0; i < n; i++) out[i] = a[i] & b[i];
  return out;
}

function cloneValidity(validity) {
  return validity === null ? null : validity.slice();
}

function collectValid(column) {
  const out = [];
  for (let i = 0; i < column.length; i++) if (column.isValid(i)) out.push(column.data[i]);
  return out;
}

function quantileOf(values, p) {
  if (values.length === 0) return null;
  values.sort((a, b) => a - b);
  const idx = (values.length - 1) * p;
  const lo = Math.floor(idx);
  const hi = Math.ceil(idx);
  if (lo === hi) return values[lo];
  return values[lo] + (values[hi] - values[lo]) * (idx - lo);
}

function sumStats(column) {
  const num = toNumeric(column);
  let sum = 0;
  let sumsq = 0;
  let count = 0;
  for (let i = 0; i < num.length; i++) {
    if (!num.isValid(i)) continue;
    const v = num.data[i];
    sum += v;
    sumsq += v * v;
    count++;
  }
  if (count < 2) return { variance: null };
  return { variance: (sumsq - (sum * sum) / count) / (count - 1) };
}

export { quantileOf };
