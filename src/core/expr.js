/**
 * expr.js — the expression tree, or: how to describe work without doing it.
 *
 * Expressions are little immutable descriptions of computations over
 * columns. `col("age").gt(25)` doesn't compare anything; it builds a tree
 * that the engine later evaluates in one vectorized pass, on typed arrays,
 * without ever constructing a row object. This is the entire trick behind
 * polars, and now it's ours too. Imitation, flattery, etc.
 *
 * Usage:
 *   df.filter(col("age").gt(25).and(col("city").isIn(["NYC", "SF"])))
 *   df.withColumns(col("income").div(12).alias("monthly"))
 *   df.groupBy("city").agg(col("income").mean().alias("avg_income"))
 */

/**
 * An expression node with a fluent API. Every method returns a *new* Expr;
 * nothing here mutates, so feel free to reuse subexpressions like the
 * responsible adult you occasionally are.
 */
export class Expr {
  /** @param {object} node internal tree node. You never build these directly. */
  constructor(node) {
    this.node = node;
  }

  // ---- arithmetic: the four food groups, plus seasoning -------------------

  /** this + other. Numbers only; we're not doing JavaScript's "1" + 1 here. */
  add(other) {
    return binary("add", this, other);
  }
  /** this - other. */
  sub(other) {
    return binary("sub", this, other);
  }
  /** this * other. */
  mul(other) {
    return binary("mul", this, other);
  }
  /** this / other. Division by zero gives you Infinity, as tradition demands. */
  div(other) {
    return binary("div", this, other);
  }
  /** this % other. */
  mod(other) {
    return binary("mod", this, other);
  }
  /** this ** other. */
  pow(other) {
    return binary("pow", this, other);
  }
  /** Negation. The unary minus you know and tolerate. */
  neg() {
    return unary("neg", this);
  }
  /** Absolute value. */
  abs() {
    return unary("abs", this);
  }
  /** Natural log. Non-positive inputs return NaN and a silent judgment. */
  log() {
    return unary("log", this);
  }
  /** e^x. */
  exp() {
    return unary("exp", this);
  }
  /** Square root. */
  sqrt() {
    return unary("sqrt", this);
  }
  /** Round to n decimal places (default 0). */
  round(decimals = 0) {
    return new Expr({ kind: "unary", op: "round", input: this.node, param: decimals });
  }

  // ---- comparison: where filters come from --------------------------------

  /** this > other */
  gt(other) {
    return binary("gt", this, other);
  }
  /** this >= other */
  gte(other) {
    return binary("gte", this, other);
  }
  /** this < other */
  lt(other) {
    return binary("lt", this, other);
  }
  /** this <= other */
  lte(other) {
    return binary("lte", this, other);
  }
  /** this == other (with null-safe semantics: null equals nothing, not even itself) */
  eq(other) {
    return binary("eq", this, other);
  }
  /** this != other */
  neq(other) {
    return binary("neq", this, other);
  }

  // ---- boolean algebra -----------------------------------------------------

  /** Logical AND. Both sides must be boolean expressions, obviously. */
  and(other) {
    return binary("and", this, other);
  }
  /** Logical OR. */
  or(other) {
    return binary("or", this, other);
  }
  /** Logical NOT. Turns your carefully crafted condition inside out. */
  not() {
    return unary("not", this);
  }

  // ---- null wrangling ------------------------------------------------------

  /** True where the value is null. For finding the holes in your data. */
  isNull() {
    return unary("isNull", this);
  }
  /** True where the value is not null. For finding the data in your holes. */
  isNotNull() {
    return unary("isNotNull", this);
  }
  /**
   * Replace nulls with a value. The data-science equivalent of spackle.
   * @param {*} value the filler; a literal or another expression
   */
  fillNull(value) {
    return new Expr({ kind: "binary", op: "fillNull", left: this.node, right: wrap(value).node });
  }

  // ---- membership & ranges -------------------------------------------------

  /**
   * True where the value is one of `values`. Backed by a Set, so go ahead
   * and pass a long list; we won't do anything quadratic about it.
   */
  isIn(values) {
    return new Expr({ kind: "isin", input: this.node, values: [...values] });
  }
  /** True where lo <= value <= hi. Inclusive on both ends, like a good hug. */
  between(lo, hi) {
    return new Expr({ kind: "between", input: this.node, lo, hi });
  }

  // ---- shape-shifting --------------------------------------------------------

  /** Cast to a dtype: "f64" | "i32" | "bool" | "str" | "date". */
  cast(dtype) {
    return new Expr({ kind: "cast", input: this.node, dtype });
  }
  /**
   * Name the output. Without an alias, derived columns get an auto-generated
   * name you will not enjoy typing.
   */
  alias(name) {
    return new Expr({ kind: "alias", input: this.node, name });
  }

  // ---- aggregations: many numbers in, one number out -------------------------

  /** Sum. Nulls are skipped, not summed — we're not monsters. */
  sum() {
    return agg("sum", this);
  }
  /** Arithmetic mean. */
  mean() {
    return agg("mean", this);
  }
  /** Minimum. */
  min() {
    return agg("min", this);
  }
  /** Maximum. */
  max() {
    return agg("max", this);
  }
  /** Count of non-null values. Honest counting. */
  count() {
    return agg("count", this);
  }
  /** Number of distinct values. */
  nUnique() {
    return agg("nUnique", this);
  }
  /** First non-null value in the group. */
  first() {
    return agg("first", this);
  }
  /** Last non-null value in the group. */
  last() {
    return agg("last", this);
  }
  /** Median, i.e. the 0.5 quantile with better PR. */
  median() {
    return agg("median", this);
  }
  /** Sample standard deviation (ddof = 1, like the textbooks). */
  std() {
    return agg("std", this);
  }
  /** Sample variance (ddof = 1). */
  var() {
    return agg("var", this);
  }
  /** Quantile in [0, 1], linear interpolation. */
  quantile(p) {
    return new Expr({ kind: "agg", op: "quantile", input: this.node, param: p });
  }
  /** Concatenate group values into a plain array. For when you must. */
  implode() {
    return agg("implode", this);
  }

  // ---- window-ish operations (whole-column, order-sensitive) -----------------

  /** Shift values down by n (or up for negative n). Vacated slots become null. */
  shift(n = 1) {
    return new Expr({ kind: "window", op: "shift", input: this.node, param: n });
  }
  /** Running total. */
  cumSum() {
    return new Expr({ kind: "window", op: "cumSum", input: this.node });
  }
  /** value[i] - value[i - n]. The poor man's derivative. */
  diff(n = 1) {
    return new Expr({ kind: "window", op: "diff", input: this.node, param: n });
  }
  /** Centered nowhere, trailing window mean of size n. Warm-up rows are null. */
  rollingMean(n) {
    return new Expr({ kind: "window", op: "rollingMean", input: this.node, param: n });
  }
  /** Rank by value, ascending, ties averaged. Nulls rank as null. */
  rank() {
    return new Expr({ kind: "window", op: "rank", input: this.node });
  }

  /**
   * Partition a window operation by key columns, polars-style: the window
   * runs within each partition in row order, and results land back on
   * their original rows. The panel workhorse:
   *
   *   col("paid").shift(1).over("member_id")   // previous month, per member
   *
   * Null keys form their own partition; multiple keys partition by the
   * tuple. Only window operations (shift, cumSum, diff, rollingMean, rank)
   * accept .over(); aggregations already have groupBy().
   *
   * @param {...string} keys partition columns
   * @returns {Expr}
   */
  over(...keys) {
    const flat = keys.flat();
    if (this.node.kind !== "window") {
      throw new Error(
        `.over() modifies window operations (shift, cumSum, diff, rollingMean, rank); for aggregations use groupBy().agg().`
      );
    }
    if (flat.length === 0) throw new Error(`.over() needs at least one key column.`);
    return new Expr({ ...this.node, over: flat });
  }

  // ---- escape hatch -----------------------------------------------------------

  /**
   * Apply an arbitrary JS function element-wise. This abandons every
   * optimization we worked so hard on, so use it when the vectorized ops
   * genuinely can't express what you need — not because typing `.mul(2)`
   * felt like too much ceremony.
   * @param {(value: any) => any} fn
   * @param {string} [dtype] output dtype hint, if you know it
   */
  map(fn, dtype = null) {
    return new Expr({ kind: "map", input: this.node, fn, dtype });
  }

  /** String operations. `col("name").str.lower()` and friends. */
  get str() {
    const self = this;
    return {
      /** Lowercase. */
      lower: () => strOp("lower", self),
      /** Uppercase. */
      upper: () => strOp("upper", self),
      /** String length in code units, because this is JavaScript. */
      len: () => strOp("len", self),
      /** True where the value contains `sub`. */
      contains: (sub) => strOp("contains", self, sub),
      /** True where the value starts with `prefix`. */
      startsWith: (prefix) => strOp("startsWith", self, prefix),
      /** True where the value ends with `suffix`. */
      endsWith: (suffix) => strOp("endsWith", self, suffix),
      /** Replace all occurrences of `search` (string or RegExp) with `replacement`. */
      replace: (search, replacement) => strOp("replace", self, search, replacement),
      /** Trim whitespace from both ends. */
      strip: () => strOp("strip", self),
      /** Slice characters [start, end). */
      slice: (start, end) => strOp("slice", self, start, end),
    };
  }

  /** Date parts. `col("when").dt.year()` etc. Local time, for better or worse. */
  get dt() {
    const self = this;
    return {
      year: () => dtOp("year", self),
      month: () => dtOp("month", self), // 1-12, not JavaScript's 0-11 prank
      day: () => dtOp("day", self),
      weekday: () => dtOp("weekday", self), // 0 = Sunday
      hour: () => dtOp("hour", self),
    };
  }
}

/**
 * Reference a column by name. The atom from which all expressions grow.
 * @param {string} name column name
 * @returns {Expr}
 */
export function col(name) {
  return new Expr({ kind: "col", name });
}

/**
 * A literal value, for when one side of your expression is just... a value.
 * You usually don't need this — `col("x").gt(5)` wraps the 5 for you — but
 * it's here for explicitness enthusiasts.
 * @param {*} value
 * @returns {Expr}
 */
export function lit(value) {
  return new Expr({ kind: "lit", value });
}

/**
 * Conditional expression: when(cond).then(a).otherwise(b).
 * A vectorized ternary, and the polite alternative to .map().
 *
 * @param {Expr} cond a boolean expression
 * @returns {{then: (value: any) => {otherwise: (value: any) => Expr}}}
 */
export function when(cond) {
  return {
    then(thenValue) {
      return {
        otherwise(elseValue) {
          return new Expr({
            kind: "ternary",
            cond: wrap(cond).node,
            then: wrap(thenValue).node,
            otherwise: wrap(elseValue).node,
          });
        },
      };
    },
  };
}

/**
 * The output name an expression will produce: its alias if you gave one,
 * the column name if it's a bare col(), or a synthesized name that should
 * motivate you to use .alias() next time.
 */
export function outputName(expr) {
  let node = expr instanceof Expr ? expr.node : expr;
  if (node.kind === "alias") return node.name;
  // walk down the leftmost spine looking for a column reference
  let cur = node;
  while (cur) {
    if (cur.kind === "col") return cur.name;
    cur = cur.input ?? cur.left ?? null;
  }
  return "literal";
}

/** Coerce a raw value into an Expr, leaving actual Exprs alone. */
export function wrap(value) {
  return value instanceof Expr ? value : lit(value);
}

function binary(op, left, right) {
  return new Expr({ kind: "binary", op, left: left.node, right: wrap(right).node });
}

function unary(op, input) {
  return new Expr({ kind: "unary", op, input: input.node });
}

function agg(op, input) {
  return new Expr({ kind: "agg", op, input: input.node });
}

function strOp(op, input, ...args) {
  return new Expr({ kind: "str", op, input: input.node, args });
}

function dtOp(op, input) {
  return new Expr({ kind: "dt", op, input: input.node });
}
