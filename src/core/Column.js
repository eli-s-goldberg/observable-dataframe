/**
 * Column.js — typed, columnar storage.
 *
 * A Column stores values the way computers actually enjoy them: contiguous,
 * typed, and free of per-row object allocations. Strings are
 * dictionary-encoded (each unique string stored once, rows store integer
 * codes), dates are epoch milliseconds, and nulls live in a validity mask
 * instead of masquerading as NaN or "" like they used to. We don't talk
 * about how it used to be.
 *
 * Supported dtypes:
 *   - "f64":  Float64Array
 *   - "i32":  Int32Array
 *   - "bool": Uint8Array (0/1, because JavaScript has no bit to spare)
 *   - "str":  Uint32Array codes + a dictionary of unique strings
 *   - "date": Float64Array of epoch milliseconds
 */

const DTYPES = new Set(["f64", "i32", "bool", "str", "date"]);

const DTYPE_ALIASES = new Map([
  ["string", "str"],
  ["str", "str"],
  ["text", "str"],
  ["varchar", "str"],
  ["int", "i32"],
  ["integer", "i32"],
  ["i32", "i32"],
  ["float", "f64"],
  ["double", "f64"],
  ["number", "f64"],
  ["f64", "f64"],
  ["bool", "bool"],
  ["boolean", "bool"],
  ["date", "date"],
  ["datetime", "date"],
]);

/** Map pandas/polars-style dtype names to internal codes. */
export function normalizeDtype(dtype) {
  if (dtype == null) return dtype;
  const key = String(dtype).toLowerCase();
  return DTYPE_ALIASES.get(key) ?? key;
}

/**
 * A single typed column with an optional validity mask.
 *
 * You will rarely construct one of these by hand — that's what
 * `Column.from()` and the DataFrame constructor are for. But you could.
 * Nobody's stopping you.
 */
export class Column {
  /**
   * @param {string} dtype one of "f64" | "i32" | "bool" | "str" | "date"
   * @param {Float64Array|Int32Array|Uint8Array|Uint32Array} data typed backing store
   * @param {object} [options]
   * @param {Uint8Array|null} [options.validity] 1 = valid, 0 = null. Omit if everything is valid (how nice for you).
   * @param {string[]} [options.dict] dictionary for "str" columns; codes index into this
   */
  constructor(dtype, data, { validity = null, dict = null } = {}) {
    dtype = normalizeDtype(dtype);
    if (!DTYPES.has(dtype)) {
      throw new Error(
        `Unknown dtype "${dtype}". Your options are: ${[...DTYPES].join(", ")}. Choose wisely.`
      );
    }
    if (dtype === "str" && !dict) {
      throw new Error(`A "str" column without a dictionary is just a pile of meaningless integers.`);
    }
    this.dtype = dtype;
    this.data = data;
    this.validity = validity;
    this.dict = dict;
  }

  /** Number of values, null or otherwise. */
  get length() {
    return this.data.length;
  }

  /**
   * Build a Column from a plain JavaScript array, inferring the dtype by
   * inspecting the values. Inference rules, in order of prejudice:
   * booleans → "bool", Dates → "date", numbers → "i32" if they all happen
   * to be safe integers, else "f64", everything else → "str".
   *
   * @param {Array} values plain JS values; null/undefined become nulls
   * @param {string} [dtype] skip inference and assert a dtype, if you know better (you might!)
   * @returns {Column}
   */
  static from(values, dtype = null) {
    const n = values.length;
    if (dtype != null) dtype = normalizeDtype(dtype);
    if (dtype == null) dtype = inferDtype(values);
    let validity = null;
    const markNull = (i) => {
      if (!validity) validity = new Uint8Array(n).fill(1);
      validity[i] = 0;
    };

    switch (dtype) {
      case "f64": {
        const data = new Float64Array(n);
        for (let i = 0; i < n; i++) {
          const v = values[i];
          if (v == null || (typeof v === "number" && Number.isNaN(v))) markNull(i);
          else data[i] = Number(v);
        }
        return new Column("f64", data, { validity });
      }
      case "i32": {
        const data = new Int32Array(n);
        for (let i = 0; i < n; i++) {
          const v = values[i];
          if (v == null || (typeof v === "number" && Number.isNaN(v))) markNull(i);
          else data[i] = v | 0;
        }
        return new Column("i32", data, { validity });
      }
      case "bool": {
        const data = new Uint8Array(n);
        for (let i = 0; i < n; i++) {
          const v = values[i];
          if (v == null) markNull(i);
          else data[i] = v ? 1 : 0;
        }
        return new Column("bool", data, { validity });
      }
      case "date": {
        const data = new Float64Array(n);
        for (let i = 0; i < n; i++) {
          const v = values[i];
          const t =
            v == null
              ? NaN
              : v instanceof Date
                ? v.getTime()
                : typeof v === "number"
                  ? v
                  : Date.parse(v);
          if (Number.isNaN(t)) markNull(i);
          else data[i] = t;
        }
        return new Column("date", data, { validity });
      }
      case "str": {
        const data = new Uint32Array(n);
        const dict = [];
        const codes = new Map();
        for (let i = 0; i < n; i++) {
          const v = values[i];
          if (v == null) {
            markNull(i);
            continue;
          }
          const s = typeof v === "string" ? v : String(v);
          let code = codes.get(s);
          if (code === undefined) {
            code = dict.length;
            codes.set(s, code);
            dict.push(s);
          }
          data[i] = code;
        }
        return new Column("str", data, { validity, dict });
      }
      default:
        throw new Error(`Unhandled dtype "${dtype}". This should be impossible. Congratulations.`);
    }
  }

  /** Is row i a real value (true) or a polite absence (false)? */
  isValid(i) {
    return this.validity === null || this.validity[i] === 1;
  }

  /** Count of nulls. Zero, ideally, but data rarely cooperates. */
  nullCount() {
    if (this.validity === null) return 0;
    let c = 0;
    for (let i = 0; i < this.validity.length; i++) c += 1 - this.validity[i];
    return c;
  }

  /**
   * Get the JS value at row i: string for "str", Date for "date", boolean
   * for "bool", number otherwise, and null when the data has nothing to say.
   */
  get(i) {
    if (!this.isValid(i)) return null;
    switch (this.dtype) {
      case "str":
        return this.dict[this.data[i]];
      case "date":
        return new Date(this.data[i]);
      case "bool":
        return this.data[i] === 1;
      default:
        return this.data[i];
    }
  }

  /**
   * Gather rows by index — the workhorse behind filter, sort, join, and
   * every other operation that shuffles rows without touching values.
   * Dictionary is shared, not copied, because we are not made of memory.
   *
   * @param {Uint32Array|number[]} indices row indices to keep, in order
   * @returns {Column}
   */
  take(indices) {
    const n = indices.length;
    const data = new this.data.constructor(n);
    for (let i = 0; i < n; i++) data[i] = this.data[indices[i]];
    let validity = null;
    if (this.validity !== null) {
      validity = new Uint8Array(n);
      for (let i = 0; i < n; i++) validity[i] = this.validity[indices[i]];
      if (allValid(validity)) validity = null;
    }
    return new Column(this.dtype, data, { validity, dict: this.dict });
  }

  /** Contiguous slice, [start, end). Like Array.slice but less nostalgic. */
  slice(start, end) {
    const data = this.data.slice(start, end);
    const validity = this.validity ? this.validity.slice(start, end) : null;
    return new Column(this.dtype, data, { validity, dict: this.dict });
  }

  /**
   * Materialize the whole column as a plain JS array. This is the exit ramp
   * to Plot, tables, and anything else that wants objects — use it at the
   * edges, not in the middle of a pipeline.
   */
  toArray() {
    const n = this.length;
    const out = new Array(n);
    for (let i = 0; i < n; i++) out[i] = this.get(i);
    return out;
  }

  /**
   * Logical JS values, the sanctioned accessor for extension modules:
   * strings decoded from the dictionary, Dates constructed from epoch ms,
   * booleans as booleans, and null where the validity mask says so. With
   * `validOnly: true` the nulls are skipped entirely, which is what every
   * statistic wants anyway. Prefer this over reading `.data` raw; codes
   * are not strings, no matter how confident the loop looks.
   *
   * @param {{validOnly?: boolean}} [options]
   * @returns {Array}
   */
  values({ validOnly = false } = {}) {
    if (!validOnly) return this.toArray();
    const n = this.length;
    const out = [];
    for (let i = 0; i < n; i++) if (this.isValid(i)) out.push(this.get(i));
    return out;
  }

  /**
   * Cast to another dtype, converting values in the least surprising way we
   * could think of. Casting "str" to "f64" parses numbers; failures become
   * nulls rather than exceptions, because you were going to fillNull anyway.
   *
   * @param {string} dtype target dtype
   * @returns {Column}
   */
  cast(dtype) {
    if (dtype === this.dtype) return this;
    return Column.from(this.toArray(), normalizeDtype(dtype));
  }

  /**
   * Integer group codes for this column, used by groupBy and join. For
   * "str" we reuse dictionary codes (free!); for everything else we build
   * a code table on the fly. Nulls share a single code, so they group
   * together — misery loves company.
   *
   * @returns {{codes: Int32Array, nCodes: number}}
   */
  groupCodes() {
    const n = this.length;
    const codes = new Int32Array(n);
    if (this.dtype === "str" && this.validity === null) {
      // Dictionary codes are already dense integers. Take the win.
      for (let i = 0; i < n; i++) codes[i] = this.data[i];
      return { codes, nCodes: this.dict.length };
    }
    const seen = new Map();
    let next = 0;
    const NULL_KEY = Symbol.for("odf.null");
    for (let i = 0; i < n; i++) {
      const key = this.isValid(i) ? this.data[i] : NULL_KEY;
      let code = seen.get(key);
      if (code === undefined) {
        code = next++;
        seen.set(key, code);
      }
      codes[i] = code;
    }
    return { codes, nCodes: next };
  }
}

/** Sniff a dtype from sample values. Nulls abstain from voting. */
export function inferDtype(values) {
  let sawNumber = false;
  let sawFloat = false;
  let sawBool = false;
  let sawDate = false;
  let sawString = false;
  for (let i = 0; i < values.length; i++) {
    const v = values[i];
    if (v == null) continue;
    const t = typeof v;
    if (t === "number") {
      if (Number.isNaN(v)) continue;
      sawNumber = true;
      if (!Number.isInteger(v) || Math.abs(v) > 2147483647) sawFloat = true;
    } else if (t === "boolean") sawBool = true;
    else if (v instanceof Date) sawDate = true;
    else {
      sawString = true;
      break; // strings taint everything; no need to keep looking
    }
  }
  if (sawString) return "str";
  if (sawDate && !sawNumber && !sawBool) return "date";
  if (sawBool && !sawNumber && !sawDate) return "bool";
  if (sawNumber && !sawBool && !sawDate) return sawFloat ? "f64" : "i32";
  if (!sawNumber && !sawBool && !sawDate) return "f64"; // all null: pick something harmless
  return "str"; // mixed types: welcome to stringville, population: your data
}

function allValid(validity) {
  for (let i = 0; i < validity.length; i++) if (validity[i] === 0) return false;
  return true;
}
