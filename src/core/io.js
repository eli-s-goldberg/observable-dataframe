/**
 * io.js — getting data in and out without ceremony.
 *
 * CSV parsing rides on d3-dsv, which has spent a decade learning where the
 * commas hide. Type coercion happens per column after parsing, so a column
 * of "1", "2", "oops" honestly becomes strings instead of mostly-numbers-
 * with-a-surprise.
 */

import { csvParse, tsvParse, dsvFormat } from "d3-dsv";
import { normalizeDtype } from "./Column.js";
import { DataFrame } from "./DataFrame.js";

const ISO_DATE = /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2})?/;
const NUMERIC = /^-?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$/;

/**
 * Parse CSV text into a DataFrame with per-column type inference:
 * all-numeric columns become numbers, ISO-looking dates become dates,
 * "true"/"false" become booleans, and everything else stays a string,
 * as nature intended. Empty cells become nulls, not empty strings —
 * we've litigated this internally and the nulls won.
 *
 * @param {string} text raw CSV
 * @param {{delimiter?: string, dtypes?: Record<string, string>}} [options]
 * @returns {DataFrame}
 */
export function fromCSV(text, { delimiter = ",", dtypes = {} } = {}) {
  const parse =
    delimiter === "," ? csvParse : delimiter === "\t" ? tsvParse : dsvFormat(delimiter).parse;
  const rows = parse(text);
  const names = rows.columns ?? (rows.length ? Object.keys(rows[0]) : []);
  const out = {};
  const outDtypes = {};

  for (const name of names) {
    const raw = new Array(rows.length);
    for (let i = 0; i < rows.length; i++) {
      const v = rows[i][name];
      raw[i] = v === "" || v === undefined ? null : v;
    }
    if (dtypes[name]) {
      const dtype = normalizeDtype(dtypes[name]);
      out[name] = coerce(raw, dtype);
      outDtypes[name] = dtype;
      continue;
    }
    const kind = sniff(raw);
    out[name] = coerce(raw, kind);
    outDtypes[name] = kind;
  }
  return new DataFrame(out, { dtypes: outDtypes });
}

/**
 * Convenience for Observable: `await fromCSVUrl(FileAttachment("x.csv").url())`
 * or any other URL your CORS policy will tolerate.
 */
export async function fromCSVUrl(url, options) {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Fetching ${url} returned ${response.status}. The data is not coming.`);
  }
  return fromCSV(await response.text(), options);
}

/** Serialize a DataFrame back to CSV text. Dates become ISO strings; nulls become empty cells. */
export function toCSV(df, { delimiter = "," } = {}) {
  const names = df.columns;
  const esc = (v) => {
    if (v == null) return "";
    const s = v instanceof Date ? v.toISOString() : String(v);
    return s.includes(delimiter) || s.includes('"') || s.includes("\n")
      ? '"' + s.replaceAll('"', '""') + '"'
      : s;
  };
  const lines = [names.map(esc).join(delimiter)];
  const cols = names.map((n) => df.getColumn(n));
  for (let i = 0; i < df.height; i++) {
    lines.push(cols.map((c) => esc(c.get(i))).join(delimiter));
  }
  return lines.join("\n");
}

function sniff(values) {
  let sawNumber = false;
  let sawDate = false;
  let sawBool = false;
  let sawOther = false;
  let sawAny = false;
  for (const v of values) {
    if (v == null) continue;
    sawAny = true;
    if (typeof v !== "string") {
      // FileAttachment({typed: true}) may hand us real types already.
      if (typeof v === "number") sawNumber = true;
      else if (v instanceof Date) sawDate = true;
      else if (typeof v === "boolean") sawBool = true;
      else sawOther = true;
      continue;
    }
    if (NUMERIC.test(v)) sawNumber = true;
    else if (ISO_DATE.test(v)) sawDate = true;
    else if (v === "true" || v === "false") sawBool = true;
    else {
      sawOther = true;
      break;
    }
  }
  if (!sawAny || sawOther) return "str";
  if (sawNumber && !sawDate && !sawBool) return "f64";
  if (sawDate && !sawNumber && !sawBool) return "date";
  if (sawBool && !sawNumber && !sawDate) return "bool";
  return "str"; // mixed bag: strings, the dtype of last resort
}

function coerce(values, dtype) {
  dtype = normalizeDtype(dtype);
  switch (dtype) {
    case "f64":
    case "i32":
      return values.map((v) => (v == null ? null : typeof v === "number" ? v : Number(v)));
    case "date":
      return values.map((v) =>
        v == null ? null : v instanceof Date ? v : new Date(v)
      );
    case "bool":
      return values.map((v) =>
        v == null ? null : typeof v === "boolean" ? v : v === "true"
      );
    case "str":
      return values.map((v) => (v == null ? null : String(v)));
    default:
      return values.map((v) => (v == null ? null : String(v)));
  }
}
