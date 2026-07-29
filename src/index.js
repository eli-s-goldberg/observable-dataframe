/**
 * observable-dataframe — a columnar, expression-based DataFrame for the
 * browser. Polars-style syntax, typed-array internals, zero servers harmed.
 *
 *   import { DataFrame, col, lit, when } from "observable-dataframe";
 *
 *   const df = DataFrame.fromRows(rows)
 *     .filter(col("age").gt(25))
 *     .groupBy("city")
 *     .agg(col("income").mean().alias("avg_income"));
 *
 * Statistics live in "observable-dataframe/stats", plot primitives in
 * "observable-dataframe/plots", claims panel helpers in
 * "observable-dataframe/data", and page layout helpers in
 * "observable-dataframe/layouts". The core stays dependency-light on purpose;
 * it has no idea what a pixel is.
 */

export { DataFrame, GroupBy } from "./core/DataFrame.js";
export { Column, inferDtype, normalizeDtype } from "./core/Column.js";
export { col, lit, when, Expr } from "./core/expr.js";
export { fromCSV, fromCSVUrl, toCSV } from "./core/io.js";
export { random } from "./core/random.js";
