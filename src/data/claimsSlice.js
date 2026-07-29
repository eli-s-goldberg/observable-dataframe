/**
 * claimsSlice.js — load the published member-month claims slice into a DataFrame.
 *
 * Reads the CSV a data loader publishes, whether that CSV came from a local
 * extract or from the loader's synthetic fallback; the parser only cares about
 * the columns and dtypes. Everything downstream — describe, groupBy, joins with
 * experiment panels — starts here.
 */

import { DataFrame } from "../core/DataFrame.js";
import { col } from "../core/expr.js";
import { fromCSV } from "../core/io.js";

/** Default path to the published sample (source parquets stay gitignored). */
export const DEFAULT_CLAIMS_SLICE_PATH = "data/samples/claims_member_month.csv";

/**
 * Load member-month claims slice from CSV into a typed DataFrame.
 *
 * Columns: person_id, month, medical_claims, pharmacy_fills, pharmacy_paid,
 * enrolled_flag
 *
 * @param {string} text CSV contents
 */
export function claimsSliceFromCSV(text) {
  return fromCSV(text, {
    dtypes: {
      person_id: "str",
      month: "str",
      medical_claims: "i32",
      pharmacy_fills: "i32",
      pharmacy_paid: "f64",
      enrolled_flag: "i32",
    },
  });
}

/**
 * Filter to enrolled member-months only.
 *
 * @param {DataFrame} df
 */
export function enrolledMemberMonths(df) {
  return df.filter((row) => row.enrolled_flag === 1);
}

/**
 * Member-level rollup: total utilization and months of data per person.
 *
 * @param {DataFrame} df
 */
export function memberRollup(df) {
  return df
    .groupBy("person_id")
    .agg(
      col("medical_claims").sum().alias("medical_claims_total"),
      col("pharmacy_fills").sum().alias("pharmacy_fills_total"),
      col("pharmacy_paid").sum().alias("pharmacy_paid_total"),
      col("month").count().alias("member_months")
    );
}

/**
 * Monthly cohort totals across all members in the slice.
 *
 * @param {DataFrame} df
 */
export function monthlyTrend(df) {
  return df
    .groupBy("month")
    .agg(
      col("medical_claims").sum().alias("medical_claims"),
      col("pharmacy_fills").sum().alias("pharmacy_fills"),
      col("pharmacy_paid").sum().alias("pharmacy_paid"),
      col("person_id").count().alias("member_months")
    )
    .sort("month");
}
