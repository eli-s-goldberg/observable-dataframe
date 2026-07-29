/**
 * claimsSlice.js — load the published member-month claims slice into a DataFrame.
 *
 * Reads the CSV a data loader publishes; the parser only cares about the
 * columns and dtypes. Everything downstream, describe and groupBy and the panel
 * estimators, starts here. The panel the docs publish comes from
 * simulateClaimsPanel.js, so the schema below is the schema that generator
 * emits.
 */

import { DataFrame } from "../core/DataFrame.js";
import { col } from "../core/expr.js";
import { fromCSV } from "../core/io.js";

/** Default path to the published sample (source parquets stay gitignored). */
export const DEFAULT_CLAIMS_SLICE_PATH = "data/samples/claims_member_month.csv";

/**
 * Load member-month claims slice from CSV into a typed DataFrame.
 *
 * Columns: person_id, month, period, cohort, treated_now, medical_claims,
 * pharmacy_fills, pharmacy_paid, enrolled_flag. Columns absent from the CSV are
 * simply absent from the DataFrame, so a narrower panel still parses.
 *
 * `cohort` is the member's adoption period, or 0 for never treated, which is
 * the encoding the panel estimators read as a clean comparison group.
 *
 * @param {string} text CSV contents
 */
export function claimsSliceFromCSV(text) {
  return fromCSV(text, {
    dtypes: {
      person_id: "str",
      month: "str",
      period: "i32",
      cohort: "i32",
      treated_now: "i32",
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
