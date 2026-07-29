/**
 * Data loader: member-month claims slice for docs.
 *
 * Observable Framework runs this at build/preview time and caches the CSV.
 * See https://observablehq.com/framework/data-loaders
 *
 * Resolution order:
 *   1. data/samples/claims_member_month.csv, if you have a local extract
 *   2. claims_member_month.csv alongside this loader. Gitignored, because a
 *      real one holds member identifiers.
 *   3. synthetic fallback, which is the path a fresh checkout takes, so
 *      preview and build work with no local data at all
 */

import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

async function tryRead(path) {
  try {
    return await readFile(path, "utf8");
  } catch {
    return null;
  }
}

function syntheticCsv() {
  const header = "person_id,month,medical_claims,pharmacy_fills,pharmacy_paid,enrolled_flag\n";
  const rows = [];
  for (let m = 0; m < 12; m++) {
    const month = `2024-${String(m + 1).padStart(2, "0")}`;
    for (let p = 0; p < 40; p++) {
      const enrolled = m < 11 || p % 3 !== 0 ? 1 : 0;
      rows.push(
        [
          `p${p}`,
          month,
          Math.floor((p + m) % 5),
          Math.floor((p * m) % 3),
          ((p + 1) * (m + 1) * 12.5).toFixed(2),
          enrolled,
        ].join(",")
      );
    }
  }
  return header + rows.join("\n");
}

const fromBuild = await tryRead(fileURLToPath(import.meta.resolve("../../data/samples/claims_member_month.csv")));
const fromFixture = await tryRead(fileURLToPath(import.meta.resolve("./claims_member_month.csv")));
const csv = fromBuild ?? fromFixture ?? syntheticCsv();

if (!fromBuild && !fromFixture) {
  console.warn("claims_member_month.csv.js: no local sample found, emitting synthetic panel");
}

process.stdout.write(csv);
