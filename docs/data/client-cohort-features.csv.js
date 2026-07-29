/**
 * Synthetic member features for balance dot plots (age × arm × gender).
 */

import { csvFormat } from "d3-dsv";

let s = 7;
const rand = () => (s = (s * 1664525 + 1013904223) % 4294967296) / 4294967296;

const rows = [];
for (let i = 0; i < 600; i++) {
  const treatment = rand() < 0.48;
  const gender = rand() < 0.55 ? "F" : "M";
  rows.push({
    member_id: `m${i}`,
    age: Math.round(58 + rand() * 28),
    gender,
    study_arm: treatment ? "Treatment" : "Control",
    treatment_indicator: treatment ? 1 : 0,
  });
}

process.stdout.write(csvFormat(rows));
