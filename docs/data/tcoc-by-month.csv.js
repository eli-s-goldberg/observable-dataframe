/**
 * Synthetic monthly cost effects for two-group comparison box plots.
 * Replace with a loader that queries your warehouse when ready.
 */

import { csvFormat } from "d3-dsv";

const months = ["Jan-22", "Feb-22", "Mar-22", "Apr-22", "May-22", "Jun-22",
  "Jul-22", "Aug-22", "Sep-22", "Oct-22", "Nov-22", "Dec-22"];

let s = 42;
const rand = () => (s = (s * 1664525 + 1013904223) % 4294967296) / 4294967296;
const gauss = () => Math.sqrt(-2 * Math.log(1 - rand())) * Math.cos(2 * Math.PI * rand());

const rows = [];
for (const group of ["treatment", "comparison"]) {
  const shift = group === "treatment" ? -800 : -400;
  for (const date_category of months) {
    for (let i = 0; i < 35; i++) {
      rows.push({
        group,
        date_category,
        effect: Math.round(shift + gauss() * 1200 + (i % 5) * 40),
      });
    }
  }
}

process.stdout.write(csvFormat(rows));
