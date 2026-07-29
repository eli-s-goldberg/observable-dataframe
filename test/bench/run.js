/**
 * bench/run.js — the receipts.
 *
 * Compares observable-dataframe against Arquero on the operations people
 * actually run: filter, groupby-agg, sort, join. Run with `npm run bench`.
 * Numbers are medians of several runs, because means reward nobody's
 * garbage collector.
 */

import * as aq from "arquero";
import { DataFrame, col } from "../../src/index.js";

const CITIES = ["NYC", "SF", "LA", "CHI", "HOU", "PHX", "PHL", "SA", "SD", "DAL"];
const SEGMENTS = ["low", "medium", "high"];

function makeData(n, seed = 42) {
  // Deterministic LCG so both libraries chew identical data.
  let s = seed;
  const rand = () => (s = (s * 1664525 + 1013904223) % 4294967296) / 4294967296;
  const id = new Array(n);
  const city = new Array(n);
  const segment = new Array(n);
  const age = new Array(n);
  const income = new Array(n);
  for (let i = 0; i < n; i++) {
    id[i] = i;
    city[i] = CITIES[(rand() * CITIES.length) | 0];
    segment[i] = SEGMENTS[(rand() * SEGMENTS.length) | 0];
    age[i] = 18 + ((rand() * 60) | 0);
    income[i] = Math.round(rand() * 150000);
  }
  return { id, city, segment, age, income };
}

function median(times) {
  const t = times.slice().sort((a, b) => a - b);
  return t[(t.length / 2) | 0];
}

function bench(fn, runs = 5) {
  fn(); // warm-up: JIT gets one free look
  const times = [];
  for (let r = 0; r < runs; r++) {
    const t0 = performance.now();
    fn();
    times.push(performance.now() - t0);
  }
  return median(times);
}

const SIZES = [10_000, 100_000, 1_000_000];
const results = [];

for (const n of SIZES) {
  const cols = makeData(n);
  const odf = DataFrame.fromColumns(cols);
  const at = aq.table(cols);

  const smallCols = makeData(Math.max(1000, n / 100), 7);
  const odfSmall = DataFrame.fromColumns(smallCols);
  const atSmall = aq.table(smallCols);

  const cases = {
    // Arquero ops are lazy views; .reify() forces the work so we're timing
    // sorts against sorts, not sorts against IOUs.
    "filter (2 predicates)": {
      odf: () => odf.filter(col("age").gt(40).and(col("city").eq("NYC"))),
      aq: () => at.filter((d) => d.age > 40 && d.city === "NYC").reify(),
    },
    "groupby(city).mean": {
      odf: () => odf.groupBy("city").agg(col("income").mean().alias("avg")),
      aq: () => at.groupby("city").rollup({ avg: (d) => aq.op.mean(d.income) }),
    },
    "groupby(city,segment) 3 aggs": {
      odf: () =>
        odf
          .groupBy("city", "segment")
          .agg(col("income").mean().alias("m"), col("income").max().alias("hi"), col("age").count().alias("n")),
      aq: () =>
        at.groupby("city", "segment").rollup({
          m: (d) => aq.op.mean(d.income),
          hi: (d) => aq.op.max(d.income),
          n: (d) => aq.op.count(),
        }),
    },
    "sort(income desc)": {
      odf: () => odf.sort("income", { descending: true }),
      aq: () => at.orderby(aq.desc("income")).reify(),
    },
    "join on id (n x n/100)": {
      odf: () => odf.join(odfSmall, { on: "id" }),
      aq: () => at.join(atSmall, "id"),
    },
    "withColumn arithmetic": {
      odf: () => odf.withColumns(col("income").div(12).alias("monthly")),
      aq: () => at.derive({ monthly: (d) => d.income / 12 }).reify(),
    },
  };

  for (const [name, impl] of Object.entries(cases)) {
    const tOdf = bench(impl.odf);
    const tAq = bench(impl.aq);
    results.push({
      rows: n.toLocaleString(),
      operation: name,
      "odf (ms)": +tOdf.toFixed(2),
      "arquero (ms)": +tAq.toFixed(2),
      ratio: +(tAq / tOdf).toFixed(2),
    });
  }
}

console.table(results);
console.log("ratio > 1 means observable-dataframe is faster. We accept compliments.");
