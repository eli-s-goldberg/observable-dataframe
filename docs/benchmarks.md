---
toc: false
theme: [air, wide]
title: Benchmarks
---

```js
import { injectPageStyle } from "observable-dataframe/layouts";
injectPageStyle();
```

<div class="page-title-header">
  <div class="page-title">Benchmarks</div>
  <div class="divider"></div>
  Measured against Arquero (both eager, both reified), medians of 5 runs
  after warm-up. Run <code>npm run bench</code> to reproduce locally, or
  press the button below to rerun a subset in <em>this</em> browser tab,
  which is after all the deployment target.
</div>

## Reference numbers (Node 22, Apple Silicon)

| Rows | Operation | odf (ms) | Arquero (ms) | Speedup |
|---:|---|---:|---:|---:|
| 100k | groupby(city).mean | 0.45 | 5.62 | **12.6×** |
| 100k | groupby(city,segment), 3 aggs | 2.27 | 8.44 | **3.7×** |
| 100k | filter (2 predicates) | 0.92 | 1.04 | 1.1× |
| 100k | sort(income desc) | 17.99 | 17.79 | 1.0× |
| 100k | join on id | 1.77 | 2.69 | **1.5×** |
| 100k | withColumn arithmetic | 0.13 | 0.35 | **2.6×** |
| 1M | groupby(city).mean | 4.13 | 55.38 | **13.4×** |
| 1M | groupby(city,segment), 3 aggs | 23.69 | 84.97 | **3.6×** |
| 1M | filter (2 predicates) | 8.06 | 10.37 | 1.3× |
| 1M | sort(income desc) | 223.08 | 237.10 | 1.1× |
| 1M | join on id | 13.42 | 20.59 | **1.5×** |
| 1M | withColumn arithmetic | 1.29 | 8.12 | **6.3×** |

The story: grouped aggregation — the operation dashboards actually spend
their lives in — is where the columnar layout and reused group ids pay off.
Filters, sorts, and joins are competitive. At 10k rows everything is under
2ms in both libraries and you should choose on API, not speed.

## Run it here

```js
const go = view(Inputs.button("Run benchmark in this tab (~10s)"));
```

```js
import { DataFrame, col } from "observable-dataframe";

const results = (() => {
  if (!go) return [];
  const N = 500_000;
  const CITIES = ["NYC", "SF", "LA", "CHI", "HOU", "PHX", "PHL", "SA", "SD", "DAL"];
  let s = 42;
  const rand = () => (s = (s * 1664525 + 1013904223) % 4294967296) / 4294967296;
  const cols = { city: [], age: [], income: [] };
  for (let i = 0; i < N; i++) {
    cols.city.push(CITIES[(rand() * 10) | 0]);
    cols.age.push(18 + ((rand() * 60) | 0));
    cols.income.push(Math.round(rand() * 150000));
  }
  const df = DataFrame.fromColumns(cols);

  const time = (fn) => {
    fn();
    const times = [];
    for (let r = 0; r < 5; r++) {
      const t0 = performance.now();
      fn();
      times.push(performance.now() - t0);
    }
    return times.sort((a, b) => a - b)[2];
  };

  return [
    { operation: "filter (2 predicates)", ms: time(() => df.filter(col("age").gt(40).and(col("city").eq("NYC")))) },
    { operation: "groupby(city).mean", ms: time(() => df.groupBy("city").agg(col("income").mean())) },
    { operation: "sort(income desc)", ms: time(() => df.sort("income", { descending: true })) },
    { operation: "withColumn arithmetic", ms: time(() => df.withColumns(col("income").div(12).alias("m"))) },
  ].map((r) => ({ rows: "500,000", ...r, ms: +r.ms.toFixed(2) }));
})();

display(results.length ? Inputs.table(results) : html`<em>Press the button. The typed arrays are ready.</em>`);
```
