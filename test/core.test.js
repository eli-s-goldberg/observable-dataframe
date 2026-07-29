/**
 * core.test.js — proof that the engine does what the docstrings claim.
 */

import { describe, it, expect } from "vitest";
import { DataFrame, col, lit, when, fromCSV, toCSV, Column, random } from "../src/index.js";

const people = () =>
  DataFrame.fromRows([
    { name: "Alice", age: 30, city: "NYC", income: 90000 },
    { name: "Bob", age: 25, city: "SF", income: 80000 },
    { name: "Carol", age: 35, city: "NYC", income: 120000 },
    { name: "Dave", age: 28, city: "SF", income: 75000 },
    { name: "Eve", age: null, city: "NYC", income: null },
  ]);

describe("construction & dtypes", () => {
  it("infers dtypes from rows", () => {
    const df = people();
    expect(df.dtypes).toEqual({ name: "str", age: "i32", city: "str", income: "i32" });
    expect(df.shape).toEqual([5, 4]);
  });

  it("builds from columns", () => {
    const df = DataFrame.fromColumns({ a: [1.5, 2.5], b: ["x", "y"] });
    expect(df.dtypes.a).toBe("f64");
    expect(df.dtypes.b).toBe("str");
  });

  it("rejects ragged columns", () => {
    expect(() => DataFrame.fromColumns({ a: [1, 2], b: [1] })).toThrow(/rectangular/);
  });

  it("dictionary-encodes strings", () => {
    const c = Column.from(["a", "b", "a", "a"]);
    expect(c.dict).toEqual(["a", "b"]);
    expect(Array.from(c.data)).toEqual([0, 1, 0, 0]);
  });

  it("tracks nulls in a validity mask, not sentinels", () => {
    const df = people();
    expect(df.getColumn("age").nullCount()).toBe(1);
    expect(df.row(4).age).toBeNull();
  });

  it("handles dates", () => {
    const df = DataFrame.fromRows([{ d: new Date("2024-01-15") }, { d: new Date("2024-06-01") }]);
    expect(df.dtypes.d).toBe("date");
    expect(df.row(0).d).toEqual(new Date("2024-01-15"));
  });
});

describe("random & sample", () => {
  it("random(seed) replays the same sequence", () => {
    const a = random(42);
    const b = random(42);
    const seqA = [a(), a(), a()];
    const seqB = [b(), b(), b()];
    expect(seqA).toEqual(seqB);
    for (const v of seqA) expect(v).toBeGreaterThanOrEqual(0);
    for (const v of seqA) expect(v).toBeLessThan(1);
    expect(() => random(1.5)).toThrow(/integer/);
  });

  it("sample is deterministic with a seed", () => {
    const df = people();
    const a = df.sample(3, { seed: 5 }).toRows();
    const b = df.sample(3, { seed: 5 }).toRows();
    expect(a).toEqual(b);
    expect(a).toHaveLength(3);
  });

  it("sample without replacement has no duplicate rows", () => {
    const df = people();
    const names = df.sample(5, { seed: 9 }).toRows().map((r) => r.name);
    expect(new Set(names).size).toBe(5);
    expect(() => df.sample(6)).toThrow(/without replacement/);
  });

  it("sample with replacement may repeat and honors n", () => {
    const drawn = people().sample(50, { seed: 1, withReplacement: true });
    expect(drawn.height).toBe(50);
    const names = new Set(drawn.toRows().map((r) => r.name));
    expect(names.size).toBeLessThanOrEqual(5);
  });
});

describe("grouped windows: over()", () => {
  const panel = () =>
    DataFrame.fromRows([
      { member: "a", month: 1, paid: 100 },
      { member: "b", month: 1, paid: 400 },
      { member: "a", month: 2, paid: 250 },
      { member: "b", month: 2, paid: 100 },
      { member: "a", month: 3, paid: 90 },
    ]);

  it("shift(1).over(member) lags within each member, preserving row order", () => {
    const out = panel()
      .withColumns(col("paid").shift(1).over("member").alias("prev"))
      .toRows()
      .map((r) => r.prev);
    expect(out).toEqual([null, null, 100, 400, 250]);
  });

  it("diff and cumSum respect partitions", () => {
    const rows = panel()
      .withColumns(
        col("paid").diff(1).over("member").alias("delta"),
        col("paid").cumSum().over("member").alias("running")
      )
      .toRows();
    expect(rows.map((r) => r.delta)).toEqual([null, null, 150, -300, -160]);
    expect(rows.map((r) => r.running)).toEqual([100, 400, 350, 500, 440]);
  });

  it("rollingMean warms up per partition", () => {
    const out = panel()
      .withColumns(col("paid").rollingMean(2).over("member").alias("avg"))
      .toRows()
      .map((r) => r.avg);
    expect(out).toEqual([null, null, 175, 250, 170]);
  });

  it("rank averages ties, over() ranks within partitions", () => {
    const df = DataFrame.fromRows([
      { g: "x", v: 10 },
      { g: "x", v: 30 },
      { g: "y", v: 20 },
      { g: "x", v: 10 },
      { g: "y", v: 5 },
    ]);
    expect(df.withColumns(col("v").rank().alias("r")).toRows().map((r) => r.r)).toEqual([
      2.5, 5, 4, 2.5, 1,
    ]);
    expect(
      df.withColumns(col("v").rank().over("g").alias("r")).toRows().map((r) => r.r)
    ).toEqual([1.5, 3, 2, 1.5, 1]);
  });

  it("null keys form their own partition; multiple keys partition by tuple", () => {
    const df = DataFrame.fromRows([
      { k: null, sub: 1, v: 1 },
      { k: "a", sub: 1, v: 2 },
      { k: null, sub: 1, v: 3 },
      { k: "a", sub: 2, v: 4 },
      { k: "a", sub: 1, v: 5 },
    ]);
    expect(
      df.withColumns(col("v").cumSum().over("k").alias("c")).toRows().map((r) => r.c)
    ).toEqual([1, 2, 4, 6, 11]);
    expect(
      df.withColumns(col("v").shift(1).over("k", "sub").alias("p")).toRows().map((r) => r.p)
    ).toEqual([null, null, 1, null, 2]);
  });

  it("over() refuses non-window expressions", () => {
    expect(() => col("v").sum().over("g")).toThrow(/window operations/);
  });
});

describe("Column.values", () => {
  it("decodes strings from the dictionary, nulls included", () => {
    const c = Column.from(["a", null, "b", "a"]);
    expect(c.values()).toEqual(["a", null, "b", "a"]);
  });

  it("skips nulls with validOnly", () => {
    const c = Column.from([1, null, 3]);
    expect(c.values({ validOnly: true })).toEqual([1, 3]);
    expect(c.values()).toEqual([1, null, 3]);
  });

  it("returns Dates for date columns, not epoch ms", () => {
    const c = Column.from([new Date("2024-01-15"), null]);
    expect(c.values({ validOnly: true })).toEqual([new Date("2024-01-15")]);
  });

  it("returns booleans for bool columns, not 0/1", () => {
    const c = Column.from([true, false, null]);
    expect(c.values({ validOnly: true })).toEqual([true, false]);
  });
});

describe("filter", () => {
  it("filters with comparison expressions", () => {
    const df = people().filter(col("age").gt(27));
    expect(df.height).toBe(3);
    expect(df.toRows().map((r) => r.name)).toEqual(["Alice", "Carol", "Dave"]);
  });

  it("nulls never pass a filter", () => {
    const df = people().filter(col("age").lt(100));
    expect(df.height).toBe(4); // Eve's null age stays home
  });

  it("combines with and/or/not", () => {
    const df = people().filter(col("city").eq(lit("NYC")).and(col("income").gte(100000)));
    expect(df.toRows().map((r) => r.name)).toEqual(["Carol"]);
    const df2 = people().filter(col("age").lt(28).or(col("age").gt(32)));
    expect(df2.toRows().map((r) => r.name)).toEqual(["Bob", "Carol"]);
  });

  it("isIn and between", () => {
    expect(people().filter(col("name").isIn(["Alice", "Dave"])).height).toBe(2);
    expect(people().filter(col("age").between(25, 30)).height).toBe(3);
  });

  it("isNull / isNotNull", () => {
    expect(people().filter(col("age").isNull()).toRows()[0].name).toBe("Eve");
    expect(people().filter(col("age").isNotNull()).height).toBe(4);
  });

  it("string ops", () => {
    expect(people().filter(col("name").str.startsWith("A")).height).toBe(1);
    expect(people().filter(col("name").str.contains("a")).toRows().map((r) => r.name))
      .toEqual(["Carol", "Dave"]);
  });

  it("supports a row-predicate slow lane", () => {
    expect(people().filter((row) => row.age === 25).toRows()[0].name).toBe("Bob");
  });
});

describe("select & withColumns", () => {
  it("selects by name and expression", () => {
    const df = people().select("name", col("income").div(12).round(0).alias("monthly"));
    expect(df.columns).toEqual(["name", "monthly"]);
    expect(df.row(0).monthly).toBe(7500);
  });

  it("select of pure aggregations yields one row", () => {
    const df = people().select(col("age").mean().alias("avg_age"), col("income").max());
    expect(df.height).toBe(1);
    expect(df.row(0).avg_age).toBeCloseTo(29.5);
    expect(df.row(0).income).toBe(120000);
  });

  it("withColumns adds and replaces", () => {
    const df = people().withColumns(
      col("income").div(1000).alias("income_k"),
      col("age").fillNull(0).alias("age")
    );
    expect(df.row(0).income_k).toBe(90);
    expect(df.row(4).age).toBe(0);
    expect(df.width).toBe(5);
  });

  it("supports when/then/otherwise", () => {
    const df = people().withColumns(
      when(col("age").gte(30)).then("senior").otherwise("junior").alias("tier")
    );
    expect(df.toRows().map((r) => r.tier)).toEqual(["senior", "junior", "senior", "junior", "junior"]);
  });

  it("named-expression object form (polars kwargs, at home)", () => {
    const df = people().withColumns({ doubled: col("age").mul(2), label: "cohort" });
    expect(df.row(0).doubled).toBe(60);
    expect(df.row(0).label).toBe("cohort");
  });

  it("fn(row) in withColumns is refused with directions", () => {
    expect(() => people().withColumns({ adult: (r) => r.age >= 18 })).toThrow(/no longer takes fn\(row\)/);
  });

  it("map escape hatch", () => {
    const df = people().withColumns(col("name").map((s) => s.length).alias("len"));
    expect(df.row(0).len).toBe(5);
  });
});

describe("sort", () => {
  it("sorts numerically", () => {
    const df = people().sort("age");
    expect(df.toRows().map((r) => r.name)).toEqual(["Bob", "Dave", "Alice", "Carol", "Eve"]);
  });

  it("sorts strings as strings, descending", () => {
    const df = people().sort("name", { descending: true });
    expect(df.row(0).name).toBe("Eve");
  });

  it("multi-key with mixed directions; nulls last", () => {
    const df = people().sort(["city", "income"], { descending: [false, true] });
    expect(df.toRows().map((r) => r.name)).toEqual(["Carol", "Alice", "Eve", "Bob", "Dave"]);
  });

  it("the pandas aliases are gone, on purpose", () => {
    const df = people();
    expect(df.sort_values).toBeUndefined();
    expect(df.groupby).toBeUndefined();
    expect(df.with_columns).toBeUndefined();
    expect(df.to_data).toBeUndefined();
    expect(df.loc).toBeUndefined();
  });
});

describe("groupBy", () => {
  it("aggregates with expressions", () => {
    const df = people()
      .groupBy("city")
      .agg(col("income").mean().alias("avg"), col("name").count().alias("n"))
      .sort("city");
    expect(df.toRows()).toEqual([
      { city: "NYC", avg: 105000, n: 3 },
      { city: "SF", avg: 77500, n: 2 },
    ]);
  });

  it("supports min/max/std/median/nUnique/first/last", () => {
    const df = people()
      .groupBy("city")
      .agg(
        col("income").min().alias("lo"),
        col("income").max().alias("hi"),
        col("income").median().alias("mid"),
        col("name").nUnique().alias("uniq"),
        col("name").first().alias("first")
      )
      .sort("city");
    const nyc = df.row(0);
    expect(nyc.lo).toBe(90000);
    expect(nyc.hi).toBe(120000);
    expect(nyc.mid).toBe(105000);
    expect(nyc.uniq).toBe(3);
    expect(nyc.first).toBe("Alice");
  });

  it("supports arithmetic over aggregations", () => {
    const df = people()
      .groupBy("city")
      .agg(col("income").sum().div(col("age").sum()).alias("ratio"))
      .sort("city");
    expect(df.row(0).ratio).toBeCloseTo(210000 / 65);
  });

  it("object agg specs are rejected — expressions only", () => {
    expect(() => people().groupBy("city").agg({ income: ["mean", "max"] })).toThrow(/expressions/);
  });

  it("multi-column keys don't collide on delimiters", () => {
    const df = DataFrame.fromRows([
      { a: "x_y", b: "z", v: 1 },
      { a: "x", b: "y_z", v: 2 },
    ]);
    const g = df.groupBy("a", "b").count();
    expect(g.height).toBe(2); // v1 would have merged these. We remember.
  });

  it("count() shortcut and group iteration", () => {
    const g = people().groupBy("city");
    expect(g.count().sort("city").toRows()).toEqual([
      { city: "NYC", count: 3 },
      { city: "SF", count: 2 },
    ]);
    const seen = [];
    for (const [key, sub] of people().groupBy("city")) seen.push([key.city, sub.height]);
    expect(seen.sort()).toEqual([["NYC", 3], ["SF", 2]]);
  });
});

describe("join", () => {
  const left = () =>
    DataFrame.fromRows([
      { id: 1, name: "Alice" },
      { id: 2, name: "Bob" },
      { id: 3, name: "Carol" },
    ]);
  const right = () =>
    DataFrame.fromRows([
      { id: 1, score: 0.9 },
      { id: 3, score: 0.7 },
      { id: 4, score: 0.5 },
    ]);

  it("inner join", () => {
    const df = left().join(right(), { on: "id" });
    expect(df.sort("id").toRows()).toEqual([
      { id: 1, name: "Alice", score: 0.9 },
      { id: 3, name: "Carol", score: 0.7 },
    ]);
  });

  it("left join pads with nulls", () => {
    const df = left().join(right(), { on: "id", how: "left" }).sort("id");
    expect(df.row(1)).toEqual({ id: 2, name: "Bob", score: null });
  });

  it("right and outer joins", () => {
    const r = left().join(right(), { on: "id", how: "right" }).sort("id");
    expect(r.height).toBe(3);
    expect(r.row(2)).toEqual({ id: 4, name: null, score: 0.5 });

    const o = left().join(right(), { on: "id", how: "outer" }).sort("id");
    expect(o.height).toBe(4);
    expect(o.toRows().map((x) => x.id)).toEqual([1, 2, 3, 4]);
  });

  it("one-to-many multiplies rows like it should", () => {
    const many = DataFrame.fromRows([
      { id: 1, tag: "a" },
      { id: 1, tag: "b" },
    ]);
    expect(left().join(many, { on: "id" }).height).toBe(2);
  });

  it("leftOn/rightOn and suffixing", () => {
    const other = DataFrame.fromRows([{ key: 1, name: "Other" }]);
    const df = left().join(other, { leftOn: "id", rightOn: "key" });
    expect(df.columns).toContain("name");
    expect(df.columns).toContain("name_right");
  });

  it("null keys never match", () => {
    const l = DataFrame.fromRows([{ id: null, v: 1 }]);
    const r = DataFrame.fromRows([{ id: null, w: 2 }]);
    expect(l.join(r, { on: "id" }).height).toBe(0);
  });
});

describe("reshape & misc", () => {
  it("unique respects subsets", () => {
    expect(people().unique({ subset: ["city"] }).height).toBe(2);
  });

  it("concat aligns by name and fills gaps with null", () => {
    const a = DataFrame.fromRows([{ x: 1, y: "a" }]);
    const b = DataFrame.fromRows([{ x: 2, z: true }]);
    const df = DataFrame.concat([a, b]);
    expect(df.height).toBe(2);
    expect(df.row(0).z).toBeNull();
    expect(df.row(1).y).toBeNull();
  });

  it("pivot goes wide", () => {
    const long = DataFrame.fromRows([
      { year: 2023, city: "NYC", v: 10 },
      { year: 2023, city: "SF", v: 20 },
      { year: 2024, city: "NYC", v: 30 },
    ]);
    const wide = long.pivot({ index: "year", columns: "city", values: "v" });
    expect(wide.sort("year").toRows()).toEqual([
      { year: 2023, NYC: 10, SF: 20 },
      { year: 2024, NYC: 30, SF: null },
    ]);
  });

  it("pivot aggregates duplicates", () => {
    const long = DataFrame.fromRows([
      { k: "a", c: "x", v: 1 },
      { k: "a", c: "x", v: 3 },
    ]);
    expect(long.pivot({ index: "k", columns: "c", values: "v", agg: "mean" }).row(0).x).toBe(2);
  });

  it("melt goes long", () => {
    const wide = DataFrame.fromRows([{ id: 1, a: 10, b: 20 }]);
    const long = wide.melt({ idVars: ["id"] });
    expect(long.height).toBe(2);
    expect(long.toRows()).toEqual([
      { id: 1, variable: "a", value: 10 },
      { id: 1, variable: "b", value: 20 },
    ]);
  });

  it("describe summarizes numerics", () => {
    const d = people().describe();
    const rows = d.toRows();
    const mean = rows.find((r) => r.statistic === "mean");
    expect(mean.age).toBeCloseTo(29.5);
    const nulls = rows.find((r) => r.statistic === "null_count");
    expect(nulls.income).toBe(1);
  });

  it("valueCounts counts and sorts", () => {
    const vc = people().valueCounts("city");
    expect(vc.row(0)).toEqual({ city: "NYC", count: 3 });
  });

  it("corr and corrMatrix", () => {
    const df = DataFrame.fromRows([
      { x: 1, y: 2 },
      { x: 2, y: 4 },
      { x: 3, y: 6 },
    ]);
    expect(df.corr("x", "y")).toBeCloseTo(1);
    expect(df.corrMatrix().height).toBe(4);
  });

  it("dropNulls, head, tail, rename, drop", () => {
    expect(people().dropNulls().height).toBe(4);
    expect(people().dropNulls({ subset: ["name"] }).height).toBe(5);
    expect(people().head(2).height).toBe(2);
    expect(people().tail(1).row(0).name).toBe("Eve");
    expect(people().rename({ name: "person" }).columns).toContain("person");
    expect(people().drop("city").columns).not.toContain("city");
  });

  it("window ops: shift, cumSum, diff, rollingMean", () => {
    const df = DataFrame.fromRows([{ v: 1 }, { v: 2 }, { v: 3 }, { v: 4 }]).withColumns(
      col("v").shift(1).alias("prev"),
      col("v").cumSum().alias("running"),
      col("v").diff().alias("delta"),
      col("v").rollingMean(2).alias("roll")
    );
    expect(df.toRows().map((r) => r.prev)).toEqual([null, 1, 2, 3]);
    expect(df.toRows().map((r) => r.running)).toEqual([1, 3, 6, 10]);
    expect(df.toRows().map((r) => r.delta)).toEqual([null, 1, 1, 1]);
    expect(df.toRows().map((r) => r.roll)).toEqual([null, 1.5, 2.5, 3.5]);
  });

  it("date accessors", () => {
    const df = DataFrame.fromRows([{ d: new Date(2024, 5, 15, 10) }]).withColumns(
      col("d").dt.year().alias("y"),
      col("d").dt.month().alias("m"),
      col("d").dt.day().alias("day")
    );
    expect(df.row(0)).toMatchObject({ y: 2024, m: 6, day: 15 });
  });
});

describe("io", () => {
  it("parses CSV with type inference", () => {
    const df = fromCSV(`name,age,joined,active\nAlice,30,2024-01-15,true\nBob,,2023-06-01,false`);
    expect(df.dtypes).toEqual({ name: "str", age: "f64", joined: "date", active: "bool" });
    expect(df.row(1).age).toBeNull();
    expect(df.row(0).active).toBe(true);
  });

  it("respects dtype overrides", () => {
    const df = fromCSV(`zip\n02134`, { dtypes: { zip: "str" } });
    expect(df.row(0).zip).toBe("02134");
  });

  it("round-trips through toCSV", () => {
    const df = people();
    const back = fromCSV(toCSV(df));
    expect(back.height).toBe(5);
    expect(back.row(0).name).toBe("Alice");
    expect(back.row(4).income).toBeNull();
  });

  it("mixed columns stay strings", () => {
    const df = fromCSV(`v\n1\nbanana`);
    expect(df.dtypes.v).toBe("str");
  });

  it("accepts polars-style dtype aliases in overrides", () => {
    const df = fromCSV(`id,claims,paid,flag\nm1,3,12.5,1`, {
      dtypes: { id: "string", claims: "int", paid: "float", flag: "integer" },
    });
    expect(df.dtypes).toEqual({ id: "str", claims: "i32", paid: "f64", flag: "i32" });
    expect(df.row(0)).toMatchObject({ id: "m1", claims: 3, paid: 12.5, flag: 1 });
  });
});

describe("immutability", () => {
  it("operations never mutate the source frame", () => {
    const df = people();
    df.filter(col("age").gt(100));
    df.withColumns(col("age").mul(0).alias("age"));
    df.sort("name", { descending: true });
    expect(df.height).toBe(5);
    expect(df.row(0)).toMatchObject({ name: "Alice", age: 30 });
  });
});
