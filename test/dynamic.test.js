/**
 * dynamic.test.js — groupByDynamic, the time-window groupby.
 * Claims in, member-months out, boundaries where the calendar puts them.
 */

import { describe, it, expect } from "vitest";
import { DataFrame, col } from "../src/index.js";
import { parseDuration, windowStartsFor } from "../src/core/dynamic.js";

const utc = (y, m, d = 1, h = 0) => new Date(Date.UTC(y, m, d, h));

describe("parseDuration", () => {
  it("parses fixed and calendar durations", () => {
    expect(parseDuration("7d")).toEqual({ ms: 7 * 86_400_000 });
    expect(parseDuration("2w")).toEqual({ ms: 14 * 86_400_000 });
    expect(parseDuration("12h")).toEqual({ ms: 12 * 3_600_000 });
    expect(parseDuration("1mo")).toEqual({ months: 1 });
    expect(parseDuration("1q")).toEqual({ months: 3 });
    expect(parseDuration("2y")).toEqual({ months: 24 });
    expect(parseDuration(500)).toEqual({ ms: 500 });
  });

  it("rejects nonsense and fractional months", () => {
    expect(() => parseDuration("fortnightly")).toThrow(/menu/);
    expect(() => parseDuration("1.5mo")).toThrow(/fractional/);
  });
});

describe("windowStartsFor", () => {
  it("assigns one window when period equals every", () => {
    const spec = { every: { ms: 100 }, period: { ms: 100 }, offset: null };
    expect(windowStartsFor(250, spec, "left")).toEqual([200]);
    expect(windowStartsFor(200, spec, "left")).toEqual([200]); // left-closed start
    expect(windowStartsFor(200, spec, "right")).toEqual([100]); // right-closed end
  });

  it("overlapping periods put a value in several windows", () => {
    const spec = { every: { ms: 100 }, period: { ms: 300 }, offset: null };
    expect(windowStartsFor(250, spec, "left")).toEqual([0, 100, 200]);
  });

  it("calendar months land on month boundaries regardless of month length", () => {
    const spec = { every: { months: 1 }, period: { months: 1 }, offset: null };
    expect(windowStartsFor(utc(2024, 1, 29).getTime(), spec, "left")).toEqual([utc(2024, 1).getTime()]); // leap Feb 29
    expect(windowStartsFor(utc(2024, 2, 1).getTime(), spec, "left")).toEqual([utc(2024, 2).getTime()]); // Mar 1 opens March
  });
});

describe("groupByDynamic", () => {
  const claims = DataFrame.fromRows([
    { person: "a", date: utc(2024, 0, 5), paid: 100 },
    { person: "a", date: utc(2024, 0, 20), paid: 50 },
    { person: "a", date: utc(2024, 1, 3), paid: 70 },
    { person: "b", date: utc(2024, 0, 10), paid: 30 },
    { person: "b", date: utc(2024, 2, 15), paid: 90 },
  ]);

  it("rolls claims into calendar months", () => {
    const monthly = claims
      .groupByDynamic({ indexColumn: "date", every: "1mo" })
      .agg(col("paid").sum().alias("paid"), col("person").count().alias("claims"));
    expect(monthly.height).toBe(3);
    expect(monthly.toRows()).toEqual([
      { date: utc(2024, 0), paid: 180, claims: 3 },
      { date: utc(2024, 1), paid: 70, claims: 1 },
      { date: utc(2024, 2), paid: 90, claims: 1 },
    ]);
  });

  it("adds by keys: the member-month panel in one call", () => {
    const panel = claims
      .groupByDynamic({ indexColumn: "date", every: "1mo", by: ["person"] })
      .agg(col("paid").sum().alias("paid"));
    expect(panel.height).toBe(4); // a: Jan+Feb, b: Jan+Mar
    const aJan = panel.toRows().find((r) => r.person === "a" && r.date.getTime() === utc(2024, 0).getTime());
    expect(aJan.paid).toBe(150);
    // windows arrive sorted, per the docstring's promise
    const dates = panel.toRows().map((r) => r.date.getTime());
    expect(dates).toEqual([...dates].sort((x, y) => x - y));
  });

  it("quarterly windows respect the calendar", () => {
    const quarterly = claims
      .groupByDynamic({ indexColumn: "date", every: "1q" })
      .agg(col("paid").sum().alias("paid"));
    expect(quarterly.height).toBe(1); // all of Jan-Mar 2024 is Q1
    expect(quarterly.row(0)).toEqual({ date: utc(2024, 0), paid: 340 });
  });

  it("fixed-width windows on numeric index columns", () => {
    const readings = DataFrame.fromRows(
      Array.from({ length: 30 }, (_, i) => ({ t: i, value: i }))
    );
    const windowed = readings
      .groupByDynamic({ indexColumn: "t", every: 10 })
      .agg(col("value").mean().alias("avg"), col("value").count().alias("n"));
    expect(windowed.toRows()).toEqual([
      { t: 0, avg: 4.5, n: 10 },
      { t: 10, avg: 14.5, n: 10 },
      { t: 20, avg: 24.5, n: 10 },
    ]);
  });

  it("period > every builds overlapping (rolling) windows", () => {
    const readings = DataFrame.fromRows(Array.from({ length: 12 }, (_, i) => ({ t: i, v: 1 })));
    const rolling = readings
      .groupByDynamic({ indexColumn: "t", every: 3, period: 6 })
      .agg(col("v").count().alias("n"));
    // interior windows hold 6 rows; edges taper
    const interior = rolling.toRows().filter((r) => r.t >= 0 && r.t <= 6);
    for (const w of interior) expect(w.n).toBe(6);
    // duplicated rows mean total window membership exceeds row count
    const total = rolling.toRows().reduce((a, r) => a + r.n, 0);
    expect(total).toBeGreaterThan(readings.height);
  });

  it("offset shifts the boundaries", () => {
    const readings = DataFrame.fromRows(Array.from({ length: 10 }, (_, i) => ({ t: i, v: i })));
    const shifted = readings
      .groupByDynamic({ indexColumn: "t", every: 5, offset: 2 })
      .agg(col("v").count().alias("n"));
    expect(shifted.toRows().map((r) => r.t)).toEqual([-3, 2, 7]);
  });

  it("closed='right' moves the boundary claim", () => {
    const rows = DataFrame.fromRows([{ t: 10, v: 1 }]);
    const left = rows.groupByDynamic({ indexColumn: "t", every: 10 }).agg(col("v").count().alias("n"));
    const right = rows.groupByDynamic({ indexColumn: "t", every: 10, closed: "right" }).agg(col("v").count().alias("n"));
    expect(left.row(0).t).toBe(10); // t=10 starts the [10,20) window
    expect(right.row(0).t).toBe(0); // t=10 ends the (0,10] window
  });

  it("null dates join no window; string indexes are refused", () => {
    const withNull = DataFrame.fromRows([
      { date: utc(2024, 0, 5), v: 1 },
      { date: null, v: 99 },
    ]);
    const out = withNull.groupByDynamic({ indexColumn: "date", every: "1mo" }).agg(col("v").sum().alias("v"));
    expect(out.height).toBe(1);
    expect(out.row(0).v).toBe(1);

    const strIdx = DataFrame.fromRows([{ month: "2024-01", v: 1 }]);
    expect(() => strIdx.groupByDynamic({ indexColumn: "month", every: "1mo" })).toThrow(/timeline/);
  });
});
