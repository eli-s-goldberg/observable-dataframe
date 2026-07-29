import { describe, it, expect } from "vitest";
import { claimsSliceFromCSV } from "../src/data/claimsSlice.js";

describe("claimsSliceFromCSV", () => {
  it("loads CSV without dtype alias errors", () => {
    const text = [
      "person_id,month,medical_claims,pharmacy_fills,pharmacy_paid,enrolled_flag",
      "p1,2024-01,2,1,45.5,1",
      "p2,2024-02,0,0,0,0",
    ].join("\n");
    const df = claimsSliceFromCSV(text);
    expect(df.dtypes).toEqual({
      person_id: "str",
      month: "str",
      medical_claims: "i32",
      pharmacy_fills: "i32",
      pharmacy_paid: "f64",
      enrolled_flag: "i32",
    });
    expect(df.height).toBe(2);
    expect(df.row(0).person_id).toBe("p1");
  });
});
