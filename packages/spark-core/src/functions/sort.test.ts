import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  asc,
  desc,
  asc_nulls_first,
  asc_nulls_last,
  desc_nulls_first,
  desc_nulls_last,
} from "./sort.js";

describe("sort functions", () => {
  it("asc creates sortOrder ascending nulls_last", () => {
    const r = asc("x");
    assert.equal(r._expr.type, "sortOrder");
    if (r._expr.type === "sortOrder") {
      assert.equal(r._expr.direction, "ascending");
      assert.equal(r._expr.nullOrdering, "nulls_last");
    }
  });

  it("desc creates sortOrder descending nulls_last", () => {
    const r = desc("x");
    assert.equal(r._expr.type, "sortOrder");
    if (r._expr.type === "sortOrder") {
      assert.equal(r._expr.direction, "descending");
      assert.equal(r._expr.nullOrdering, "nulls_last");
    }
  });

  it("asc_nulls_first/asc_nulls_last", () => {
    const r1 = asc_nulls_first("x");
    if (r1._expr.type === "sortOrder") {
      assert.equal(r1._expr.direction, "ascending");
      assert.equal(r1._expr.nullOrdering, "nulls_first");
    }
    const r2 = asc_nulls_last("x");
    if (r2._expr.type === "sortOrder") {
      assert.equal(r2._expr.direction, "ascending");
      assert.equal(r2._expr.nullOrdering, "nulls_last");
    }
  });

  it("desc_nulls_first/desc_nulls_last", () => {
    const r1 = desc_nulls_first("x");
    if (r1._expr.type === "sortOrder") {
      assert.equal(r1._expr.direction, "descending");
      assert.equal(r1._expr.nullOrdering, "nulls_first");
    }
    const r2 = desc_nulls_last("x");
    if (r2._expr.type === "sortOrder") {
      assert.equal(r2._expr.direction, "descending");
      assert.equal(r2._expr.nullOrdering, "nulls_last");
    }
  });
});
