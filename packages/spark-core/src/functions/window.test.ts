import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  row_number, rank, dense_rank, cume_dist, percent_rank, nth_value,
  lag, lead, ntile,
} from "./window.js";

import type { Column } from "../column.js";

function assertFn(c: Column, name: string, argCount?: number) {
  assert.equal(c._expr.type, "unresolvedFunction");
  if (c._expr.type === "unresolvedFunction") {
    assert.equal(c._expr.name, name);
    if (argCount !== undefined) assert.equal(c._expr.arguments.length, argCount);
  }
}

describe("window functions", () => {
  it("row_number/rank/dense_rank/cume_dist/percent_rank are zero-arg", () => {
    assertFn(row_number(), "row_number", 0);
    assertFn(rank(), "rank", 0);
    assertFn(dense_rank(), "dense_rank", 0);
    assertFn(cume_dist(), "cume_dist", 0);
    assertFn(percent_rank(), "percent_rank", 0);
  });

  it("nth_value takes col and offset", () => {
    const r = nth_value("salary", 2);
    if (r._expr.type === "unresolvedFunction") {
      assert.equal(r._expr.name, "nth_value");
      assert.ok(r._expr.arguments.length >= 2);
    }
  });

  it("lag with default offset", () => {
    const result = lag("salary");
    if (result._expr.type === "unresolvedFunction") {
      assert.equal(result._expr.name, "lag");
      assert.equal(result._expr.arguments.length, 2);
    }
  });

  it("lag with custom offset and default value", () => {
    const result = lag("salary", 2, "fallback");
    if (result._expr.type === "unresolvedFunction") {
      assert.equal(result._expr.arguments.length, 3);
    }
  });

  it("lead creates unresolvedFunction", () => {
    assertFn(lead("salary"), "lead");
  });

  it("ntile creates unresolvedFunction", () => {
    assertFn(ntile(4), "ntile", 1);
  });
});
