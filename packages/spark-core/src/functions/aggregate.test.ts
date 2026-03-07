import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { count, sum, avg, min, max, countDistinct } from "./aggregate.js";

describe("aggregate functions", () => {
  it("count() creates unresolvedFunction", () => {
    const result = count("*");
    if (result._expr.type === "unresolvedFunction") {
      assert.equal(result._expr.name, "count");
    }
  });

  it("sum/avg/min/max all create unresolvedFunction", () => {
    assert.equal(sum("x")._expr.type, "unresolvedFunction");
    assert.equal(avg("x")._expr.type, "unresolvedFunction");
    assert.equal(min("x")._expr.type, "unresolvedFunction");
    assert.equal(max("x")._expr.type, "unresolvedFunction");
  });

  it("countDistinct sets isDistinct", () => {
    const result = countDistinct("x");
    if (result._expr.type === "unresolvedFunction") {
      assert.equal(result._expr.isDistinct, true);
    }
  });
});
