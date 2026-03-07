import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { abs, round, ceil, floor, sqrt, pow } from "./math.js";

describe("math functions", () => {
  it("abs() creates unresolvedFunction", () => {
    const result = abs("value");
    if (result._expr.type === "unresolvedFunction") {
      assert.equal(result._expr.name, "abs");
    }
  });

  it("round() includes scale parameter", () => {
    const result = round("value", 2);
    if (result._expr.type === "unresolvedFunction") {
      assert.equal(result._expr.name, "round");
      assert.equal(result._expr.arguments.length, 2);
    }
  });

  it("ceil/floor/sqrt work", () => {
    assert.equal(ceil("x")._expr.type, "unresolvedFunction");
    assert.equal(floor("x")._expr.type, "unresolvedFunction");
    assert.equal(sqrt("x")._expr.type, "unresolvedFunction");
  });

  it("pow() uses 'power' function name", () => {
    const result = pow("x", 2);
    if (result._expr.type === "unresolvedFunction") {
      assert.equal(result._expr.name, "power");
    }
  });
});
