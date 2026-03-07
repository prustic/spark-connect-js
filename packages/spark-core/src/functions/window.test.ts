import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { row_number, rank, dense_rank, lag, lead, ntile } from "./window.js";

describe("window functions", () => {
  it("row_number() creates unresolvedFunction", () => {
    const result = row_number();
    assert.equal(result._expr.type, "unresolvedFunction");
    if (result._expr.type === "unresolvedFunction") {
      assert.equal(result._expr.name, "row_number");
      assert.equal(result._expr.arguments.length, 0);
    }
  });

  it("rank() creates unresolvedFunction", () => {
    const expr = rank()._expr;
    assert.equal(expr.type, "unresolvedFunction");
    if (expr.type === "unresolvedFunction") {
      assert.equal(expr.name, "rank");
    }
  });

  it("dense_rank() creates unresolvedFunction", () => {
    const expr = dense_rank()._expr;
    assert.equal(expr.type, "unresolvedFunction");
    if (expr.type === "unresolvedFunction") {
      assert.equal(expr.name, "dense_rank");
    }
  });

  it("lag() with default offset", () => {
    const result = lag("salary");
    if (result._expr.type === "unresolvedFunction") {
      assert.equal(result._expr.name, "lag");
      assert.equal(result._expr.arguments.length, 2); // column + offset
    }
  });

  it("lag() with custom offset and default value", () => {
    const result = lag("salary", 2, "fallback");
    if (result._expr.type === "unresolvedFunction") {
      assert.equal(result._expr.arguments.length, 3);
    }
  });

  it("lead() creates unresolvedFunction", () => {
    const result = lead("salary");
    if (result._expr.type === "unresolvedFunction") {
      assert.equal(result._expr.name, "lead");
    }
  });

  it("ntile() creates unresolvedFunction", () => {
    const result = ntile(4);
    if (result._expr.type === "unresolvedFunction") {
      assert.equal(result._expr.name, "ntile");
      assert.equal(result._expr.arguments.length, 1);
    }
  });
});
