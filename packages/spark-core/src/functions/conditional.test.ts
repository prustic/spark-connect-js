import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { col, lit } from "../column.js";
import { when, cast, coalesce, isnull } from "./conditional.js";

describe("when/otherwise", () => {
  it("builds a when expression with otherwise", () => {
    const result = when(col("age").gt(lit(18)), lit("adult")).otherwise(lit("child"));
    assert.equal(result._expr.type, "unresolvedFunction");
    if (result._expr.type === "unresolvedFunction") {
      assert.equal(result._expr.name, "when");
      assert.equal(result._expr.arguments.length, 3); // condition, value, otherwise
    }
  });

  it("chains multiple when clauses", () => {
    const result = when(col("age").gt(lit(18)), lit("adult"))
      .when(col("age").gt(lit(12)), lit("teen"))
      .otherwise(lit("child"));
    if (result._expr.type === "unresolvedFunction") {
      assert.equal(result._expr.arguments.length, 5); // 2 pairs + otherwise
    }
  });

  it("builds when without otherwise via toColumn", () => {
    const result = when(col("age").gt(lit(18)), lit("adult")).toColumn();
    if (result._expr.type === "unresolvedFunction") {
      assert.equal(result._expr.arguments.length, 2); // condition + value only
    }
  });
});

describe("cast()", () => {
  it("creates a cast expression from a Column", () => {
    const result = cast(col("id"), "string");
    assert.equal(result._expr.type, "cast");
    if (result._expr.type === "cast") {
      assert.equal(result._expr.targetType, "string");
    }
  });

  it("creates a cast expression from a string column name", () => {
    const result = cast("price", "double");
    assert.equal(result._expr.type, "cast");
  });

  it("Column.cast() method works", () => {
    const result = col("id").cast("string");
    assert.equal(result._expr.type, "cast");
    if (result._expr.type === "cast") {
      assert.equal(result._expr.targetType, "string");
    }
  });
});

describe("coalesce/isnull", () => {
  it("coalesce handles multiple columns", () => {
    const result = coalesce("a", "b", "c");
    if (result._expr.type === "unresolvedFunction") {
      assert.equal(result._expr.arguments.length, 3);
    }
  });

  it("isnull creates unresolvedFunction", () => {
    assert.equal(isnull("x")._expr.type, "unresolvedFunction");
  });
});
