import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { col, lit } from "./column.js";

describe("col()", () => {
  it("creates an unresolved attribute expression", () => {
    const c = col("age");
    assert.deepStrictEqual(c._expr, { type: "unresolvedAttribute", name: "age" });
  });
});

describe("lit()", () => {
  it("creates a string literal", () => {
    const c = lit("hello");
    assert.deepStrictEqual(c._expr, { type: "literal", value: "hello" });
  });

  it("creates a number literal", () => {
    assert.deepStrictEqual(lit(42)._expr, { type: "literal", value: 42 });
  });

  it("creates a boolean literal", () => {
    assert.deepStrictEqual(lit(true)._expr, { type: "literal", value: true });
  });

  it("creates a bigint literal", () => {
    assert.deepStrictEqual(lit(9007199254740993n)._expr, {
      type: "literal",
      value: 9007199254740993n,
    });
  });

  it("creates a null literal", () => {
    assert.deepStrictEqual(lit(null)._expr, { type: "literal", value: null });
  });
});

describe("Column comparison operators", () => {
  const age = col("age");
  const threshold = lit(30);

  it("gt", () => {
    const expr = age.gt(threshold)._expr;
    assert.equal(expr.type, "gt");
  });

  it("lt", () => {
    assert.equal(age.lt(threshold)._expr.type, "lt");
  });

  it("eq", () => {
    assert.equal(age.eq(threshold)._expr.type, "eq");
  });

  it("neq", () => {
    assert.equal(age.neq(threshold)._expr.type, "neq");
  });

  it("gte", () => {
    assert.equal(age.gte(threshold)._expr.type, "gte");
  });

  it("lte", () => {
    assert.equal(age.lte(threshold)._expr.type, "lte");
  });

  it("preserves left and right operands", () => {
    const result = age.gt(threshold)._expr;
    assert.equal(result.type, "gt");
    if (result.type === "gt") {
      assert.deepStrictEqual(result.left, { type: "unresolvedAttribute", name: "age" });
      assert.deepStrictEqual(result.right, { type: "literal", value: 30 });
    }
  });
});

describe("Column logical operators", () => {
  const a = col("x").gt(lit(1));
  const b = col("y").lt(lit(10));

  it("and", () => {
    assert.equal(a.and(b)._expr.type, "and");
  });

  it("or", () => {
    assert.equal(a.or(b)._expr.type, "or");
  });
});

describe("Column arithmetic operators", () => {
  const x = col("x");
  const y = col("y");

  it("plus → add", () => {
    assert.equal(x.plus(y)._expr.type, "add");
  });

  it("minus → subtract", () => {
    assert.equal(x.minus(y)._expr.type, "subtract");
  });

  it("multiply", () => {
    assert.equal(x.multiply(y)._expr.type, "multiply");
  });

  it("divide", () => {
    assert.equal(x.divide(y)._expr.type, "divide");
  });
});

describe("Column aliasing", () => {
  it("alias() wraps expression", () => {
    const c = col("salary").alias("sal");
    assert.equal(c._expr.type, "alias");
    if (c._expr.type === "alias") {
      assert.equal(c._expr.name, "sal");
      assert.deepStrictEqual(c._expr.inner, { type: "unresolvedAttribute", name: "salary" });
    }
  });

  it("as() delegates to alias()", () => {
    const c = col("salary").as("sal");
    assert.equal(c._expr.type, "alias");
  });
});

describe("Column immutability", () => {
  it("operations return new Column instances", () => {
    const original = col("x");
    const filtered = original.gt(lit(5));
    assert.notEqual(original, filtered);
    assert.equal(original._expr.type, "unresolvedAttribute");
    assert.equal(filtered._expr.type, "gt");
  });
});
