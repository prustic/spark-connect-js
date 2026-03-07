import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { col, lit } from "../column.js";
import {
  when, cast, coalesce, isnull, isnan, isnotnull, nanvl,
  ifnull, nvl, nvl2, nullif, expr, monotonically_increasing_id,
  spark_partition_id, typeof_, uuid, broadcast,
} from "./conditional.js";

import type { Column } from "../column.js";

function assertFn(c: Column, name: string, argCount?: number) {
  assert.equal(c._expr.type, "unresolvedFunction");
  if (c._expr.type === "unresolvedFunction") {
    assert.equal(c._expr.name, name);
    if (argCount !== undefined) assert.equal(c._expr.arguments.length, argCount);
  }
}

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

describe("null/predicate functions", () => {
  it("coalesce handles multiple columns", () => {
    const result = coalesce("a", "b", "c");
    if (result._expr.type === "unresolvedFunction") {
      assert.equal(result._expr.arguments.length, 3);
    }
  });

  it("isnull/isnan/isnotnull", () => {
    assertFn(isnull("x"), "isnull", 1);
    assertFn(isnan("x"), "isnan", 1);
    assertFn(isnotnull("x"), "isnotnull", 1);
  });

  it("nanvl takes two columns", () => {
    assertFn(nanvl("x", "y"), "nanvl", 2);
  });

  it("ifnull/nvl are aliases", () => {
    assertFn(ifnull("x", "y"), "ifnull", 2);
    assertFn(nvl("x", "y"), "ifnull", 2);
  });

  it("nvl2 takes three args", () => {
    assertFn(nvl2("x", "y", "z"), "nvl2", 3);
  });

  it("nullif takes two args", () => {
    assertFn(nullif("x", "y"), "nullif", 2);
  });
});

describe("utility functions", () => {
  it("expr() creates expressionString", () => {
    const result = expr("id + 1");
    assert.equal(result._expr.type, "expressionString");
    if (result._expr.type === "expressionString") {
      assert.equal(result._expr.expression, "id + 1");
    }
  });

  it("monotonically_increasing_id() is zero-arg", () => {
    assertFn(monotonically_increasing_id(), "monotonically_increasing_id", 0);
  });

  it("spark_partition_id() is zero-arg", () => {
    assertFn(spark_partition_id(), "spark_partition_id", 0);
  });

  it("typeof_ creates unresolvedFunction", () => {
    assertFn(typeof_("x"), "typeof", 1);
  });

  it("uuid() is zero-arg", () => {
    assertFn(uuid(), "uuid", 0);
  });

  it("broadcast passes through column", () => {
    const result = broadcast(col("x"));
    // broadcast is a pass-through, not an unresolvedFunction
    assert.equal(result._expr.type, "unresolvedAttribute");
  });
});
