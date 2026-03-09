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

describe("Column.isNull() / isNotNull()", () => {
  it("isNull() produces unresolvedFunction isnull", () => {
    const expr = col("x").isNull()._expr;
    assert.equal(expr.type, "unresolvedFunction");
    if (expr.type === "unresolvedFunction") {
      assert.equal(expr.name, "isnull");
      assert.equal(expr.arguments.length, 1);
    }
  });

  it("isNotNull() produces unresolvedFunction isnotnull", () => {
    const expr = col("x").isNotNull()._expr;
    assert.equal(expr.type, "unresolvedFunction");
    if (expr.type === "unresolvedFunction") {
      assert.equal(expr.name, "isnotnull");
    }
  });
});

describe("Column.isin()", () => {
  it("produces unresolvedFunction in with literal values", () => {
    const expr = col("status").isin("active", "pending")._expr;
    assert.equal(expr.type, "unresolvedFunction");
    if (expr.type === "unresolvedFunction") {
      assert.equal(expr.name, "in");
      // first arg is the column, then the two literals
      assert.equal(expr.arguments.length, 3);
      assert.equal(expr.arguments[0].type, "unresolvedAttribute");
      assert.deepStrictEqual(expr.arguments[1], { type: "literal", value: "active" });
      assert.deepStrictEqual(expr.arguments[2], { type: "literal", value: "pending" });
    }
  });
});

describe("Column.between()", () => {
  it("composes gte + lte + and", () => {
    const expr = col("age").between(lit(18), lit(65))._expr;
    assert.equal(expr.type, "and");
  });
});

describe("Column string matching", () => {
  it("like()", () => {
    const expr = col("name").like("%John%")._expr;
    assert.equal(expr.type, "unresolvedFunction");
    if (expr.type === "unresolvedFunction") {
      assert.equal(expr.name, "like");
      assert.equal(expr.arguments.length, 2);
    }
  });

  it("rlike()", () => {
    const expr = col("name").rlike("^J.*n$")._expr;
    assert.equal(expr.type, "unresolvedFunction");
    if (expr.type === "unresolvedFunction") {
      assert.equal(expr.name, "rlike");
    }
  });

  it("startsWith()", () => {
    const expr = col("name").startsWith("Jo")._expr;
    assert.equal(expr.type, "unresolvedFunction");
    if (expr.type === "unresolvedFunction") {
      assert.equal(expr.name, "startswith");
    }
  });

  it("endsWith()", () => {
    const expr = col("name").endsWith("hn")._expr;
    assert.equal(expr.type, "unresolvedFunction");
    if (expr.type === "unresolvedFunction") {
      assert.equal(expr.name, "endswith");
    }
  });

  it("contains()", () => {
    const expr = col("name").contains("oh")._expr;
    assert.equal(expr.type, "unresolvedFunction");
    if (expr.type === "unresolvedFunction") {
      assert.equal(expr.name, "contains");
    }
  });
});

describe("Column.over()", () => {
  it("produces window expression with partition and order specs", async () => {
    // Dynamic import to avoid circular dep issues
    const { WindowSpec } = await import("./window.js");
    const spec = new WindowSpec().partitionBy("dept").orderBy("salary");
    const expr = col("salary").over(spec)._expr;
    assert.equal(expr.type, "window");
    if (expr.type === "window") {
      assert.equal(expr.partitionSpec.length, 1);
      assert.equal(expr.orderSpec.length, 1);
    }
  });
});

describe("Column sort ordering variants", () => {
  it("asc_nulls_first()", () => {
    const expr = col("x").asc_nulls_first()._expr;
    assert.equal(expr.type, "sortOrder");
    if (expr.type === "sortOrder") {
      assert.equal(expr.direction, "ascending");
      assert.equal(expr.nullOrdering, "nulls_first");
    }
  });

  it("asc_nulls_last() is same as asc()", () => {
    const expr = col("x").asc_nulls_last()._expr;
    assert.equal(expr.type, "sortOrder");
    if (expr.type === "sortOrder") {
      assert.equal(expr.direction, "ascending");
      assert.equal(expr.nullOrdering, "nulls_last");
    }
  });

  it("desc_nulls_first()", () => {
    const expr = col("x").desc_nulls_first()._expr;
    assert.equal(expr.type, "sortOrder");
    if (expr.type === "sortOrder") {
      assert.equal(expr.direction, "descending");
      assert.equal(expr.nullOrdering, "nulls_first");
    }
  });

  it("desc_nulls_last() is same as desc()", () => {
    const expr = col("x").desc_nulls_last()._expr;
    assert.equal(expr.type, "sortOrder");
    if (expr.type === "sortOrder") {
      assert.equal(expr.direction, "descending");
      assert.equal(expr.nullOrdering, "nulls_last");
    }
  });
});

describe("Column.isNaN()", () => {
  it("produces isnan function call", () => {
    const expr = col("x").isNaN()._expr;
    assert.equal(expr.type, "unresolvedFunction");
    if (expr.type === "unresolvedFunction") {
      assert.equal(expr.name, "isnan");
      assert.equal(expr.arguments.length, 1);
    }
  });
});

describe("Column.eqNullSafe()", () => {
  it("produces <=> function call", () => {
    const expr = col("x").eqNullSafe(col("y"))._expr;
    assert.equal(expr.type, "unresolvedFunction");
    if (expr.type === "unresolvedFunction") {
      assert.equal(expr.name, "<=>");
      assert.equal(expr.arguments.length, 2);
    }
  });
});

describe("Column bitwise operators", () => {
  it("bitwiseAND()", () => {
    const expr = col("x").bitwiseAND(col("y"))._expr;
    assert.equal(expr.type, "unresolvedFunction");
    if (expr.type === "unresolvedFunction") assert.equal(expr.name, "&");
  });

  it("bitwiseOR()", () => {
    const expr = col("x").bitwiseOR(col("y"))._expr;
    assert.equal(expr.type, "unresolvedFunction");
    if (expr.type === "unresolvedFunction") assert.equal(expr.name, "|");
  });

  it("bitwiseXOR()", () => {
    const expr = col("x").bitwiseXOR(col("y"))._expr;
    assert.equal(expr.type, "unresolvedFunction");
    if (expr.type === "unresolvedFunction") assert.equal(expr.name, "^");
  });
});

describe("Column.substr()", () => {
  it("produces substring function call", () => {
    const expr = col("name").substr(1, 3)._expr;
    assert.equal(expr.type, "unresolvedFunction");
    if (expr.type === "unresolvedFunction") {
      assert.equal(expr.name, "substring");
      assert.equal(expr.arguments.length, 3);
    }
  });
});

describe("Column.ilike()", () => {
  it("produces ilike function call", () => {
    const expr = col("name").ilike("%john%")._expr;
    assert.equal(expr.type, "unresolvedFunction");
    if (expr.type === "unresolvedFunction") {
      assert.equal(expr.name, "ilike");
      assert.equal(expr.arguments.length, 2);
    }
  });
});

describe("Column struct/map/array field access", () => {
  it("getField()", () => {
    const expr = col("address").getField("city")._expr;
    assert.equal(expr.type, "unresolvedFunction");
    if (expr.type === "unresolvedFunction") {
      assert.equal(expr.name, "get_field");
      assert.equal(expr.arguments.length, 2);
    }
  });

  it("getItem()", () => {
    const expr = col("items").getItem(0)._expr;
    assert.equal(expr.type, "unresolvedFunction");
    if (expr.type === "unresolvedFunction") {
      assert.equal(expr.name, "get");
      assert.equal(expr.arguments.length, 2);
    }
  });

  it("withField()", () => {
    const expr = col("address").withField("zip", col("postal_code"))._expr;
    assert.equal(expr.type, "unresolvedFunction");
    if (expr.type === "unresolvedFunction") {
      assert.equal(expr.name, "with_field");
      assert.equal(expr.arguments.length, 3);
    }
  });

  it("dropFields()", () => {
    const expr = col("address").dropFields("line2", "line3")._expr;
    assert.equal(expr.type, "unresolvedFunction");
    if (expr.type === "unresolvedFunction") {
      assert.equal(expr.name, "drop_fields");
      assert.equal(expr.arguments.length, 3); // self + 2 field names
    }
  });
});
