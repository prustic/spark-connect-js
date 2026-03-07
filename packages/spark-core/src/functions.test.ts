import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { col, lit } from "./column.js";
import {
  when,
  cast,
  upper,
  lower,
  trim,
  length,
  concat,
  substring,
  year,
  month,
  dayofmonth,
  current_date,
  datediff,
  abs,
  round,
  ceil,
  floor,
  sqrt,
  pow,
  count,
  sum,
  avg,
  min,
  max,
  countDistinct,
  coalesce,
  isnull,
  explode,
  row_number,
  rank,
  dense_rank,
  lag,
  lead,
  ntile,
} from "./functions/index.js";

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

describe("string functions", () => {
  it("upper() creates unresolvedFunction", () => {
    const result = upper("name");
    assert.equal(result._expr.type, "unresolvedFunction");
    if (result._expr.type === "unresolvedFunction") {
      assert.equal(result._expr.name, "upper");
    }
  });

  it("lower() creates unresolvedFunction", () => {
    const result = lower(col("name"));
    if (result._expr.type === "unresolvedFunction") {
      assert.equal(result._expr.name, "lower");
    }
  });

  it("trim() creates unresolvedFunction", () => {
    assert.equal(trim("name")._expr.type, "unresolvedFunction");
  });

  it("length() creates unresolvedFunction", () => {
    const result = length("name");
    if (result._expr.type === "unresolvedFunction") {
      assert.equal(result._expr.name, "length");
    }
  });

  it("concat() handles multiple columns", () => {
    const result = concat("first", "last");
    if (result._expr.type === "unresolvedFunction") {
      assert.equal(result._expr.name, "concat");
      assert.equal(result._expr.arguments.length, 2);
    }
  });

  it("substring() passes position and length", () => {
    const result = substring("name", 1, 3);
    if (result._expr.type === "unresolvedFunction") {
      assert.equal(result._expr.name, "substring");
      assert.equal(result._expr.arguments.length, 3);
    }
  });
});

describe("date functions", () => {
  it("year() creates unresolvedFunction", () => {
    const result = year("date_col");
    if (result._expr.type === "unresolvedFunction") {
      assert.equal(result._expr.name, "year");
    }
  });

  it("month() creates unresolvedFunction", () => {
    assert.equal(month("d")._expr.type, "unresolvedFunction");
  });

  it("dayofmonth() creates unresolvedFunction", () => {
    assert.equal(dayofmonth("d")._expr.type, "unresolvedFunction");
  });

  it("current_date() takes no arguments", () => {
    const result = current_date();
    if (result._expr.type === "unresolvedFunction") {
      assert.equal(result._expr.arguments.length, 0);
    }
  });

  it("datediff() accepts two columns", () => {
    const result = datediff("end", "start");
    if (result._expr.type === "unresolvedFunction") {
      assert.equal(result._expr.name, "datediff");
      assert.equal(result._expr.arguments.length, 2);
    }
  });
});

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

describe("collection/null functions", () => {
  it("coalesce handles multiple columns", () => {
    const result = coalesce("a", "b", "c");
    if (result._expr.type === "unresolvedFunction") {
      assert.equal(result._expr.arguments.length, 3);
    }
  });

  it("isnull creates unresolvedFunction", () => {
    assert.equal(isnull("x")._expr.type, "unresolvedFunction");
  });

  it("explode creates unresolvedFunction", () => {
    const result = explode("arr");
    if (result._expr.type === "unresolvedFunction") {
      assert.equal(result._expr.name, "explode");
    }
  });
});

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
