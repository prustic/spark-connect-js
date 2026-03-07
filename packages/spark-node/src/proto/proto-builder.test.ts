import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { buildRelation, buildExpression } from "./proto-builder.js";
import type { LogicalPlan, Expression as CoreExpression } from "@spark-js/core";

describe("buildRelation()", () => {
  it("builds a SQL relation", () => {
    const plan: LogicalPlan = { type: "sql", query: "SELECT 1" };
    const rel = buildRelation(plan);
    assert.equal(rel.relType.case, "sql");
    if (rel.relType.case === "sql") {
      assert.equal(rel.relType.value.query, "SELECT 1");
    }
  });

  it("builds a Read/DataSource relation", () => {
    const plan: LogicalPlan = {
      type: "read",
      format: "parquet",
      path: "/data/users",
      options: { mergeSchema: "true" },
    };
    const rel = buildRelation(plan);
    assert.equal(rel.relType.case, "read");
    if (rel.relType.case === "read") {
      assert.equal(rel.relType.value.readType.case, "dataSource");
      if (rel.relType.value.readType.case === "dataSource") {
        assert.equal(rel.relType.value.readType.value.format, "parquet");
        assert.deepStrictEqual(rel.relType.value.readType.value.paths, ["/data/users"]);
      }
    }
  });

  it("builds a Filter relation", () => {
    const plan: LogicalPlan = {
      type: "filter",
      child: { type: "sql", query: "SELECT * FROM t" },
      condition: {
        type: "gt",
        left: { type: "unresolvedAttribute", name: "x" },
        right: { type: "literal", value: 5 },
      },
    };
    const rel = buildRelation(plan);
    assert.equal(rel.relType.case, "filter");
    if (rel.relType.case === "filter") {
      assert.ok(rel.relType.value.input);
      assert.ok(rel.relType.value.condition);
    }
  });

  it("builds a Project relation", () => {
    const plan: LogicalPlan = {
      type: "project",
      child: { type: "sql", query: "SELECT * FROM t" },
      expressions: [{ type: "unresolvedAttribute", name: "a" }],
    };
    const rel = buildRelation(plan);
    assert.equal(rel.relType.case, "project");
    if (rel.relType.case === "project") {
      assert.equal(rel.relType.value.expressions.length, 1);
    }
  });

  it("builds an Aggregate relation", () => {
    const plan: LogicalPlan = {
      type: "aggregate",
      child: { type: "sql", query: "SELECT * FROM t" },
      groupingExpressions: [{ type: "unresolvedAttribute", name: "dept" }],
      aggregateExpressions: [
        {
          type: "aggregateFunction",
          name: "sum",
          arguments: [{ type: "unresolvedAttribute", name: "salary" }],
        },
      ],
    };
    const rel = buildRelation(plan);
    assert.equal(rel.relType.case, "aggregate");
    if (rel.relType.case === "aggregate") {
      assert.equal(rel.relType.value.groupingExpressions.length, 1);
      assert.equal(rel.relType.value.aggregateExpressions.length, 1);
    }
  });

  it("builds a Limit relation", () => {
    const plan: LogicalPlan = {
      type: "limit",
      child: { type: "sql", query: "SELECT * FROM t" },
      limit: 10,
    };
    const rel = buildRelation(plan);
    assert.equal(rel.relType.case, "limit");
    if (rel.relType.case === "limit") {
      assert.equal(rel.relType.value.limit, 10);
    }
  });

  it("builds nested plans (filter → project → limit)", () => {
    const plan: LogicalPlan = {
      type: "limit",
      child: {
        type: "project",
        child: {
          type: "filter",
          child: { type: "sql", query: "SELECT * FROM t" },
          condition: {
            type: "gt",
            left: { type: "unresolvedAttribute", name: "x" },
            right: { type: "literal", value: 0 },
          },
        },
        expressions: [{ type: "unresolvedAttribute", name: "x" }],
      },
      limit: 5,
    };
    const rel = buildRelation(plan);
    assert.equal(rel.relType.case, "limit");
  });
});

describe("buildExpression()", () => {
  it("builds unresolved attribute", () => {
    const expr: CoreExpression = { type: "unresolvedAttribute", name: "col1" };
    const result = buildExpression(expr);
    assert.equal(result.exprType.case, "unresolvedAttribute");
    if (result.exprType.case === "unresolvedAttribute") {
      assert.equal(result.exprType.value.unparsedIdentifier, "col1");
    }
  });

  it("builds string literal", () => {
    const result = buildExpression({ type: "literal", value: "hello" });
    assert.equal(result.exprType.case, "literal");
    if (result.exprType.case === "literal") {
      assert.equal(result.exprType.value.literalType.case, "string");
      assert.equal(result.exprType.value.literalType.value, "hello");
    }
  });

  it("builds integer literal", () => {
    const result = buildExpression({ type: "literal", value: 42 });
    if (result.exprType.case === "literal") {
      assert.equal(result.exprType.value.literalType.case, "integer");
      assert.equal(result.exprType.value.literalType.value, 42);
    }
  });

  it("builds double literal for non-integer numbers", () => {
    const result = buildExpression({ type: "literal", value: 3.14 });
    if (result.exprType.case === "literal") {
      assert.equal(result.exprType.value.literalType.case, "double");
    }
  });

  it("builds boolean literal", () => {
    const result = buildExpression({ type: "literal", value: true });
    if (result.exprType.case === "literal") {
      assert.equal(result.exprType.value.literalType.case, "boolean");
      assert.equal(result.exprType.value.literalType.value, true);
    }
  });

  it("builds bigint literal as long", () => {
    const result = buildExpression({ type: "literal", value: 9007199254740993n });
    if (result.exprType.case === "literal") {
      assert.equal(result.exprType.value.literalType.case, "long");
      assert.equal(result.exprType.value.literalType.value, 9007199254740993n);
    }
  });

  it("builds null literal", () => {
    const result = buildExpression({ type: "literal", value: null });
    assert.equal(result.exprType.case, "literal");
  });

  it("builds alias expression", () => {
    const result = buildExpression({
      type: "alias",
      inner: { type: "unresolvedAttribute", name: "x" },
      name: "renamed",
    });
    assert.equal(result.exprType.case, "alias");
    if (result.exprType.case === "alias") {
      assert.deepStrictEqual(result.exprType.value.name, ["renamed"]);
    }
  });

  it("builds aggregate function as unresolved function", () => {
    const result = buildExpression({
      type: "aggregateFunction",
      name: "sum",
      arguments: [{ type: "unresolvedAttribute", name: "salary" }],
    });
    assert.equal(result.exprType.case, "unresolvedFunction");
    if (result.exprType.case === "unresolvedFunction") {
      assert.equal(result.exprType.value.functionName, "sum");
      assert.equal(result.exprType.value.arguments.length, 1);
    }
  });

  it("builds binary operators as unresolved functions", () => {
    const ops: Array<{ type: CoreExpression["type"]; fn: string }> = [
      { type: "gt", fn: ">" },
      { type: "lt", fn: "<" },
      { type: "eq", fn: "=" },
      { type: "neq", fn: "!=" },
      { type: "gte", fn: ">=" },
      { type: "lte", fn: "<=" },
      { type: "and", fn: "and" },
      { type: "or", fn: "or" },
      { type: "add", fn: "+" },
      { type: "subtract", fn: "-" },
      { type: "multiply", fn: "*" },
      { type: "divide", fn: "/" },
    ];

    for (const { type, fn } of ops) {
      const expr = {
        type,
        left: { type: "unresolvedAttribute" as const, name: "a" },
        right: { type: "literal" as const, value: 1 },
      } as CoreExpression;
      const result = buildExpression(expr);
      assert.equal(
        result.exprType.case,
        "unresolvedFunction",
        `${type} should be unresolvedFunction`,
      );
      if (result.exprType.case === "unresolvedFunction") {
        assert.equal(result.exprType.value.functionName, fn, `${type} should map to ${fn}`);
        assert.equal(result.exprType.value.arguments.length, 2);
      }
    }
  });

  it("builds unresolvedFunction expression", () => {
    const result = buildExpression({
      type: "unresolvedFunction",
      name: "upper",
      arguments: [{ type: "unresolvedAttribute", name: "name" }],
    });
    assert.equal(result.exprType.case, "unresolvedFunction");
    if (result.exprType.case === "unresolvedFunction") {
      assert.equal(result.exprType.value.functionName, "upper");
      assert.equal(result.exprType.value.arguments.length, 1);
      assert.equal(result.exprType.value.isDistinct, false);
    }
  });

  it("builds unresolvedFunction with isDistinct", () => {
    const result = buildExpression({
      type: "unresolvedFunction",
      name: "count",
      arguments: [{ type: "unresolvedAttribute", name: "x" }],
      isDistinct: true,
    });
    if (result.exprType.case === "unresolvedFunction") {
      assert.equal(result.exprType.value.isDistinct, true);
    }
  });

  it("builds cast expression with typeStr", () => {
    const result = buildExpression({
      type: "cast",
      inner: { type: "unresolvedAttribute", name: "id" },
      targetType: "string",
    });
    assert.equal(result.exprType.case, "cast");
    if (result.exprType.case === "cast") {
      assert.equal(result.exprType.value.castToType.case, "typeStr");
      assert.equal(result.exprType.value.castToType.value, "string");
      assert.ok(result.exprType.value.expr);
    }
  });
});

describe("buildRelation() — catalog", () => {
  it("builds a listDatabases catalog relation", () => {
    const plan: LogicalPlan = {
      type: "catalog",
      operation: { op: "listDatabases" },
    };
    const rel = buildRelation(plan);
    assert.equal(rel.relType.case, "catalog");
  });

  it("builds a listTables catalog relation", () => {
    const plan: LogicalPlan = {
      type: "catalog",
      operation: { op: "listTables", dbName: "default" },
    };
    const rel = buildRelation(plan);
    assert.equal(rel.relType.case, "catalog");
  });

  it("builds a tableExists catalog relation", () => {
    const plan: LogicalPlan = {
      type: "catalog",
      operation: { op: "tableExists", tableName: "my_table" },
    };
    const rel = buildRelation(plan);
    assert.equal(rel.relType.case, "catalog");
  });
});

describe("buildRelation() — setOperation", () => {
  it("builds a union relation", () => {
    const plan: LogicalPlan = {
      type: "setOperation",
      left: { type: "sql", query: "SELECT * FROM a" },
      right: { type: "sql", query: "SELECT * FROM b" },
      opType: "union",
      isAll: true,
      byName: false,
      allowMissingColumns: false,
    };
    const rel = buildRelation(plan);
    assert.equal(rel.relType.case, "setOp");
  });

  it("builds an intersect relation", () => {
    const plan: LogicalPlan = {
      type: "setOperation",
      left: { type: "sql", query: "SELECT * FROM a" },
      right: { type: "sql", query: "SELECT * FROM b" },
      opType: "intersect",
      isAll: false,
      byName: false,
      allowMissingColumns: false,
    };
    const rel = buildRelation(plan);
    assert.equal(rel.relType.case, "setOp");
  });

  it("builds an except relation", () => {
    const plan: LogicalPlan = {
      type: "setOperation",
      left: { type: "sql", query: "SELECT * FROM a" },
      right: { type: "sql", query: "SELECT * FROM b" },
      opType: "except",
      isAll: false,
      byName: false,
      allowMissingColumns: false,
    };
    const rel = buildRelation(plan);
    assert.equal(rel.relType.case, "setOp");
  });
});

describe("buildRelation() — sample", () => {
  it("builds a sample relation", () => {
    const plan: LogicalPlan = {
      type: "sample",
      child: { type: "sql", query: "SELECT * FROM t" },
      lowerBound: 0.0,
      upperBound: 0.5,
      withReplacement: false,
      seed: 42,
    };
    const rel = buildRelation(plan);
    assert.equal(rel.relType.case, "sample");
  });
});

describe("buildRelation() — fillNa / dropNa", () => {
  it("builds a fillNa relation", () => {
    const plan: LogicalPlan = {
      type: "fillNa",
      child: { type: "sql", query: "SELECT * FROM t" },
      cols: ["age"],
      values: [0],
    };
    const rel = buildRelation(plan);
    assert.equal(rel.relType.case, "fillNa");
  });

  it("builds a dropNa relation", () => {
    const plan: LogicalPlan = {
      type: "dropNa",
      child: { type: "sql", query: "SELECT * FROM t" },
      cols: [],
    };
    const rel = buildRelation(plan);
    assert.equal(rel.relType.case, "dropNa");
  });
});

describe("buildRelation() — toDF / describe", () => {
  it("builds a toDF relation", () => {
    const plan: LogicalPlan = {
      type: "toDF",
      child: { type: "sql", query: "SELECT * FROM t" },
      columnNames: ["a", "b"],
    };
    const rel = buildRelation(plan);
    assert.equal(rel.relType.case, "toDf");
  });

  it("builds a describe (StatDescribe) relation", () => {
    const plan: LogicalPlan = {
      type: "describe",
      child: { type: "sql", query: "SELECT * FROM t" },
      cols: ["age"],
    };
    const rel = buildRelation(plan);
    assert.equal(rel.relType.case, "describe");
  });
});

describe("buildExpression() — window", () => {
  it("builds a window expression with partition and order", () => {
    const result = buildExpression({
      type: "window",
      windowFunction: { type: "unresolvedFunction", name: "row_number", arguments: [] },
      partitionSpec: [{ type: "unresolvedAttribute", name: "dept" }],
      orderSpec: [
        {
          expression: { type: "unresolvedAttribute", name: "salary" },
          direction: "descending",
          nullOrdering: "nulls_last",
        },
      ],
    });
    assert.equal(result.exprType.case, "window");
  });

  it("builds a window expression with frame spec", () => {
    const result = buildExpression({
      type: "window",
      windowFunction: {
        type: "unresolvedFunction",
        name: "sum",
        arguments: [{ type: "unresolvedAttribute", name: "amount" }],
      },
      partitionSpec: [],
      orderSpec: [
        {
          expression: { type: "unresolvedAttribute", name: "date" },
          direction: "ascending",
          nullOrdering: "nulls_last",
        },
      ],
      frameSpec: {
        frameType: "row",
        lower: { type: "unbounded" },
        upper: { type: "currentRow" },
      },
    });
    assert.equal(result.exprType.case, "window");
    if (result.exprType.case === "window") {
      assert.ok(result.exprType.value.frameSpec);
    }
  });
});

describe("buildRelation() — readTable", () => {
  it("builds a Read.NamedTable relation", () => {
    const result = buildRelation({
      type: "readTable",
      tableName: "my_db.my_table",
      options: { mergeSchema: "true" },
    });
    assert.equal(result.relType.case, "read");
    if (result.relType.case === "read") {
      assert.equal(result.relType.value.readType.case, "namedTable");
      if (result.relType.value.readType.case === "namedTable") {
        assert.equal(result.relType.value.readType.value.unparsedIdentifier, "my_db.my_table");
        assert.deepStrictEqual(result.relType.value.readType.value.options, {
          mergeSchema: "true",
        });
      }
    }
  });
});

describe("buildRelation() — localRelation", () => {
  it("builds a LocalRelation with data and schema", () => {
    const data = new Uint8Array([1, 2, 3]);
    const result = buildRelation({
      type: "localRelation",
      data,
      schema: "id INT, name STRING",
    });
    assert.equal(result.relType.case, "localRelation");
    if (result.relType.case === "localRelation") {
      assert.deepStrictEqual(result.relType.value.data, data);
      assert.equal(result.relType.value.schema, "id INT, name STRING");
    }
  });

  it("builds a LocalRelation with only schema", () => {
    const result = buildRelation({
      type: "localRelation",
      schema: "id INT",
    });
    assert.equal(result.relType.case, "localRelation");
    if (result.relType.case === "localRelation") {
      assert.equal(result.relType.value.data, undefined);
      assert.equal(result.relType.value.schema, "id INT");
    }
  });
});
