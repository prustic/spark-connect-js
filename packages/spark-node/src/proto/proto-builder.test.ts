import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { buildRelation, buildExpression } from "./proto-builder.js";
import type { LogicalPlan, Expression as CoreExpression } from "@spark-connect-js/core";
import { UnsupportedOperationError } from "@spark-connect-js/core";
import { Expression_Cast_EvalMode } from "@spark-connect-js/connect";

describe("buildRelation()", () => {
  it("builds a SQL relation", () => {
    const plan: LogicalPlan = { type: "sql", query: "SELECT 1" };
    const rel = buildRelation(plan);
    assert.equal(rel.relType.case, "sql");
    if (rel.relType.case === "sql") {
      assert.equal(rel.relType.value.query, "SELECT 1");
    }
  });

  it("stamps RelationCommon.planId when the plan carries one", () => {
    const plan: LogicalPlan = { type: "sql", query: "SELECT 1", planId: 42n };
    const rel = buildRelation(plan);
    assert.equal(rel.common?.planId, 42n);
  });

  it("omits RelationCommon when the plan has no planId", () => {
    const plan: LogicalPlan = { type: "sql", query: "SELECT 1" };
    const rel = buildRelation(plan);
    assert.equal(rel.common, undefined);
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
      assert.equal(rel.relType.value.isStreaming, false);
      assert.equal(rel.relType.value.readType.case, "dataSource");
      if (rel.relType.value.readType.case === "dataSource") {
        assert.equal(rel.relType.value.readType.value.format, "parquet");
        assert.deepStrictEqual(rel.relType.value.readType.value.paths, ["/data/users"]);
      }
    }
  });

  it("preserves an empty path for non-streaming reads (no silent behavior change)", () => {
    const plan: LogicalPlan = {
      type: "read",
      format: "noop",
      path: "",
      options: {},
    };
    const rel = buildRelation(plan);
    assert.equal(rel.relType.case, "read");
    if (rel.relType.case === "read") {
      assert.equal(rel.relType.value.isStreaming, false);
      if (rel.relType.value.readType.case === "dataSource") {
        assert.deepStrictEqual(rel.relType.value.readType.value.paths, [""]);
      }
    }
  });

  it("builds a streaming Read with isStreaming=true and omits empty path", () => {
    const plan: LogicalPlan = {
      type: "read",
      format: "rate",
      path: "",
      options: { rowsPerSecond: "5" },
      isStreaming: true,
    };
    const rel = buildRelation(plan);
    assert.equal(rel.relType.case, "read");
    if (rel.relType.case === "read") {
      assert.equal(rel.relType.value.isStreaming, true);
      if (rel.relType.value.readType.case === "dataSource") {
        assert.equal(rel.relType.value.readType.value.format, "rate");
        assert.deepStrictEqual(rel.relType.value.readType.value.paths, []);
      }
    }
  });

  it("builds a streaming readTable with isStreaming=true", () => {
    const plan: LogicalPlan = {
      type: "readTable",
      tableName: "events",
      options: {},
      isStreaming: true,
    };
    const rel = buildRelation(plan);
    assert.equal(rel.relType.case, "read");
    if (rel.relType.case === "read") {
      assert.equal(rel.relType.value.isStreaming, true);
      assert.equal(rel.relType.value.readType.case, "namedTable");
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
      assert.equal(result.exprType.value.planId, undefined);
    }
  });

  it("forwards planId on unresolved attribute when present (df.col disambiguation)", () => {
    const expr: CoreExpression = { type: "unresolvedAttribute", name: "id", planId: 99n };
    const result = buildExpression(expr);
    if (result.exprType.case === "unresolvedAttribute") {
      assert.equal(result.exprType.value.planId, 99n);
    } else {
      assert.fail("expected unresolvedAttribute");
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

  it("builds null literal with a NullType DataType", () => {
    const result = buildExpression({ type: "literal", value: null });
    assert.equal(result.exprType.case, "literal");
    if (result.exprType.case === "literal") {
      assert.equal(result.exprType.value.literalType.case, "null");
      if (result.exprType.value.literalType.case === "null") {
        assert.equal(result.exprType.value.literalType.value.kind.case, "null");
      }
    }
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
      assert.equal(result.exprType.value.isDistinct, false);
    }
  });

  it("builds aggregate function with isDistinct", () => {
    const result = buildExpression({
      type: "aggregateFunction",
      name: "count",
      arguments: [{ type: "unresolvedAttribute", name: "id" }],
      isDistinct: true,
    });
    assert.equal(result.exprType.case, "unresolvedFunction");
    if (result.exprType.case === "unresolvedFunction") {
      assert.equal(result.exprType.value.functionName, "count");
      assert.equal(result.exprType.value.isDistinct, true);
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
      assert.equal(result.exprType.value.evalMode, Expression_Cast_EvalMode.UNSPECIFIED);
    }
  });

  it("builds cast with the try eval mode", () => {
    const result = buildExpression({
      type: "cast",
      inner: { type: "unresolvedAttribute", name: "id" },
      targetType: "int",
      evalMode: "try",
    });
    if (result.exprType.case !== "cast") {
      assert.fail("expected a cast expression");
    }
    assert.equal(result.exprType.value.evalMode, Expression_Cast_EvalMode.TRY);
  });

  it("lowers caseWhen to the when function, flattening branches", () => {
    const branch = (n: number) => ({
      condition: { type: "unresolvedAttribute" as const, name: `c${String(n)}` },
      value: { type: "literal" as const, value: n },
    });
    const result = buildExpression({
      type: "caseWhen",
      branches: [branch(1), branch(2)],
      elseValue: { type: "literal", value: 0 },
    });
    if (result.exprType.case !== "unresolvedFunction") {
      assert.fail("expected an unresolvedFunction expression");
    }
    assert.equal(result.exprType.value.functionName, "when");
    assert.equal(result.exprType.value.arguments.length, 5);
    assert.equal(result.exprType.value.isDistinct, false);
  });

  it("omits the trailing argument when caseWhen has no else value", () => {
    const result = buildExpression({
      type: "caseWhen",
      branches: [
        {
          condition: { type: "unresolvedAttribute", name: "c" },
          value: { type: "literal", value: 1 },
        },
      ],
    });
    if (result.exprType.case !== "unresolvedFunction") {
      assert.fail("expected an unresolvedFunction expression");
    }
    assert.equal(result.exprType.value.arguments.length, 2);
  });
});

describe("buildRelation() - catalog", () => {
  /** Extract the catalog catType from a relation, asserting the outer shape. */
  function catType(plan: LogicalPlan) {
    const rel = buildRelation(plan);
    assert.equal(rel.relType.case, "catalog");
    assert.ok(rel.relType.value, "catalog value must exist");
    return (rel.relType.value as { catType: { case: string; value: unknown } }).catType;
  }

  it("builds currentDatabase", () => {
    const cat = catType({ type: "catalog", operation: { op: "currentDatabase" } });
    assert.equal(cat.case, "currentDatabase");
  });

  it("builds setCurrentDatabase", () => {
    const cat = catType({
      type: "catalog",
      operation: { op: "setCurrentDatabase", dbName: "test_db" },
    });
    assert.equal(cat.case, "setCurrentDatabase");
    assert.equal((cat.value as { dbName: string }).dbName, "test_db");
  });

  it("builds listDatabases with pattern", () => {
    const cat = catType({
      type: "catalog",
      operation: { op: "listDatabases", pattern: "test*" },
    });
    assert.equal(cat.case, "listDatabases");
    assert.equal((cat.value as { pattern: string }).pattern, "test*");
  });

  it("builds listTables with dbName and pattern", () => {
    const cat = catType({
      type: "catalog",
      operation: { op: "listTables", dbName: "default", pattern: "emp*" },
    });
    assert.equal(cat.case, "listTables");
    assert.equal((cat.value as { dbName: string }).dbName, "default");
    assert.equal((cat.value as { pattern: string }).pattern, "emp*");
  });

  it("builds listColumns", () => {
    const cat = catType({
      type: "catalog",
      operation: { op: "listColumns", tableName: "my_table", dbName: "default" },
    });
    assert.equal(cat.case, "listColumns");
    assert.equal((cat.value as { tableName: string }).tableName, "my_table");
    assert.equal((cat.value as { dbName: string }).dbName, "default");
  });

  it("builds listFunctions with dbName and pattern", () => {
    const cat = catType({
      type: "catalog",
      operation: { op: "listFunctions", dbName: "default", pattern: "count*" },
    });
    assert.equal(cat.case, "listFunctions");
    assert.equal((cat.value as { dbName: string }).dbName, "default");
    assert.equal((cat.value as { pattern: string }).pattern, "count*");
  });

  it("builds listCatalogs with pattern", () => {
    const cat = catType({
      type: "catalog",
      operation: { op: "listCatalogs", pattern: "spark*" },
    });
    assert.equal(cat.case, "listCatalogs");
    assert.equal((cat.value as { pattern: string }).pattern, "spark*");
  });

  it("builds getDatabase", () => {
    const cat = catType({
      type: "catalog",
      operation: { op: "getDatabase", dbName: "default" },
    });
    assert.equal(cat.case, "getDatabase");
    assert.equal((cat.value as { dbName: string }).dbName, "default");
  });

  it("builds getTable with dbName", () => {
    const cat = catType({
      type: "catalog",
      operation: { op: "getTable", tableName: "my_table", dbName: "default" },
    });
    assert.equal(cat.case, "getTable");
    assert.equal((cat.value as { tableName: string }).tableName, "my_table");
    assert.equal((cat.value as { dbName: string }).dbName, "default");
  });

  it("builds getFunction", () => {
    const cat = catType({
      type: "catalog",
      operation: { op: "getFunction", functionName: "count" },
    });
    assert.equal(cat.case, "getFunction");
    assert.equal((cat.value as { functionName: string }).functionName, "count");
  });

  it("builds tableExists", () => {
    const cat = catType({
      type: "catalog",
      operation: { op: "tableExists", tableName: "my_table" },
    });
    assert.equal(cat.case, "tableExists");
    assert.equal((cat.value as { tableName: string }).tableName, "my_table");
  });

  it("builds databaseExists", () => {
    const cat = catType({
      type: "catalog",
      operation: { op: "databaseExists", dbName: "test_db" },
    });
    assert.equal(cat.case, "databaseExists");
    assert.equal((cat.value as { dbName: string }).dbName, "test_db");
  });

  it("builds functionExists with dbName", () => {
    const cat = catType({
      type: "catalog",
      operation: { op: "functionExists", functionName: "count", dbName: "default" },
    });
    assert.equal(cat.case, "functionExists");
    assert.equal((cat.value as { functionName: string }).functionName, "count");
    assert.equal((cat.value as { dbName: string }).dbName, "default");
  });

  it("builds isCached", () => {
    const cat = catType({
      type: "catalog",
      operation: { op: "isCached", tableName: "my_table" },
    });
    assert.equal(cat.case, "isCached");
    assert.equal((cat.value as { tableName: string }).tableName, "my_table");
  });

  it("builds dropTempView", () => {
    const cat = catType({
      type: "catalog",
      operation: { op: "dropTempView", viewName: "my_view" },
    });
    assert.equal(cat.case, "dropTempView");
    assert.equal((cat.value as { viewName: string }).viewName, "my_view");
  });

  it("builds dropGlobalTempView", () => {
    const cat = catType({
      type: "catalog",
      operation: { op: "dropGlobalTempView", viewName: "my_global_view" },
    });
    assert.equal(cat.case, "dropGlobalTempView");
    assert.equal((cat.value as { viewName: string }).viewName, "my_global_view");
  });

  it("builds currentCatalog", () => {
    const cat = catType({ type: "catalog", operation: { op: "currentCatalog" } });
    assert.equal(cat.case, "currentCatalog");
  });

  it("builds setCurrentCatalog", () => {
    const cat = catType({
      type: "catalog",
      operation: { op: "setCurrentCatalog", catalogName: "my_catalog" },
    });
    assert.equal(cat.case, "setCurrentCatalog");
    assert.equal((cat.value as { catalogName: string }).catalogName, "my_catalog");
  });

  it("builds cacheTable without storageLevel", () => {
    const cat = catType({
      type: "catalog",
      operation: { op: "cacheTable", tableName: "my_table" },
    });
    assert.equal(cat.case, "cacheTable");
    assert.equal((cat.value as { tableName: string }).tableName, "my_table");
  });

  it("builds cacheTable with storageLevel", () => {
    const cat = catType({
      type: "catalog",
      operation: {
        op: "cacheTable",
        tableName: "my_table",
        storageLevel: {
          useDisk: true,
          useMemory: true,
          useOffHeap: false,
          deserialized: false,
          replication: 1,
        },
      },
    });
    assert.equal(cat.case, "cacheTable");
    const val = cat.value as { tableName: string; storageLevel: { useDisk: boolean } };
    assert.equal(val.tableName, "my_table");
    assert.equal(val.storageLevel.useDisk, true);
  });

  it("builds uncacheTable", () => {
    const cat = catType({
      type: "catalog",
      operation: { op: "uncacheTable", tableName: "my_table" },
    });
    assert.equal(cat.case, "uncacheTable");
    assert.equal((cat.value as { tableName: string }).tableName, "my_table");
  });

  it("builds clearCache", () => {
    const cat = catType({ type: "catalog", operation: { op: "clearCache" } });
    assert.equal(cat.case, "clearCache");
  });

  it("builds refreshTable", () => {
    const cat = catType({
      type: "catalog",
      operation: { op: "refreshTable", tableName: "my_table" },
    });
    assert.equal(cat.case, "refreshTable");
    assert.equal((cat.value as { tableName: string }).tableName, "my_table");
  });

  it("builds refreshByPath", () => {
    const cat = catType({
      type: "catalog",
      operation: { op: "refreshByPath", path: "/data/my_table" },
    });
    assert.equal(cat.case, "refreshByPath");
    assert.equal((cat.value as { path: string }).path, "/data/my_table");
  });

  it("builds recoverPartitions", () => {
    const cat = catType({
      type: "catalog",
      operation: { op: "recoverPartitions", tableName: "my_table" },
    });
    assert.equal(cat.case, "recoverPartitions");
    assert.equal((cat.value as { tableName: string }).tableName, "my_table");
  });

  it("builds createTable minimal", () => {
    const cat = catType({
      type: "catalog",
      operation: { op: "createTable", tableName: "new_table" },
    });
    assert.equal(cat.case, "createTable");
    assert.equal((cat.value as { tableName: string }).tableName, "new_table");
  });

  it("builds createTable with all options", () => {
    const cat = catType({
      type: "catalog",
      operation: {
        op: "createTable",
        tableName: "new_table",
        path: "/data/tables/new_table",
        source: "parquet",
        description: "A test table",
        schema: "name string, age integer",
        options: { compression: "snappy" },
      },
    });
    assert.equal(cat.case, "createTable");
    const val = cat.value as {
      tableName: string;
      path: string;
      source: string;
      description: string;
      schema: { kind: { case: string } };
    };
    assert.equal(val.tableName, "new_table");
    assert.equal(val.path, "/data/tables/new_table");
    assert.equal(val.source, "parquet");
    assert.equal(val.description, "A test table");
    assert.equal(val.schema.kind.case, "unparsed");
  });
});

describe("buildRelation() - setOperation", () => {
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

describe("buildRelation() - sample", () => {
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

describe("buildRelation() - fillNa / dropNa", () => {
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

describe("buildRelation() - toDF / describe", () => {
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

describe("buildExpression() - window", () => {
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

describe("buildRelation() - readTable", () => {
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

describe("buildRelation() - localRelation", () => {
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

describe("buildRelation() - range", () => {
  it("builds a Range relation", () => {
    const result = buildRelation({
      type: "range",
      start: 0,
      end: 100,
      step: 2,
    });
    assert.equal(result.relType.case, "range");
    if (result.relType.case === "range") {
      assert.equal(result.relType.value.start, 0n);
      assert.equal(result.relType.value.end, 100n);
      assert.equal(result.relType.value.step, 2n);
    }
  });

  it("builds a Range relation with numPartitions", () => {
    const result = buildRelation({
      type: "range",
      start: 0,
      end: 50,
      step: 1,
      numPartitions: 4,
    });
    if (result.relType.case === "range") {
      assert.equal(result.relType.value.numPartitions, 4);
    }
  });
});

describe("buildRelation() - withColumnsRenamed", () => {
  it("builds a WithColumnsRenamed relation", () => {
    const result = buildRelation({
      type: "withColumnsRenamed",
      child: { type: "sql", query: "SELECT * FROM t" },
      renames: [
        { colName: "old_name", newColName: "new_name" },
        { colName: "a", newColName: "b" },
      ],
    });
    assert.equal(result.relType.case, "withColumnsRenamed");
    if (result.relType.case === "withColumnsRenamed") {
      assert.ok(result.relType.value.input);
      assert.equal(result.relType.value.renames.length, 2);
      assert.equal(result.relType.value.renames[0].colName, "old_name");
      assert.equal(result.relType.value.renames[0].newColName, "new_name");
    }
  });
});

describe("buildRelation() - subqueryAlias", () => {
  it("builds a SubqueryAlias relation", () => {
    const result = buildRelation({
      type: "subqueryAlias",
      child: { type: "sql", query: "SELECT * FROM t" },
      alias: "t1",
    });
    assert.equal(result.relType.case, "subqueryAlias");
    if (result.relType.case === "subqueryAlias") {
      assert.ok(result.relType.value.input);
      assert.equal(result.relType.value.alias, "t1");
    }
  });
});

describe("buildRelation() - hint", () => {
  it("builds a Hint relation", () => {
    const result = buildRelation({
      type: "hint",
      child: { type: "sql", query: "SELECT * FROM t" },
      name: "broadcast",
      parameters: [],
    });
    assert.equal(result.relType.case, "hint");
    if (result.relType.case === "hint") {
      assert.ok(result.relType.value.input);
      assert.equal(result.relType.value.name, "broadcast");
      assert.equal(result.relType.value.parameters.length, 0);
    }
  });

  it("builds a Hint relation with parameters", () => {
    const result = buildRelation({
      type: "hint",
      child: { type: "sql", query: "SELECT * FROM t" },
      name: "repartition",
      parameters: [{ type: "literal", value: 10 }],
    });
    if (result.relType.case === "hint") {
      assert.equal(result.relType.value.parameters.length, 1);
    }
  });
});

describe("buildRelation() - tail", () => {
  it("builds a Tail relation", () => {
    const result = buildRelation({
      type: "tail",
      child: { type: "sql", query: "SELECT * FROM t" },
      limit: 5,
    });
    assert.equal(result.relType.case, "tail");
    if (result.relType.case === "tail") {
      assert.ok(result.relType.value.input);
      assert.equal(result.relType.value.limit, 5);
    }
  });
});

describe("buildRelation() - repartition", () => {
  it("builds a Repartition relation with shuffle", () => {
    const result = buildRelation({
      type: "repartition",
      child: { type: "sql", query: "SELECT * FROM t" },
      numPartitions: 10,
      shuffle: true,
    });
    assert.equal(result.relType.case, "repartition");
    if (result.relType.case === "repartition") {
      assert.ok(result.relType.value.input);
      assert.equal(result.relType.value.numPartitions, 10);
      assert.equal(result.relType.value.shuffle, true);
    }
  });

  it("builds a coalesce (shuffle=false)", () => {
    const result = buildRelation({
      type: "repartition",
      child: { type: "sql", query: "SELECT * FROM t" },
      numPartitions: 1,
      shuffle: false,
    });
    if (result.relType.case === "repartition") {
      assert.equal(result.relType.value.shuffle, false);
    }
  });
});

describe("buildRelation() - repartitionByExpression", () => {
  it("builds a RepartitionByExpression relation", () => {
    const result = buildRelation({
      type: "repartitionByExpression",
      child: { type: "sql", query: "SELECT * FROM t" },
      partitionExprs: [{ type: "unresolvedAttribute", name: "dept" }],
      numPartitions: 8,
    });
    assert.equal(result.relType.case, "repartitionByExpression");
    if (result.relType.case === "repartitionByExpression") {
      assert.ok(result.relType.value.input);
      assert.equal(result.relType.value.partitionExprs.length, 1);
      assert.equal(result.relType.value.numPartitions, 8);
    }
  });
});

describe("buildRelation() - summary", () => {
  it("builds a StatSummary relation", () => {
    const result = buildRelation({
      type: "summary",
      child: { type: "sql", query: "SELECT * FROM t" },
      statistics: ["count", "mean", "stddev"],
    });
    assert.equal(result.relType.case, "summary");
    if (result.relType.case === "summary") {
      assert.ok(result.relType.value.input);
      assert.deepStrictEqual(result.relType.value.statistics, ["count", "mean", "stddev"]);
    }
  });
});

describe("buildRelation() - naReplace", () => {
  it("builds a NAReplace relation", () => {
    const result = buildRelation({
      type: "naReplace",
      child: { type: "sql", query: "SELECT * FROM t" },
      cols: ["salary"],
      replacements: [
        { oldValue: 0, newValue: 100 },
        { oldValue: "unknown", newValue: "N/A" },
      ],
    });
    assert.equal(result.relType.case, "replace");
    if (result.relType.case === "replace") {
      assert.ok(result.relType.value.input);
      assert.deepStrictEqual(result.relType.value.cols, ["salary"]);
      assert.equal(result.relType.value.replacements.length, 2);
    }
  });

  it("uses double (not integer) for numeric replace values", () => {
    const result = buildRelation({
      type: "naReplace",
      child: { type: "sql", query: "SELECT * FROM t" },
      cols: [],
      replacements: [{ oldValue: 42, newValue: 99 }],
    });
    if (result.relType.case === "replace") {
      const rep = result.relType.value.replacements[0];
      assert.equal(rep.oldValue?.literalType.case, "double");
      assert.equal(rep.newValue?.literalType.case, "double");
    }
  });
});

describe("buildRelation() - unpivot", () => {
  it("builds an Unpivot relation with explicit values", () => {
    const result = buildRelation({
      type: "unpivot",
      child: { type: "sql", query: "SELECT * FROM t" },
      ids: [{ type: "unresolvedAttribute", name: "id" }],
      values: [
        { type: "unresolvedAttribute", name: "q1" },
        { type: "unresolvedAttribute", name: "q2" },
      ],
      variableColumnName: "quarter",
      valueColumnName: "revenue",
    });
    assert.equal(result.relType.case, "unpivot");
    if (result.relType.case === "unpivot") {
      assert.ok(result.relType.value.input);
      assert.equal(result.relType.value.ids.length, 1);
      assert.ok(result.relType.value.values);
      assert.equal(result.relType.value.values.values.length, 2);
      assert.equal(result.relType.value.variableColumnName, "quarter");
      assert.equal(result.relType.value.valueColumnName, "revenue");
    }
  });

  it("builds an Unpivot relation without explicit values", () => {
    const result = buildRelation({
      type: "unpivot",
      child: { type: "sql", query: "SELECT * FROM t" },
      ids: [{ type: "unresolvedAttribute", name: "id" }],
      variableColumnName: "var",
      valueColumnName: "val",
    });
    if (result.relType.case === "unpivot") {
      assert.equal(result.relType.value.values, undefined);
    }
  });
});

describe("buildRelation() - stat functions", () => {
  it("builds a StatCorr relation", () => {
    const result = buildRelation({
      type: "statCorr",
      child: { type: "sql", query: "SELECT * FROM t" },
      col1: "height",
      col2: "weight",
      method: "pearson",
    });
    assert.equal(result.relType.case, "corr");
    if (result.relType.case === "corr") {
      assert.ok(result.relType.value.input);
      assert.equal(result.relType.value.col1, "height");
      assert.equal(result.relType.value.col2, "weight");
      assert.equal(result.relType.value.method, "pearson");
    }
  });

  it("builds a StatCov relation", () => {
    const result = buildRelation({
      type: "statCov",
      child: { type: "sql", query: "SELECT * FROM t" },
      col1: "x",
      col2: "y",
    });
    assert.equal(result.relType.case, "cov");
    if (result.relType.case === "cov") {
      assert.equal(result.relType.value.col1, "x");
      assert.equal(result.relType.value.col2, "y");
    }
  });

  it("builds a StatCrosstab relation", () => {
    const result = buildRelation({
      type: "statCrosstab",
      child: { type: "sql", query: "SELECT * FROM t" },
      col1: "dept",
      col2: "status",
    });
    assert.equal(result.relType.case, "crosstab");
    if (result.relType.case === "crosstab") {
      assert.equal(result.relType.value.col1, "dept");
      assert.equal(result.relType.value.col2, "status");
    }
  });

  it("builds a StatFreqItems relation", () => {
    const result = buildRelation({
      type: "statFreqItems",
      child: { type: "sql", query: "SELECT * FROM t" },
      cols: ["category", "region"],
      support: 0.3,
    });
    assert.equal(result.relType.case, "freqItems");
    if (result.relType.case === "freqItems") {
      assert.deepStrictEqual(result.relType.value.cols, ["category", "region"]);
      assert.equal(result.relType.value.support, 0.3);
    }
  });

  it("builds a StatApproxQuantile relation", () => {
    const result = buildRelation({
      type: "statApproxQuantile",
      child: { type: "sql", query: "SELECT * FROM t" },
      cols: ["salary"],
      probabilities: [0.25, 0.5, 0.75],
      relativeError: 0.01,
    });
    assert.equal(result.relType.case, "approxQuantile");
    if (result.relType.case === "approxQuantile") {
      assert.deepStrictEqual(result.relType.value.cols, ["salary"]);
      assert.deepStrictEqual(result.relType.value.probabilities, [0.25, 0.5, 0.75]);
      assert.equal(result.relType.value.relativeError, 0.01);
    }
  });

  it("builds a WithWatermark relation", () => {
    const result = buildRelation({
      type: "watermark",
      child: { type: "sql", query: "SELECT * FROM events" },
      eventTime: "ts",
      delayThreshold: "10 minutes",
    });
    assert.equal(result.relType.case, "withWatermark");
    if (result.relType.case === "withWatermark") {
      assert.equal(result.relType.value.eventTime, "ts");
      assert.equal(result.relType.value.delayThreshold, "10 minutes");
      assert.equal(result.relType.value.input?.relType.case, "sql");
    }
  });

  it("builds a CollectMetrics relation", () => {
    const result = buildRelation({
      type: "collectMetrics",
      child: { type: "sql", query: "SELECT * FROM t" },
      name: "stats",
      metrics: [
        {
          type: "alias",
          name: "total",
          inner: { type: "unresolvedAttribute", name: "n" },
        },
      ],
    });
    assert.equal(result.relType.case, "collectMetrics");
    if (result.relType.case === "collectMetrics") {
      assert.equal(result.relType.value.name, "stats");
      assert.equal(result.relType.value.metrics.length, 1);
      assert.equal(result.relType.value.input?.relType.case, "sql");
    }
  });
});

describe("buildRelation() - aggregate groupTypes", () => {
  it("builds a rollup aggregate", () => {
    const result = buildRelation({
      type: "aggregate",
      child: { type: "sql", query: "SELECT * FROM t" },
      groupType: "rollup",
      groupingExpressions: [{ type: "unresolvedAttribute", name: "dept" }],
      aggregateExpressions: [
        {
          type: "aggregateFunction",
          name: "sum",
          arguments: [{ type: "unresolvedAttribute", name: "salary" }],
        },
      ],
    });
    assert.equal(result.relType.case, "aggregate");
    if (result.relType.case === "aggregate") {
      assert.equal(result.relType.value.groupType, 2); // ROLLUP
    }
  });

  it("builds a cube aggregate", () => {
    const result = buildRelation({
      type: "aggregate",
      child: { type: "sql", query: "SELECT * FROM t" },
      groupType: "cube",
      groupingExpressions: [{ type: "unresolvedAttribute", name: "dept" }],
      aggregateExpressions: [
        {
          type: "aggregateFunction",
          name: "count",
          arguments: [{ type: "unresolvedAttribute", name: "id" }],
        },
      ],
    });
    if (result.relType.case === "aggregate") {
      assert.equal(result.relType.value.groupType, 3); // CUBE
    }
  });

  it("builds a pivot aggregate", () => {
    const result = buildRelation({
      type: "aggregate",
      child: { type: "sql", query: "SELECT * FROM t" },
      groupType: "pivot",
      groupingExpressions: [{ type: "unresolvedAttribute", name: "dept" }],
      aggregateExpressions: [
        {
          type: "aggregateFunction",
          name: "sum",
          arguments: [{ type: "unresolvedAttribute", name: "salary" }],
        },
      ],
      pivot: {
        col: { type: "unresolvedAttribute", name: "year" },
        values: [2023, 2024, 2025],
      },
    });
    if (result.relType.case === "aggregate") {
      assert.equal(result.relType.value.groupType, 4); // PIVOT
      assert.ok(result.relType.value.pivot);
      assert.equal(result.relType.value.pivot.values.length, 3);
    }
  });

  it("pivot with a null value emits a NullType literal (not LITERALTYPE_NOT_SET)", () => {
    const result = buildRelation({
      type: "aggregate",
      child: { type: "sql", query: "SELECT * FROM t" },
      groupType: "pivot",
      groupingExpressions: [{ type: "unresolvedAttribute", name: "dept" }],
      aggregateExpressions: [
        {
          type: "aggregateFunction",
          name: "sum",
          arguments: [{ type: "unresolvedAttribute", name: "salary" }],
        },
      ],
      pivot: {
        col: { type: "unresolvedAttribute", name: "year" },
        values: [null],
      },
    });
    if (result.relType.case === "aggregate" && result.relType.value.pivot) {
      const lit = result.relType.value.pivot.values[0];
      assert.equal(lit.literalType.case, "null");
      if (lit.literalType.case === "null") {
        assert.equal(lit.literalType.value.kind.case, "null");
      }
    }
  });
});

describe("buildRelation() - sort", () => {
  it("builds a Sort relation", () => {
    const result = buildRelation({
      type: "sort",
      child: { type: "sql", query: "SELECT * FROM t" },
      order: [
        {
          expression: { type: "unresolvedAttribute", name: "age" },
          direction: "descending",
          nullOrdering: "nulls_last",
        },
      ],
      isGlobal: true,
    });
    assert.equal(result.relType.case, "sort");
    if (result.relType.case === "sort") {
      assert.ok(result.relType.value.input);
      assert.equal(result.relType.value.order.length, 1);
      assert.equal(result.relType.value.isGlobal, true);
    }
  });
});

describe("buildExpression() - expressionString", () => {
  it("builds an expressionString expression", () => {
    const result = buildExpression({
      type: "expressionString",
      expression: "col1 + col2",
    });
    assert.equal(result.exprType.case, "expressionString");
    if (result.exprType.case === "expressionString") {
      assert.equal(result.exprType.value.expression, "col1 + col2");
    }
  });
});

describe("buildExpression() - sortOrder", () => {
  it("sortOrder delegates to inner expression", () => {
    const result = buildExpression({
      type: "sortOrder",
      inner: { type: "unresolvedAttribute", name: "x" },
      direction: "ascending",
      nullOrdering: "nulls_first",
    });
    assert.equal(result.exprType.case, "unresolvedAttribute");
    if (result.exprType.case === "unresolvedAttribute") {
      assert.equal(result.exprType.value.unparsedIdentifier, "x");
    }
  });
});

describe("exhaustive checks", () => {
  it("buildRelation throws UnsupportedOperationError on unsupported plan type", () => {
    const bogus = { type: "bogus" } as unknown as LogicalPlan;
    assert.throws(() => buildRelation(bogus), UnsupportedOperationError);
  });

  it("buildExpression throws UnsupportedOperationError on unsupported expression type", () => {
    const bogus = { type: "bogus" } as unknown as CoreExpression;
    assert.throws(() => buildExpression(bogus), UnsupportedOperationError);
  });
});
