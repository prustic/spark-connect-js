import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { PlanBuilder } from "./plan-builder.js";
import type { CatalogOperation } from "./logical-plan.js";

/** Helper: build a catalog plan and return the inner catalog object. */
function buildCatalog(operation: CatalogOperation): Record<string, unknown> {
  const result = PlanBuilder.toRelation({ type: "catalog", operation }) as {
    catalog: Record<string, unknown>;
  };
  assert.ok(result.catalog, "expected a catalog key in the relation");
  return result.catalog;
}

describe("PlanBuilder catalog operations", () => {
  it("listDatabases", () => {
    assert.deepStrictEqual(buildCatalog({ op: "listDatabases", pattern: "test_*" }), {
      listDatabases: { pattern: "test_*" },
    });
  });

  it("listDatabases without pattern", () => {
    assert.deepStrictEqual(buildCatalog({ op: "listDatabases" }), {
      listDatabases: { pattern: undefined },
    });
  });

  it("listTables", () => {
    assert.deepStrictEqual(buildCatalog({ op: "listTables", dbName: "mydb", pattern: "t_*" }), {
      listTables: { dbName: "mydb", pattern: "t_*" },
    });
  });

  it("listColumns", () => {
    assert.deepStrictEqual(buildCatalog({ op: "listColumns", tableName: "users", dbName: "db" }), {
      listColumns: { tableName: "users", dbName: "db" },
    });
  });

  it("listFunctions", () => {
    assert.deepStrictEqual(buildCatalog({ op: "listFunctions", dbName: "db1", pattern: "avg*" }), {
      listFunctions: { dbName: "db1", pattern: "avg*" },
    });
  });

  it("listCatalogs", () => {
    assert.deepStrictEqual(buildCatalog({ op: "listCatalogs", pattern: "spark*" }), {
      listCatalogs: { pattern: "spark*" },
    });
  });

  it("getDatabase", () => {
    assert.deepStrictEqual(buildCatalog({ op: "getDatabase", dbName: "default" }), {
      getDatabase: { dbName: "default" },
    });
  });

  it("getTable", () => {
    assert.deepStrictEqual(buildCatalog({ op: "getTable", tableName: "t1", dbName: "db" }), {
      getTable: { tableName: "t1", dbName: "db" },
    });
  });

  it("getFunction", () => {
    assert.deepStrictEqual(buildCatalog({ op: "getFunction", functionName: "avg", dbName: "db" }), {
      getFunction: { functionName: "avg", dbName: "db" },
    });
  });

  it("tableExists", () => {
    assert.deepStrictEqual(buildCatalog({ op: "tableExists", tableName: "t1", dbName: "db" }), {
      tableExists: { tableName: "t1", dbName: "db" },
    });
  });

  it("databaseExists", () => {
    assert.deepStrictEqual(buildCatalog({ op: "databaseExists", dbName: "mydb" }), {
      databaseExists: { dbName: "mydb" },
    });
  });

  it("functionExists", () => {
    assert.deepStrictEqual(
      buildCatalog({ op: "functionExists", functionName: "avg", dbName: "db" }),
      { functionExists: { functionName: "avg", dbName: "db" } },
    );
  });

  it("isCached", () => {
    assert.deepStrictEqual(buildCatalog({ op: "isCached", tableName: "t1" }), {
      isCached: { tableName: "t1" },
    });
  });

  it("dropTempView", () => {
    assert.deepStrictEqual(buildCatalog({ op: "dropTempView", viewName: "v1" }), {
      dropTempView: { viewName: "v1" },
    });
  });

  it("dropGlobalTempView", () => {
    assert.deepStrictEqual(buildCatalog({ op: "dropGlobalTempView", viewName: "gv1" }), {
      dropGlobalTempView: { viewName: "gv1" },
    });
  });

  it("currentDatabase", () => {
    assert.deepStrictEqual(buildCatalog({ op: "currentDatabase" }), {
      currentDatabase: {},
    });
  });

  it("setCurrentDatabase", () => {
    assert.deepStrictEqual(buildCatalog({ op: "setCurrentDatabase", dbName: "newdb" }), {
      setCurrentDatabase: { dbName: "newdb" },
    });
  });

  it("currentCatalog", () => {
    assert.deepStrictEqual(buildCatalog({ op: "currentCatalog" }), {
      currentCatalog: {},
    });
  });

  it("setCurrentCatalog", () => {
    assert.deepStrictEqual(buildCatalog({ op: "setCurrentCatalog", catalogName: "hive" }), {
      setCurrentCatalog: { catalogName: "hive" },
    });
  });

  it("cacheTable without storageLevel", () => {
    assert.deepStrictEqual(buildCatalog({ op: "cacheTable", tableName: "t1" }), {
      cacheTable: { tableName: "t1", storageLevel: undefined },
    });
  });

  it("cacheTable with storageLevel", () => {
    const sl = {
      useDisk: true,
      useMemory: true,
      useOffHeap: false,
      deserialized: false,
      replication: 1,
    };
    assert.deepStrictEqual(buildCatalog({ op: "cacheTable", tableName: "t1", storageLevel: sl }), {
      cacheTable: { tableName: "t1", storageLevel: sl },
    });
  });

  it("uncacheTable", () => {
    assert.deepStrictEqual(buildCatalog({ op: "uncacheTable", tableName: "t1" }), {
      uncacheTable: { tableName: "t1" },
    });
  });

  it("clearCache", () => {
    assert.deepStrictEqual(buildCatalog({ op: "clearCache" }), {
      clearCache: {},
    });
  });

  it("refreshTable", () => {
    assert.deepStrictEqual(buildCatalog({ op: "refreshTable", tableName: "t1" }), {
      refreshTable: { tableName: "t1" },
    });
  });

  it("refreshByPath", () => {
    assert.deepStrictEqual(buildCatalog({ op: "refreshByPath", path: "/data/t1" }), {
      refreshByPath: { path: "/data/t1" },
    });
  });

  it("recoverPartitions", () => {
    assert.deepStrictEqual(buildCatalog({ op: "recoverPartitions", tableName: "t1" }), {
      recoverPartitions: { tableName: "t1" },
    });
  });

  it("createTable with minimal options", () => {
    assert.deepStrictEqual(buildCatalog({ op: "createTable", tableName: "t1" }), {
      createTable: { tableName: "t1" },
    });
  });

  it("createTable with all options", () => {
    assert.deepStrictEqual(
      buildCatalog({
        op: "createTable",
        tableName: "t1",
        path: "/data/t1",
        source: "parquet",
        description: "test table",
        schema: "id INT, name STRING",
        options: { key: "value" },
      }),
      {
        createTable: {
          tableName: "t1",
          path: "/data/t1",
          source: "parquet",
          description: "test table",
          schema: { unparsed: { dataTypeString: "id INT, name STRING" } },
          options: { key: "value" },
        },
      },
    );
  });
});

describe("PlanBuilder toExpression", () => {
  it("unresolvedAttribute", () => {
    assert.deepStrictEqual(
      PlanBuilder.toExpression({ type: "unresolvedAttribute", name: "col1" }),
      {
        unresolvedAttribute: { unparsedIdentifier: "col1" },
      },
    );
  });

  it("literal null", () => {
    assert.deepStrictEqual(PlanBuilder.toExpression({ type: "literal", value: null }), {
      literal: { null: {} },
    });
  });

  it("literal string", () => {
    assert.deepStrictEqual(PlanBuilder.toExpression({ type: "literal", value: "hello" }), {
      literal: { string: "hello" },
    });
  });

  it("literal boolean", () => {
    assert.deepStrictEqual(PlanBuilder.toExpression({ type: "literal", value: true }), {
      literal: { boolean: true },
    });
  });

  it("literal bigint", () => {
    assert.deepStrictEqual(PlanBuilder.toExpression({ type: "literal", value: 123n }), {
      literal: { long: "123" },
    });
  });

  it("literal number (double)", () => {
    assert.deepStrictEqual(PlanBuilder.toExpression({ type: "literal", value: 3.14 }), {
      literal: { double: 3.14 },
    });
  });

  it("alias", () => {
    assert.deepStrictEqual(
      PlanBuilder.toExpression({
        type: "alias",
        inner: { type: "unresolvedAttribute", name: "col1" },
        name: "alias1",
      }),
      {
        alias: {
          expr: { unresolvedAttribute: { unparsedIdentifier: "col1" } },
          name: ["alias1"],
        },
      },
    );
  });

  it("aggregateFunction", () => {
    assert.deepStrictEqual(
      PlanBuilder.toExpression({
        type: "aggregateFunction",
        name: "sum",
        arguments: [{ type: "unresolvedAttribute", name: "amount" }],
        isDistinct: true,
      }),
      {
        unresolvedFunction: {
          functionName: "sum",
          arguments: [{ unresolvedAttribute: { unparsedIdentifier: "amount" } }],
          isDistinct: true,
        },
      },
    );
  });

  it("aggregateFunction defaults isDistinct to false", () => {
    const result = PlanBuilder.toExpression({
      type: "aggregateFunction",
      name: "count",
      arguments: [{ type: "literal", value: 1 }],
    });
    assert.strictEqual(
      (result as { unresolvedFunction: { isDistinct: boolean } }).unresolvedFunction.isDistinct,
      false,
    );
  });

  it("binary operator gt", () => {
    assert.deepStrictEqual(
      PlanBuilder.toExpression({
        type: "gt",
        left: { type: "unresolvedAttribute", name: "age" },
        right: { type: "literal", value: 18 },
      }),
      {
        unresolvedFunction: {
          functionName: ">",
          arguments: [
            { unresolvedAttribute: { unparsedIdentifier: "age" } },
            { literal: { double: 18 } },
          ],
          isDistinct: false,
        },
      },
    );
  });

  it("binary operator lt", () => {
    const result = PlanBuilder.toExpression({
      type: "lt",
      left: { type: "literal", value: 1 },
      right: { type: "literal", value: 2 },
    });
    assert.strictEqual(
      (result as { unresolvedFunction: { functionName: string } }).unresolvedFunction.functionName,
      "<",
    );
  });

  it("binary operator eq", () => {
    const result = PlanBuilder.toExpression({
      type: "eq",
      left: { type: "literal", value: "a" },
      right: { type: "literal", value: "b" },
    });
    assert.strictEqual(
      (result as { unresolvedFunction: { functionName: string } }).unresolvedFunction.functionName,
      "=",
    );
  });

  it("binary operator neq", () => {
    const result = PlanBuilder.toExpression({
      type: "neq",
      left: { type: "literal", value: 1 },
      right: { type: "literal", value: 2 },
    });
    assert.strictEqual(
      (result as { unresolvedFunction: { functionName: string } }).unresolvedFunction.functionName,
      "!=",
    );
  });

  it("binary operator gte", () => {
    const result = PlanBuilder.toExpression({
      type: "gte",
      left: { type: "literal", value: 5 },
      right: { type: "literal", value: 3 },
    });
    assert.strictEqual(
      (result as { unresolvedFunction: { functionName: string } }).unresolvedFunction.functionName,
      ">=",
    );
  });

  it("binary operator lte", () => {
    const result = PlanBuilder.toExpression({
      type: "lte",
      left: { type: "literal", value: 3 },
      right: { type: "literal", value: 5 },
    });
    assert.strictEqual(
      (result as { unresolvedFunction: { functionName: string } }).unresolvedFunction.functionName,
      "<=",
    );
  });

  it("binary operator and", () => {
    const result = PlanBuilder.toExpression({
      type: "and",
      left: { type: "literal", value: true },
      right: { type: "literal", value: false },
    });
    assert.strictEqual(
      (result as { unresolvedFunction: { functionName: string } }).unresolvedFunction.functionName,
      "and",
    );
  });

  it("binary operator or", () => {
    const result = PlanBuilder.toExpression({
      type: "or",
      left: { type: "literal", value: true },
      right: { type: "literal", value: false },
    });
    assert.strictEqual(
      (result as { unresolvedFunction: { functionName: string } }).unresolvedFunction.functionName,
      "or",
    );
  });

  it("binary operator add", () => {
    const result = PlanBuilder.toExpression({
      type: "add",
      left: { type: "literal", value: 1 },
      right: { type: "literal", value: 2 },
    });
    assert.strictEqual(
      (result as { unresolvedFunction: { functionName: string } }).unresolvedFunction.functionName,
      "+",
    );
  });

  it("binary operator subtract", () => {
    const result = PlanBuilder.toExpression({
      type: "subtract",
      left: { type: "literal", value: 5 },
      right: { type: "literal", value: 3 },
    });
    assert.strictEqual(
      (result as { unresolvedFunction: { functionName: string } }).unresolvedFunction.functionName,
      "-",
    );
  });

  it("binary operator multiply", () => {
    const result = PlanBuilder.toExpression({
      type: "multiply",
      left: { type: "literal", value: 3 },
      right: { type: "literal", value: 4 },
    });
    assert.strictEqual(
      (result as { unresolvedFunction: { functionName: string } }).unresolvedFunction.functionName,
      "*",
    );
  });

  it("binary operator divide", () => {
    const result = PlanBuilder.toExpression({
      type: "divide",
      left: { type: "literal", value: 10 },
      right: { type: "literal", value: 2 },
    });
    assert.strictEqual(
      (result as { unresolvedFunction: { functionName: string } }).unresolvedFunction.functionName,
      "/",
    );
  });

  it("sortOrder", () => {
    // sortOrder just extracts the inner expression
    const result = PlanBuilder.toExpression({
      type: "sortOrder",
      inner: { type: "unresolvedAttribute", name: "col1" },
      direction: "ascending",
      nullOrdering: "nulls_first",
    });
    assert.deepStrictEqual(result, {
      unresolvedAttribute: { unparsedIdentifier: "col1" },
    });
  });

  it("unresolvedFunction", () => {
    assert.deepStrictEqual(
      PlanBuilder.toExpression({
        type: "unresolvedFunction",
        name: "upper",
        arguments: [{ type: "unresolvedAttribute", name: "name" }],
        isDistinct: false,
      }),
      {
        unresolvedFunction: {
          functionName: "upper",
          arguments: [{ unresolvedAttribute: { unparsedIdentifier: "name" } }],
          isDistinct: false,
        },
      },
    );
  });

  it("unresolvedFunction defaults isDistinct to false", () => {
    const result = PlanBuilder.toExpression({
      type: "unresolvedFunction",
      name: "lower",
      arguments: [{ type: "literal", value: "ABC" }],
    });
    assert.strictEqual(
      (result as { unresolvedFunction: { isDistinct: boolean } }).unresolvedFunction.isDistinct,
      false,
    );
  });

  it("expressionString", () => {
    assert.deepStrictEqual(
      PlanBuilder.toExpression({ type: "expressionString", expression: "col1 + 1" }),
      {
        expressionString: { expression: "col1 + 1" },
      },
    );
  });

  it("cast", () => {
    assert.deepStrictEqual(
      PlanBuilder.toExpression({
        type: "cast",
        inner: { type: "unresolvedAttribute", name: "str_col" },
        targetType: "int",
      }),
      {
        cast: {
          expr: { unresolvedAttribute: { unparsedIdentifier: "str_col" } },
          typeStr: "int",
        },
      },
    );
  });

  it("window without frameSpec", () => {
    const result = PlanBuilder.toExpression({
      type: "window",
      windowFunction: {
        type: "aggregateFunction",
        name: "sum",
        arguments: [{ type: "unresolvedAttribute", name: "amt" }],
      },
      partitionSpec: [{ type: "unresolvedAttribute", name: "dept" }],
      orderSpec: [
        {
          expression: { type: "unresolvedAttribute", name: "date" },
          direction: "ascending",
          nullOrdering: "nulls_last",
        },
      ],
    });
    const w = (result as { window: Record<string, unknown> }).window;
    assert.ok(w.windowFunction);
    assert.ok(Array.isArray(w.partitionSpec));
    assert.ok(Array.isArray(w.orderSpec));
    assert.strictEqual(w.frameSpec, undefined);
  });

  it("window with row frameSpec using currentRow and unbounded", () => {
    const result = PlanBuilder.toExpression({
      type: "window",
      windowFunction: {
        type: "aggregateFunction",
        name: "count",
        arguments: [{ type: "literal", value: 1 }],
      },
      partitionSpec: [],
      orderSpec: [],
      frameSpec: {
        frameType: "row",
        lower: { type: "unbounded" },
        upper: { type: "currentRow" },
      },
    });
    const w = (result as { window: { frameSpec: Record<string, unknown> } }).window;
    assert.deepStrictEqual(w.frameSpec, {
      frameType: "ROW",
      lower: { unbounded: true },
      upper: { currentRow: true },
    });
  });

  it("window with range frameSpec using value bounds", () => {
    const result = PlanBuilder.toExpression({
      type: "window",
      windowFunction: {
        type: "aggregateFunction",
        name: "avg",
        arguments: [{ type: "unresolvedAttribute", name: "price" }],
      },
      partitionSpec: [],
      orderSpec: [],
      frameSpec: {
        frameType: "range",
        lower: { type: "value", value: { type: "literal", value: -10 } },
        upper: { type: "value", value: { type: "literal", value: 10 } },
      },
    });
    const w = (result as { window: { frameSpec: Record<string, unknown> } }).window;
    assert.strictEqual((w.frameSpec as { frameType: string }).frameType, "RANGE");
    assert.deepStrictEqual((w.frameSpec as { lower: { value: unknown } }).lower, {
      value: { literal: { double: -10 } },
    });
    assert.deepStrictEqual((w.frameSpec as { upper: { value: unknown } }).upper, {
      value: { literal: { double: 10 } },
    });
  });
});

describe("PlanBuilder toRelation", () => {
  it("aggregate with pivot using string values", () => {
    const result = PlanBuilder.toRelation({
      type: "aggregate",
      child: { type: "sql", query: "SELECT * FROM t" },
      groupingExpressions: [{ type: "unresolvedAttribute", name: "category" }],
      aggregateExpressions: [
        {
          type: "aggregateFunction",
          name: "sum",
          arguments: [{ type: "unresolvedAttribute", name: "amount" }],
        },
      ],
      groupType: "pivot",
      pivot: {
        col: { type: "unresolvedAttribute", name: "year" },
        values: ["2023", "2024", "2025"],
      },
    });
    const agg = (result as { aggregate: Record<string, unknown> }).aggregate;
    assert.strictEqual(agg.groupType, "GROUP_TYPE_PIVOT");
    const pivot = agg.pivot as { col: unknown; values: Array<{ literal: { string: string } }> };
    assert.deepStrictEqual(pivot.values, [
      { literal: { string: "2023" } },
      { literal: { string: "2024" } },
      { literal: { string: "2025" } },
    ]);
  });

  it("aggregate with pivot using number values", () => {
    const result = PlanBuilder.toRelation({
      type: "aggregate",
      child: { type: "sql", query: "SELECT * FROM t" },
      groupingExpressions: [{ type: "unresolvedAttribute", name: "category" }],
      aggregateExpressions: [
        { type: "aggregateFunction", name: "count", arguments: [{ type: "literal", value: 1 }] },
      ],
      groupType: "pivot",
      pivot: {
        col: { type: "unresolvedAttribute", name: "quarter" },
        values: [1, 2, 3, 4],
      },
    });
    const agg = (result as { aggregate: Record<string, unknown> }).aggregate;
    const pivot = agg.pivot as { values: Array<{ literal: { double: number } }> };
    assert.deepStrictEqual(pivot.values, [
      { literal: { double: 1 } },
      { literal: { double: 2 } },
      { literal: { double: 3 } },
      { literal: { double: 4 } },
    ]);
  });

  it("aggregate with pivot using boolean values", () => {
    const result = PlanBuilder.toRelation({
      type: "aggregate",
      child: { type: "sql", query: "SELECT * FROM t" },
      groupingExpressions: [{ type: "unresolvedAttribute", name: "category" }],
      aggregateExpressions: [
        {
          type: "aggregateFunction",
          name: "sum",
          arguments: [{ type: "unresolvedAttribute", name: "amount" }],
        },
      ],
      groupType: "pivot",
      pivot: {
        col: { type: "unresolvedAttribute", name: "is_active" },
        values: [true, false],
      },
    });
    const agg = (result as { aggregate: Record<string, unknown> }).aggregate;
    const pivot = agg.pivot as { values: Array<{ literal: { boolean: boolean } }> };
    assert.deepStrictEqual(pivot.values, [
      { literal: { boolean: true } },
      { literal: { boolean: false } },
    ]);
  });

  it("naReplace with string replacements", () => {
    const result = PlanBuilder.toRelation({
      type: "naReplace",
      child: { type: "sql", query: "SELECT * FROM t" },
      cols: ["name"],
      replacements: [
        { oldValue: "unknown", newValue: "N/A" },
        { oldValue: "missing", newValue: "default" },
      ],
    });
    const replace = (result as { replace: Record<string, unknown> }).replace;
    assert.deepStrictEqual(replace.cols, ["name"]);
    const replacements = replace.replacements as Array<{
      oldValue: { literal: unknown };
      newValue: { literal: unknown };
    }>;
    assert.deepStrictEqual(replacements[0].oldValue, { literal: { string: "unknown" } });
    assert.deepStrictEqual(replacements[0].newValue, { literal: { string: "N/A" } });
  });

  it("naReplace with number replacements", () => {
    const result = PlanBuilder.toRelation({
      type: "naReplace",
      child: { type: "sql", query: "SELECT * FROM t" },
      cols: ["value"],
      replacements: [
        { oldValue: -1, newValue: 0 },
        { oldValue: 999, newValue: 100 },
      ],
    });
    const replace = (result as { replace: Record<string, unknown> }).replace;
    const replacements = replace.replacements as Array<{
      oldValue: { literal: unknown };
      newValue: { literal: unknown };
    }>;
    assert.deepStrictEqual(replacements[0].oldValue, { literal: { double: -1 } });
    assert.deepStrictEqual(replacements[0].newValue, { literal: { double: 0 } });
    assert.deepStrictEqual(replacements[1].oldValue, { literal: { double: 999 } });
    assert.deepStrictEqual(replacements[1].newValue, { literal: { double: 100 } });
  });

  it("naReplace with boolean replacements", () => {
    const result = PlanBuilder.toRelation({
      type: "naReplace",
      child: { type: "sql", query: "SELECT * FROM t" },
      cols: ["flag"],
      replacements: [{ oldValue: false, newValue: true }],
    });
    const replace = (result as { replace: Record<string, unknown> }).replace;
    const replacements = replace.replacements as Array<{
      oldValue: { literal: unknown };
      newValue: { literal: unknown };
    }>;
    assert.deepStrictEqual(replacements[0].oldValue, { literal: { boolean: false } });
    assert.deepStrictEqual(replacements[0].newValue, { literal: { boolean: true } });
  });

  it("watermark", () => {
    const result = PlanBuilder.toRelation({
      type: "watermark",
      child: { type: "sql", query: "SELECT * FROM events" },
      eventTime: "ts",
      delayThreshold: "10 minutes",
    });
    const withWatermark = (result as { withWatermark: Record<string, unknown> }).withWatermark;
    assert.equal(withWatermark.eventTime, "ts");
    assert.equal(withWatermark.delayThreshold, "10 minutes");
    assert.ok(withWatermark.input);
  });
});
