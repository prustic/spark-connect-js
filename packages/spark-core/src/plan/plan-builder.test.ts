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
