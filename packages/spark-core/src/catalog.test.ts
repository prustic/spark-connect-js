import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { Catalog } from "./catalog.js";
import { DataFrame } from "./data-frame.js";
import { StructType, StructField } from "./types/struct.js";
import type { SparkSession } from "./spark-session.js";
import type { CatalogOperation } from "./plan/logical-plan.js";

/**
 * Create a Catalog with a stub session — just enough to let
 * DataFrame._fromPlan store the session reference without executing anything.
 */
function makeCatalog(): Catalog {
  return new Catalog({} as SparkSession);
}

/** Extract the CatalogOperation from the DataFrame plan. */
function planOp(df: DataFrame): CatalogOperation {
  if (df._plan.type !== "catalog") {
    throw new Error(`expected catalog plan, got ${df._plan.type}`);
  }
  return df._plan.operation;
}

describe("Catalog — sync DataFrame methods", () => {
  it("listDatabases() builds correct operation", () => {
    const op = planOp(makeCatalog().listDatabases());
    assert.equal(op.op, "listDatabases");
  });

  it("listDatabases(pattern) passes pattern", () => {
    const op = planOp(makeCatalog().listDatabases("test*"));
    assert.equal(op.op, "listDatabases");
    assert.equal((op as { pattern: string }).pattern, "test*");
  });

  it("listTables() builds correct operation", () => {
    const op = planOp(makeCatalog().listTables("default", "emp*"));
    assert.equal(op.op, "listTables");
    assert.equal((op as { dbName: string }).dbName, "default");
    assert.equal((op as { pattern: string }).pattern, "emp*");
  });

  it("listColumns() builds correct operation", () => {
    const op = planOp(makeCatalog().listColumns("my_table", "db1"));
    assert.equal(op.op, "listColumns");
    assert.equal((op as { tableName: string }).tableName, "my_table");
    assert.equal((op as { dbName: string }).dbName, "db1");
  });

  it("listFunctions() builds correct operation", () => {
    const op = planOp(makeCatalog().listFunctions("db1", "count*"));
    assert.equal(op.op, "listFunctions");
    assert.equal((op as { dbName: string }).dbName, "db1");
  });

  it("listCatalogs() builds correct operation", () => {
    const op = planOp(makeCatalog().listCatalogs("spark*"));
    assert.equal(op.op, "listCatalogs");
    assert.equal((op as { pattern: string }).pattern, "spark*");
  });

  it("getDatabase() builds correct operation", () => {
    const op = planOp(makeCatalog().getDatabase("default"));
    assert.equal(op.op, "getDatabase");
    assert.equal((op as { dbName: string }).dbName, "default");
  });

  it("getTable() builds correct operation", () => {
    const op = planOp(makeCatalog().getTable("t1", "db1"));
    assert.equal(op.op, "getTable");
    assert.equal((op as { tableName: string }).tableName, "t1");
    assert.equal((op as { dbName: string }).dbName, "db1");
  });

  it("getFunction() builds correct operation", () => {
    const op = planOp(makeCatalog().getFunction("count", "db1"));
    assert.equal(op.op, "getFunction");
    assert.equal((op as { functionName: string }).functionName, "count");
    assert.equal((op as { dbName: string }).dbName, "db1");
  });

  it("createTable() builds correct operation with options", () => {
    const schema = new StructType([
      new StructField("id", "long"),
      new StructField("name", "string"),
    ]);
    const op = planOp(
      makeCatalog().createTable("new_table", {
        path: "/data/t",
        source: "parquet",
        description: "A table",
        schema,
        options: { compression: "snappy" },
      }),
    );
    assert.equal(op.op, "createTable");
    assert.equal((op as { tableName: string }).tableName, "new_table");
    assert.equal((op as { path: string }).path, "/data/t");
    assert.equal((op as { source: string }).source, "parquet");
    assert.equal((op as { description: string }).description, "A table");
    assert.equal((op as { schema: string }).schema, schema.toDDL());
    assert.deepStrictEqual((op as { options: Record<string, string> }).options, {
      compression: "snappy",
    });
  });

  it("createTable() works with minimal args", () => {
    const op = planOp(makeCatalog().createTable("t1"));
    assert.equal(op.op, "createTable");
    assert.equal((op as { tableName: string }).tableName, "t1");
  });

  it("createExternalTable() delegates to createTable()", () => {
    const op = planOp(makeCatalog().createExternalTable("ext_table", { source: "csv" }));
    assert.equal(op.op, "createTable");
    assert.equal((op as { tableName: string }).tableName, "ext_table");
    assert.equal((op as { source: string }).source, "csv");
  });
});
