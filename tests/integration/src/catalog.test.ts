import { describe, it, after } from "node:test";
import assert from "node:assert/strict";
import { StructType, StructField } from "@spark-connect-js/node";
import { spark, stopSession, tempTable } from "./setup.js";

describe("Catalog API", () => {
  after(stopSession);

  it("currentCatalog() returns spark_catalog", async () => {
    const catalog = await spark().catalog.currentCatalog();
    assert.equal(catalog, "spark_catalog");
  });

  it("listCatalogs() returns at least spark_catalog", async () => {
    const rows = await spark().catalog.listCatalogs().collect();
    assert.ok(rows.some((r) => r["name"] === "spark_catalog"));
  });

  it("currentDatabase() returns default", async () => {
    const db = await spark().catalog.currentDatabase();
    assert.equal(db, "default");
  });

  it("getDatabase() returns metadata for default", async () => {
    const rows = await spark().catalog.getDatabase("default").collect();
    assert.equal(rows.length, 1);
    assert.equal(rows[0]["name"], "default");
  });

  it("listDatabases() includes default", async () => {
    const rows = await spark().catalog.listDatabases().collect();
    assert.ok(rows.some((r) => r["name"] === "default"));
  });

  it("databaseExists() returns true for default, false for nonexistent", async () => {
    assert.equal(await spark().catalog.databaseExists("default"), true);
    assert.equal(await spark().catalog.databaseExists("no_such_db_xyz"), false);
  });

  it("listFunctions() returns non-empty results", async () => {
    const rows = await spark().catalog.listFunctions().collect();
    assert.ok(rows.length > 0);
  });

  it("functionExists() returns true for count, false for nonexistent", async () => {
    assert.equal(await spark().catalog.functionExists("count"), true);
    assert.equal(await spark().catalog.functionExists("no_such_fn_xyz"), false);
  });

  it("getFunction() returns metadata for count", async () => {
    const rows = await spark().catalog.getFunction("count").collect();
    assert.equal(rows.length, 1);
    assert.equal(rows[0]["name"], "count");
  });

  it("listTables() reflects temp views", async () => {
    const name = tempTable("cat_list");
    await spark().range(1).createOrReplaceTempView(name);
    const rows = await spark().catalog.listTables().collect();
    assert.ok(rows.some((r) => r["name"] === name));
  });

  it("tableExists() returns true for temp view, false for nonexistent", async () => {
    const name = tempTable("cat_exists");
    await spark().range(1).createOrReplaceTempView(name);
    assert.equal(await spark().catalog.tableExists(name), true);
    assert.equal(await spark().catalog.tableExists("no_such_table_xyz"), false);
  });

  it("getTable() returns metadata for temp view", async () => {
    const name = tempTable("cat_get");
    await spark().range(1).createOrReplaceTempView(name);
    const rows = await spark().catalog.getTable(name).collect();
    assert.equal(rows.length, 1);
    assert.equal(rows[0]["name"], name);
  });

  it("listColumns() returns columns of a temp view", async () => {
    const name = tempTable("cat_cols");
    await spark().range(1).createOrReplaceTempView(name);
    const rows = await spark().catalog.listColumns(name).collect();
    assert.ok(rows.some((r) => r["name"] === "id"));
  });

  it("dropTempView() returns true then false", async () => {
    const name = tempTable("cat_drop");
    await spark().range(1).createOrReplaceTempView(name);
    assert.equal(await spark().catalog.dropTempView(name), true);
    assert.equal(await spark().catalog.dropTempView(name), false);
  });

  it("dropGlobalTempView() returns true then false", async () => {
    const name = tempTable("cat_gdrop");
    await spark().range(1).createOrReplaceGlobalTempView(name);
    assert.equal(await spark().catalog.dropGlobalTempView(name), true);
    assert.equal(await spark().catalog.dropGlobalTempView(name), false);
  });

  it("cacheTable() + isCached() + uncacheTable() lifecycle", async () => {
    const name = tempTable("cat_cache");
    await spark().range(10).createOrReplaceTempView(name);

    await spark().catalog.cacheTable(name);
    assert.equal(await spark().catalog.isCached(name), true);

    await spark().catalog.uncacheTable(name);
    assert.equal(await spark().catalog.isCached(name), false);
  });

  it("cacheTable() with storageLevel", async () => {
    const name = tempTable("cat_cache_sl");
    await spark().range(10).createOrReplaceTempView(name);

    await spark().catalog.cacheTable(name, {
      useDisk: true,
      useMemory: true,
      useOffHeap: false,
      deserialized: false,
      replication: 1,
    });
    assert.equal(await spark().catalog.isCached(name), true);

    await spark().catalog.uncacheTable(name);
  });

  it("clearCache() uncaches all tables", async () => {
    const name = tempTable("cat_clear");
    await spark().range(10).createOrReplaceTempView(name);

    await spark().catalog.cacheTable(name);
    assert.equal(await spark().catalog.isCached(name), true);

    await spark().catalog.clearCache();
    assert.equal(await spark().catalog.isCached(name), false);
  });

  it("refreshTable() does not throw on temp view", async () => {
    const name = tempTable("cat_refresh");
    await spark().range(1).createOrReplaceTempView(name);
    await spark().catalog.refreshTable(name);
  });

  it("setCurrentCatalog() + currentCatalog() roundtrip", async () => {
    const original = await spark().catalog.currentCatalog();
    await spark().catalog.setCurrentCatalog("spark_catalog");
    const current = await spark().catalog.currentCatalog();
    assert.equal(current, "spark_catalog");
    // Restore original in case it was different
    await spark().catalog.setCurrentCatalog(original);
  });

  it("createTable() creates a managed table with schema", async () => {
    const name = tempTable("cat_create");
    const schema = new StructType([
      new StructField("id", "long"),
      new StructField("name", "string"),
    ]);
    const df = spark().catalog.createTable(name, {
      source: "parquet",
      schema,
    });
    const rows = await df.collect();
    assert.ok(Array.isArray(rows));
    // Clean up
    await spark().sql(`DROP TABLE IF EXISTS ${name}`).collect();
  });
});
