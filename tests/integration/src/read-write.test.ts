import { describe, it, after } from "node:test";
import assert from "node:assert/strict";
import { col, lit } from "@spark-connect-js/node";
import { spark, stopSession, tempPath, tempTable } from "./setup.js";

describe("read / write", () => {
  after(stopSession);

  it("read.schema() applies DDL schema to csv", async () => {
    const path = tempPath("schema_csv");
    await spark()
      .sql("SELECT 1 AS id, 'alice' AS name UNION ALL SELECT 2, 'bob'")
      .write.mode("overwrite")
      .option("header", "false")
      .csv(path);

    const rows = await spark()
      .read.schema("id BIGINT, name STRING")
      .option("header", "false")
      .csv(path)
      .collect();

    assert.ok(rows.length >= 2);
    assert.equal(typeof rows[0]["id"], "number");
    assert.equal(typeof rows[0]["name"], "string");
  });

  it("read.text() returns single 'value' column", async () => {
    const path = tempPath("text");
    await spark()
      .sql("SELECT 'hello' AS value UNION ALL SELECT 'world'")
      .write.mode("overwrite")
      .text(path);

    const rows = await spark().read.text(path).collect();
    assert.ok(rows.length >= 2);
    assert.ok("value" in rows[0]);
  });

  it("write.json() + read.json() roundtrip", async () => {
    const path = tempPath("json");
    await spark().range(3).write.mode("overwrite").json(path);
    const rows = await spark().read.json(path).collect();
    assert.equal(rows.length, 3);
  });

  it("write.csv() + read.csv() roundtrip", async () => {
    const path = tempPath("csv");
    await spark()
      .range(3)
      .withColumn("label", lit("x"))
      .write.mode("overwrite")
      .option("header", "true")
      .csv(path);
    const rows = await spark().read.option("header", "true").csv(path).collect();
    assert.equal(rows.length, 3);
    assert.ok("label" in rows[0]);
  });

  it("write.parquet() + read.parquet() roundtrip", async () => {
    const path = tempPath("parquet");
    await spark().range(5).write.mode("overwrite").parquet(path);
    const rows = await spark().read.parquet(path).collect();
    assert.equal(rows.length, 5);
  });

  it("write.orc() + read.orc() roundtrip", async () => {
    const path = tempPath("orc");
    await spark().range(4).write.mode("overwrite").orc(path);
    const rows = await spark().read.orc(path).collect();
    assert.equal(rows.length, 4);
  });

  it("write.text() + read.text() roundtrip", async () => {
    const path = tempPath("text_rt");
    await spark()
      .sql("SELECT CAST(id AS STRING) AS value FROM range(3)")
      .write.mode("overwrite")
      .text(path);
    const rows = await spark().read.text(path).collect();
    assert.equal(rows.length, 3);
  });

  it("bucketBy() + saveAsTable()", async () => {
    const table = tempTable("bucket");
    await spark()
      .sql(`DROP TABLE IF EXISTS ${table}`)
      .collect()
      .catch(() => {});
    await spark()
      .range(10)
      .selectExpr("id", "id % 3 AS category")
      .write.mode("overwrite")
      .bucketBy(4, "category")
      .saveAsTable(table);

    const rows = await spark().read.table(table).collect();
    assert.equal(rows.length, 10);
  });

  it("insertInto() appends to existing table", async () => {
    const table = tempTable("insert");
    await spark().range(3).toDF("id").write.mode("overwrite").saveAsTable(table);
    await spark().range(2).toDF("id").write.insertInto(table);

    const rows = await spark().read.table(table).collect();
    assert.equal(rows.length, 5);
  });
});

describe("writeTo (DataFrameWriterV2)", () => {
  after(stopSession);

  it("createOrReplace() creates a table", async () => {
    const table = tempTable("v2_create");
    await spark().range(5).writeTo(table).using("delta").createOrReplace();
    const rows = await spark().read.table(table).collect();
    assert.equal(rows.length, 5);
  });

  it("append() adds rows to existing table", async () => {
    const table = tempTable("v2_append");
    await spark().range(3).writeTo(table).using("delta").createOrReplace();
    await spark().range(2).writeTo(table).append();
    const rows = await spark().read.table(table).collect();
    assert.equal(rows.length, 5);
  });

  it("replace() replaces table contents", async () => {
    // replace() requires REPLACE TABLE AS SELECT, which Iceberg supports but Delta does not
    const table = `iceberg.db.test_${Date.now()}_v2_replace`;
    await spark().range(10).writeTo(table).createOrReplace();
    await spark().range(3).writeTo(table).replace();
    const rows = await spark().read.table(table).collect();
    assert.equal(rows.length, 3);
  });

  it("overwrite() with condition replaces matching rows", async () => {
    const table = tempTable("v2_overwrite");
    await spark()
      .range(10)
      .selectExpr("id", "id % 2 AS category")
      .writeTo(table)
      .using("delta")
      .createOrReplace();

    await spark()
      .sql("SELECT CAST(100 AS BIGINT) AS id, CAST(0 AS BIGINT) AS category UNION ALL SELECT CAST(101 AS BIGINT) AS id, CAST(0 AS BIGINT) AS category")
      .writeTo(table)
      .overwrite(col("category").eq(lit(0)));

    const rows = await spark().read.table(table).collect();
    // 5 odd rows kept + 2 new rows = 7
    assert.equal(rows.length, 7);
  });

  it("overwritePartitions() replaces partitions dynamically", async () => {
    const table = tempTable("v2_overwrite_parts");
    await spark()
      .range(6)
      .selectExpr("id", "id % 2 AS part")
      .writeTo(table)
      .using("delta")
      .partitionedBy(col("part"))
      .createOrReplace();

    await spark().sql("SELECT CAST(99 AS BIGINT) AS id, CAST(0 AS BIGINT) AS part").writeTo(table).overwritePartitions();

    const rows = await spark().read.table(table).collect();
    // 3 rows with part=1 kept + 1 new row with part=0 = 4
    assert.equal(rows.length, 4);
  });
});
