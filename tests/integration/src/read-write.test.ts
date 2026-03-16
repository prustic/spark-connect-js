import { describe, it, after } from "node:test";
import assert from "node:assert/strict";
import { lit } from "@spark-connect-js/node";
import { spark, stopSession } from "./setup.js";

describe("read / write", () => {
  after(stopSession);

  it("read.schema() applies DDL schema to csv", async () => {
    await spark()
      .sql("SELECT 1 AS id, 'alice' AS name UNION ALL SELECT 2, 'bob'")
      .write.mode("overwrite")
      .option("header", "false")
      .csv("/tmp/spark-io-test-schema.csv");

    const rows = await spark()
      .read.schema("id BIGINT, name STRING")
      .option("header", "false")
      .csv("/tmp/spark-io-test-schema.csv")
      .collect();

    assert.ok(rows.length >= 2);
    assert.equal(typeof rows[0]["id"], "number");
    assert.equal(typeof rows[0]["name"], "string");
  });

  it("read.text() returns single 'value' column", async () => {
    await spark()
      .sql("SELECT 'hello' AS value UNION ALL SELECT 'world'")
      .write.mode("overwrite")
      .text("/tmp/spark-io-test-text");

    const rows = await spark().read.text("/tmp/spark-io-test-text").collect();
    assert.ok(rows.length >= 2);
    assert.ok("value" in rows[0]);
  });

  it("write.json() + read.json() roundtrip", async () => {
    const path = "/tmp/spark-io-test-json";
    await spark().range(3).write.mode("overwrite").json(path);
    const rows = await spark().read.json(path).collect();
    assert.equal(rows.length, 3);
  });

  it("write.csv() + read.csv() roundtrip", async () => {
    const path = "/tmp/spark-io-test-csv";
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
    const path = "/tmp/spark-io-test-parquet";
    await spark().range(5).write.mode("overwrite").parquet(path);
    const rows = await spark().read.parquet(path).collect();
    assert.equal(rows.length, 5);
  });

  it("write.orc() + read.orc() roundtrip", async () => {
    const path = "/tmp/spark-io-test-orc";
    await spark().range(4).write.mode("overwrite").orc(path);
    const rows = await spark().read.orc(path).collect();
    assert.equal(rows.length, 4);
  });

  it("write.text() + read.text() roundtrip", async () => {
    const path = "/tmp/spark-io-test-text-rt";
    await spark()
      .sql("SELECT CAST(id AS STRING) AS value FROM range(3)")
      .write.mode("overwrite")
      .text(path);
    const rows = await spark().read.text(path).collect();
    assert.equal(rows.length, 3);
  });

  it("bucketBy() + saveAsTable()", async () => {
    const table = "spark_io_bucket_test";
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
    const table = "spark_io_insert_test";
    // Create table with 3 rows
    await spark().range(3).toDF("id").write.mode("overwrite").saveAsTable(table);

    // Insert 2 more rows
    await spark().range(2).toDF("id").write.insertInto(table);

    const rows = await spark().read.table(table).collect();
    assert.equal(rows.length, 5);
  });
});
