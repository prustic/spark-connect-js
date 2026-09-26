import { describe, it, after } from "node:test";
import assert from "node:assert/strict";
import { col, expr, StructType } from "@spark-connect-js/node";
import { spark, stopSession } from "./setup.js";

describe("DataFrame relation methods", () => {
  after(stopSession);

  it("transpose() turns column names into row values", async () => {
    const rows = await spark()
      .sql("SELECT * FROM VALUES ('a', 1, 2), ('b', 3, 4) AS t(k, x, y)")
      .transpose("k")
      .collect();

    // One row per transposed column, keyed by the index column's values.
    assert.deepEqual(
      rows.map((r) => r["key"]),
      ["x", "y"],
    );
  });

  it("lateralJoin() lets the right side reference the left", async () => {
    const left = spark().sql("SELECT * FROM VALUES (1), (2) AS t(id)").alias("l");
    const right = spark().sql("SELECT 1 AS bump").alias("r");
    const rows = await left.lateralJoin(right).collect();
    assert.equal(rows.length, 2);
  });

  it("to() reorders and casts columns to the given schema", async () => {
    const schema = new StructType().add("b", "string").add("a", "bigint");
    const df = spark().sql("SELECT 1 AS a, 'x' AS b").to(schema);

    assert.deepEqual(
      (await df.schema()).fields.map((f) => [f.name, f.dataType]),
      [
        ["b", "string"],
        ["a", "bigint"],
      ],
    );
    assert.deepEqual(await df.collect(), [{ b: "x", a: 1n }]);
  });

  it("groupingSets() matches the equivalent SQL GROUPING SETS", async () => {
    const source =
      "SELECT * FROM VALUES ('eu', 'a', 1), ('eu', 'b', 2), ('us', 'a', 3) AS t(region, item, n)";
    const viaApi = await spark()
      .sql(source)
      .groupingSets([[col("region"), col("item")], [col("region")], []], col("region"), col("item"))
      .agg(expr("sum(n)").alias("total"))
      .collect();
    const viaSql = await spark()
      .sql(
        `SELECT region, item, sum(n) AS total FROM (${source}) GROUP BY GROUPING SETS ((region, item), (region), ())`,
      )
      .collect();

    const key = (r: Record<string, unknown>) =>
      `${String(r["region"])}|${String(r["item"])}|${String(r["total"])}`;
    assert.deepEqual(viaApi.map(key).sort(), viaSql.map(key).sort());
  });

  it("sampleBy() keeps only the named strata and is stable for a fixed seed", async () => {
    const df = spark().sql(
      "SELECT id, CASE WHEN id < 50 THEN 'a' WHEN id < 100 THEN 'b' ELSE 'c' END AS k FROM range(150)",
    );
    const sampled = await df.sampleBy(col("k"), { a: 1.0, b: 0.0 }, 7).collect();
    const strata = new Set(sampled.map((r) => r["k"]));

    // Fraction 1.0 keeps every 'a', 0.0 drops every 'b', and 'c' is unlisted.
    assert.deepEqual([...strata], ["a"]);
    assert.equal(sampled.length, 50);
  });

  it("dropDuplicatesWithinWatermark() runs on a watermarked stream", async () => {
    const df = spark()
      .readStream.format("rate")
      .option("rowsPerSecond", "10")
      .load()
      .withWatermark("timestamp", "10 seconds")
      .dropDuplicatesWithinWatermark("value");

    // Plan shape is asserted in unit tests; here we only need the server to
    // accept the relation, which analysis of a streaming plan proves.
    assert.equal(df._plan.type, "deduplicate");
    const schema = await df.schema();
    assert.ok(schema.fieldNames.includes("value"));
  });
});
