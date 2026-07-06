import { describe, it, after } from "node:test";
import assert from "node:assert/strict";
import { Observation, col, count, sum, max } from "@spark-connect-js/node";
import { spark, stopSession } from "./setup.js";

describe("DataFrame.observe()", () => {
  after(stopSession);

  it("delivers observed metrics with the action, no second pass", async () => {
    const obs = new Observation("stats");
    const rows = await spark()
      .sql(
        `
        SELECT * FROM VALUES
          ('a', 10),
          ('b', 20),
          ('c', 30)
        AS t(k, v)
      `,
      )
      .observe(
        obs,
        count(col("v")).alias("rows"),
        sum(col("v")).alias("total"),
        max(col("k")).alias("last_key"),
      )
      .collect();

    assert.equal(rows.length, 3);
    assert.deepStrictEqual(obs.get, { rows: 3n, total: 60n, last_key: "c" });
  });

  it("string-named observation still computes and returns the rows", async () => {
    const rows = await spark()
      .sql("SELECT * FROM VALUES (1), (2) AS t(v)")
      .observe("unnamed_route", count(col("v")).alias("n"))
      .collect();
    assert.equal(rows.length, 2);
  });

  it("observation on a filtered frame reflects the filter", async () => {
    const obs = new Observation("filtered");
    await spark()
      .sql("SELECT * FROM VALUES (1), (2), (3), (4) AS t(v)")
      .filter(col("v").gt(2))
      .observe(obs, count(col("v")).alias("n"))
      .collect();
    assert.deepStrictEqual(obs.get, { n: 2n });
  });

  it("delivers metrics on a write action, not just reads", async () => {
    const obs = new Observation("write_stats");
    const session = spark();
    try {
      await session
        .sql("SELECT * FROM VALUES (1), (2), (3) AS t(v)")
        .observe(obs, count(col("v")).alias("n"))
        .write.mode("overwrite")
        .saveAsTable("observe_write_probe");
      assert.deepStrictEqual(obs.get, { n: 3n });
    } finally {
      await session.sql("DROP TABLE IF EXISTS observe_write_probe").collect();
    }
  });
});
