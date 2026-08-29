import { describe, it, after } from "node:test";
import assert from "node:assert/strict";
import { col, lit, when } from "@spark-connect-js/node";
import { spark, stopSession } from "./setup.js";

describe("Column expression methods", () => {
  after(stopSession);

  it("evaluates a when/otherwise chain built from Column methods", async () => {
    const label = when(col("id").lt(1), lit("low"))
      .when(col("id").lt(2), lit("mid"))
      .otherwise(lit("high"))
      .alias("label");

    const rows = await spark().range(3).select(col("id"), label).collect();
    assert.deepEqual(
      rows.map((r) => r["label"]),
      ["low", "mid", "high"],
    );
  });

  it("yields NULL for unmatched rows when the chain has no otherwise", async () => {
    const label = when(col("id").lt(1), lit("low")).alias("label");
    const rows = await spark().range(2).select(label).collect();
    assert.deepEqual(
      rows.map((r) => r["label"]),
      ["low", null],
    );
  });

  it("try_cast yields NULL where cast would fail", async () => {
    const rows = await spark()
      .sql("SELECT 'abc' AS v")
      .select(col("v").try_cast("int").alias("n"))
      .collect();
    assert.equal(rows[0]["n"], null);
  });

  it("resolves the operator function names on the server", async () => {
    const rows = await spark()
      .sql("SELECT 7 AS a, true AS flag")
      .select(
        col("a").mod(3).alias("m"),
        col("a").pow(2).alias("p"),
        col("a").negate().alias("n"),
        col("flag").not().alias("f"),
      )
      .collect();

    assert.equal(Number(rows[0]["m"]), 1);
    assert.equal(Number(rows[0]["p"]), 49);
    assert.equal(Number(rows[0]["n"]), -7);
    assert.equal(rows[0]["f"], false);
  });

  it("name() and astype() behave as their aliases", async () => {
    const rows = await spark().range(1).select(col("id").astype("string").name("text")).collect();
    assert.equal(rows[0]["text"], "0");
  });
});
