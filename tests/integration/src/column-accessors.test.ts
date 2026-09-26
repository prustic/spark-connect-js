import { describe, it, after } from "node:test";
import assert from "node:assert/strict";
import { col, lit } from "@spark-connect-js/node";
import { spark, stopSession } from "./setup.js";

describe("Column struct, array, and map accessors", () => {
  after(stopSession);

  const source = () =>
    spark()
      .sql("SELECT named_struct('x', 1, 'y', 2) AS s, array(10, 20) AS arr, map('k', 5) AS m")
      .alias("t");

  const one = async (df: ReturnType<typeof source>) => (await df.collect())[0]["r"];

  it("getField() reads a struct field", async () => {
    assert.equal(await one(source().select(col("s").getField("x").alias("r"))), 1);
  });

  it("getItem() reads an array element and a map value", async () => {
    assert.equal(await one(source().select(col("arr").getItem(1).alias("r"))), 20);
    assert.equal(await one(source().select(col("m").getItem("k").alias("r"))), 5);
  });

  it("getItem() past the end of an array throws, as PySpark does under ANSI", async () => {
    await assert.rejects(
      source().select(col("arr").getItem(5).alias("r")).collect(),
      /INVALID_ARRAY_INDEX/,
    );
  });

  it("withField() adds a new field and replaces an existing one", async () => {
    assert.deepEqual(await one(source().select(col("s").withField("z", lit(3)).alias("r"))), {
      x: 1,
      y: 2,
      z: 3,
    });
    assert.deepEqual(await one(source().select(col("s").withField("x", lit(9)).alias("r"))), {
      x: 9,
      y: 2,
    });
  });

  it("dropFields() removes one field or several", async () => {
    assert.deepEqual(await one(source().select(col("s").dropFields("y").alias("r"))), { x: 1 });

    const wide = spark().sql("SELECT named_struct('a', 1, 'b', 2, 'c', 3) AS s");
    assert.deepEqual(await one(wide.select(col("s").dropFields("a", "c").alias("r"))), { b: 2 });
  });

  it("df.col('*') expands this frame's columns and df.col('t.s') still resolves", async () => {
    const df = source();
    const all = await df.select(df.col("*")).collect();
    assert.deepEqual(Object.keys(all[0]), ["s", "arr", "m"]);

    const nested = await df.select(df.col("t.s")).collect();
    assert.deepEqual(nested[0]["s"], { x: 1, y: 2 });
  });
});
