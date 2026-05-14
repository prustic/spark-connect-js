import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { SparkSession } from "../spark-session.js";
import type { Transport } from "../spark-session.js";
import { InvalidInputError } from "../errors.js";
import type { ReadPlan, ReadTablePlan } from "../plan/logical-plan.js";

function mockTransport(): Transport {
  return {
    async *executePlan(): AsyncIterable<Uint8Array> {
      // no-op
    },
  };
}

function newSession(): SparkSession {
  return SparkSession.builder()
    .remote("sc://localhost:15002")
    .transport(mockTransport())
    .getOrCreate();
}

describe("DataStreamReader", () => {
  it("load() builds a streaming Read plan with isStreaming=true", () => {
    const spark = newSession();
    const df = spark.readStream.format("rate").option("rowsPerSecond", "5").load();
    const plan = df._plan as ReadPlan;
    assert.equal(plan.type, "read");
    assert.equal(plan.format, "rate");
    assert.equal(plan.isStreaming, true);
    assert.equal(plan.options["rowsPerSecond"], "5");
    assert.equal(plan.path, "");
    assert.equal(plan.schema, undefined);
  });

  it("load(path) preserves the path on the streaming Read plan", () => {
    const spark = newSession();
    const df = spark.readStream.format("json").load("/data/events");
    const plan = df._plan as ReadPlan;
    assert.equal(plan.path, "/data/events");
    assert.equal(plan.isStreaming, true);
  });

  it("schema() accepts a DDL string", () => {
    const spark = newSession();
    const df = spark.readStream.schema("id INT, ts TIMESTAMP").format("rate").load();
    const plan = df._plan as ReadPlan;
    assert.equal(plan.schema, "id INT, ts TIMESTAMP");
  });

  it("schema() accepts a StructType-like object via toDDL()", () => {
    const spark = newSession();
    const df = spark.readStream
      .schema({ toDDL: () => "id INT" })
      .format("rate")
      .load();
    const plan = df._plan as ReadPlan;
    assert.equal(plan.schema, "id INT");
  });

  it("schema() rejects empty strings", () => {
    const spark = newSession();
    assert.throws(() => spark.readStream.schema("   "), InvalidInputError);
  });

  it("options() merges over option() calls", () => {
    const spark = newSession();
    const df = spark.readStream
      .format("rate")
      .option("rowsPerSecond", "5")
      .options({ numPartitions: "2", rowsPerSecond: "10" })
      .load();
    const plan = df._plan as ReadPlan;
    assert.deepEqual(plan.options, { rowsPerSecond: "10", numPartitions: "2" });
  });

  it("option() stringifies non-string values", () => {
    const spark = newSession();
    const df = spark.readStream
      .format("rate")
      .option("rowsPerSecond", 5)
      .option("includeTimestamp", true)
      .load();
    const plan = df._plan as ReadPlan;
    assert.equal(plan.options["rowsPerSecond"], "5");
    assert.equal(plan.options["includeTimestamp"], "true");
  });

  it("table(name) builds a streaming readTable plan", () => {
    const spark = newSession();
    const df = spark.readStream.table("events");
    const plan = df._plan as ReadTablePlan;
    assert.equal(plan.type, "readTable");
    assert.equal(plan.tableName, "events");
    assert.equal(plan.isStreaming, true);
  });

  it("table() rejects an empty tableName", () => {
    const spark = newSession();
    assert.throws(() => spark.readStream.table(""), InvalidInputError);
  });

  it("load() rejects an empty path string (use no arg for path-less sources)", () => {
    const spark = newSession();
    assert.throws(() => spark.readStream.format("rate").load(""), InvalidInputError);
  });

  it("format shortcuts dispatch through .format().load(path)", () => {
    const spark = newSession();
    for (const [method, expected] of [
      ["json", "json"],
      ["csv", "csv"],
      ["parquet", "parquet"],
      ["orc", "orc"],
      ["text", "text"],
    ] as const) {
      const df = (
        spark.readStream as unknown as Record<string, (p: string) => { _plan: ReadPlan }>
      )[method]("/path");
      assert.equal(df._plan.format, expected);
      assert.equal(df._plan.isStreaming, true);
      assert.equal(df._plan.path, "/path");
    }
  });

  it("options object is copied per .load() call (no aliasing across DataFrames)", () => {
    const spark = newSession();
    const reader = spark.readStream.format("rate").option("a", "1");
    const df1 = reader.load();
    reader.option("a", "2");
    const plan1 = df1._plan as ReadPlan;
    assert.equal(plan1.options["a"], "1");
  });
});
