import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { SparkSession } from "./spark-session.js";
import type { Transport } from "./spark-session.js";
import type { Column } from "./column.js";
import { InvalidInputError } from "./errors.js";

function mockCommandTransport() {
  const commandCalls: Record<string, unknown>[] = [];
  const transport: Transport & { commandCalls: Record<string, unknown>[] } = {
    commandCalls,
    async *executePlan(): AsyncIterable<Uint8Array> {},
    async executeCommand(_sid: string, cmd: Record<string, unknown>): Promise<void> {
      commandCalls.push(cmd);
    },
  };
  return transport;
}

function makeSession(transport: Transport) {
  return SparkSession.builder().remote("sc://localhost:15002").transport(transport).getOrCreate();
}

describe("DataFrameWriterV2", () => {
  it("create() sends writeOperationV2 with create mode", async () => {
    const transport = mockCommandTransport();
    const spark = makeSession(transport);
    const df = spark.sql("SELECT 1");
    await df.writeTo("my_catalog.db.table").create();
    const cmd = transport.commandCalls[0];
    assert.equal(cmd.type, "writeOperationV2");
    assert.equal(cmd.tableName, "my_catalog.db.table");
    assert.equal(cmd.mode, "create");
  });

  it("replace() sends writeOperationV2 with replace mode", async () => {
    const transport = mockCommandTransport();
    const spark = makeSession(transport);
    const df = spark.sql("SELECT 1");
    await df.writeTo("my_table").replace();
    const cmd = transport.commandCalls[0];
    assert.equal(cmd.type, "writeOperationV2");
    assert.equal(cmd.mode, "replace");
  });

  it("createOrReplace() sends writeOperationV2 with createOrReplace mode", async () => {
    const transport = mockCommandTransport();
    const spark = makeSession(transport);
    const df = spark.sql("SELECT 1");
    await df.writeTo("my_table").createOrReplace();
    const cmd = transport.commandCalls[0];
    assert.equal(cmd.type, "writeOperationV2");
    assert.equal(cmd.mode, "createOrReplace");
  });

  it("append() sends writeOperationV2 with append mode", async () => {
    const transport = mockCommandTransport();
    const spark = makeSession(transport);
    const df = spark.sql("SELECT 1");
    await df.writeTo("my_table").append();
    const cmd = transport.commandCalls[0];
    assert.equal(cmd.type, "writeOperationV2");
    assert.equal(cmd.mode, "append");
  });

  it("overwritePartitions() sends writeOperationV2 with overwritePartitions mode", async () => {
    const transport = mockCommandTransport();
    const spark = makeSession(transport);
    const df = spark.sql("SELECT 1");
    await df.writeTo("my_table").overwritePartitions();
    const cmd = transport.commandCalls[0];
    assert.equal(cmd.type, "writeOperationV2");
    assert.equal(cmd.mode, "overwritePartitions");
  });

  it("overwrite() sends writeOperationV2 with overwrite mode and condition", async () => {
    const transport = mockCommandTransport();
    const spark = makeSession(transport);
    const { col, lit } = await import("./column.js");
    const df = spark.sql("SELECT 1");
    await df.writeTo("my_table").overwrite(col("date").eq(lit("2024-01-01")));
    const cmd = transport.commandCalls[0];
    assert.equal(cmd.type, "writeOperationV2");
    assert.equal(cmd.mode, "overwrite");
    assert.ok(cmd.overwriteCondition, "overwriteCondition should be set");
  });

  it("using() sets provider", async () => {
    const transport = mockCommandTransport();
    const spark = makeSession(transport);
    const df = spark.sql("SELECT 1");
    await df.writeTo("my_table").using("iceberg").create();
    const cmd = transport.commandCalls[0];
    assert.equal(cmd.provider, "iceberg");
  });

  it("option() and options() set write options", async () => {
    const transport = mockCommandTransport();
    const spark = makeSession(transport);
    const df = spark.sql("SELECT 1");
    await df
      .writeTo("my_table")
      .option("key1", "val1")
      .options({ key2: "val2", key3: "val3" })
      .create();
    const cmd = transport.commandCalls[0];
    const opts = cmd.options as Record<string, string>;
    assert.equal(opts.key1, "val1");
    assert.equal(opts.key2, "val2");
    assert.equal(opts.key3, "val3");
  });

  it("tableProperty() sets table properties", async () => {
    const transport = mockCommandTransport();
    const spark = makeSession(transport);
    const df = spark.sql("SELECT 1");
    await df.writeTo("my_table").tableProperty("write.format.default", "parquet").create();
    const cmd = transport.commandCalls[0];
    const props = cmd.tableProperties as Record<string, string>;
    assert.equal(props["write.format.default"], "parquet");
  });

  it("partitionedBy() sets partitioning column expressions", async () => {
    const transport = mockCommandTransport();
    const spark = makeSession(transport);
    const { col } = await import("./column.js");
    const df = spark.sql("SELECT 1");
    await df.writeTo("my_table").partitionedBy(col("year"), col("month")).create();
    const cmd = transport.commandCalls[0];
    const cols = cmd.partitioningColumns as unknown[];
    assert.equal(cols.length, 2);
  });

  it("clusterBy() sets clustering column names", async () => {
    const transport = mockCommandTransport();
    const spark = makeSession(transport);
    const df = spark.sql("SELECT 1");
    await df.writeTo("my_table").clusterBy("col_a", "col_b").create();
    const cmd = transport.commandCalls[0];
    assert.deepStrictEqual(cmd.clusteringColumns, ["col_a", "col_b"]);
  });

  it("provider defaults to undefined when using() is not called", async () => {
    const transport = mockCommandTransport();
    const spark = makeSession(transport);
    const df = spark.sql("SELECT 1");
    await df.writeTo("my_table").append();
    const cmd = transport.commandCalls[0];
    assert.equal(cmd.provider, undefined);
  });

  it("full builder chain works", async () => {
    const transport = mockCommandTransport();
    const spark = makeSession(transport);
    const { col } = await import("./column.js");
    const df = spark.sql("SELECT 1");
    await df
      .writeTo("catalog.db.events")
      .using("iceberg")
      .option("fanout-enabled", "true")
      .tableProperty("format-version", "2")
      .partitionedBy(col("date"))
      .createOrReplace();
    const cmd = transport.commandCalls[0];
    assert.equal(cmd.type, "writeOperationV2");
    assert.equal(cmd.tableName, "catalog.db.events");
    assert.equal(cmd.provider, "iceberg");
    assert.equal(cmd.mode, "createOrReplace");
    assert.equal((cmd.options as Record<string, string>)["fanout-enabled"], "true");
    assert.equal((cmd.tableProperties as Record<string, string>)["format-version"], "2");
    assert.equal((cmd.partitioningColumns as unknown[]).length, 1);
  });
});

describe("DataFrameWriterV2 condition validation", () => {
  it("overwrite() rejects a non-Column condition", async () => {
    const transport = mockCommandTransport();
    const spark = makeSession(transport);
    await assert.rejects(
      spark
        .sql("SELECT 1")
        .writeTo("t")
        .overwrite(42 as unknown as Column),
      (err: unknown) =>
        err instanceof InvalidInputError &&
        /overwrite\(\) condition must be a Column/.test(err.message),
    );
    assert.equal(transport.commandCalls.length, 0);
  });
});
