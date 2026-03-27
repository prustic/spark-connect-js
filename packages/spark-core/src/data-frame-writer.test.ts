import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { SparkSession } from "./spark-session.js";
import type { Transport } from "./spark-session.js";

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

describe("DataFrameWriter format shortcuts", () => {
  it("json() writes with json format", async () => {
    const transport = mockCommandTransport();
    const spark = SparkSession.builder()
      .remote("sc://localhost:15002")
      .transport(transport)
      .getOrCreate();
    const df = spark.sql("SELECT 1");
    await df.write.json("/output/data.json");
    const cmd = transport.commandCalls[0];
    assert.equal(cmd.source, "json");
    assert.deepStrictEqual(cmd.saveType, { case: "path", value: "/output/data.json" });
  });

  it("csv() writes with csv format", async () => {
    const transport = mockCommandTransport();
    const spark = SparkSession.builder()
      .remote("sc://localhost:15002")
      .transport(transport)
      .getOrCreate();
    const df = spark.sql("SELECT 1");
    await df.write.csv("/output/data.csv");
    const cmd = transport.commandCalls[0];
    assert.equal(cmd.source, "csv");
  });

  it("parquet() writes with parquet format", async () => {
    const transport = mockCommandTransport();
    const spark = SparkSession.builder()
      .remote("sc://localhost:15002")
      .transport(transport)
      .getOrCreate();
    const df = spark.sql("SELECT 1");
    await df.write.parquet("/output/data.parquet");
    const cmd = transport.commandCalls[0];
    assert.equal(cmd.source, "parquet");
  });

  it("orc() writes with orc format", async () => {
    const transport = mockCommandTransport();
    const spark = SparkSession.builder()
      .remote("sc://localhost:15002")
      .transport(transport)
      .getOrCreate();
    const df = spark.sql("SELECT 1");
    await df.write.orc("/output/data.orc");
    const cmd = transport.commandCalls[0];
    assert.equal(cmd.source, "orc");
  });

  it("text() writes with text format", async () => {
    const transport = mockCommandTransport();
    const spark = SparkSession.builder()
      .remote("sc://localhost:15002")
      .transport(transport)
      .getOrCreate();
    const df = spark.sql("SELECT 1");
    await df.write.text("/output/data.txt");
    const cmd = transport.commandCalls[0];
    assert.equal(cmd.source, "text");
  });
});

describe("DataFrameWriter.bucketBy()", () => {
  it("bucketBy() sets bucket info in command", async () => {
    const transport = mockCommandTransport();
    const spark = SparkSession.builder()
      .remote("sc://localhost:15002")
      .transport(transport)
      .getOrCreate();
    const df = spark.sql("SELECT 1");
    await df.write.bucketBy(16, "id", "name").saveAsTable("bucketed_table");
    const cmd = transport.commandCalls[0];
    assert.deepStrictEqual(cmd.bucketBy, { numBuckets: 16, columnNames: ["id", "name"] });
  });

  it("save without bucketBy omits bucketBy field", async () => {
    const transport = mockCommandTransport();
    const spark = SparkSession.builder()
      .remote("sc://localhost:15002")
      .transport(transport)
      .getOrCreate();
    const df = spark.sql("SELECT 1");
    await df.write.save("/output");
    const cmd = transport.commandCalls[0];
    assert.equal(cmd.bucketBy, undefined);
  });

  it("save() strips bucketBy from path writes", async () => {
    const transport = mockCommandTransport();
    const spark = SparkSession.builder()
      .remote("sc://localhost:15002")
      .transport(transport)
      .getOrCreate();
    const df = spark.sql("SELECT 1");
    await df.write.bucketBy(4, "id").save("/output");
    const cmd = transport.commandCalls[0];
    assert.equal(cmd.bucketBy, undefined);
  });

  it("bucketBy() throws on zero", () => {
    const transport = mockCommandTransport();
    const spark = SparkSession.builder()
      .remote("sc://localhost:15002")
      .transport(transport)
      .getOrCreate();
    const df = spark.sql("SELECT 1");
    assert.throws(() => df.write.bucketBy(0, "id"), { message: /positive integer/ });
  });

  it("bucketBy() throws on negative", () => {
    const transport = mockCommandTransport();
    const spark = SparkSession.builder()
      .remote("sc://localhost:15002")
      .transport(transport)
      .getOrCreate();
    const df = spark.sql("SELECT 1");
    assert.throws(() => df.write.bucketBy(-1, "id"), { message: /positive integer/ });
  });

  it("bucketBy() throws on non-integer", () => {
    const transport = mockCommandTransport();
    const spark = SparkSession.builder()
      .remote("sc://localhost:15002")
      .transport(transport)
      .getOrCreate();
    const df = spark.sql("SELECT 1");
    assert.throws(() => df.write.bucketBy(3.5, "id"), { message: /positive integer/ });
  });
});

describe("DataFrameWriter.insertInto()", () => {
  it("insertInto() sends writeOperation with insertInto saveMethod", async () => {
    const transport = mockCommandTransport();
    const spark = SparkSession.builder()
      .remote("sc://localhost:15002")
      .transport(transport)
      .getOrCreate();
    const df = spark.sql("SELECT 1");
    await df.write.insertInto("existing_table");
    const cmd = transport.commandCalls[0];
    assert.equal(cmd.type, "writeOperation");
    const saveType = cmd.saveType as {
      case: string;
      value: { tableName: string; saveMethod: string };
    };
    assert.equal(saveType.case, "table");
    assert.equal(saveType.value.tableName, "existing_table");
    assert.equal(saveType.value.saveMethod, "insertInto");
  });
});

describe("DataFrameWriter defensive copies", () => {
  it("partitionBy() does not leak caller array mutations", async () => {
    const transport = mockCommandTransport();
    const spark = SparkSession.builder()
      .remote("sc://localhost:15002")
      .transport(transport)
      .getOrCreate();
    const df = spark.sql("SELECT 1");
    const cols = ["a", "b"];
    const writer = df.write.partitionBy(...cols);
    cols.push("c"); // mutate original — should not affect writer
    await writer.save("/out");
    const cmd = transport.commandCalls[0];
    assert.deepStrictEqual(cmd.partitioningColumns, ["a", "b"]);
  });

  it("sortBy() does not leak caller array mutations", async () => {
    const transport = mockCommandTransport();
    const spark = SparkSession.builder()
      .remote("sc://localhost:15002")
      .transport(transport)
      .getOrCreate();
    const df = spark.sql("SELECT 1");
    const cols = ["x"];
    const writer = df.write.sortBy(...cols);
    cols.push("y"); // mutate original — should not affect writer
    await writer.save("/out");
    const cmd = transport.commandCalls[0];
    assert.deepStrictEqual(cmd.sortColumnNames, ["x"]);
  });
});
