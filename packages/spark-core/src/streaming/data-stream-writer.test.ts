import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { SparkSession } from "../spark-session.js";
import type { Transport } from "../spark-session.js";
import { InvalidInputError, SparkClientError } from "../errors.js";
import { Trigger } from "./trigger.js";

interface CapturingTransport extends Transport {
  commands: Record<string, unknown>[];
  /** Override the response payload(s) the transport returns for a command. */
  setResponses(responses: Record<string, unknown>[]): void;
}

function capturingTransport(): CapturingTransport {
  let pending: Record<string, unknown>[] = [
    {
      type: "writeStreamOperationStartResult",
      queryId: {
        id: "00000000-0000-0000-0000-00000000aaaa",
        runId: "00000000-0000-0000-0000-00000000bbbb",
      },
      name: "q1",
    },
  ];
  const commands: Record<string, unknown>[] = [];
  return {
    commands,
    setResponses(r) {
      pending = r;
    },
    async *executePlan(): AsyncIterable<Uint8Array> {
      // no-op
    },
    async executeCommandResponses(_session, command) {
      commands.push(command);
      return pending;
    },
  };
}

function newSession(transport: CapturingTransport): SparkSession {
  return SparkSession.builder().remote("sc://localhost:15002").transport(transport).getOrCreate();
}

describe("DataStreamWriter", () => {
  it("start() issues a writeStreamOperationStart command and returns a StreamingQuery", async () => {
    const t = capturingTransport();
    const spark = newSession(t);
    const df = spark.readStream.format("rate").load();
    const query = await df.writeStream
      .format("memory")
      .queryName("q1")
      .outputMode("append")
      .start();

    assert.equal(t.commands.length, 1);
    const cmd = t.commands[0];
    assert.equal(cmd["type"], "writeStreamOperationStart");
    assert.equal(cmd["format"], "memory");
    assert.equal(cmd["queryName"], "q1");
    assert.equal(cmd["outputMode"], "append");
    assert.equal(query.id, "00000000-0000-0000-0000-00000000aaaa");
    assert.equal(query.runId, "00000000-0000-0000-0000-00000000bbbb");
    assert.equal(query.name, "q1");
  });

  it("start(path) sets a path sink", async () => {
    const t = capturingTransport();
    const spark = newSession(t);
    const df = spark.readStream.format("rate").load();
    await df.writeStream.format("parquet").start("/out");
    const cmd = t.commands[0];
    assert.deepEqual(cmd["sink"], { kind: "path", value: "/out" });
  });

  it("toTable(name) sets a table sink", async () => {
    const t = capturingTransport();
    const spark = newSession(t);
    const df = spark.readStream.format("rate").load();
    await df.writeStream.format("parquet").toTable("events");
    const cmd = t.commands[0];
    assert.deepEqual(cmd["sink"], { kind: "table", value: "events" });
  });

  it("toTable() rejects an empty name", async () => {
    const t = capturingTransport();
    const spark = newSession(t);
    const df = spark.readStream.format("rate").load();
    await assert.rejects(() => df.writeStream.format("parquet").toTable(""), InvalidInputError);
  });

  it("queryName() rejects an empty name", () => {
    const t = capturingTransport();
    const spark = newSession(t);
    const df = spark.readStream.format("rate").load();
    assert.throws(() => df.writeStream.queryName(""), InvalidInputError);
  });

  it("start() rejects an empty path string (use no arg for path-less sinks)", async () => {
    const t = capturingTransport();
    const spark = newSession(t);
    const df = spark.readStream.format("rate").load();
    await assert.rejects(() => df.writeStream.format("memory").start(""), InvalidInputError);
  });

  it("trigger() forwards Trigger.processingTime", async () => {
    const t = capturingTransport();
    const spark = newSession(t);
    const df = spark.readStream.format("rate").load();
    await df.writeStream.format("memory").trigger(Trigger.processingTime("10 seconds")).start();
    assert.deepEqual(t.commands[0]["trigger"], { kind: "processingTime", interval: "10 seconds" });
  });

  it("trigger() forwards Trigger.availableNow", async () => {
    const t = capturingTransport();
    const spark = newSession(t);
    const df = spark.readStream.format("rate").load();
    await df.writeStream.format("memory").trigger(Trigger.availableNow()).start();
    assert.deepEqual(t.commands[0]["trigger"], { kind: "availableNow" });
  });

  it("partitionBy() forwards column names", async () => {
    const t = capturingTransport();
    const spark = newSession(t);
    const df = spark.readStream.format("rate").load();
    await df.writeStream.format("parquet").partitionBy("year", "month").start("/out");
    assert.deepEqual(t.commands[0]["partitioningColumnNames"], ["year", "month"]);
  });

  it("options merge across option() and options()", async () => {
    const t = capturingTransport();
    const spark = newSession(t);
    const df = spark.readStream.format("rate").load();
    await df.writeStream
      .format("memory")
      .option("checkpointLocation", "/tmp/c")
      .options({ truncate: "false" })
      .start();
    assert.deepEqual(t.commands[0]["options"], {
      checkpointLocation: "/tmp/c",
      truncate: "false",
    });
  });

  it("options() stringifies non-string values", async () => {
    const t = capturingTransport();
    const spark = newSession(t);
    const df = spark.readStream.format("rate").load();
    await df.writeStream
      .format("memory")
      .options({ truncate: false, numRows: 20, mode: "append" })
      .start();
    assert.deepEqual(t.commands[0]["options"], {
      truncate: "false",
      numRows: "20",
      mode: "append",
    });
  });

  it('name="" in response is normalized to undefined on the StreamingQuery', async () => {
    const t = capturingTransport();
    t.setResponses([
      {
        type: "writeStreamOperationStartResult",
        queryId: { id: "id-a", runId: "id-b" },
        name: "",
      },
    ]);
    const spark = newSession(t);
    const df = spark.readStream.format("rate").load();
    const query = await df.writeStream.format("memory").start();
    assert.equal(query.name, undefined);
  });

  it("rejects start() with neither format nor a sink destination", async () => {
    const t = capturingTransport();
    const spark = newSession(t);
    const df = spark.readStream.format("rate").load();
    await assert.rejects(() => df.writeStream.start(), InvalidInputError);
    assert.equal(t.commands.length, 0);
  });

  it("throws SparkClientError when the server returns no start result", async () => {
    const t = capturingTransport();
    t.setResponses([]);
    const spark = newSession(t);
    const df = spark.readStream.format("rate").load();
    await assert.rejects(() => df.writeStream.format("memory").start(), SparkClientError);
  });

  it("throws SparkClientError when the start result has no queryId", async () => {
    const t = capturingTransport();
    t.setResponses([{ type: "writeStreamOperationStartResult", name: "q1" }]);
    const spark = newSession(t);
    const df = spark.readStream.format("rate").load();
    await assert.rejects(() => df.writeStream.format("memory").start(), SparkClientError);
  });
});
