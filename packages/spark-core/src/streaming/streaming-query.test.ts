import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { SparkSession } from "../spark-session.js";
import type { Transport } from "../spark-session.js";
import { InvalidInputError, SparkClientError } from "../errors.js";
import { StreamingQuery } from "./streaming-query.js";

interface CapturingTransport extends Transport {
  commands: Record<string, unknown>[];
  setResponses(responses: Record<string, unknown>[]): void;
}

function capturingTransport(): CapturingTransport {
  let pending: Record<string, unknown>[] = [];
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

function makeQuery(transport: CapturingTransport): StreamingQuery {
  const spark = SparkSession.builder()
    .remote("sc://localhost:15002")
    .transport(transport)
    .getOrCreate();
  return new StreamingQuery(spark, "id-a", "run-b", "q1");
}

describe("StreamingQuery", () => {
  it("status() decodes the StatusResult payload", async () => {
    const t = capturingTransport();
    t.setResponses([
      {
        type: "streamingQueryCommandResult",
        resultType: "status",
        status: {
          statusMessage: "running",
          isDataAvailable: true,
          isTriggerActive: false,
          isActive: true,
        },
      },
    ]);
    const query = makeQuery(t);
    const status = await query.status();
    assert.deepEqual(status, {
      message: "running",
      isDataAvailable: true,
      isTriggerActive: false,
      isActive: true,
    });
    const cmd = t.commands[0];
    assert.equal(cmd["type"], "streamingQueryCommand");
    assert.equal(cmd["op"], "status");
    assert.deepEqual(cmd["queryId"], { id: "id-a", runId: "run-b" });
  });

  it("isActive() returns status.isActive", async () => {
    const t = capturingTransport();
    t.setResponses([
      {
        type: "streamingQueryCommandResult",
        resultType: "status",
        status: {
          statusMessage: "stopped",
          isDataAvailable: false,
          isTriggerActive: false,
          isActive: false,
        },
      },
    ]);
    const query = makeQuery(t);
    assert.equal(await query.isActive(), false);
  });

  it("lastProgress() issues op=lastProgress and parses the JSON", async () => {
    const t = capturingTransport();
    t.setResponses([
      {
        type: "streamingQueryCommandResult",
        resultType: "recentProgress",
        recentProgressJson: ['{"batchId":0,"numInputRows":10}'],
      },
    ]);
    const query = makeQuery(t);
    const progress = await query.lastProgress();
    assert.deepEqual(progress, { batchId: 0, numInputRows: 10 });
    assert.equal(t.commands[0]["op"], "lastProgress");
  });

  it("lastProgress() throws SparkClientError on malformed progress JSON", async () => {
    const t = capturingTransport();
    t.setResponses([
      {
        type: "streamingQueryCommandResult",
        resultType: "recentProgress",
        recentProgressJson: ["{not valid json"],
      },
    ]);
    const query = makeQuery(t);
    await assert.rejects(() => query.lastProgress(), SparkClientError);
  });

  it("recentProgress() throws SparkClientError on malformed progress JSON", async () => {
    const t = capturingTransport();
    t.setResponses([
      {
        type: "streamingQueryCommandResult",
        resultType: "recentProgress",
        recentProgressJson: ['{"batchId":0}', "garbage"],
      },
    ]);
    const query = makeQuery(t);
    await assert.rejects(() => query.recentProgress(), SparkClientError);
  });

  it("lastProgress() returns null when the server reports no progress", async () => {
    const t = capturingTransport();
    t.setResponses([
      {
        type: "streamingQueryCommandResult",
        resultType: "recentProgress",
        recentProgressJson: [],
      },
    ]);
    const query = makeQuery(t);
    assert.equal(await query.lastProgress(), null);
  });

  it("recentProgress() parses every entry and preserves order", async () => {
    const t = capturingTransport();
    t.setResponses([
      {
        type: "streamingQueryCommandResult",
        resultType: "recentProgress",
        recentProgressJson: ['{"batchId":0}', '{"batchId":1}'],
      },
    ]);
    const query = makeQuery(t);
    assert.deepEqual(await query.recentProgress(), [{ batchId: 0 }, { batchId: 1 }]);
    assert.equal(t.commands[0]["op"], "recentProgress");
  });

  it("exception() returns null when the server reports no failure", async () => {
    const t = capturingTransport();
    t.setResponses([
      { type: "streamingQueryCommandResult", resultType: "exception", exception: {} },
    ]);
    const query = makeQuery(t);
    assert.equal(await query.exception(), null);
  });

  it("exception() returns null when only errorClass is set (keys on message, like PySpark/Scala)", async () => {
    const t = capturingTransport();
    t.setResponses([
      {
        type: "streamingQueryCommandResult",
        resultType: "exception",
        exception: { errorClass: "SOME_CLASS" },
      },
    ]);
    const query = makeQuery(t);
    assert.equal(await query.exception(), null);
  });

  it("exception() returns the message with optional errorClass/stackTrace omitted", async () => {
    const t = capturingTransport();
    t.setResponses([
      {
        type: "streamingQueryCommandResult",
        resultType: "exception",
        exception: { exceptionMessage: "boom" },
      },
    ]);
    const query = makeQuery(t);
    assert.deepEqual(await query.exception(), { message: "boom" });
  });

  it("exception() decodes message/errorClass/stackTrace", async () => {
    const t = capturingTransport();
    t.setResponses([
      {
        type: "streamingQueryCommandResult",
        resultType: "exception",
        exception: {
          exceptionMessage: "boom",
          errorClass: "ANALYSIS_ERROR",
          stackTrace: "at ...",
        },
      },
    ]);
    const query = makeQuery(t);
    assert.deepEqual(await query.exception(), {
      message: "boom",
      errorClass: "ANALYSIS_ERROR",
      stackTrace: "at ...",
    });
  });

  it("explain(extended) forwards the flag and returns the result string", async () => {
    const t = capturingTransport();
    t.setResponses([
      { type: "streamingQueryCommandResult", resultType: "explain", explain: "== Plan ==" },
    ]);
    const query = makeQuery(t);
    const out = await query.explain(true);
    assert.equal(out, "== Plan ==");
    assert.equal(t.commands[0]["extended"], true);
  });

  it("stop() issues op=stop and resolves", async () => {
    const t = capturingTransport();
    t.setResponses([{ type: "streamingQueryCommandResult" }]);
    const query = makeQuery(t);
    await query.stop();
    assert.equal(t.commands[0]["op"], "stop");
  });

  it("processAllAvailable() issues op=processAllAvailable", async () => {
    const t = capturingTransport();
    t.setResponses([{ type: "streamingQueryCommandResult" }]);
    const query = makeQuery(t);
    await query.processAllAvailable();
    assert.equal(t.commands[0]["op"], "processAllAvailable");
  });

  it("awaitTermination() without timeout returns undefined", async () => {
    const t = capturingTransport();
    t.setResponses([{ type: "streamingQueryCommandResult" }]);
    const query = makeQuery(t);
    assert.equal(await query.awaitTermination(), undefined);
    assert.equal(t.commands[0]["timeoutMs"], undefined);
  });

  it("awaitTermination(timeoutMs) forwards the timeout and returns terminated", async () => {
    const t = capturingTransport();
    t.setResponses([
      { type: "streamingQueryCommandResult", resultType: "awaitTermination", terminated: true },
    ]);
    const query = makeQuery(t);
    assert.equal(await query.awaitTermination(5000), true);
    assert.equal(t.commands[0]["timeoutMs"], 5000);
  });

  it("awaitTermination(0) is accepted (boundary)", async () => {
    const t = capturingTransport();
    t.setResponses([
      { type: "streamingQueryCommandResult", resultType: "awaitTermination", terminated: false },
    ]);
    const query = makeQuery(t);
    assert.equal(await query.awaitTermination(0), false);
    assert.equal(t.commands[0]["timeoutMs"], 0);
  });

  it("awaitTermination() rejects negative, non-finite, or non-integer timeouts", async () => {
    const t = capturingTransport();
    const query = makeQuery(t);
    await assert.rejects(() => query.awaitTermination(-1), InvalidInputError);
    await assert.rejects(() => query.awaitTermination(Number.NaN), InvalidInputError);
    await assert.rejects(() => query.awaitTermination(Number.POSITIVE_INFINITY), InvalidInputError);
    await assert.rejects(() => query.awaitTermination(1000.5), InvalidInputError);
  });

  it("throws SparkClientError when the server returns no streamingQueryCommandResult", async () => {
    const t = capturingTransport();
    t.setResponses([]);
    const query = makeQuery(t);
    await assert.rejects(() => query.status(), SparkClientError);
  });
});
