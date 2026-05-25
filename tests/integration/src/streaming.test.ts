import { describe, it, after } from "node:test";
import assert from "node:assert/strict";
import { SparkConnectError, Trigger } from "@spark-connect-js/node";
import { spark, stopSession } from "./setup.js";

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/** Wait until `predicate()` returns truthy or the deadline expires. */
async function waitUntil(
  predicate: () => Promise<boolean>,
  timeoutMs: number,
  intervalMs = 200,
): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return true;
    await new Promise((r) => setTimeout(r, intervalMs));
  }
  return false;
}

describe("Structured Streaming round-trip", () => {
  after(stopSession);

  it("rate source + memory sink: starts, reports progress, stops", async () => {
    const queryName = `q_${Math.random().toString(36).slice(2, 10)}`;
    const df = spark()
      .readStream.format("rate")
      .option("rowsPerSecond", "10")
      .option("numPartitions", "1")
      .load();

    const query = await df.writeStream
      .format("memory")
      .queryName(queryName)
      .outputMode("append")
      .trigger(Trigger.processingTime("500 milliseconds"))
      .start();

    try {
      assert.match(query.id, UUID_RE);
      assert.match(query.runId, UUID_RE);
      assert.equal(query.name, queryName);

      assert.equal(await query.isActive(), true);

      // Wait for at least one progress report (we asked for 500ms triggers).
      const ok = await waitUntil(async () => (await query.lastProgress()) !== null, 10_000);
      assert.ok(ok, "expected lastProgress() to return a progress report within 10s");

      const status = await query.status();
      assert.equal(typeof status.message, "string");

      const recent = await query.recentProgress();
      assert.ok(recent.length >= 1, "expected at least one progress entry");
      assert.equal(typeof recent[0]["batchId"], "number");
    } finally {
      await query.stop();
    }

    assert.equal(await query.isActive(), false);
  });

  it("awaitTermination(timeoutMs) returns false on a running query and true after stop", async () => {
    const query = await spark()
      .readStream.format("rate")
      .option("rowsPerSecond", "5")
      .option("numPartitions", "1")
      .load()
      .writeStream.format("memory")
      .queryName(`q_at_${Math.random().toString(36).slice(2, 10)}`)
      .outputMode("append")
      .trigger(Trigger.processingTime("500 milliseconds"))
      .start();

    try {
      assert.equal(await query.awaitTermination(250), false);
    } finally {
      await query.stop();
    }
    assert.equal(await query.awaitTermination(1000), true);
  });

  it("explain(extended) returns a non-empty plan string", async () => {
    const query = await spark()
      .readStream.format("rate")
      .option("rowsPerSecond", "1")
      .option("numPartitions", "1")
      .load()
      .writeStream.format("memory")
      .queryName(`q_ex_${Math.random().toString(36).slice(2, 10)}`)
      .outputMode("append")
      .start();

    try {
      const plan = await query.explain(true);
      assert.equal(typeof plan, "string");
      assert.ok(plan.length > 0, "expected a non-empty explain plan");
    } finally {
      await query.stop();
    }
  });

  it("start() surfaces server-side errors as SparkConnectError with errorClass", async () => {
    const df = spark().readStream.format("no_such_streaming_source").load();
    let caught: unknown;
    try {
      await df.writeStream.format("memory").queryName("doomed").outputMode("append").start();
    } catch (err) {
      caught = err;
    }
    if (!(caught instanceof SparkConnectError)) {
      throw new Error(`expected SparkConnectError, got ${String(caught)}`);
    }
    assert.ok(
      typeof caught.errorClass === "string" && caught.errorClass.length > 0,
      `expected errorClass on the streaming start failure, got "${String(caught.errorClass)}"`,
    );
  });
});
