import { describe, it, after } from "node:test";
import assert from "node:assert/strict";
import { col, MEMORY_ONLY } from "@spark-connect-js/node";
import { spark, stopSession, tempPath } from "./setup.js";

describe("DataFrame methods answered by the server", () => {
  after(stopSession);

  it("isStreaming() distinguishes a batch frame from a streaming one", async () => {
    assert.equal(await spark().range(3).isStreaming(), false);
    assert.equal(await spark().readStream.format("rate").load().isStreaming(), true);
  });

  it("isLocal() is true only when the top of the plan is local data or a command result", async () => {
    const rows = spark().createDataFrame([{ id: 1n }]);
    assert.equal(await rows.isLocal(), true);
    // Connect turns an executed SQL command into local data.
    assert.equal(await spark().sql("SHOW DATABASES").isLocal(), true);
    // Only the top node counts, so a transformation over local data is not local.
    assert.equal(await rows.filter("id > 0").isLocal(), false);
    assert.equal(await spark().range(10).isLocal(), false);
  });

  it("inputFiles() lists the files behind a file-based read, and nothing otherwise", async () => {
    const path = tempPath("input_files");
    await spark().range(3).write.mode("overwrite").parquet(path);

    const files = await spark().read.parquet(path).inputFiles();
    assert.ok(files.length > 0);
    for (const file of files) {
      assert.match(file, /input_files/);
    }
    assert.deepEqual(await spark().range(3).inputFiles(), []);
  });

  it("checkpoint() returns a frame that keeps working through filters and joins", async () => {
    // The docker server is started with spark.checkpoint.dir, which a client
    // cannot set at runtime.
    const checkpointed = await spark().range(5).checkpoint();
    const other = spark().range(3);

    assert.equal(await checkpointed.count(), 5n);
    assert.equal(await checkpointed.filter(col("id").gt(2)).count(), 2n);
    assert.equal(
      await checkpointed.join(other, checkpointed.col("id").eq(other.col("id"))).count(),
      3n,
    );
  });

  it("localCheckpoint() works lazily with an explicit storage level", async () => {
    const checkpointed = await spark().range(5).localCheckpoint(false, MEMORY_ONLY);
    assert.equal(await checkpointed.count(), 5n);
  });

  it("releasing a checkpoint frees the server-held relation", async () => {
    const checkpointed = await spark().range(5).checkpoint();
    assert.equal(await checkpointed.count(), 5n);

    const plan = checkpointed._plan;
    assert.ok(plan.type === "cachedRemoteRelation");
    // Normally triggered by garbage collection, which a test cannot force.
    await spark()._releaseCachedRelation(plan.relationId);
    await assert.rejects(checkpointed.count());
  });
});
