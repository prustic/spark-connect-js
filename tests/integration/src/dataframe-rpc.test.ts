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

  it("isLocal() is true for client-supplied rows and false for server-computed ones", async () => {
    assert.equal(
      await spark()
        .createDataFrame([{ id: 1n }])
        .isLocal(),
      true,
    );
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
});
