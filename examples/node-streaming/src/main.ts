import { connect, Trigger, type StreamingQueryListener } from "@spark-connect-js/node";

const SPARK_REMOTE = process.env["SPARK_REMOTE"] ?? "sc://localhost:15002";

const session = connect(SPARK_REMOTE);

// Subscribe to streaming-query lifecycle events. The bus opens lazily on the
// first addListener and tears down on the last removeListener. Callbacks
// dispatch serially.
const listener: StreamingQueryListener = {
  onQueryStarted: (e) => {
    console.log(`started    ${e.id} run=${e.runId}`);
  },
  onQueryProgress: (e) => {
    const batchId = e["batchId"] as number;
    const inputRate = (e["inputRowsPerSecond"] as number | undefined) ?? 0;
    console.log(`progress   batch=${batchId} inputRowsPerSecond=${inputRate.toFixed(1)}`);
  },
  onQueryTerminated: (e) => {
    console.log(`terminated ${e.id} ${e.exception ?? "(clean)"}`);
  },
};

await session.streams.addListener(listener);

// Rate source emits (timestamp, value) rows at the configured rate. Memory
// sink keeps batches in an in-process Spark table for inspection.
const query = await session.readStream
  .format("rate")
  .option("rowsPerSecond", "5")
  .load()
  .writeStream.format("memory")
  .queryName("rate_to_memory")
  .outputMode("append")
  .trigger(Trigger.processingTime("1 second"))
  .start();

const activeIds = (await session.streams.active()).map((q) => q.id);
console.log(`active queries: [${activeIds.join(", ")}]`);

// Block until the query terminates or 5 seconds elapse, whichever first.
// onQueryProgress fires for each trigger interval in between.
await query.awaitTermination(5_000);

// query.lastProgress() returns the most recent batch directly, as a
// pull-style alternative to the listener bus.
const last = await query.lastProgress();
const lastBatchId = (last?.["batchId"] as number | undefined) ?? "none";
console.log(`last batch: ${lastBatchId}`);

await query.stop();
await session.streams.removeListener(listener);

const remaining = await session.streams.active();
console.log(`after stop: ${remaining.length} active queries`);

await session.stop();
console.log("session stopped");
