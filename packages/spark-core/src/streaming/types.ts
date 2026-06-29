/**
 * Public types for Structured Streaming.
 *
 * @see [Spark source: StreamingQueryStatus.scala](https://github.com/apache/spark/blob/master/sql/api/src/main/scala/org/apache/spark/sql/streaming/StreamingQueryStatus.scala)
 */

/**
 * Output mode of a streaming write.
 *
 * - `"append"` only writes new rows since the last trigger.
 * - `"complete"` rewrites the entire aggregated result every trigger; valid
 *   only for aggregations.
 * - `"update"` writes rows that changed since the last trigger.
 *
 * Not every sink supports every mode; consult Spark's docs for the source-sink
 * matrix.
 */
export type StreamingOutputMode = "append" | "complete" | "update";

/**
 * Snapshot of a streaming query's runtime state, as returned by
 * {@link StreamingQuery.status}.
 */
export interface StreamingQueryStatus {
  /** Human-readable description of what the query is doing right now. */
  message: string;
  /** Whether new input data is available to be processed. */
  isDataAvailable: boolean;
  /** Whether a trigger is currently active (processing a batch). */
  isTriggerActive: boolean;
  /** Whether the query is still running (not yet stopped or failed). */
  isActive: boolean;
}

/**
 * One progress report from a streaming query. Spark Connect sends progress
 * as a JSON string; this is the parsed shape.
 *
 * The named fields below cover the values Spark sets on nearly every report;
 * the index signature lets newer Spark versions or third-party
 * `StreamingQueryListener` events add fields without breaking compilation.
 */
export interface StreamingQueryProgress {
  /** Spark-internal query identifier (the `StreamingQuery.id`). */
  id?: string;
  /** Run identifier for the current execution attempt. */
  runId?: string;
  /** Optional query name supplied via `writeStream.queryName(...)`. */
  name?: string;
  /** ISO-8601 timestamp the batch was emitted at. */
  timestamp?: string;
  /** Monotonically increasing batch number within this run. */
  batchId?: number;
  /** End-to-end duration of the batch in milliseconds. */
  batchDuration?: number;
  /** Per-phase durations (e.g. `addBatch`, `getOffset`, `triggerExecution`). */
  durationMs?: Record<string, number>;
  /** Total input rows processed in this batch. Long on the server. */
  numInputRows?: number | bigint;
  /** Average input rate over this batch. */
  inputRowsPerSecond?: number;
  /** Average processed rate over this batch. */
  processedRowsPerSecond?: number;
  /** Per-state-operator metrics. */
  stateOperators?: unknown[];
  /** Per-source progress (offsets, rows, rates). */
  sources?: unknown[];
  /** Sink description. */
  sink?: unknown;
  /** Forward-compat for fields Spark adds in newer versions. */
  [key: string]: unknown;
}

/**
 * The exception that caused a streaming query to fail, as reported by the
 * server. Fields are optional because the server only sets them when the
 * query has actually failed.
 */
export interface StreamingQueryException {
  /** The original exception's `toString()`. */
  message?: string;
  /** Spark error class identifier (e.g. `"DATA_SOURCE_NOT_FOUND"`). */
  errorClass?: string;
  /** Server-side stack trace, preformatted as a single string. */
  stackTrace?: string;
}
