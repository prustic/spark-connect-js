/**
 * Public types for Structured Streaming.
 *
 * @see [Spark source: StreamingQueryStatus.scala](https://github.com/apache/spark/blob/master/sql/api/src/main/scala/org/apache/spark/sql/streaming/StreamingQueryStatus.scala)
 */

import type { Row } from "../types/row.js";

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
 * Per-source progress within one batch: offsets consumed, row counts, and
 * rates for a single input source.
 */
export interface SourceProgress {
  /** Source description, e.g. `RateStreamV2[rowsPerSecond=50, ...]`. */
  description: string;
  /**
   * Offset at the start of the batch. Opaque and source-specific: a string
   * on `lastProgress()`, parsed (number or object) on bus events, `null` at
   * batch 0.
   */
  startOffset: string | number | Record<string, unknown> | null;
  /** Offset at the end of the batch. Same shape as `startOffset`. */
  endOffset: string | number | Record<string, unknown> | null;
  /** Latest offset available at the source. Same shape as `startOffset`. */
  latestOffset: string | number | Record<string, unknown> | null;
  /** Rows read from this source in this batch. */
  numInputRows: number;
  /**
   * Average input rate over this batch. Absent on listener-bus events when
   * the server computes NaN or Infinity (division by a zero-length window).
   */
  inputRowsPerSecond?: number;
  /** Average processing rate over this batch. Absent like `inputRowsPerSecond`. */
  processedRowsPerSecond?: number;
  /** Source-specific metrics. Omitted on listener-bus events when empty. */
  metrics?: Record<string, string>;
}

/** Sink-side progress within one batch. */
export interface SinkProgress {
  /** Sink description, e.g. `MemorySink`. */
  description: string;
  /** Rows written to the sink in this batch. */
  numOutputRows: number;
  /** Sink-specific metrics. Omitted on listener-bus events when empty. */
  metrics?: Record<string, string>;
}

/**
 * Metrics for one stateful operator (windowed aggregation, stream-stream
 * join, deduplication) within one batch.
 */
export interface StateOperatorProgress {
  /** Operator name, e.g. `stateStoreSave`. */
  operatorName: string;
  numRowsTotal: number;
  numRowsUpdated: number;
  allUpdatesTimeMs: number;
  numRowsRemoved: number;
  allRemovalsTimeMs: number;
  commitTimeMs: number;
  memoryUsedBytes: number;
  numRowsDroppedByWatermark: number;
  numShufflePartitions: number;
  numStateStoreInstances: number;
  /** State-store implementation metrics. */
  customMetrics: Record<string, number>;
}

/**
 * One progress report from a streaming query, parsed from the server's JSON.
 * The server serializes `lastProgress()` and listener-bus events differently,
 * so field presence varies per path (noted per field).
 *
 * The index signature admits fields from newer Spark versions without
 * weakening the named ones; do not remove it to "tighten" the interface.
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
  /**
   * Event-time watermark state for watermarked queries: ISO-8601 strings
   * under the keys `min`, `avg`, `watermark`, `max`.
   */
  eventTime?: Record<string, string>;
  /**
   * Total input rows in this batch. Absent from `lastProgress()` and
   * `recentProgress()`, where the per-source values in
   * `sources[].numInputRows` are authoritative; present on listener-bus
   * events. See {@link totalInputRows} for a path-independent total.
   */
  numInputRows?: number;
  /** Average input rate. Same per-path presence as `numInputRows`. */
  inputRowsPerSecond?: number;
  /** Average processing rate. Same per-path presence as `numInputRows`. */
  processedRowsPerSecond?: number;
  /** Per-state-operator metrics for stateful queries. */
  stateOperators?: StateOperatorProgress[];
  /** Per-source progress (offsets, rows, rates). */
  sources?: SourceProgress[];
  /** Sink-side progress. */
  sink?: SinkProgress;
  /**
   * Observed metrics keyed by `df.observe(name, ...)` name. Empty until an
   * observation is attached to the query. Values arrive JSON-decoded here
   * (longs as plain numbers), unlike batch `Observation.get`, which decodes
   * with the Arrow policy.
   */
  observedMetrics?: Record<string, Row>;
  /** Forward-compat for fields Spark adds in newer versions. */
  [key: string]: unknown;
}

/**
 * Sum of `sources[].numInputRows`. Spark Connect reports row counts
 * per-source rather than top-level, so this is the Connect equivalent of
 * classic Spark's `progress.numInputRows`.
 */
export function totalInputRows(progress: StreamingQueryProgress): number {
  return (progress.sources ?? []).reduce((sum, s) => sum + (s.numInputRows ?? 0), 0);
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

/**
 * Reshape the observed-metrics payload into the declared `Record<string, Row>`.
 *
 * The server sends each metric as a `{ values, schema }` wrapper rather than a
 * row object, so without this the declared type silently lies and every field
 * access yields `undefined`. Values are zipped against the schema field names.
 * Payloads already in row form pass through unchanged.
 *
 * @internal
 */
export function normalizeProgress(progress: StreamingQueryProgress): StreamingQueryProgress {
  const observed = progress.observedMetrics;
  if (observed === undefined || observed === null) {
    return progress;
  }

  const normalized: Record<string, Row> = {};
  for (const [name, value] of Object.entries(observed)) {
    normalized[name] = isMetricWrapper(value) ? metricToRow(value) : value;
  }
  progress.observedMetrics = normalized;

  return progress;
}

interface MetricWrapper {
  values: unknown[];
  schema: { fields: { name?: string }[] };
}

// Both halves are required to discriminate: a metric row may legitimately have
// a column named `values` holding an array, and rewriting that would drop its
// siblings. Anything unrecognized is left alone.
function isMetricWrapper(value: unknown): value is MetricWrapper {
  if (typeof value !== "object" || value === null) {
    return false;
  }
  const candidate = value as { values?: unknown; schema?: { fields?: unknown } };

  return (
    Array.isArray(candidate.values) &&
    typeof candidate.schema === "object" &&
    candidate.schema !== null &&
    Array.isArray(candidate.schema.fields)
  );
}

function metricToRow(wrapper: MetricWrapper): Row {
  const row: Row = {};
  wrapper.values.forEach((value, i) => {
    row[wrapper.schema.fields[i]?.name ?? `col_${String(i)}`] = value;
  });

  return row;
}
