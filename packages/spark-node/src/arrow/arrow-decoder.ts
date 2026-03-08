/**
 * Decodes Apache Arrow IPC stream data into JS-native Row objects.
 *
 * @see sql/core/src/main/scala/org/apache/spark/sql/execution/arrow/ArrowConverters.scala
 * @see https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format
 *
 * Each response from Spark Connect is a sequence of Arrow IPC messages:
 *   [Schema Message] → [RecordBatch 1] → [RecordBatch 2] → ... → [EOS]
 *
 * The `apache-arrow` JS library handles parsing. We use RecordBatchReader
 * to decode batches as they arrive from gRPC.
 */

import type { Row } from "@spark-connect-js/core";

export class ArrowDecoder {
  /**
   * Decode Arrow IPC stream chunks into an array of Row objects.
   *
   * @param chunks - Arrow IPC stream data (schema + record batches)
   */
  static async decode(chunks: Uint8Array[]): Promise<Row[]> {
    if (chunks.length === 0) return [];

    // Dynamic import of apache-arrow
    const arrow = await import("apache-arrow");

    const rows: Row[] = [];

    // Each chunk from Spark Connect is a self-contained Arrow IPC stream
    // (schema message + record batch).  We must decode each one separately
    // rather than concatenating, because RecordBatchReader.from() only
    // reads the first IPC stream in a buffer.
    for (const chunk of chunks) {
      const reader = arrow.RecordBatchReader.from(chunk);

      for (const batch of reader) {
        const schema = batch.schema;

        for (let rowIdx = 0; rowIdx < batch.numRows; rowIdx++) {
          const row: Row = {};
          for (let colIdx = 0; colIdx < schema.fields.length; colIdx++) {
            const field = schema.fields[colIdx];
            const vector = batch.getChildAt(colIdx);
            const val: unknown = vector?.get(rowIdx) ?? null;
            row[field.name] = coerceValue(val);
          }
          rows.push(row);
        }
      }
    }

    return rows;
  }
}

/**
 * Coerce Arrow-native values to JS-friendly types.
 *
 * - BigInt → Number (when value fits in safe integer range)
 * - Arrow Struct/Map → plain object (toJSON if available)
 * - Everything else passes through unchanged (string, number, boolean, null,
 *   Date objects from Timestamp columns, Uint8Array from Binary columns).
 */
function coerceValue(val: unknown): unknown {
  if (val === null || val === undefined) return null;

  if (typeof val === "bigint") {
    // Arrow decodes Int64/LongType as BigInt.  Convert to Number when the
    // value fits, for easier downstream usage with JSON/logging.
    if (val >= Number.MIN_SAFE_INTEGER && val <= Number.MAX_SAFE_INTEGER) {
      return Number(val);
    }
    return val; // keep as BigInt for large values to avoid precision loss
  }

  // Arrow Struct vectors return Map-like objects with a toJSON method
  if (
    typeof val === "object" &&
    val !== null &&
    "toJSON" in val &&
    typeof (val as { toJSON: unknown }).toJSON === "function"
  ) {
    return (val as { toJSON(): unknown }).toJSON();
  }

  return val;
}
