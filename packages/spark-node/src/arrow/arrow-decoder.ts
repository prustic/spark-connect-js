/**
 * ─── ArrowDecoder ───────────────────────────────────────────────────────────
 *
 * Decodes Apache Arrow IPC stream data into JS-native Row objects.
 *
 * @see Spark Arrow serialisation: sql/core/src/main/scala/org/apache/spark/sql/execution/arrow/ArrowConverters.scala
 * @see Arrow IPC format: https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format
 *
 * ─── Why Arrow? ─────────────────────────────────────────────────────────────
 *
 * Spark Connect uses Arrow as its result serialisation format because:
 *
 *   1. **Columnar layout** — Arrow stores data column-by-column, matching
 *      Spark's internal Tungsten columnar format.  This means serialisation
 *      from Spark to Arrow is nearly zero-copy on the JVM side.
 *
 *   2. **Zero-copy reads** — Arrow buffers can be memory-mapped or shared
 *      without deserialisation.  In Node.js, the Arrow buffer arrives as a
 *      Buffer (pointing to V8 heap or off-heap memory), and the `apache-arrow`
 *      library reads directly from that memory without JSON parsing.
 *
 *   3. **Language-agnostic** — The same Arrow IPC format is read by Python
 *      (pyarrow), Java, Rust, C++, and now JavaScript.
 *
 * ─── Arrow IPC Streaming Format ─────────────────────────────────────────────
 *
 * The response from Spark Connect is a sequence of Arrow IPC messages:
 *
 *   [Schema Message] → [RecordBatch 1] → [RecordBatch 2] → ... → [EOS]
 *
 * Each message is prefixed with:
 *   - 4 bytes: continuation indicator (0xFFFFFFFF)
 *   - 4 bytes: metadata length (flatbuffer size)
 *   - N bytes: flatbuffer metadata (schema or record batch descriptor)
 *   - M bytes: body (raw Arrow array data, aligned to 8 bytes)
 *
 * The `apache-arrow` JS library handles all of this parsing.  We use its
 * RecordBatchReader to incrementally decode batches as they arrive from gRPC.
 *
 * ─── Memory Considerations ──────────────────────────────────────────────────
 *
 * Arrow record batches are backed by ArrayBuffers.  In Node.js:
 *   - Small Buffers (< 8KB) come from the Buffer pool (shared slab)
 *   - Large Buffers get their own ArrayBuffer allocation
 *   - The Arrow library may create views (subarrays) without copying
 *
 * We must be careful NOT to retain references to individual columns or vectors
 * after they've been converted to JS values, or the entire underlying
 * ArrayBuffer stays alive — a classic memory leak pattern.
 */

import type { Row } from "@spark-js/core";

/**
 * Decodes Arrow IPC stream chunks into Row arrays.
 *
 * Uses the `apache-arrow` npm package for actual decoding.  The import is
 * dynamic to avoid hard failures if the package isn't installed (it's listed
 * as a dependency of @spark-js/node, but dynamic import gives better errors).
 */
export class ArrowDecoder {
  /**
   * Decode concatenated Arrow IPC stream chunks into an array of Row objects.
   *
   * @param chunks - Arrow IPC stream data (schema + record batches)
   * @returns Array of plain JS objects, one per row
   *
   * Conversion from Arrow columnar → Row objects:
   *   For each RecordBatch:
   *     For each row index 0..batch.numRows:
   *       Create a Row object with { columnName: value } for each column
   *
   * This is inherently an O(rows × columns) operation.  For large datasets,
   * consuming Arrow Tables directly (without Row conversion) is faster.
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
