/**
 * ─── Row ────────────────────────────────────────────────────────────────────
 *
 * A Row is the JS-native representation of one record returned by a DataFrame
 * action.  It maps column names to JS values. *
 * @see Spark source: sql/api/src/main/scala/org/apache/spark/sql/Row.scala *
 * When Arrow IPC batches are decoded, each row is materialised as a plain
 * object.  This is intentionally kept as a simple Record type — we don't
 * wrap it in a class because:
 *   1. JSON.stringify works out of the box for logging/debugging.
 *   2. Destructuring works naturally: `const { name, age } = row;`
 *   3. No prototype overhead for millions of rows.
 *
 * ⚠️  Values may be:
 *   - `bigint` for Spark LongType columns (to preserve precision)
 *   - `Uint8Array` for BinaryType columns
 *   - `null` for nullable columns
 *   - `Date` for TimestampType / DateType (with precision caveats)
 */
export type Row = Record<string, unknown>;
