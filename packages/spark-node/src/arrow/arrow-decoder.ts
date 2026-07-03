/**
 * Decodes Apache Arrow IPC stream data into JS-native Row objects.
 *
 * @see [Spark source: ArrowConverters.scala](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/arrow/ArrowConverters.scala)
 * @see [Arrow IPC streaming format](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format)
 *
 * Each response from Spark Connect is a sequence of Arrow IPC messages:
 *   [Schema Message] then [RecordBatch 1] then [RecordBatch 2] then ... then [EOS]
 *
 * The `apache-arrow` JS library handles parsing. We use RecordBatchReader
 * to decode batches as they arrive from gRPC.
 *
 * Coercion is **type-driven**: every value flows through a dispatch keyed on
 * `field.type.typeId`. This is what lets Decimal honor scale, Timestamp and
 * Date round-trip to `Date`, and Map preserve non-string keys. The old
 * value-only path lost the schema before coercion and produced silent decode
 * bugs.
 */

import type { Row } from "@spark-connect-js/core";
import {
  Type,
  type DataType,
  type Int,
  type Decimal,
  type Map_,
  type Struct,
  type List,
  type FixedSizeList,
} from "apache-arrow";

export class ArrowDecoder {
  /**
   * Decode Arrow IPC stream chunks into an array of Row objects.
   *
   * @param chunks - Arrow IPC stream data (schema + record batches)
   */
  static async decode(chunks: Uint8Array[]): Promise<Row[]> {
    if (chunks.length === 0) return [];

    const arrow = await import("apache-arrow");
    const rows: Row[] = [];

    // Each chunk from Spark Connect is a self-contained Arrow IPC stream
    // (schema message + record batch). Decode each separately rather than
    // concatenating, because RecordBatchReader.from() only reads the first
    // IPC stream in a buffer.
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
            row[field.name] = coerceValue(val, field.type as DataType);
          }
          rows.push(row);
        }
      }
    }

    return rows;
  }
}

function coerceValue(val: unknown, type: DataType): unknown {
  if (val === null || val === undefined) return null;

  switch (type.typeId) {
    case Type.Int:
      return coerceInt(val, type as Int);

    case Type.Decimal:
      return formatDecimal(val, (type as Decimal).scale);

    case Type.Date:
      return coerceDate(val);

    case Type.Timestamp:
      return coerceTimestamp(val);

    case Type.Map:
      return coerceMap(val, type as Map_);

    case Type.Struct:
      return coerceStruct(val, type as Struct);

    case Type.List:
    case Type.FixedSizeList:
      return coerceList(val, type as List | FixedSizeList);

    default:
      // Float, Bool, Utf8/LargeUtf8, Binary/LargeBinary/FixedSizeBinary,
      // smaller Ints (already number), null. Pass through.
      return val;
  }
}

function coerceInt(val: unknown, type: Int): unknown {
  if (type.bitWidth !== 64) {
    return val;
  }
  if (typeof val === "number") {
    return BigInt(val);
  }
  return val;
}

/**
 * Format an Arrow decimal value into a fixed-point string honoring `scale`.
 * Returns e.g. `"1.50"` for `DECIMAL(10,2)` value `1.5`. The unscaled value
 * arrives as a BigNum (or a number for small decimals). `String(val)` yields
 * the unscaled-integer digits, into which we insert the decimal point.
 */
function formatDecimal(val: unknown, scale: number): string {
  const raw = String(val);
  if (scale <= 0) return raw;

  const negative = raw.startsWith("-");
  const digits = negative ? raw.slice(1) : raw;
  const padded = digits.padStart(scale + 1, "0");
  const intPart = padded.slice(0, padded.length - scale);
  const fracPart = padded.slice(padded.length - scale);

  return (negative ? "-" : "") + intPart + "." + fracPart;
}

function coerceDate(val: unknown): Date {
  if (val instanceof Date) return val;
  // apache-arrow surfaces both DateDay and DateMillisecond as a JS number in
  // milliseconds since epoch (it does the day-to-ms conversion internally).
  return new Date(Number(val));
}

function coerceTimestamp(val: unknown): Date {
  if (val instanceof Date) return val;
  // Arrow surfaces timestamps as a number (ms with optional fractional sub-ms)
  // or a bigint for nanosecond resolution. Number(val) covers both. Date
  // truncates the fractional part, so micro/nano precision is lost
  // (documented on the Row type).
  return new Date(Number(val));
}

function coerceMap(val: unknown, type: Map_): Map<unknown, unknown> {
  // Map's wire shape is List<Struct{key, value}>. The single outer child is
  // the entries struct, whose own children are key and value.
  const entriesType = type.children[0].type as Struct;
  const keyType = entriesType.children[0].type as DataType;
  const valueType = entriesType.children[1].type as DataType;
  const out = new Map<unknown, unknown>();
  for (const [k, v] of val as Iterable<[unknown, unknown]>) {
    out.set(coerceValue(k, keyType), coerceValue(v, valueType));
  }
  return out;
}

function coerceStruct(val: unknown, type: Struct): Row {
  const out: Row = {};
  const fieldTypes = new Map<string, DataType>();
  for (const child of type.children) {
    fieldTypes.set(child.name, child.type as DataType);
  }
  for (const [name, v] of val as Iterable<[string, unknown]>) {
    const childType = fieldTypes.get(name);
    out[name] = childType ? coerceValue(v, childType) : v;
  }
  return out;
}

function coerceList(val: unknown, type: List | FixedSizeList): unknown[] {
  const elemType = type.children[0].type as DataType;
  const items: unknown[] = [];
  for (const v of val as Iterable<unknown>) {
    items.push(coerceValue(v, elemType));
  }
  return items;
}
