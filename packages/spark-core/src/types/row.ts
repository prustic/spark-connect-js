import { InvalidInputError } from "../errors.js";

/**
 * A Row is the JS-native representation of one record returned by a DataFrame
 * action. It maps column names to JavaScript values.
 *
 * @see [Spark source: Row.scala](https://github.com/apache/spark/blob/master/sql/api/src/main/scala/org/apache/spark/sql/Row.scala)
 *
 * When Arrow IPC batches are decoded, each row is materialised as a plain
 * object.  This is intentionally kept as a simple Record type because we don't
 * wrap it in a class because:
 *   1. JSON.stringify works out of the box for logging/debugging.
 *   2. Destructuring works naturally: `const { name, age } = row;`
 *   3. No prototype overhead for millions of rows.
 *
 * Values:
 *   - `number` for `IntegerType`, `ShortType`, `ByteType`, `FloatType`, `DoubleType`
 *   - `string` for `StringType` and `DecimalType` (decimals decode as
 *     fixed-point strings honoring scale, e.g. `"1.50"` for `DECIMAL(10,2)`,
 *     since JS has no native arbitrary-precision decimal)
 *   - `boolean` for `BooleanType`
 *   - `Uint8Array` for `BinaryType`
 *   - `Date` for `DateType` and `TimestampType`. Sub-millisecond precision is
 *     lost: Spark's micro/nano timestamps truncate to ms.
 *   - `Map<K, V>` for `MapType`. Preserves non-string keys, unlike a plain
 *     object. Note that `JSON.stringify(map)` is `"{}"`.
 *   - `object` for `StructType`. Nested rows recurse with the same rules.
 *   - `unknown[]` for `ArrayType`. Recurses with the same rules.
 *   - `null` for nullable columns
 *
 * For compile-time typed access, narrow with `df.as<Schema>().collect()`. For
 * runtime typed access (schema not known statically), use the {@link row}
 * accessor namespace.
 */
export type Row = Record<string, unknown>;

function require(r: Row, col: string): unknown {
  if (r === null || r === undefined) {
    throw new InvalidInputError(`Cannot read column "${col}" from a null/undefined row.`);
  }
  if (!Object.prototype.hasOwnProperty.call(r, col)) {
    const available = Object.keys(r).join(", ");
    throw new InvalidInputError(`Column "${col}" not found in row. Available: ${available}`);
  }
  return r[col];
}

function mismatch(col: string, expected: string, actual: unknown): InvalidInputError {
  const actualType = actual === null ? "null" : typeof actual;
  return new InvalidInputError(`Column "${col}" expected ${expected}, got ${actualType}`);
}

/**
 * Typed runtime accessors for {@link Row} values. Each getter checks the
 * column exists and the value matches the expected JavaScript type. Returns
 * `null` when the value is `null`. Throws {@link InvalidInputError} on a
 * type mismatch or missing column.
 *
 * Use when the schema isn't known at compile time. The alternative,
 * `df.as<Schema>().collect()`, gives full compile-time typing instead.
 * Matches the Scala `Row.getInt`, `getLong`, and `getAs[T]` pattern.
 *
 * @example
 *   import { row } from "@spark-connect-js/core";
 *
 *   const [stats] = await df.collect();
 *   const total = row.getLong(stats, "count");      // bigint | null
 *   const mean  = row.getDouble(stats, "average");  // number | null
 */
export const row = {
  /** Spark `IntegerType` / `ShortType` / `ByteType`. */
  getInt(r: Row, col: string): number | null {
    const v = require(r, col);
    if (v === null) return null;
    if (typeof v !== "number" || !Number.isInteger(v)) throw mismatch(col, "integer", v);
    return v;
  },

  /** Spark `LongType` (decoded as `bigint` to preserve 64-bit precision). */
  getLong(r: Row, col: string): bigint | null {
    const v = require(r, col);
    if (v === null) return null;
    if (typeof v !== "bigint") throw mismatch(col, "bigint", v);
    return v;
  },

  /** Spark `DoubleType` / `FloatType`. */
  getDouble(r: Row, col: string): number | null {
    const v = require(r, col);
    if (v === null) return null;
    if (typeof v !== "number") throw mismatch(col, "number", v);
    return v;
  },

  /** Spark `StringType`. Also covers `DecimalType` (decoded as `string`). */
  getString(r: Row, col: string): string | null {
    const v = require(r, col);
    if (v === null) return null;
    if (typeof v !== "string") throw mismatch(col, "string", v);
    return v;
  },

  /** Spark `BooleanType`. */
  getBoolean(r: Row, col: string): boolean | null {
    const v = require(r, col);
    if (v === null) return null;
    if (typeof v !== "boolean") throw mismatch(col, "boolean", v);
    return v;
  },

  /** Spark `BinaryType`. */
  getBinary(r: Row, col: string): Uint8Array | null {
    const v = require(r, col);
    if (v === null) return null;
    if (!(v instanceof Uint8Array)) throw mismatch(col, "Uint8Array", v);
    return v;
  },

  /** Spark `DateType` / `TimestampType`. */
  getDate(r: Row, col: string): Date | null {
    const v = require(r, col);
    if (v === null) return null;
    if (!(v instanceof Date)) throw mismatch(col, "Date", v);
    return v;
  },
};
