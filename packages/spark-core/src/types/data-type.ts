/**
 * ─── DataType ───────────────────────────────────────────────────────────────
 *
 * Mirrors Spark's DataType hierarchy for type-safe schema operations.
 *
 * @see Spark source: sql/api/src/main/scala/org/apache/spark/sql/types/DataType.scala
 * @see All types: sql/api/src/main/scala/org/apache/spark/sql/types/
 *
 * Spark types live in `org.apache.spark.sql.types`.  The critical mapping
 * concern is JS number (float64) vs Spark's integer types:
 *
 *   Spark Type     | Size    | JS Equivalent
 *   ───────────────|─────────|──────────────
 *   ByteType       | 1 byte  | number (safe)
 *   ShortType      | 2 bytes | number (safe)
 *   IntegerType    | 4 bytes | number (safe)
 *   LongType       | 8 bytes | bigint (number loses precision > 2^53)
 *   FloatType      | 4 bytes | number
 *   DoubleType     | 8 bytes | number
 *   DecimalType    | varies  | string (no native arbitrary-precision in JS)
 *   StringType     |         | string
 *   BooleanType    |         | boolean
 *   BinaryType     |         | Uint8Array
 *   TimestampType  |         | Date (lossy — microsecond precision lost)
 *   DateType       |         | Date
 *
 * Arrow's type system closely mirrors Spark's, which is why Arrow is the
 * ideal serialisation format for Spark Connect results.
 */

export enum DataType {
  Boolean = "boolean",
  Byte = "byte",
  Short = "short",
  Integer = "integer",
  Long = "long",
  Float = "float",
  Double = "double",
  Decimal = "decimal",
  String = "string",
  Binary = "binary",
  Timestamp = "timestamp",
  Date = "date",
  Array = "array",
  Map = "map",
  Struct = "struct",
  Null = "null",
}
