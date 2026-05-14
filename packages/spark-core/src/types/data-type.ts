/**
 * Mirrors Spark's `DataType` hierarchy for type-safe schema operations.
 *
 * @see [Spark source: DataType.scala](https://github.com/apache/spark/blob/master/sql/api/src/main/scala/org/apache/spark/sql/types/DataType.scala)
 * @see [Spark source: all SQL types](https://github.com/apache/spark/tree/master/sql/api/src/main/scala/org/apache/spark/sql/types)
 *
 * Spark types live in `org.apache.spark.sql.types`. The main mapping constraint
 * in JavaScript is that `number` is IEEE-754 `float64`, so it cannot represent
 * every Spark integer value exactly.
 *
 * | Spark type | Size | JavaScript representation |
 * | --- | --- | --- |
 * | `ByteType` | 1 byte | `number` |
 * | `ShortType` | 2 bytes | `number` |
 * | `IntegerType` | 4 bytes | `number` |
 * | `LongType` | 8 bytes | `bigint` when precision matters |
 * | `FloatType` | 4 bytes | `number` |
 * | `DoubleType` | 8 bytes | `number` |
 * | `DecimalType` | variable | string-like decimal encoding |
 * | `StringType` | variable | `string` |
 * | `BooleanType` | 1 bit | `boolean` |
 * | `BinaryType` | variable | `Uint8Array` |
 * | `TimestampType` | 8 bytes | `Date` with microsecond precision loss |
 * | `DateType` | 4 bytes | `Date` |
 *
 * Arrow's type system is close enough to Spark's SQL types that it can carry
 * result batches efficiently with very little translation on the client side.
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
