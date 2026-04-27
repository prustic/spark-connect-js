// Schema types: model the structure of a DataFrame.
//
// In Spark, StructType is a Seq[StructField]. We model the same shape so the
// client can describe schemas, interpret Arrow batches returned by Spark
// Connect, and surface column metadata to user code.
//
// @see [Spark source: StructType.scala](https://github.com/apache/spark/blob/master/sql/api/src/main/scala/org/apache/spark/sql/types/StructType.scala)
// @see [Spark source: StructField.scala](https://github.com/apache/spark/blob/master/sql/api/src/main/scala/org/apache/spark/sql/types/StructField.scala)

import type { DataType } from "./data-type.js";

/**
 * Metadata for a single column: its name, type, nullability, and any
 * free-form key/value annotations.
 *
 * Mirrors Spark's `StructField`. Used inside {@link Schema} and returned by
 * `DataFrame.schema()`.
 */
export interface FieldDescriptor {
  /** Column name. */
  name: string;
  /** Spark data type for the column. */
  dataType: DataType;
  /** Whether the column accepts null values. */
  nullable: boolean;
  /** Optional free-form metadata attached to the field. */
  metadata?: Record<string, string>;
}

/**
 * The schema of a DataFrame: an ordered list of {@link FieldDescriptor}s.
 *
 * Mirrors Spark's `StructType`. Returned by `DataFrame.schema()` and accepted
 * by readers that take an explicit schema (see `DataFrameReader.schema()`).
 */
export interface Schema {
  /** Columns of the DataFrame, in declaration order. */
  fields: FieldDescriptor[];
}
