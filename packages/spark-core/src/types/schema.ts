/**
 * ─── Schema ─────────────────────────────────────────────────────────────────
 *
 * Represents the schema (StructType) of a DataFrame.
 *
 * @see Spark source (StructType): sql/api/src/main/scala/org/apache/spark/sql/types/StructType.scala
 * @see Spark source (StructField): sql/api/src/main/scala/org/apache/spark/sql/types/StructField.scala
 *
 * In Spark, StructType is a Seq[StructField] where each StructField has:
 *   - name: String
 *   - dataType: DataType
 *   - nullable: Boolean
 *   - metadata: Metadata
 *
 * We model the same structure for use in type-safe operations and for
 * interpreting Arrow schemas returned by Spark Connect.
 */

import type { DataType } from "./data-type.js";

export interface FieldDescriptor {
  name: string;
  dataType: DataType;
  nullable: boolean;
  metadata?: Record<string, string>;
}

export interface Schema {
  fields: FieldDescriptor[];
}
