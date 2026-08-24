import { InvalidInputError, type Row, type StructType } from "@spark-connect-js/core";
import {
  Table,
  Schema,
  Field,
  Utf8,
  Int32,
  Int64,
  Float64,
  Bool,
  TimestampMillisecond,
  vectorFromArray,
  tableToIPC,
  type Vector,
} from "apache-arrow";

/**
 * Encode `Row[]` values into Arrow IPC streaming bytes for
 * `SparkSession.createDataFrame([...])`. Strings emit as `Utf8`, never
 * `Dictionary<Int32, Utf8>`, because Spark's LocalRelation reader ignores
 * the dictionary batch and would see bare integer indices.
 *
 * Arrow types are always inferred from the values. A provided schema only
 * contributes per-field nullability.
 */
export class ArrowEncoder {
  static encode(rows: Row[], schema?: StructType): Uint8Array {
    if (rows.length === 0) {
      throw new InvalidInputError(
        "createDataFrame([...]) requires a non-empty array of rows. " +
          "Use spark.range(0) or spark.sql(...) for an empty DataFrame.",
      );
    }

    const fieldNames = Object.keys(rows[0]);
    if (fieldNames.length === 0) {
      throw new InvalidInputError("createDataFrame([...]) rows must have at least one field.");
    }

    const nonNullable = new Set(
      (schema?.fields ?? []).filter((f) => !f.nullable).map((f) => f.name),
    );

    const columns: Record<string, Vector> = {};
    for (const name of fieldNames) {
      const values = rows.map((r) => r[name]);
      const vector = buildColumnVector(name, values);
      if (nonNullable.has(name) && vector.nullCount > 0) {
        const nullAt = values.findIndex((v) => v === null || v === undefined);
        throw new InvalidInputError(
          `createDataFrame([...]): column "${name}" is NOT NULL in the schema ` +
            `but row ${String(nullAt)} contains a null.`,
        );
      }
      columns[name] = vector;
    }

    if (nonNullable.size === 0) {
      return tableToIPC(new Table(columns), "stream");
    }

    // Table offers no per-field nullability input, so hand it a prebuilt schema.
    const fields = fieldNames.map(
      (name) => new Field(name, columns[name].type, !nonNullable.has(name)),
    );
    return tableToIPC(new Table(new Schema(fields), columns), "stream");
  }
}

function buildColumnVector(name: string, values: unknown[]): Vector {
  const sample = values.find((v) => v !== null && v !== undefined);

  // All-null column: default to nullable Utf8. Rare in practice.
  if (sample === undefined) {
    return vectorFromArray(values as (string | null)[], new Utf8());
  }

  const category = categoryOf(sample);
  if (category === null) {
    throw new InvalidInputError(
      `createDataFrame([...]): unsupported value type "${typeName(sample)}" in column "${name}". ` +
        "Supported types: string, number, boolean, bigint, Date, null. " +
        "For richer types, build the Arrow IPC bytes yourself and pass a Uint8Array.",
    );
  }

  for (let i = 0; i < values.length; i++) {
    const v = values[i];
    if (v === null || v === undefined) continue;
    if (categoryOf(v) !== category) {
      throw new InvalidInputError(
        `createDataFrame([...]): column "${name}" mixes ${category} and ${typeName(v)} ` +
          `values (row ${i}). Split into separate columns or normalize the type.`,
      );
    }
  }

  switch (category) {
    case "string":
      return vectorFromArray(values as (string | null)[], new Utf8());
    case "boolean":
      return vectorFromArray(values as (boolean | null)[], new Bool());
    case "bigint":
      return vectorFromArray(values as (bigint | null)[], new Int64());
    case "Date":
      return vectorFromArray(values as (Date | null)[], new TimestampMillisecond());
    case "number": {
      const allInts = values.every((v) => v === null || v === undefined || Number.isInteger(v));
      const type = allInts ? new Int32() : new Float64();
      return vectorFromArray(values as (number | null)[], type);
    }
  }
}

type Category = "string" | "number" | "boolean" | "bigint" | "Date";

function categoryOf(v: unknown): Category | null {
  if (v instanceof Date) return "Date";
  const t = typeof v;
  if (t === "string" || t === "number" || t === "boolean" || t === "bigint") return t;
  return null;
}

function typeName(v: unknown): string {
  return v instanceof Date ? "Date" : typeof v;
}
