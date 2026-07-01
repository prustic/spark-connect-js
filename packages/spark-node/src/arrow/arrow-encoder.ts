/**
 * Encode plain JS `Row[]` values into Arrow IPC streaming bytes for
 * `SparkSession.createDataFrame([...])`.
 *
 * Strings are always emitted as materialized `Utf8` vectors, never
 * `Dictionary<Int32, Utf8>`. apache-arrow's `tableFromArrays` defaults to
 * dictionary encoding for string columns, but Spark's LocalRelation reader
 * does not apply the dictionary batch, so the server sees bare integer
 * indices instead of the string values. Bypassing dictionary encoding at
 * build time is the fix.
 */

import { InvalidInputError, type Row } from "@spark-connect-js/core";
import {
  Table,
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

export class ArrowEncoder {
  static encode(rows: Row[]): Uint8Array {
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

    const columns: Record<string, Vector> = {};
    for (const name of fieldNames) {
      const values = rows.map((r) => r[name]);
      columns[name] = buildColumnVector(name, values);
    }

    return tableToIPC(new Table(columns), "stream");
  }
}

function buildColumnVector(name: string, values: unknown[]): Vector {
  const sample = values.find((v) => v !== null && v !== undefined);

  // All-null column: default to nullable Utf8. Rare in practice.
  if (sample === undefined) {
    return vectorFromArray(values as (string | null)[], new Utf8());
  }

  if (typeof sample === "string") {
    return vectorFromArray(values as (string | null)[], new Utf8());
  }
  if (typeof sample === "boolean") {
    return vectorFromArray(values as (boolean | null)[], new Bool());
  }
  if (typeof sample === "bigint") {
    return vectorFromArray(values as (bigint | null)[], new Int64());
  }
  if (sample instanceof Date) {
    return vectorFromArray(values as (Date | null)[], new TimestampMillisecond());
  }
  if (typeof sample === "number") {
    const allInts = values.every((v) => v === null || v === undefined || Number.isInteger(v));
    if (allInts) {
      return vectorFromArray(values as (number | null)[], new Int32());
    }
    return vectorFromArray(values as (number | null)[], new Float64());
  }

  throw new InvalidInputError(
    `createDataFrame([...]): unsupported value type "${typeof sample}" in column "${name}". ` +
      "Supported types: string, number, boolean, bigint, Date, null. " +
      "For richer types, build the Arrow IPC bytes yourself and pass a Uint8Array.",
  );
}
