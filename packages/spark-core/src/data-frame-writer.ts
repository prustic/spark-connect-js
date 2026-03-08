/**
 * DataFrameWriter
 *
 * Provides methods for writing a DataFrame to external storage systems.
 * Mirrors Spark's `DataFrameWriter` (accessed via `df.write`).
 *
 * @see Spark source: sql/core/src/main/scala/org/apache/spark/sql/DataFrameWriter.scala
 * @see Spark Connect: WriteOperation in commands.proto
 *
 * Usage:
 *   await df.write.format("parquet").mode("overwrite").save("/path/to/output");
 *   await df.write.format("parquet").saveAsTable("my_table");
 *
 * The writer sends a WriteOperation Command through the Spark Connect
 * ExecutePlan RPC. Unlike queries (which return Arrow data), write
 * operations are fire-and-forget commands with no return data.
 */

import type { DataFrame } from "./data-frame.js";

export type SaveMode = "append" | "overwrite" | "error" | "ignore";

export class DataFrameWriter {
  private readonly _df: DataFrame;
  private _format: string = "parquet";
  private _mode: SaveMode = "error";
  private _options: Record<string, string> = {};
  private _partitionBy: string[] = [];
  private _sortBy: string[] = [];

  constructor(df: DataFrame) {
    this._df = df;
  }

  /** Set the output format (e.g. "parquet", "json", "csv", "orc", "delta"). */
  format(fmt: string): this {
    this._format = fmt;
    return this;
  }

  /**
   * Set the save mode:
   *   - "append" — Append to existing data
   *   - "overwrite" — Overwrite existing data
   *   - "error" (default) — Error if data already exists
   *   - "ignore" — Silently ignore if data already exists
   */
  mode(m: SaveMode): this {
    this._mode = m;
    return this;
  }

  /** Set a single write option. */
  option(key: string, value: string): this {
    this._options[key] = value;
    return this;
  }

  /** Set multiple write options. */
  options(opts: Record<string, string>): this {
    Object.assign(this._options, opts);
    return this;
  }

  /** Partition the output by the given column names. */
  partitionBy(...columns: string[]): this {
    this._partitionBy = columns;
    return this;
  }

  /** Sort the output within each partition by the given column names. */
  sortBy(...columns: string[]): this {
    this._sortBy = columns;
    return this;
  }

  /**
   * Save the DataFrame to the given path.
   *
   * Sends a WriteOperation command through the Spark Connect RPC.
   */
  async save(path: string): Promise<void> {
    await this._df._session._executeCommand({
      type: "writeOperation",
      plan: this._df._plan,
      source: this._format,
      mode: this._mode,
      saveType: { case: "path", value: path },
      options: { ...this._options },
      partitioningColumns: this._partitionBy,
      sortColumnNames: this._sortBy,
    });
  }

  /**
   * Save the DataFrame as a named table.
   *
   * @param tableName - The fully qualified or unqualified table name
   */
  async saveAsTable(tableName: string): Promise<void> {
    await this._df._session._executeCommand({
      type: "writeOperation",
      plan: this._df._plan,
      source: this._format,
      mode: this._mode,
      saveType: {
        case: "table",
        value: { tableName, saveMethod: "saveAsTable" },
      },
      options: { ...this._options },
      partitioningColumns: this._partitionBy,
      sortColumnNames: this._sortBy,
    });
  }
}
