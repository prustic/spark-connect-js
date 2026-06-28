import type { DataFrame } from "./data-frame.js";
import { InvalidInputError } from "./errors.js";

/**
 * Save mode for {@link DataFrameWriter.mode}.
 *
 * - `append` - add new data to existing output.
 * - `overwrite` - replace existing output.
 * - `error` *(default)* - fail if the target already exists.
 * - `ignore` - silently skip the write if the target already exists.
 */
export type SaveMode = "append" | "overwrite" | "error" | "ignore";

/**
 * Writes the contents of a {@link DataFrame} to an external storage system
 * or a catalog table.
 *
 * Obtained via {@link DataFrame.write}. Mirrors Spark's path-oriented
 * `DataFrameWriter` (V1). For catalog-aware, atomic table writes, use
 * {@link DataFrameWriterV2} via `df.writeTo(table)`.
 *
 * @example
 * ```ts
 * await df.write.format("parquet").mode("overwrite").save("/path/to/output");
 * await df.write.mode("append").saveAsTable("analytics.events");
 * ```
 *
 * @see [Spark source: DataFrameWriter.scala](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/DataFrameWriter.scala)
 */
export class DataFrameWriter {
  private readonly _df: DataFrame;
  private _format: string = "parquet";
  private _mode: SaveMode = "error";
  private _options: Record<string, string> = {};
  private _partitionBy: string[] = [];
  private _sortBy: string[] = [];
  private _bucketBy: { numBuckets: number; columnNames: string[] } | undefined;

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
   *   - "append" - Append to existing data
   *   - "overwrite" - Overwrite existing data
   *   - "error" (default) - Error if data already exists
   *   - "ignore" - Silently ignore if data already exists
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
   * Bucket the output by the given columns with a fixed number of buckets.
   * Only applicable when saving to a table (saveAsTable).
   */
  bucketBy(numBuckets: number, col: string, ...cols: string[]): this {
    if (!Number.isInteger(numBuckets) || numBuckets <= 0) {
      throw new InvalidInputError(
        `bucketBy requires numBuckets to be a positive integer, but got: ${numBuckets}`,
      );
    }
    this._bucketBy = { numBuckets, columnNames: [col, ...cols] };
    return this;
  }

  /** Build the common command payload fields. */
  private _commandFields() {
    return {
      type: "writeOperation" as const,
      plan: this._df._plan,
      source: this._format,
      mode: this._mode,
      options: { ...this._options },
      partitioningColumns: this._partitionBy,
      sortColumnNames: this._sortBy,
      ...(this._bucketBy != null && { bucketBy: this._bucketBy }),
    };
  }

  /**
   * Save the DataFrame to the given path.
   *
   * Sends a WriteOperation command through the Spark Connect RPC.
   */
  async save(path: string): Promise<void> {
    const { bucketBy: _, ...fields } = this._commandFields();
    await this._df._session._executeCommand({
      ...fields,
      saveType: { case: "path", value: path },
    });
  }

  /**
   * Save the DataFrame as a named table.
   *
   * @param tableName - The fully qualified or unqualified table name
   */
  async saveAsTable(tableName: string): Promise<void> {
    await this._df._session._executeCommand({
      ...this._commandFields(),
      saveType: {
        case: "table",
        value: { tableName, saveMethod: "saveAsTable" },
      },
    });
  }

  /**
   * Insert the DataFrame's contents into the given table.
   * Unlike saveAsTable, insertInto does not create the table;
   * it must already exist.
   */
  async insertInto(tableName: string): Promise<void> {
    const {
      bucketBy: _b,
      partitioningColumns: _p,
      sortColumnNames: _s,
      ...fields
    } = this._commandFields();
    await this._df._session._executeCommand({
      ...fields,
      saveType: {
        case: "table",
        value: { tableName, saveMethod: "insertInto" },
      },
    });
  }

  /** Shortcut for .format("json").save(path). */
  async json(path: string): Promise<void> {
    await this.format("json").save(path);
  }

  /** Shortcut for .format("csv").save(path). */
  async csv(path: string): Promise<void> {
    await this.format("csv").save(path);
  }

  /** Shortcut for .format("parquet").save(path). */
  async parquet(path: string): Promise<void> {
    await this.format("parquet").save(path);
  }

  /** Shortcut for .format("orc").save(path). */
  async orc(path: string): Promise<void> {
    await this.format("orc").save(path);
  }

  /** Shortcut for .format("text").save(path). */
  async text(path: string): Promise<void> {
    await this.format("text").save(path);
  }
}
