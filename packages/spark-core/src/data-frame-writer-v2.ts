import type { DataFrame } from "./data-frame.js";
import type { Column } from "./column.js";
import { toColumnCondition } from "./column.js";
import { InvalidInputError } from "./errors.js";
import type { Expression } from "./plan/logical-plan.js";

/**
 * Writes a {@link DataFrame} to a catalog table using the DataSource V2 API.
 *
 * Obtained via `df.writeTo(tableName)`. Unlike the path-oriented
 * {@link DataFrameWriter}, V2 is catalog-aware and supports atomic table
 * operations (`create`, `replace`, `createOrReplace`) and partition-level
 * writes (`append`, `overwrite`, `overwritePartitions`).
 *
 * @example Create a new Iceberg table
 * ```ts
 * await df.writeTo("analytics.events").using("iceberg").create();
 * ```
 *
 * @example Append to an existing table
 * ```ts
 * await df.writeTo("analytics.events").append();
 * ```
 *
 * @example Overwrite a specific partition
 * ```ts
 * await df.writeTo("analytics.events").overwrite(col("day").eq(lit("2026-04-21")));
 * ```
 *
 * @see [Spark source: DataFrameWriterV2.scala](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/DataFrameWriterV2.scala)
 */
export class DataFrameWriterV2 {
  private readonly _df: DataFrame;
  private readonly _tableName: string;
  private _provider: string | undefined;
  private _options: Record<string, string> = {};
  private _tableProperties: Record<string, string> = {};
  private _partitioningColumns: Expression[] = [];
  private _clusteringColumns: string[] = [];

  constructor(df: DataFrame, tableName: string) {
    this._df = df;
    this._tableName = tableName;
  }

  /** Specify the data source provider (e.g. "iceberg", "delta", "parquet"). */
  using(provider: string): this {
    this._provider = provider;
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

  /** Set a single table property. */
  tableProperty(key: string, value: string): this {
    this._tableProperties[key] = value;
    return this;
  }

  /**
   * Partition the output table by the given Column expressions.
   * Applies to `create`, `replace`, and `createOrReplace`.
   */
  partitionedBy(...columns: Column[]): this {
    this._partitioningColumns = columns.map((c) => c._expr);
    return this;
  }

  /**
   * Cluster the output table by the given column names.
   * Applies to `create`, `replace`, and `createOrReplace`.
   */
  clusterBy(...columns: string[]): this {
    this._clusteringColumns = [...columns];
    return this;
  }

  /** Build the common command payload fields. */
  private _commandFields() {
    return {
      type: "writeOperationV2" as const,
      plan: this._df._plan,
      tableName: this._tableName,
      provider: this._provider,
      options: { ...this._options },
      tableProperties: { ...this._tableProperties },
      partitioningColumns: [...this._partitioningColumns],
      clusteringColumns: [...this._clusteringColumns],
    };
  }

  /** Create a new table with the DataFrame's data and schema. */
  async create(): Promise<void> {
    await this._df._session._executeCommand({
      ...this._commandFields(),
      mode: "create",
    });
  }

  /** Replace an existing table with the DataFrame's data and schema. */
  async replace(): Promise<void> {
    await this._df._session._executeCommand({
      ...this._commandFields(),
      mode: "replace",
    });
  }

  /** Create a table or replace it if it already exists. */
  async createOrReplace(): Promise<void> {
    await this._df._session._executeCommand({
      ...this._commandFields(),
      mode: "createOrReplace",
    });
  }

  /** Append the DataFrame's data to the table. */
  async append(): Promise<void> {
    await this._df._session._executeCommand({
      ...this._commandFields(),
      mode: "append",
    });
  }

  /**
   * Overwrite rows matching the given condition.
   * Rows in the table that match the condition are replaced; others are kept.
   */
  async overwrite(condition: Column): Promise<void> {
    const cond = toColumnCondition(condition, "overwrite()");
    if (cond === undefined) {
      throw new InvalidInputError("overwrite() requires a condition.");
    }

    await this._df._session._executeCommand({
      ...this._commandFields(),
      mode: "overwrite",
      overwriteCondition: cond._expr,
    });
  }

  /**
   * Dynamically overwrite partitions: replaces only the partitions
   * present in the DataFrame, leaving other partitions untouched.
   */
  async overwritePartitions(): Promise<void> {
    await this._df._session._executeCommand({
      ...this._commandFields(),
      mode: "overwritePartitions",
    });
  }
}
