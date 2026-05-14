import { DataFrame } from "../data-frame.js";
import { InvalidInputError } from "../errors.js";
import type { SparkSession } from "../spark-session.js";

/**
 * Fluent reader for a streaming source, obtained via `spark.readStream`.
 * Configure the format, options, and schema, then terminate with `load(path?)`,
 * `table(tableName)`, or a format shortcut (`csv`, `json`, `parquet`, `orc`,
 * `text`). The returned {@link DataFrame} carries `isStreaming: true` on its
 * `Read` plan.
 *
 * @example
 * ```ts
 * spark.readStream
 *   .format("rate")
 *   .option("rowsPerSecond", "5")
 *   .load();
 *
 * spark.readStream
 *   .schema("id INT, name STRING")
 *   .json("/data/incoming/");
 * ```
 *
 * @see [Spark source: DataStreamReader.scala](https://github.com/apache/spark/blob/master/sql/api/src/main/scala/org/apache/spark/sql/streaming/DataStreamReader.scala)
 */
export class DataStreamReader {
  private readonly _session: SparkSession;
  private _format: string = "";
  private _schema: string | undefined;
  private _options: Record<string, string> = {};

  constructor(session: SparkSession) {
    this._session = session;
  }

  /**
   * Set the schema for the data source. Accepts a DDL-formatted string
   * (`"id INT, name STRING"`) or a `StructType`-like object with a `toDDL()`
   * method.
   */
  schema(schema: string | { toDDL(): string }): this {
    const ddl =
      typeof schema === "string"
        ? schema
        : typeof schema === "object" && schema !== null && typeof schema.toDDL === "function"
          ? schema.toDDL()
          : undefined;
    if (ddl === undefined) {
      throw new InvalidInputError(
        "DataStreamReader.schema() expects a DDL string (e.g. 'id INT, name STRING') or an object with toDDL().",
      );
    }
    if (ddl.trim().length === 0) {
      throw new InvalidInputError("DataStreamReader.schema() received an empty schema string.");
    }
    this._schema = ddl;
    return this;
  }

  /** Set the source format (e.g. `"rate"`, `"kafka"`, `"parquet"`). */
  format(source: string): this {
    this._format = source;
    return this;
  }

  /** Set a single source option. Booleans and numbers are stringified. */
  option(key: string, value: string | number | boolean): this {
    this._options[key] = String(value);
    return this;
  }

  /** Set multiple source options at once. */
  options(opts: Record<string, string>): this {
    Object.assign(this._options, opts);
    return this;
  }

  /**
   * Build the streaming `Read` plan. `path` is optional because sources like
   * `rate`, `kafka`, and custom connectors don't take one.
   */
  load(path?: string): DataFrame {
    if (path !== undefined && path.length === 0) {
      throw new InvalidInputError(
        "DataStreamReader.load: path must be a non-empty string. Omit the argument for path-less sources like 'rate' or 'kafka'.",
      );
    }
    return DataFrame._fromPlan(this._session, {
      type: "read",
      format: this._format,
      path: path ?? "",
      options: { ...this._options },
      isStreaming: true,
      ...(this._schema !== undefined && { schema: this._schema }),
    });
  }

  /** Read a streaming named table (catalog table or temp view). */
  table(tableName: string): DataFrame {
    if (tableName.length === 0) {
      throw new InvalidInputError("DataStreamReader.table: tableName must be non-empty.");
    }
    return DataFrame._fromPlan(this._session, {
      type: "readTable",
      tableName,
      options: { ...this._options },
      isStreaming: true,
    });
  }

  /** Shortcut for `.format("json").load(path)`. */
  json(path: string): DataFrame {
    return this.format("json").load(path);
  }

  /** Shortcut for `.format("csv").load(path)`. */
  csv(path: string): DataFrame {
    return this.format("csv").load(path);
  }

  /** Shortcut for `.format("parquet").load(path)`. */
  parquet(path: string): DataFrame {
    return this.format("parquet").load(path);
  }

  /** Shortcut for `.format("orc").load(path)`. */
  orc(path: string): DataFrame {
    return this.format("orc").load(path);
  }

  /** Shortcut for `.format("text").load(path)`. */
  text(path: string): DataFrame {
    return this.format("text").load(path);
  }
}
