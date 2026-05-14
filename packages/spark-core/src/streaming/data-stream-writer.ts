import type { DataFrame } from "../data-frame.js";
import { InvalidInputError, SparkClientError } from "../errors.js";
import { StreamingQuery } from "./streaming-query.js";
import type { Trigger } from "./trigger.js";
import type { StreamingOutputMode } from "./types.js";

/**
 * Fluent writer for a streaming `DataFrame`, obtained via
 * `df.writeStream`. Configure the output format, options, output mode, and
 * trigger; terminate with `start(path?)` or `toTable(name)` to launch the
 * query on the server.
 *
 * `foreach` / `foreachBatch` are not yet supported because they require JS
 * UDF execution, which isn't shipped.
 *
 * @example
 * ```ts
 * const query = await df.writeStream
 *   .format("memory")
 *   .queryName("inflight")
 *   .outputMode("append")
 *   .trigger(Trigger.processingTime("5 seconds"))
 *   .start();
 * await query.processAllAvailable();
 * await query.stop();
 * ```
 *
 * @see [Spark source: DataStreamWriter.scala](https://github.com/apache/spark/blob/master/sql/api/src/main/scala/org/apache/spark/sql/streaming/DataStreamWriter.scala)
 */
export class DataStreamWriter {
  private readonly _df: DataFrame;
  private _format: string = "";
  private _options: Record<string, string> = {};
  private _outputMode: StreamingOutputMode | undefined;
  private _queryName: string | undefined;
  private _trigger: Trigger | undefined;
  private _partitionBy: string[] = [];

  constructor(df: DataFrame) {
    this._df = df;
  }

  /** Set the sink format (e.g. `"memory"`, `"console"`, `"parquet"`, `"kafka"`). */
  format(source: string): this {
    this._format = source;
    return this;
  }

  /** Set a single sink option. Booleans and numbers are stringified. */
  option(key: string, value: string | number | boolean): this {
    this._options[key] = String(value);
    return this;
  }

  /** Set multiple sink options at once. */
  options(opts: Record<string, string>): this {
    Object.assign(this._options, opts);
    return this;
  }

  /**
   * How the streaming query writes results into the sink.
   *
   * Not every sink supports every mode; see Spark's docs for the matrix.
   */
  outputMode(mode: StreamingOutputMode): this {
    this._outputMode = mode;
    return this;
  }

  /** Name the query so it can be looked up via `spark.streams.get(name)`. */
  queryName(name: string): this {
    if (name.length === 0) {
      throw new InvalidInputError("DataStreamWriter.queryName: name must be non-empty.");
    }
    this._queryName = name;
    return this;
  }

  /** Configure the trigger policy. See {@link Trigger} for the variants. */
  trigger(t: Trigger): this {
    this._trigger = t;
    return this;
  }

  /** Partition the sink output by the given column names. */
  partitionBy(...columns: string[]): this {
    this._partitionBy = columns;
    return this;
  }

  /**
   * Start the streaming query against the given path (file sinks) or no
   * path (for `memory`, `console`, `kafka`, etc.). Returns a handle for
   * inspecting and controlling the running query.
   */
  async start(path?: string): Promise<StreamingQuery> {
    if (path !== undefined && path.length === 0) {
      throw new InvalidInputError(
        "DataStreamWriter.start: path must be a non-empty string. Omit the argument for path-less sinks like 'memory', 'console', or 'kafka'.",
      );
    }
    return this._start(path === undefined ? undefined : { kind: "path", value: path });
  }

  /**
   * Start the streaming query and write output to the named table. The table
   * is created if it doesn't exist (sink semantics depend on format).
   */
  async toTable(tableName: string): Promise<StreamingQuery> {
    if (tableName.length === 0) {
      throw new InvalidInputError("DataStreamWriter.toTable: tableName must be non-empty.");
    }
    return this._start({ kind: "table", value: tableName });
  }

  private async _start(
    sink: { kind: "path"; value: string } | { kind: "table"; value: string } | undefined,
  ): Promise<StreamingQuery> {
    if (this._format.length === 0 && sink === undefined) {
      throw new InvalidInputError(
        "DataStreamWriter requires either format(...) or a sink destination via start(path) / toTable(name).",
      );
    }
    const responses = await this._df._session._executeCommandResponses({
      type: "writeStreamOperationStart",
      plan: this._df._plan,
      format: this._format,
      options: { ...this._options },
      partitioningColumnNames: this._partitionBy,
      ...(this._trigger !== undefined && { trigger: this._trigger }),
      ...(this._outputMode !== undefined && { outputMode: this._outputMode }),
      ...(this._queryName !== undefined && { queryName: this._queryName }),
      ...(sink !== undefined && { sink }),
    });
    const result = responses.find((r) => r["type"] === "writeStreamOperationStartResult") as
      | {
          type: "writeStreamOperationStartResult";
          queryId?: { id: string; runId: string };
          name: string;
          queryStartedEventJson?: string;
        }
      | undefined;
    if (result === undefined || result.queryId === undefined) {
      throw new SparkClientError(
        "Spark Connect server did not return a writeStreamOperationStartResult with a queryId.",
      );
    }
    return new StreamingQuery(
      this._df._session,
      result.queryId.id,
      result.queryId.runId,
      result.name.length === 0 ? undefined : result.name,
    );
  }
}
