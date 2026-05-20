import { DataFrame } from "./data-frame.js";
import { Catalog } from "./catalog.js";
import { UDFRegistration } from "./udf-registration.js";
import { RuntimeConfig } from "./runtime-config.js";
import { InvalidConfigError, InvalidInputError, UnsupportedOperationError } from "./errors.js";
import type { LogicalPlan } from "./plan/logical-plan.js";
import type { Row } from "./types/row.js";
import { DataStreamReader } from "./streaming/data-stream-reader.js";
import { StreamingQueryManager } from "./streaming/streaming-query-manager.js";

// crypto.randomUUID() is available globally in Node 19+, Deno, and all modern
// browsers, but TypeScript's ES2023 lib doesn't include it since it's a Web
// Crypto API, not an ECMAScript builtin.  We declare the minimal shape here
// to keep spark-core free of @types/node or DOM lib dependencies.
declare const crypto: { randomUUID(): string };

/**
 * Per-call options forwarded by `SparkSession` to the transport.
 * Today carries tags only; future fields (operation_id override, deadline,
 * cancel signal) slot in here without further interface churn.
 */
export interface ExecuteOptions {
  /** Tags attached to this operation; surface for {@link Transport.interrupt}. */
  tags?: readonly string[];
}

/**
 * The network seam between {@link SparkSession} and a Spark Connect server.
 * Runtime adapters (e.g. `@spark-connect-js/node`) implement this interface to
 * provide actual gRPC I/O, while `@spark-connect-js/core` stays platform-agnostic.
 *
 * Most methods are optional: a minimal in-memory `Transport` for testing only
 * needs `executePlan`. A full runtime adapter implements them all.
 */
export interface Transport {
  /** Execute a plan and stream raw Arrow IPC buffers back to the client. */
  executePlan(
    sessionId: string,
    plan: LogicalPlan,
    options?: ExecuteOptions,
  ): AsyncIterable<Uint8Array>;

  /** Execute a command (write, createView, etc.) that returns no Arrow data. */
  executeCommand?(
    sessionId: string,
    command: Record<string, unknown>,
    options?: ExecuteOptions,
  ): Promise<void>;

  /**
   * Execute a command and collect any non-Arrow result payloads from the
   * `ExecutePlanResponse` stream. Used by streaming commands (e.g.
   * `WriteStreamOperationStart`, `StreamingQueryCommand`) where the server
   * returns a structured result.
   *
   * Each entry in the returned array is a JSON-like shape carrying a `type`
   * discriminator (e.g. `"writeStreamOperationStartResult"`,
   * `"streamingQueryCommandResult"`) plus the decoded payload fields.
   */
  executeCommandResponses?(
    sessionId: string,
    command: Record<string, unknown>,
    options?: ExecuteOptions,
  ): Promise<Record<string, unknown>[]>;

  /** Send an AnalyzePlan request (schema, explain, etc.) and return the response. */
  analyzePlan?(
    sessionId: string,
    request: Record<string, unknown>,
  ): Promise<Record<string, unknown>>;

  /** Get/set/unset runtime configuration via the Config RPC. */
  config?(sessionId: string, operation: Record<string, unknown>): Promise<Record<string, unknown>>;

  /**
   * Cancel running operations. Returns the operation IDs the server reports
   * as interrupted. The `request` shape is one of:
   *   `{ type: "all" }`
   *   `{ type: "tag", tag: string }`
   *   `{ type: "operationId", operationId: string }`
   */
  interrupt?(sessionId: string, request: Record<string, unknown>): Promise<string[]>;

  /** Release server-side session state. Called from `SparkSession.stop()`. */
  releaseSession?(sessionId: string): Promise<void>;

  /** Close the underlying connection. Called from `SparkSession.stop()`. */
  close?(): void;
}

/**
 * Decodes Arrow IPC byte buffers into plain JavaScript {@link Row} objects.
 * Injected by the runtime adapter; the core package has no Arrow dependency.
 */
export type ArrowDecoderFn = (chunks: Uint8Array[]) => Promise<Row[]>;

/**
 * Construction parameters for a {@link SparkSession}. Most users build a session
 * via the runtime adapter's builder (e.g. `SparkSessionBuilder` from
 * `@spark-connect-js/node`); this config is what the builder hands to
 * `SparkSession._create` internally.
 */
export interface SparkSessionConfig {
  /** Spark Connect endpoint, e.g. `"sc://localhost:15002"`. */
  remote: string;

  /** Transport implementation injected by the runtime adapter. */
  transport: Transport;

  /**
   * Arrow IPC to `Row[]` decoder injected by the runtime adapter. If not
   * provided, `collect()` and similar actions will throw at runtime.
   */
  arrowDecoder?: ArrowDecoderFn;

  /** Optional session ID override for reattaching to an existing server-side session. */
  sessionId?: string;
}

/**
 * The client handle for a Spark Connect session.
 *
 * Holds the transport, session identifier, and runtime-adapter hooks (for
 * example the Arrow decoder). All DataFrame operations are scheduled against
 * a `SparkSession`; most applications create one session at startup and
 * reuse it.
 *
 * Construct a session with the runtime-specific builder, for example
 * `SparkSessionBuilder` from `@spark-connect-js/node`.
 *
 * @example
 * ```ts
 * import { SparkSessionBuilder } from "@spark-connect-js/node";
 *
 * const spark = await SparkSessionBuilder
 *   .remote("sc://localhost:15002")
 *   .build();
 *
 * const df = await spark.sql("SELECT 1 AS n");
 * console.log(await df.collect());
 * ```
 *
 * @see [Spark source: SparkSession.scala](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/SparkSession.scala)
 */
export class SparkSession {
  readonly sessionId: string;
  private readonly transport: Transport;
  private readonly remote: string;
  private readonly _tagSet: Set<string> = new Set();
  /** @internal */
  readonly _arrowDecoder: ArrowDecoderFn | undefined;

  /** @internal Called by SparkSessionBuilder to construct the session. */
  static _create(config: SparkSessionConfig): SparkSession {
    return new SparkSession(config);
  }

  private constructor(config: SparkSessionConfig) {
    this.sessionId = config.sessionId ?? crypto.randomUUID();
    this.transport = config.transport;
    this.remote = config.remote;
    this._arrowDecoder = config.arrowDecoder;
  }

  // Builder

  static builder(): SparkSessionBuilder {
    return new SparkSessionBuilder();
  }

  // DataFrame entry points

  /** Access the session catalog for inspecting databases, tables, and columns. */
  readonly catalog: Catalog = new Catalog(this);

  /** Register Java UDFs and UDAFs as SQL functions. */
  readonly udf: UDFRegistration = new UDFRegistration(this);

  /** Read and write Spark configuration entries on the connected server. */
  readonly conf: RuntimeConfig = new RuntimeConfig(this);

  /** Returns a DataFrameReader for building Read plans. */
  get read(): DataFrameReader {
    return new DataFrameReader(this);
  }

  /**
   * Returns a {@link DataStreamReader} for building streaming Read plans.
   * The resulting {@link DataFrame} carries `isStreaming: true` and can only
   * be consumed via `df.writeStream`.
   */
  get readStream(): DataStreamReader {
    return new DataStreamReader(this);
  }

  /** Manage the streaming queries running on this session (`active`, `get`, …). */
  get streams(): StreamingQueryManager {
    return new StreamingQueryManager(this);
  }

  /** Execute a SQL query. */
  sql(query: string): DataFrame {
    return DataFrame._fromPlan(this, {
      type: "sql",
      query,
    });
  }

  /**
   * Create a DataFrame with a single `id` column containing a sequence of
   * integers from `start` (inclusive) to `end` (exclusive), incrementing by `step`.
   *
   * Mirrors PySpark's `spark.range(start, end, step, numPartitions)`.
   *
   * @example
   *   spark.range(10)            // 0, 1, 2, ..., 9
   *   spark.range(1, 10)         // 1, 2, 3, ..., 9
   *   spark.range(0, 10, 2)      // 0, 2, 4, 6, 8
   */
  range(startOrEnd: number, end?: number, step = 1, numPartitions?: number): DataFrame {
    const start = end === undefined ? 0 : startOrEnd;
    const actualEnd = end === undefined ? startOrEnd : end;
    return DataFrame._fromPlan(this, {
      type: "range",
      start,
      end: actualEnd,
      step,
      numPartitions,
    });
  }

  /**
   * Create a DataFrame from Arrow IPC data.
   *
   * @param data  - Arrow IPC streaming format bytes
   * @param schema - Optional DDL-formatted schema string (e.g. "id INT, name STRING")
   *
   * @example
   *   const arrowData = ArrowEncoder.encode(rows, schema);
   *   const df = spark.createDataFrame(arrowData);
   */
  createDataFrame(data: Uint8Array, schema?: string): DataFrame {
    return DataFrame._fromPlan(this, {
      type: "localRelation",
      data,
      schema,
    });
  }

  /** @internal Used by DataFrame to send plans via the injected transport */
  _executePlan(plan: LogicalPlan): AsyncIterable<Uint8Array> {
    return this.transport.executePlan(this.sessionId, plan, this._executeOptions());
  }

  /** @internal Used by DataFrameWriter to send commands via the injected transport */
  async _executeCommand(command: Record<string, unknown>): Promise<void> {
    if (!this.transport.executeCommand) {
      throw new UnsupportedOperationError(
        `Transport ${this.transport.constructor.name} does not support command execution. ` +
          "Use a full Transport implementation (e.g. GrpcTransport) that supports all operations.",
      );
    }
    await this.transport.executeCommand(this.sessionId, command, this._executeOptions());
  }

  /**
   * @internal Used by streaming classes (DataStreamWriter, StreamingQuery) to
   * issue commands that return structured non-Arrow result payloads.
   */
  async _executeCommandResponses(
    command: Record<string, unknown>,
  ): Promise<Record<string, unknown>[]> {
    if (!this.transport.executeCommandResponses) {
      throw new UnsupportedOperationError(
        `Transport ${this.transport.constructor.name} does not support executeCommandResponses. ` +
          "Use a full Transport implementation (e.g. GrpcTransport) that supports streaming commands.",
      );
    }
    return this.transport.executeCommandResponses(this.sessionId, command, this._executeOptions());
  }

  /** @internal Snapshot of per-call options at the time of dispatch. */
  private _executeOptions(): ExecuteOptions {
    return this._tagSet.size === 0 ? {} : { tags: Array.from(this._tagSet) };
  }

  /** @internal Used by DataFrame.schema()/explain() via the injected transport */
  async _analyzePlan(request: Record<string, unknown>): Promise<Record<string, unknown>> {
    if (!this.transport.analyzePlan) {
      throw new UnsupportedOperationError(
        `Transport ${this.transport.constructor.name} does not support analyzePlan. ` +
          "Use a full Transport implementation (e.g. GrpcTransport) that supports all operations.",
      );
    }
    return this.transport.analyzePlan(this.sessionId, request);
  }

  /** @internal Used by RuntimeConfig via the injected transport */
  async _config(operation: Record<string, unknown>): Promise<Record<string, unknown>> {
    if (!this.transport.config) {
      throw new UnsupportedOperationError(
        `Transport ${this.transport.constructor.name} does not support config. ` +
          "Use a full Transport implementation (e.g. GrpcTransport) that supports all operations.",
      );
    }
    return this.transport.config(this.sessionId, operation);
  }

  // Operation tags
  // @see Spark source: sql/api/src/main/scala/org/apache/spark/sql/SparkSession.scala (addTag, removeTag)
  // PySpark: pyspark.sql.SparkSession.addTag

  /**
   * Tag every subsequent operation on this session with `tag`. Tags are
   * carried on `ExecutePlanRequest.tags` and let you cancel a group of
   * operations with {@link interruptTag}.
   *
   * @throws InvalidInputError if the tag contains `,` or is empty.
   */
  addTag(tag: string): void {
    validateTag(tag);
    this._tagSet.add(tag);
  }

  /** Remove a previously added tag. No-op if the tag wasn't set. */
  removeTag(tag: string): void {
    this._tagSet.delete(tag);
  }

  /** Return a snapshot of the currently active tags. */
  getTags(): string[] {
    return Array.from(this._tagSet);
  }

  /** Drop all active tags. */
  clearTags(): void {
    this._tagSet.clear();
  }

  // Interrupt
  // @see Spark source: sql/api/src/main/scala/org/apache/spark/sql/SparkSession.scala (interruptAll, interruptTag, interruptOperation)

  /** Interrupt every running operation in this session. */
  async interruptAll(): Promise<string[]> {
    return this._interrupt({ type: "all" });
  }

  /** Interrupt every running operation tagged with `tag`. */
  async interruptTag(tag: string): Promise<string[]> {
    validateTag(tag);
    return this._interrupt({ type: "tag", tag });
  }

  /** Interrupt a single running operation by its operation ID. */
  async interruptOperation(operationId: string): Promise<string[]> {
    if (operationId.length === 0) {
      throw new InvalidInputError("Spark Connect operation ID must be non-empty.");
    }
    return this._interrupt({ type: "operationId", operationId });
  }

  private async _interrupt(request: Record<string, unknown>): Promise<string[]> {
    if (!this.transport.interrupt) {
      throw new UnsupportedOperationError(
        `Transport ${this.transport.constructor.name} does not support interrupt. ` +
          "Use a full Transport implementation (e.g. GrpcTransport) that supports all operations.",
      );
    }
    return this.transport.interrupt(this.sessionId, request);
  }

  /**
   * Return the Apache Spark version reported by the connected server.
   *
   * One AnalyzePlan RPC. Result is not cached; call once and store if you
   * need it repeatedly.
   *
   * Mirrors `pyspark.sql.SparkSession.version`.
   */
  async version(): Promise<string> {
    const result = await this._analyzePlan({ type: "sparkVersion" });
    return result["version"] as string;
  }

  /**
   * Stop the session: releases server-side state and closes the transport.
   */
  async stop(): Promise<void> {
    if (this.transport.releaseSession) {
      await this.transport.releaseSession(this.sessionId);
    }
    if (this.transport.close) {
      this.transport.close();
    }
  }
}

/**
 * Fluent builder for {@link SparkSession}. Returned by `SparkSession.builder()`
 * in `@spark-connect-js/core`; runtime adapters (e.g. `@spark-connect-js/node`)
 * usually subclass it to add their own transport defaults.
 *
 * @example
 * ```ts
 * const spark = SparkSession.builder()
 *   .remote("sc://localhost:15002")
 *   .transport(myTransport)
 *   .arrowDecoder(myDecoder)
 *   .getOrCreate();
 * ```
 */
export class SparkSessionBuilder {
  private config: Partial<SparkSessionConfig> = {};

  /** Set the Spark Connect endpoint URL (`sc://host:port`). Required. */
  remote(connectionString: string): this {
    this.config.remote = connectionString;
    return this;
  }

  /** Inject a {@link Transport} implementation. Required. */
  transport(t: Transport): this {
    this.config.transport = t;
    return this;
  }

  /** Inject an Arrow IPC decoder. Required for `collect()` and similar actions. */
  arrowDecoder(decoder: ArrowDecoderFn): this {
    this.config.arrowDecoder = decoder;
    return this;
  }

  /**
   * Reuse an existing server-side session by ID. Must be a canonical UUID
   * string in the form `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`. If unset, a
   * fresh UUID is generated on the client.
   */
  sessionId(id: string): this {
    validateUuid(id, "sessionId");
    this.config.sessionId = id;
    return this;
  }

  /**
   * Construct the session. In Spark Connect, "getOrCreate" is a server-side
   * concept: the server may return an existing session if the session ID
   * matches. On the client we just instantiate the handle.
   */
  getOrCreate(): SparkSession {
    if (!this.config.remote) {
      throw new InvalidConfigError(
        "SparkSession requires a remote URL. Call .remote('sc://host:port') on the builder.",
      );
    }
    if (!this.config.transport) {
      throw new InvalidConfigError(
        "SparkSession requires a Transport implementation. " +
          "Use a runtime adapter that provides a Transport (e.g. GrpcTransport) or supply a custom one.",
      );
    }
    return SparkSession._create(this.config as SparkSessionConfig);
  }
}

// Validation helpers shared between session and builder

/**
 * Validate a Spark Connect operation tag. The proto comment requires tags
 * to be non-empty and free of `,`; the server splits on commas internally.
 *
 * @see ExecutePlanRequest.tags in spark/connect/base.proto
 */
function validateTag(tag: string): void {
  if (tag.length === 0) {
    throw new InvalidInputError("Spark Connect operation tag must be non-empty.");
  }
  if (tag.includes(",")) {
    throw new InvalidInputError(`Spark Connect operation tag must not contain ',', got "${tag}".`);
  }
}

/**
 * Canonical UUID format `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` (36 chars,
 * hex with `-` at fixed positions). The reference clients all validate via
 * a stdlib parser (PySpark `uuid.UUID(s)`, Scala `UUID.fromString(s)`); Node
 * has no stdlib UUID parser and even the npm `uuid` library uses a regex
 * internally. With zero runtime deps in core, this is the canonical form.
 */
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

function validateUuid(value: string, field: string): void {
  if (!UUID_RE.test(value)) {
    throw new InvalidInputError(
      `Spark Connect ${field} must be a valid UUID string, got "${value}".`,
    );
  }
}

/**
 * Fluent reader for loading data into a {@link DataFrame}. Returned by
 * `spark.read`; configure the format, options, and schema, then terminate
 * with a format shortcut (`csv`, `json`, `parquet`, `orc`, `text`) or `.load()`.
 *
 * @example
 * ```ts
 * spark.read.parquet("s3://bucket/events/");
 *
 * spark.read
 *   .schema("id INT, name STRING")
 *   .option("header", "true")
 *   .csv("/data/people.csv");
 * ```
 *
 * @see [Spark source: DataFrameReader.scala](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/DataFrameReader.scala)
 */
export class DataFrameReader {
  private session: SparkSession;
  private _format: string = "parquet";
  private _schema: string | undefined;
  private _options: Record<string, string> = {};

  constructor(session: SparkSession) {
    this.session = session;
  }

  /**
   * Set the schema for the data source.
   * Accepts a DDL-formatted string (e.g. "name STRING, age INT")
   * or a StructType with a toDDL() method.
   */
  schema(schema: string | { toDDL(): string }): this {
    let ddl: string;
    if (typeof schema === "string") {
      ddl = schema;
    } else if (
      typeof schema === "object" &&
      schema !== null &&
      typeof schema.toDDL === "function"
    ) {
      ddl = schema.toDDL();
    } else {
      throw new InvalidInputError(
        "schema must be a DDL string (e.g. 'id INT, name STRING') or an object with a toDDL() method",
      );
    }
    if (!ddl.trim()) {
      throw new InvalidInputError(
        "DataFrameReader.schema() received an empty schema string. " +
          "Provide a DDL string like 'id INT, name STRING'.",
      );
    }
    this._schema = ddl;
    return this;
  }

  format(fmt: string): this {
    this._format = fmt;
    return this;
  }

  option(key: string, value: string): this {
    this._options[key] = value;
    return this;
  }

  options(opts: Record<string, string>): this {
    Object.assign(this._options, opts);
    return this;
  }

  /**
   * Trigger a Read plan node.  The resulting DataFrame is lazy; no data is
   * fetched until .collect() or an action is called.
   *
   * This maps to Spark Connect's `Relation.Read` with `ReadType.DataSource`:
   *   { format: "parquet", paths: [...], options: {...} }
   */
  load(path: string): DataFrame {
    return DataFrame._fromPlan(this.session, {
      type: "read",
      format: this._format,
      path,
      options: { ...this._options },
      ...(this._schema != null && { schema: this._schema }),
    });
  }

  /** Read a named table (catalog table or temp view). */
  table(tableName: string): DataFrame {
    return DataFrame._fromPlan(this.session, {
      type: "readTable",
      tableName,
      options: { ...this._options },
    });
  }

  /** Shortcut for .format("json").load(path). */
  json(path: string): DataFrame {
    return this.format("json").load(path);
  }

  /** Shortcut for .format("csv").load(path). */
  csv(path: string): DataFrame {
    return this.format("csv").load(path);
  }

  /** Shortcut for .format("parquet").load(path). */
  parquet(path: string): DataFrame {
    return this.format("parquet").load(path);
  }

  /** Shortcut for .format("orc").load(path). */
  orc(path: string): DataFrame {
    return this.format("orc").load(path);
  }

  /** Shortcut for .format("text").load(path). */
  text(path: string): DataFrame {
    return this.format("text").load(path);
  }
}
