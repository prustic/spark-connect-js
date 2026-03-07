/**
 * ─── SparkSession ───────────────────────────────────────────────────────────
 *
 * The SparkSession is the single entry point for all DataFrame operations,
 * mirroring `org.apache.spark.sql.SparkSession` in the JVM.
 *
 * @see Spark source: sql/core/src/main/scala/org/apache/spark/sql/SparkSession.scala
 * @see Spark Connect: connector/connect/common/src/main/protobuf/spark/connect/base.proto
 * @see Spark Connect service: connector/connect/server/src/main/scala/org/apache/spark/sql/connect/service/SparkConnectService.scala
 *
 * In the Scala world, SparkSession owns:
 *   • The SparkContext (cluster connection, task scheduler, DAG scheduler)
 *   • The SQLContext / Catalog (schema metadata, temp views)
 *   • The SessionState (Catalyst analyzer, optimizer, physical planner)
 *
 * In Spark Connect, we do NOT own any of that.  The JVM-side server owns the
 * real session state.  Our SparkSession is a **thin client handle** that:
 *   1. Holds a session ID (UUID) to correlate requests on the server.
 *   2. Provides the builder-pattern entry for creating DataFrames.
 *   3. Delegates actual plan execution to a `Transport` injected by the
 *      runtime adapter (e.g. @spark-js/node injects a gRPC transport).
 *
 * The Transport interface is intentionally defined here in core so that this
 * package has no dependency on Node, Deno, or browser APIs.
 */

import { DataFrame } from "./data-frame.js";
import { Catalog } from "./catalog.js";
import type { LogicalPlan } from "./plan/logical-plan.js";
import type { Row } from "./types/row.js";

// crypto.randomUUID() is available globally in Node 19+, Deno, and all modern
// browsers — but TypeScript's ES2023 lib doesn't include it since it's a Web
// Crypto API, not an ECMAScript builtin.  We declare the minimal shape here
// to keep spark-core free of @types/node or DOM lib dependencies.
declare const crypto: { randomUUID(): string };

// ─── Transport Abstraction ──────────────────────────────────────────────────
// Runtime adapters implement this to provide actual network I/O.
// spark-core never imports gRPC, fetch, or any I/O primitive directly.

export interface Transport {
  /**
   * Execute a logical plan on the Spark Connect server and return raw Arrow
   * IPC buffers.  The caller (DataFrame.collect) will decode the Arrow data
   * into JS rows.
   *
   * The Uint8Array is used instead of Node Buffer to keep this interface
   * platform-agnostic.  Node's Buffer extends Uint8Array so it satisfies
   * this type automatically.
   */
  executePlan(sessionId: string, plan: LogicalPlan): AsyncIterable<Uint8Array>;

  /**
   * Execute a command (write, createView, etc.) on the server.
   * Commands don't return Arrow data — they run side effects.
   */
  executeCommand?(sessionId: string, command: Record<string, unknown>): Promise<void>;

  /**
   * Send an analyze plan request (schema, explain, etc.) and return
   * the raw response as a plain object.
   */
  analyzePlan?(
    sessionId: string,
    request: Record<string, unknown>,
  ): Promise<Record<string, unknown>>;

  /**
   * Release the server-side session, freeing temp views, cached data, etc.
   * Optional — transports that don't support it simply skip the RPC.
   */
  releaseSession?(sessionId: string): Promise<void>;

  /**
   * Close the underlying connection (e.g. gRPC channel).
   * Optional — some transports may not have persistent connections.
   */
  close?(): void;
}

// ─── Configuration ──────────────────────────────────────────────────────────

/**
 * Decodes concatenated Arrow IPC bytes into Row objects.
 * Injected by the runtime adapter (e.g. @spark-js/node provides an
 * apache-arrow based implementation).
 */
export type ArrowDecoderFn = (chunks: Uint8Array[]) => Promise<Row[]>;

export interface SparkSessionConfig {
  /** Spark Connect endpoint, e.g. "sc://localhost:15002" */
  remote: string;

  /**
   * Transport implementation injected by the runtime adapter.
   * spark-core never instantiates a transport itself — this is the
   * dependency-inversion seam that keeps the package platform-agnostic.
   */
  transport: Transport;

  /**
   * Arrow IPC → Row[] decoder function injected by the runtime adapter.
   * If not provided, collect() will throw at runtime.
   */
  arrowDecoder?: ArrowDecoderFn;

  /** Optional session ID override for reconnecting to an existing session. */
  sessionId?: string;
}

// ─── SparkSession ───────────────────────────────────────────────────────────

export class SparkSession {
  readonly sessionId: string;
  private readonly transport: Transport;
  private readonly remote: string;
  /** @internal */
  readonly _arrowDecoder: ArrowDecoderFn | undefined;

  /** @internal — called by SparkSessionBuilder to construct the session. */
  static _create(config: SparkSessionConfig): SparkSession {
    return new SparkSession(config);
  }

  private constructor(config: SparkSessionConfig) {
    this.sessionId = config.sessionId ?? crypto.randomUUID();
    this.transport = config.transport;
    this.remote = config.remote;
    this._arrowDecoder = config.arrowDecoder;
  }

  // ── Builder pattern (mirrors SparkSession.builder().remote().getOrCreate()) ──

  static builder(): SparkSessionBuilder {
    return new SparkSessionBuilder();
  }

  // ── DataFrame entry points ────────────────────────────────────────────────

  /**
   * Access the session catalog for inspecting databases, tables, and columns.
   *
   * @see Spark source: sql/core/src/main/scala/org/apache/spark/sql/catalog/Catalog.scala
   */
  readonly catalog: Catalog = new Catalog(this);

  /**
   * Read a data source.  Returns a DataFrameReader which builds the
   * Read logical plan node.
   *
   * Equivalent to:
   *   spark.read.format("parquet").load("s3://bucket/data")
   *
   * Under the hood this produces a `Relation.Read` protobuf with a
   * `ReadType.DataSource` payload.
   */
  read = new DataFrameReader(this);

  /**
   * Execute a SQL string.  The server will parse, analyse, optimise, and
   * execute the query using Spark's full Catalyst pipeline — the client
   * is uninvolved in planning.
   */
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

  /** @internal — used by DataFrame to send plans via the injected transport */
  _executePlan(plan: LogicalPlan): AsyncIterable<Uint8Array> {
    return this.transport.executePlan(this.sessionId, plan);
  }

  /** @internal — used by DataFrameWriter to send commands via the injected transport */
  async _executeCommand(command: Record<string, unknown>): Promise<void> {
    if (!this.transport.executeCommand) {
      throw new Error("Transport does not support command execution.");
    }
    await this.transport.executeCommand(this.sessionId, command);
  }

  /** @internal — used by DataFrame.schema()/explain() via the injected transport */
  async _analyzePlan(request: Record<string, unknown>): Promise<Record<string, unknown>> {
    if (!this.transport.analyzePlan) {
      throw new Error("Transport does not support analyzePlan.");
    }
    return this.transport.analyzePlan(this.sessionId, request);
  }

  /**
   * Stop the SparkSession: releases the server-side session and closes
   * the underlying transport connection.
   *
   * After calling stop(), the session should not be used again.
   *
   * @see Spark source: SparkSession.stop() in sql/core/.../SparkSession.scala
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

// ─── Builder ────────────────────────────────────────────────────────────────

class SparkSessionBuilder {
  private config: Partial<SparkSessionConfig> = {};

  remote(connectionString: string): this {
    this.config.remote = connectionString;
    return this;
  }

  transport(t: Transport): this {
    this.config.transport = t;
    return this;
  }

  arrowDecoder(decoder: ArrowDecoderFn): this {
    this.config.arrowDecoder = decoder;
    return this;
  }

  /**
   * Construct the session.  In Spark Connect, "getOrCreate" is a server-side
   * concept — the server may return an existing session if the session ID
   * matches.  On the client we simply instantiate our handle.
   */
  getOrCreate(): SparkSession {
    if (!this.config.remote) {
      throw new Error(
        "SparkSession requires a remote URL. Call .remote('sc://host:port') on the builder.",
      );
    }
    if (!this.config.transport) {
      throw new Error(
        "SparkSession requires a Transport implementation. " +
          "Use @spark-js/node's GrpcTransport or supply a custom one.",
      );
    }
    return SparkSession._create(this.config as SparkSessionConfig);
  }
}

// ─── DataFrameReader ────────────────────────────────────────────────────────
// @see Spark source: sql/core/src/main/scala/org/apache/spark/sql/DataFrameReader.scala

class DataFrameReader {
  private session: SparkSession;
  private _format: string = "parquet";
  private _options: Record<string, string> = {};

  constructor(session: SparkSession) {
    this.session = session;
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
   * Trigger a Read plan node.  The resulting DataFrame is lazy — no data is
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
}
