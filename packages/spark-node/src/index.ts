/**
 * ════════════════════════════════════════════════════════════════════════════════
 * @spark-js/node  —  Node.js Spark Connect Runtime Adapter
 * ════════════════════════════════════════════════════════════════════════════════
 *
 * This package bridges @spark-js/core's platform-agnostic DataFrame API with
 * the real world of Node.js I/O.  It is responsible for everything that
 * requires a Node.js runtime:
 *
 *   1. **gRPC Transport** — Opens a persistent HTTP/2 connection to the Spark
 *      Connect server (default port 15002) using @grpc/grpc-js.  Sends
 *      serialised protobuf plans, receives streaming Arrow-encoded responses.
 *
 *   2. **Apache Arrow Decoding** — Converts Arrow IPC record batches into
 *      JS-native Row objects using the `apache-arrow` npm package.  Arrow's
 *      columnar format enables zero-copy reads when the server uses
 *      shared-memory transport, and efficient batch decoding otherwise.
 *
 *   3. **Node Buffer Management** — gRPC delivers response chunks as Node
 *      Buffers (backed by V8's ArrayBuffer / off-heap memory depending on
 *      size).  We must handle Buffer lifecycle carefully to avoid:
 *        • Memory leaks from retained Buffer references in long-running streams
 *        • Excessive GC pauses from large short-lived allocations
 *        • Detached ArrayBuffer issues when Buffers cross async boundaries
 *
 *   4. **Async Streaming** — Spark Connect returns results as a gRPC server
 *      stream (ExecutePlanResponse).  We wrap this as an AsyncIterable so
 *      DataFrame.collect() can consume it with `for await...of`.  For large
 *      result sets, callers can process rows incrementally without buffering
 *      the entire dataset in memory.
 *
 *   5. **child_process Integration** (optional) — For local development,
 *      we can spawn a Spark Connect server as a child process, managing its
 *      lifecycle (start, health-check, graceful shutdown) from Node.js.
 *      This uses Node's child_process.spawn with proper signal handling
 *      (SIGTERM → SIGKILL escalation) and stdout/stderr piping.
 *
 * ─── Required Knowledge ─────────────────────────────────────────────────────
 *
 * Working on this package requires understanding:
 *
 *   • **Spark Connect Protocol**: The gRPC service definition
 *     (SparkConnectService), ExecutePlan RPC, the Relation/Expression protobuf
 *     schema, and how the server manages sessions.
 *     @see connector/connect/server/src/main/scala/org/apache/spark/sql/connect/service/SparkConnectService.scala
 *     @see connector/connect/common/src/main/protobuf/spark/connect/base.proto
 *
 *   • **Apache Arrow IPC**: The streaming IPC format (0xFFFFFFFF continuation
 *     bytes, schema messages, record batch messages), dictionary encoding,
 *     and how Arrow's columnar layout maps to Spark's row-oriented DataTypes.
 *
 *   • **Node.js Internals**: The event loop, libuv thread pool (gRPC I/O),
 *     Buffer pooling (Buffer.allocUnsafe vs Buffer.alloc), stream backpressure,
 *     and async_hooks for debugging resource leaks.
 *
 *   • **gRPC/HTTP2**: Connection multiplexing, flow control, keepalive pings,
 *     deadline propagation, and error codes (UNAVAILABLE, DEADLINE_EXCEEDED).
 *
 * ─── Relationship to @spark-js/core ─────────────────────────────────────────
 *
 *   @spark-js/core defines the Transport interface.  This package provides
 *   `GrpcTransport` — the concrete implementation that satisfies it.
 *
 *   Usage:
 *     import { SparkSession } from "@spark-js/core";
 *     import { GrpcTransport } from "@spark-js/node";
 *
 *     const spark = SparkSession.builder()
 *       .remote("sc://localhost:15002")
 *       .transport(new GrpcTransport("localhost:15002"))
 *       .getOrCreate();
 *
 *     const df = spark.read.format("parquet").load("s3://bucket/data");
 *     const rows = await df.filter(col("age").gt(lit(30))).collect();
 *
 * ════════════════════════════════════════════════════════════════════════════════
 */

// ─── Public API ─────────────────────────────────────────────────────────────

export { GrpcTransport } from "./transport/grpc-transport.js";
export { ArrowDecoder } from "./arrow/arrow-decoder.js";
export { SparkProcessManager } from "./process/spark-process-manager.js";
export { buildRelation, buildExpression } from "./proto/proto-builder.js";

// Re-export core for convenience — consumers only need to depend on @spark-js/node
export {
  SparkSession,
  DataFrame,
  Column,
  col,
  lit,
  GroupedData,
  DataType,
  PlanBuilder,
  SparkConnectError,
  GrpcStatusCode,
  DataFrameWriter,
  StructType,
  StructField,
  Catalog,
  WindowSpec,
  Window,
  // Functions
  when,
  WhenBuilder,
  cast,
  upper,
  lower,
  trim,
  ltrim,
  rtrim,
  length,
  concat,
  substring,
  regexp_replace,
  contains,
  startswith,
  endswith,
  year,
  month,
  dayofmonth,
  hour,
  minute,
  second,
  current_date,
  current_timestamp,
  datediff,
  date_add,
  date_sub,
  abs,
  round,
  ceil,
  floor,
  sqrt,
  pow,
  log,
  exp,
  count,
  sum,
  avg,
  min,
  max,
  countDistinct,
  coalesce,
  isnull,
  isnan,
  struct,
  array,
  explode,
  size,
  row_number,
  rank,
  dense_rank,
  lag,
  lead,
  ntile,
} from "@spark-js/core";

export type {
  Row,
  Schema,
  FieldDescriptor,
  Transport,
  ArrowDecoderFn,
  SaveMode,
  CatalogOperation,
  WindowFrame,
  FrameBoundary,
} from "@spark-js/core";

// ─── Convenience: fully-wired session factory ───────────────────────────────

import { SparkSession } from "@spark-js/core";
import { GrpcTransport } from "./transport/grpc-transport.js";
import { ArrowDecoder } from "./arrow/arrow-decoder.js";

/**
 * Parse a Spark Connect URL (sc://host:port) into host:port.
 * Falls through to the raw string if no sc:// prefix.
 */
function parseEndpoint(remote: string): string {
  if (remote.startsWith("sc://")) {
    return remote.slice("sc://".length);
  }
  return remote;
}

/**
 * Create a fully-wired SparkSession for Node.js.
 *
 * This is the primary entry point for @spark-js/node. It creates a
 * SparkSession with GrpcTransport and ArrowDecoder pre-configured.
 *
 * @example
 *   import { connect, col, lit } from "@spark-js/node";
 *
 *   const spark = connect("sc://localhost:15002");
 *   const df = spark.sql("SELECT * FROM my_table");
 *   const rows = await df.filter(col("age").gt(lit(30))).collect();
 *   await df.show();
 */
export function connect(remote: string): SparkSession {
  const endpoint = parseEndpoint(remote);
  const transport = new GrpcTransport(endpoint);
  return SparkSession.builder()
    .remote(remote)
    .transport(transport)
    .arrowDecoder((chunks) => ArrowDecoder.decode(chunks))
    .getOrCreate();
}
