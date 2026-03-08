/**
 * ════════════════════════════════════════════════════════════════════════════════
 * @spark-connect-js/node  —  Node.js Spark Connect Runtime Adapter
 * ════════════════════════════════════════════════════════════════════════════════
 *
 * This package bridges @spark-connect-js/core's platform-agnostic DataFrame API with
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
 * ─── Relationship to @spark-connect-js/core ─────────────────────────────────────────
 *
 *   @spark-connect-js/core defines the Transport interface.  This package provides
 *   `GrpcTransport` — the concrete implementation that satisfies it.
 *
 *   Usage:
 *     import { SparkSession } from "@spark-connect-js/core";
 *     import { GrpcTransport } from "@spark-connect-js/node";
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

// Re-export core for convenience — consumers only need to depend on @spark-connect-js/node
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
  // Conditional / Predicate
  when,
  WhenBuilder,
  cast,
  coalesce,
  isnull,
  isnan,
  isnotnull,
  nanvl,
  ifnull,
  nvl,
  nvl2,
  nullif,
  expr,
  monotonically_increasing_id,
  spark_partition_id,
  typeof_,
  uuid,
  broadcast,
  // String
  upper,
  lower,
  trim,
  ltrim,
  rtrim,
  btrim,
  length,
  char_length,
  character_length,
  octet_length,
  bit_length,
  concat,
  concat_ws,
  substring,
  regexp_replace,
  contains,
  startswith,
  endswith,
  split,
  initcap,
  lpad,
  rpad,
  repeat,
  reverse,
  instr,
  locate,
  translate,
  ascii,
  format_number,
  format_string,
  base64,
  unbase64,
  soundex,
  levenshtein,
  overlay,
  left,
  right,
  decode,
  encode,
  // Date / Timestamp
  year,
  month,
  dayofmonth,
  dayofweek,
  dayofyear,
  weekofyear,
  quarter,
  hour,
  minute,
  second,
  current_date,
  current_timestamp,
  datediff,
  date_add,
  date_sub,
  months_between,
  next_day,
  last_day,
  add_months,
  date_format,
  to_date,
  to_timestamp,
  from_unixtime,
  unix_timestamp,
  date_trunc,
  trunc,
  extract,
  date_part,
  // Math
  abs,
  round,
  bround,
  ceil,
  floor,
  sqrt,
  cbrt,
  pow,
  log,
  log2,
  log10,
  log1p,
  exp,
  expm1,
  sin,
  cos,
  tan,
  asin,
  acos,
  atan,
  atan2,
  sinh,
  cosh,
  tanh,
  signum,
  sign,
  degrees,
  radians,
  factorial,
  greatest,
  least,
  hex,
  unhex,
  bin,
  rand,
  randn,
  pmod,
  rint,
  hypot,
  negate,
  negative,
  positive,
  e,
  pi,
  // Aggregate
  count,
  sum,
  avg,
  mean,
  min,
  max,
  countDistinct,
  sum_distinct,
  first,
  first_value,
  last,
  last_value,
  collect_list,
  collect_set,
  array_agg,
  stddev,
  stddev_pop,
  stddev_samp,
  var_pop,
  var_samp,
  variance,
  corr,
  covar_pop,
  covar_samp,
  approx_count_distinct,
  skewness,
  kurtosis,
  product,
  any_value,
  count_if,
  max_by,
  min_by,
  percentile_approx,
  median,
  mode,
  grouping,
  grouping_id,
  bool_and,
  every,
  bool_or,
  some,
  bit_and,
  bit_or,
  bit_xor,
  // Collection / Array / Map
  struct,
  array,
  array_contains,
  array_distinct,
  array_union,
  array_intersect,
  array_except,
  array_join,
  array_sort,
  sort_array,
  array_max,
  array_min,
  array_position,
  array_remove,
  array_repeat,
  array_compact,
  arrays_overlap,
  arrays_zip,
  flatten,
  sequence,
  element_at,
  get,
  slice,
  explode,
  explode_outer,
  posexplode,
  posexplode_outer,
  inline,
  inline_outer,
  stack,
  size,
  create_map,
  map_keys,
  map_values,
  map_entries,
  map_from_entries,
  map_from_arrays,
  map_concat,
  // Window
  row_number,
  rank,
  dense_rank,
  cume_dist,
  percent_rank,
  nth_value,
  lag,
  lead,
  ntile,
  // Hash
  md5,
  sha1,
  sha2,
  hash,
  xxhash64,
  crc32,
  // JSON
  from_json,
  to_json,
  get_json_object,
  json_tuple,
  schema_of_json,
  json_array_length,
  json_object_keys,
  // Bitwise
  bitwise_not,
  bit_count,
  bit_get,
  getbit,
  shiftleft,
  shiftright,
  shiftrightunsigned,
  // CSV
  from_csv,
  to_csv,
  schema_of_csv,
  // Sort
  asc,
  desc,
  asc_nulls_first,
  asc_nulls_last,
  desc_nulls_first,
  desc_nulls_last,
} from "@spark-connect-js/core";

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
} from "@spark-connect-js/core";

// ─── Convenience: fully-wired session factory ───────────────────────────────

import { SparkSession } from "@spark-connect-js/core";
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
 * This is the primary entry point for @spark-connect-js/node. It creates a
 * SparkSession with GrpcTransport and ArrowDecoder pre-configured.
 *
 * @example
 *   import { connect, col, lit } from "@spark-connect-js/node";
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
