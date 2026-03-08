/**
 * @spark-connect-js/node — Node.js runtime adapter for Spark Connect.
 *
 * Bridges @spark-connect-js/core's platform-agnostic DataFrame API with Node.js:
 *   - gRPC transport via @grpc/grpc-js (HTTP/2, protobuf on the wire)
 *   - Arrow IPC decoding via apache-arrow
 *   - Optional child_process management for local Spark servers
 *
 * @see connector/connect/common/src/main/protobuf/spark/connect/base.proto
 * @see https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format
 *
 * @example
 *   import { SparkSession, col, lit } from "@spark-connect-js/node";
 *
 *   const spark = SparkSession.builder()
 *     .remote("sc://localhost:15002")
 *     .getOrCreate();
 *
 *   const rows = await spark.table("people")
 *     .filter(col("age").gt(lit(30)))
 *     .collect();
 */

// Public API

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

// Convenience: fully-wired session factory

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
