/**
 * ════════════════════════════════════════════════════════════════════════════════
 * @spark-js/core  —  Pure TypeScript Logical DataFrame API
 * ════════════════════════════════════════════════════════════════════════════════
 *
 * This package is the **platform-agnostic** heart of the Spark JS client.
 * It contains ZERO runtime dependencies, no Node APIs, no browser APIs — only
 * pure TypeScript types and logic that model Spark's query planning layer.
 *
 * ─── What this package does ─────────────────────────────────────────────────
 *
 *   1. Provides a fluent DataFrame API (read → filter → groupBy → agg → collect)
 *      that mirrors PySpark / Scala Spark semantics.
 *
 *   2. Builds **logical plans** — tree-structured descriptions of the query the
 *      user wants to run.  These plans are serialised as Spark Connect protocol
 *      buffers and sent to the Spark Connect server by a runtime adapter
 *      (e.g. @spark-js/node).
 *
 *   3. Defines the Column expression DSL used inside filter() and agg() so that
 *      predicate push-down and projection pruning can happen server-side on the
 *      JVM, not in JavaScript.
 *
 * ─── Why Spark internals knowledge is required ──────────────────────────────
 *
 *   • Every DataFrame method maps to a node in Spark's Catalyst logical plan
 *     tree (UnresolvedRelation → Filter → Aggregate → Project).  If the plan
 *     is constructed incorrectly the JVM will throw an AnalysisException and
 *     you will need to read Spark source to debug it.
 *
 *   • Spark Connect serialises plans with Protocol Buffers (spark.connect.*).
 *     The proto schema lives in the Spark repo under
 *     `connector/connect/common/src/main/protobuf/`.  Any change there will
 *     ripple into the types defined in this package.
 *
 *   • Column expressions must respect Spark's type system (ByteType through
 *     DecimalType, StructType, ArrayType, MapType).  JS `number` is an
 *     IEEE-754 double — mapping it naïvely to Spark's IntegerType or LongType
 *     causes silent precision loss in aggregation results.
 *
 * ─── Relationship to other packages ─────────────────────────────────────────
 *
 *   @spark-js/core  ←  imported by every runtime adapter
 *       │
 *       ├── @spark-js/node   (gRPC transport, Arrow decoding, Node Buffers)
 *       ├── @spark-js/deno   (future — Deno-native transport)
 *       └── @spark-js/web    (future — HTTP/2 + browser Arrow)
 *
 * ════════════════════════════════════════════════════════════════════════════════
 */

// ─── Core Types ──────────────────────────────────────────────────────────────

export { SparkSession } from "./spark-session.js";
export { DataFrame } from "./data-frame.js";
export { Column, col, lit } from "./column.js";
export { GroupedData } from "./grouped-data.js";
export { DataFrameWriter } from "./data-frame-writer.js";
export type { SaveMode } from "./data-frame-writer.js";
export { DataType } from "./types/data-type.js";
export { StructType, StructField } from "./types/struct.js";
export { Catalog } from "./catalog.js";
export { WindowSpec, Window } from "./window.js";
export type {
  LogicalPlan,
  Expression,
  SortOrder,
  CatalogOperation,
  WindowFrame,
  FrameBoundary,
} from "./plan/logical-plan.js";
export { PlanBuilder } from "./plan/plan-builder.js";

// ─── Functions ───────────────────────────────────────────────────────────────
export {
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
} from "./functions/index.js";

// Re-export type-only interfaces for consumers that need them without pulling
// in implementation modules.
export type { Row } from "./types/row.js";
export type { Schema, FieldDescriptor } from "./types/schema.js";
export type { SparkSessionConfig, Transport, ArrowDecoderFn } from "./spark-session.js";
export { SparkConnectError, GrpcStatusCode } from "./errors.js";
export type { GrpcStatusCode as GrpcStatusCodeType } from "./errors.js";
