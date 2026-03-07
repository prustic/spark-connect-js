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
  // Conditional
  when,
  WhenBuilder,
  // Cast
  cast,
  // String
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
  // Date/Timestamp
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
  // Math
  abs,
  round,
  ceil,
  floor,
  sqrt,
  pow,
  log,
  exp,
  // Aggregate
  count,
  sum,
  avg,
  min,
  max,
  countDistinct,
  // Collection/Null
  coalesce,
  isnull,
  isnan,
  // Misc
  struct,
  array,
  explode,
  size,
  // Window
  row_number,
  rank,
  dense_rank,
  lag,
  lead,
  ntile,
} from "./functions.js";

// Re-export type-only interfaces for consumers that need them without pulling
// in implementation modules.
export type { Row } from "./types/row.js";
export type { Schema, FieldDescriptor } from "./types/schema.js";
export type { SparkSessionConfig, Transport, ArrowDecoderFn } from "./spark-session.js";
export { SparkConnectError, GrpcStatusCode } from "./errors.js";
export type { GrpcStatusCode as GrpcStatusCodeType } from "./errors.js";
