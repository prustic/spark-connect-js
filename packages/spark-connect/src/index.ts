/**
 * @spark-js/connect — Generated TypeScript types from Spark Connect protobuf definitions.
 *
 * Re-exports the key types and schemas needed by the transport layer.
 * Consumer packages import from here rather than reaching into gen/ directly.
 */

// ─── Service ────────────────────────────────────────────────────────────────
export { SparkConnectService } from "./gen/spark/connect/base_pb.js";

// ─── Core request/response messages ─────────────────────────────────────────
export {
  type Plan,
  PlanSchema,
  type UserContext,
  UserContextSchema,
  type ExecutePlanRequest,
  ExecutePlanRequestSchema,
  type ExecutePlanResponse,
  ExecutePlanResponseSchema,
  type ExecutePlanResponse_ArrowBatch,
  type ExecutePlanResponse_ResultComplete,
  type AnalyzePlanRequest,
  AnalyzePlanRequestSchema,
  type AnalyzePlanResponse,
  AnalyzePlanResponseSchema,
  type ConfigRequest,
  ConfigRequestSchema,
  type ConfigResponse,
  ConfigResponseSchema,
} from "./gen/spark/connect/base_pb.js";

// ─── Relation (logical plan) messages ───────────────────────────────────────
export {
  type Relation,
  RelationSchema,
  type Read,
  ReadSchema,
  type Project,
  ProjectSchema,
  type Filter,
  FilterSchema,
  type Join,
  JoinSchema,
  type Aggregate,
  AggregateSchema,
  type Sort,
  SortSchema,
  type Limit,
  LimitSchema,
  type SQL,
  SQLSchema,
  type SetOperation,
  SetOperationSchema,
  type Deduplicate,
  DeduplicateSchema,
  type SubqueryAlias,
  SubqueryAliasSchema,
  type Sample,
  SampleSchema,
  type Range,
  RangeSchema,
  type Drop,
  DropSchema,
  type WithColumns,
  WithColumnsSchema,
  type WithColumnsRenamed,
  WithColumnsRenamedSchema,
  type ShowString,
  ShowStringSchema,
  type Read_DataSource,
  Read_DataSourceSchema,
  type Read_NamedTable,
  Read_NamedTableSchema,
  Aggregate_GroupType,
} from "./gen/spark/connect/relations_pb.js";

// ─── Expression messages ────────────────────────────────────────────────────
export {
  type Expression,
  ExpressionSchema,
  type Expression_Literal,
  Expression_LiteralSchema,
  type Expression_UnresolvedAttribute,
  Expression_UnresolvedAttributeSchema,
  type Expression_UnresolvedFunction,
  Expression_UnresolvedFunctionSchema,
  type Expression_Alias,
  Expression_AliasSchema,
  type Expression_SortOrder,
  Expression_SortOrderSchema,
} from "./gen/spark/connect/expressions_pb.js";

// ─── Type messages ──────────────────────────────────────────────────────────
export { type DataType, DataTypeSchema } from "./gen/spark/connect/types_pb.js";
