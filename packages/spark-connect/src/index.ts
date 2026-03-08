/**
 * @spark-connect-js/connect — Generated TypeScript types from Spark Connect protobuf definitions.
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
  type InterruptRequest,
  InterruptRequestSchema,
  type InterruptResponse,
  InterruptResponseSchema,
  type ReleaseSessionRequest,
  ReleaseSessionRequestSchema,
  type ReleaseSessionResponse,
  ReleaseSessionResponseSchema,
  type FetchErrorDetailsRequest,
  FetchErrorDetailsRequestSchema,
  type FetchErrorDetailsResponse,
  FetchErrorDetailsResponseSchema,
  AnalyzePlanRequest_Explain_ExplainModeSchema,
  type AnalyzePlanRequest_Schema,
  AnalyzePlanRequest_SchemaSchema,
  type AnalyzePlanRequest_Explain,
  AnalyzePlanRequest_ExplainSchema,
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
  type Offset,
  OffsetSchema,
  Join_JoinType,
  SetOperation_SetOpType,
  type NAFill,
  NAFillSchema,
  type NADrop,
  NADropSchema,
  type NAReplace,
  NAReplaceSchema,
  type ToDF,
  ToDFSchema,
  type StatDescribe,
  StatDescribeSchema,
  type StatSummary,
  StatSummarySchema,
  type LocalRelation,
  LocalRelationSchema,
  type Tail,
  TailSchema,
  type Hint,
  HintSchema,
  type WithColumnsRenamed_Rename,
  WithColumnsRenamed_RenameSchema,
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
  Expression_SortOrder_SortDirection,
  Expression_SortOrder_NullOrdering,
} from "./gen/spark/connect/expressions_pb.js";

// ─── Window expression messages ─────────────────────────────────────────────
export {
  type Expression_Window,
  Expression_WindowSchema,
  type Expression_Window_WindowFrame,
  Expression_Window_WindowFrameSchema,
  Expression_Window_WindowFrame_FrameType,
  type Expression_Window_WindowFrame_FrameBoundary,
  Expression_Window_WindowFrame_FrameBoundarySchema,
} from "./gen/spark/connect/expressions_pb.js";

// ─── Type messages ──────────────────────────────────────────────────────────
export { type DataType, DataTypeSchema } from "./gen/spark/connect/types_pb.js";

// ─── Catalog messages ───────────────────────────────────────────────────────
export {
  type Catalog,
  CatalogSchema,
  type CurrentDatabase,
  CurrentDatabaseSchema,
  type SetCurrentDatabase,
  SetCurrentDatabaseSchema,
  type ListDatabases,
  ListDatabasesSchema,
  type ListTables,
  ListTablesSchema,
  type ListFunctions,
  ListFunctionsSchema,
  type ListColumns,
  ListColumnsSchema,
  type GetDatabase,
  GetDatabaseSchema,
  type GetTable,
  GetTableSchema,
  type TableExists,
  TableExistsSchema,
  type DatabaseExists,
  DatabaseExistsSchema,
  type FunctionExists,
  FunctionExistsSchema,
} from "./gen/spark/connect/catalog_pb.js";

// ─── Expression.ExpressionString ────────────────────────────────────────────
export {
  type Expression_ExpressionString,
  Expression_ExpressionStringSchema,
} from "./gen/spark/connect/expressions_pb.js";

// ─── Expression.Cast messages ───────────────────────────────────────────────
export {
  type Expression_Cast,
  Expression_CastSchema,
  Expression_Cast_EvalMode,
} from "./gen/spark/connect/expressions_pb.js";

// ─── Command messages (for write operations) ────────────────────────────────
export {
  type Command,
  CommandSchema,
  type WriteOperation,
  WriteOperationSchema,
  WriteOperation_SaveMode,
  type WriteOperation_SaveTable,
  WriteOperation_SaveTableSchema,
  WriteOperation_SaveTable_TableSaveMethod,
  type CreateDataFrameViewCommand,
  CreateDataFrameViewCommandSchema,
} from "./gen/spark/connect/commands_pb.js";
