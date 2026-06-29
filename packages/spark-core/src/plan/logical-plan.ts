// Logical plan tree types. Each variant of LogicalPlan maps to a Spark Connect
// Relation message; each variant of Expression maps to an Expression message.
// DataFrame methods build the tree locally; @spark-connect-js/node's PlanBuilder
// serializes it to protobuf when an action runs.

import type { StorageLevel } from "../storage-level.js";

/**
 * One node in a logical plan's expression tree.
 *
 * Used by {@link FilterPlan.condition}, {@link ProjectPlan.expressions}, and
 * inside aggregations and window specifications. Expressions stay unresolved
 * on the client: column references are plain strings, function calls reference
 * names rather than implementations. The server's analyzer binds them against
 * the catalog and schema before execution.
 *
 * @see [Spark Connect proto: expressions.proto](https://github.com/apache/spark/blob/master/sql/connect/common/src/main/protobuf/spark/connect/expressions.proto)
 */
export type Expression =
  | { type: "unresolvedAttribute"; name: string; planId?: bigint }
  | { type: "literal"; value: string | number | boolean | bigint | null }
  | { type: "alias"; inner: Expression; name: string }
  // Comparison / logical / arithmetic operators
  | { type: "gt"; left: Expression; right: Expression }
  | { type: "lt"; left: Expression; right: Expression }
  | { type: "eq"; left: Expression; right: Expression }
  | { type: "neq"; left: Expression; right: Expression }
  | { type: "gte"; left: Expression; right: Expression }
  | { type: "lte"; left: Expression; right: Expression }
  | { type: "and"; left: Expression; right: Expression }
  | { type: "or"; left: Expression; right: Expression }
  | { type: "add"; left: Expression; right: Expression }
  | { type: "subtract"; left: Expression; right: Expression }
  | { type: "multiply"; left: Expression; right: Expression }
  | { type: "divide"; left: Expression; right: Expression }
  // Aggregate functions (sum, avg, count, min, max, etc.)
  | { type: "aggregateFunction"; name: string; arguments: Expression[]; isDistinct?: boolean }
  // Sort ordering marker (wraps an expression with direction)
  | {
      type: "sortOrder";
      inner: Expression;
      direction: "ascending" | "descending";
      nullOrdering: "nulls_first" | "nulls_last";
    }
  // Generic unresolved function call (maps to Spark's UnresolvedFunction)
  | {
      type: "unresolvedFunction";
      name: string;
      arguments: Expression[];
      isDistinct?: boolean;
    }
  // SQL expression string (maps to Spark Connect's Expression.ExpressionString)
  | { type: "expressionString"; expression: string }
  // Cast expression (maps to Spark Connect's Expression.Cast)
  | { type: "cast"; inner: Expression; targetType: string }
  // Window expression (maps to Spark Connect's Expression.Window)
  | {
      type: "window";
      windowFunction: Expression;
      partitionSpec: Expression[];
      orderSpec: SortOrder[];
      frameSpec?: WindowFrame;
    };

/** Window frame specification. */
export interface WindowFrame {
  frameType: "row" | "range";
  lower: FrameBoundary;
  upper: FrameBoundary;
}

export type FrameBoundary =
  { type: "currentRow" } | { type: "unbounded" } | { type: "value"; value: Expression };

/**
 * The logical plan tree, mirroring Spark Catalyst's internal representation.
 * Every DataFrame transformation appends a new node to the tree; the full tree
 * serializes to protobuf for Spark Connect when an action runs.
 *
 * The plan that spark-connect-js builds is the *unresolved* form: column
 * references are strings, nothing is bound to a catalog yet. The Spark Connect
 * server runs the remaining Catalyst phases (analysis, optimization, physical
 * planning, codegen) before executing.
 *
 * A malformed unresolved plan produces cryptic `AnalysisException`s on the
 * server, so the shape here matters. Each variant maps one-to-one to a Spark
 * Connect `Relation` protobuf message.
 *
 * @see [Spark source: LogicalPlan.scala](https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/logical/LogicalPlan.scala)
 * @see [Spark Connect proto: relations.proto](https://github.com/apache/spark/blob/master/sql/connect/common/src/main/protobuf/spark/connect/relations.proto)
 */
export type LogicalPlan = (
  | ReadPlan
  | ReadTablePlan
  | LocalRelationPlan
  | SqlPlan
  | FilterPlan
  | ProjectPlan
  | AggregatePlan
  | LimitPlan
  | SortPlan
  | JoinPlan
  | DropPlan
  | WithColumnsPlan
  | DeduplicatePlan
  | OffsetPlan
  | CatalogPlan
  | SetOperationPlan
  | SamplePlan
  | NAFillPlan
  | NADropPlan
  | ToDFPlan
  | DescribePlan
  | RangePlan
  | WithColumnsRenamedPlan
  | SubqueryAliasPlan
  | HintPlan
  | TailPlan
  | RepartitionPlan
  | RepartitionByExpressionPlan
  | UnpivotPlan
  | SummaryPlan
  | NAReplacePlan
  | StatCorrPlan
  | StatCovPlan
  | StatCrosstabPlan
  | StatFreqItemsPlan
  | StatApproxQuantilePlan
) & {
  /**
   * Per-DataFrame identifier set by `DataFrame._fromPlan`. Surfaces as
   * `RelationCommon.plan_id` on the wire, letting the server resolve
   * `df.col(name)` references in self-joins and same-schema joins where the
   * unqualified column name is ambiguous.
   */
  planId?: bigint;
};

/**
 * Read from a data source.
 *
 * - Spark Connect: Relation.Read { ReadType.DataSource }
 */
export interface ReadPlan {
  type: "read";
  format: string;
  path: string;
  options: Record<string, string>;
  schema?: string;
  isStreaming?: boolean;
}

/**
 * Read from a named table.
 *
 * - Spark Connect: Relation.Read { ReadType.NamedTable }
 */
export interface ReadTablePlan {
  type: "readTable";
  tableName: string;
  options: Record<string, string>;
  isStreaming?: boolean;
}

/**
 * Create a DataFrame from local data (Arrow IPC bytes).
 *
 * - Spark Connect: Relation.LocalRelation { data, schema }
 */
export interface LocalRelationPlan {
  type: "localRelation";
  data?: Uint8Array;
  schema?: string;
}

/**
 * Execute raw SQL.
 *
 * - Spark Connect: Relation.Sql { query }
 */
export interface SqlPlan {
  type: "sql";
  query: string;
}

/**
 * Filter rows by a predicate.
 *
 * - Spark Connect: Relation.Filter { child, condition }
 * - Catalyst: Filter(condition: Expression, child: LogicalPlan)
 */
export interface FilterPlan {
  type: "filter";
  child: LogicalPlan;
  condition: Expression;
}

/**
 * Select / project columns.
 *
 * - Spark Connect: Relation.Project { child, expressions }
 * - Catalyst: Project(projectList: Seq[Expression], child: LogicalPlan)
 */
export interface ProjectPlan {
  type: "project";
  child: LogicalPlan;
  expressions: Expression[];
}

/**
 * Group-by + aggregation.
 *
 * - Spark Connect: Relation.Aggregate { child, grouping_expressions, aggregate_expressions }
 * - Catalyst: Aggregate(groupingExprs, aggregateExprs, child)
 */
export interface AggregatePlan {
  type: "aggregate";
  child: LogicalPlan;
  groupingExpressions: Expression[];
  aggregateExpressions: Expression[];
  groupType?: "groupby" | "rollup" | "cube" | "pivot";
  pivot?: {
    col: Expression;
    values: Array<string | number | boolean>;
  };
}

/**
 * Limit the number of returned rows.
 *
 * - Spark Connect: Relation.Limit { child, limit }
 * - Catalyst: GlobalLimit(limitExpr, LocalLimit(limitExpr, child))
 */
export interface LimitPlan {
  type: "limit";
  child: LogicalPlan;
  limit: number;
}

/**
 * Sort rows by one or more expressions.
 *
 * - Spark Connect: Relation.Sort { child, order }
 * - Catalyst: Sort(order: Seq[SortOrder], global: Boolean, child)
 */
export interface SortPlan {
  type: "sort";
  child: LogicalPlan;
  order: SortOrder[];
  isGlobal: boolean;
}

/**
 * One column in a sort or window specification: an expression plus a direction
 * and a null-ordering policy.
 */
export interface SortOrder {
  /** The expression being sorted on. */
  expression: Expression;
  /** Sort direction. */
  direction: "ascending" | "descending";
  /** Where nulls go relative to non-null values. */
  nullOrdering: "nulls_first" | "nulls_last";
}

/**
 * Join two DataFrames.
 *
 * - Spark Connect: Relation.Join { left, right, join_condition, join_type }
 * - Catalyst: Join(left, right, joinType, condition, hint)
 */
export interface JoinPlan {
  type: "join";
  left: LogicalPlan;
  right: LogicalPlan;
  condition?: Expression;
  joinType:
    "inner" | "full_outer" | "left_outer" | "right_outer" | "left_semi" | "left_anti" | "cross";
}

/**
 * Drop columns by name.
 *
 * - Spark Connect: Relation.Drop { child, columns }
 * - Catalyst: Project with filtered columns
 */
export interface DropPlan {
  type: "drop";
  child: LogicalPlan;
  columnNames: string[];
}

/**
 * Add or replace columns.
 *
 * - Spark Connect: Relation.WithColumns { child, aliases }
 * - Catalyst: Project with new named expressions
 */
export interface WithColumnsPlan {
  type: "withColumns";
  child: LogicalPlan;
  aliases: { name: string; expression: Expression }[];
}

/**
 * Remove duplicate rows.
 *
 * - Spark Connect: Relation.Deduplicate { child, column_names, all_columns_as_keys }
 */
export interface DeduplicatePlan {
  type: "deduplicate";
  child: LogicalPlan;
  columnNames?: string[];
  allColumnsAsKeys: boolean;
}

/**
 * Skip the first N rows.
 *
 * - Spark Connect: Relation.Offset { child, offset }
 */
export interface OffsetPlan {
  type: "offset";
  child: LogicalPlan;
  offset: number;
}

/**
 * One operation against the Spark catalog (databases, tables, functions,
 * temp views, caching, metadata refresh).
 *
 * Catalog operations travel inside a {@link CatalogPlan} as a Spark Connect
 * `Relation.Catalog`; the server returns the result as a DataFrame, decoded
 * from Arrow batches like any other query.
 *
 * - Spark Connect: Relation.Catalog { cat_type oneof }
 */
export type CatalogOperation =
  | { op: "listDatabases"; pattern?: string }
  | { op: "listTables"; dbName?: string; pattern?: string }
  | { op: "listColumns"; tableName: string; dbName?: string }
  | { op: "listFunctions"; dbName?: string; pattern?: string }
  | { op: "listCatalogs"; pattern?: string }
  | { op: "getDatabase"; dbName: string }
  | { op: "getTable"; tableName: string; dbName?: string }
  | { op: "getFunction"; functionName: string; dbName?: string }
  | { op: "tableExists"; tableName: string; dbName?: string }
  | { op: "databaseExists"; dbName: string }
  | { op: "functionExists"; functionName: string; dbName?: string }
  | { op: "isCached"; tableName: string }
  | { op: "dropTempView"; viewName: string }
  | { op: "dropGlobalTempView"; viewName: string }
  | { op: "currentDatabase" }
  | { op: "setCurrentDatabase"; dbName: string }
  | { op: "currentCatalog" }
  | { op: "setCurrentCatalog"; catalogName: string }
  | {
      op: "cacheTable";
      tableName: string;
      storageLevel?: StorageLevel;
    }
  | { op: "uncacheTable"; tableName: string }
  | { op: "clearCache" }
  | { op: "refreshTable"; tableName: string }
  | { op: "refreshByPath"; path: string }
  | { op: "recoverPartitions"; tableName: string }
  | {
      op: "createTable";
      tableName: string;
      path?: string;
      source?: string;
      description?: string;
      schema?: string;
      options?: Record<string, string>;
    };

export interface CatalogPlan {
  type: "catalog";
  operation: CatalogOperation;
}

/**
 * Set operations: union, intersect, except.
 *
 * - Spark Connect: Relation.SetOperation
 */
export interface SetOperationPlan {
  type: "setOperation";
  left: LogicalPlan;
  right: LogicalPlan;
  opType: "union" | "intersect" | "except";
  isAll: boolean;
  byName: boolean;
  allowMissingColumns: boolean;
}

/**
 * Random sample of rows.
 *
 * - Spark Connect: Relation.Sample
 */
export interface SamplePlan {
  type: "sample";
  child: LogicalPlan;
  lowerBound: number;
  upperBound: number;
  withReplacement: boolean;
  seed?: number;
}

/**
 * Fill null values.
 *
 * - Spark Connect: Relation.FillNa (NAFill)
 */
export interface NAFillPlan {
  type: "fillNa";
  child: LogicalPlan;
  cols: string[];
  values: Array<string | number | boolean>;
}

/**
 * Drop rows with null values.
 *
 * - Spark Connect: Relation.DropNa (NADrop)
 */
export interface NADropPlan {
  type: "dropNa";
  child: LogicalPlan;
  cols: string[];
  minNonNulls?: number;
}

/**
 * Rename columns (return new DataFrame with renamed columns).
 *
 * - Spark Connect: Relation.ToDF
 */
export interface ToDFPlan {
  type: "toDF";
  child: LogicalPlan;
  columnNames: string[];
}

/**
 * Compute summary statistics.
 *
 * - Spark Connect: Relation.Describe (StatDescribe)
 */
export interface DescribePlan {
  type: "describe";
  child: LogicalPlan;
  cols: string[];
}

/**
 * Generate a sequence of integers.
 *
 * - Spark Connect: Relation.Range
 */
export interface RangePlan {
  type: "range";
  start: number;
  end: number;
  step: number;
  numPartitions?: number;
}

/**
 * Rename columns by name mapping.
 *
 * - Spark Connect: Relation.WithColumnsRenamed
 */
export interface WithColumnsRenamedPlan {
  type: "withColumnsRenamed";
  child: LogicalPlan;
  renames: { colName: string; newColName: string }[];
}

/**
 * Assign a name (alias) to a DataFrame / subquery.
 *
 * - Spark Connect: Relation.SubqueryAlias
 */
export interface SubqueryAliasPlan {
  type: "subqueryAlias";
  child: LogicalPlan;
  alias: string;
}

/**
 * Attach a hint to a relation.
 *
 * - Spark Connect: Relation.Hint
 */
export interface HintPlan {
  type: "hint";
  child: LogicalPlan;
  name: string;
  parameters: Expression[];
}

/**
 * Fetch the last N rows.
 *
 * - Spark Connect: Relation.Tail
 */
export interface TailPlan {
  type: "tail";
  child: LogicalPlan;
  limit: number;
}

/**
 * Repartition a DataFrame.
 *
 * - Spark Connect: Relation.Repartition
 *
 * When shuffle=true, this is repartition(). When shuffle=false, this is coalesce().
 */
export interface RepartitionPlan {
  type: "repartition";
  child: LogicalPlan;
  numPartitions: number;
  shuffle: boolean;
}

/**
 * Repartition by expression (range-based partitioning).
 *
 * - Spark Connect: Relation.RepartitionByExpression
 */
export interface RepartitionByExpressionPlan {
  type: "repartitionByExpression";
  child: LogicalPlan;
  partitionExprs: Expression[];
  numPartitions?: number;
}

/**
 * Unpivot a DataFrame from wide format to long format.
 *
 * - Spark Connect: Relation.Unpivot
 * - Catalyst: Unpivot(ids, values, variableColumnName, valueColumnName)
 */
export interface UnpivotPlan {
  type: "unpivot";
  child: LogicalPlan;
  ids: Expression[];
  values?: Expression[];
  variableColumnName: string;
  valueColumnName: string;
}

/**
 * Compute summary statistics.
 *
 * - Spark Connect: Relation.Summary (StatSummary)
 */
export interface SummaryPlan {
  type: "summary";
  child: LogicalPlan;
  statistics: string[];
}

/**
 * Replace values matching old with new.
 *
 * - Spark Connect: Relation.Replace (NAReplace)
 */
export interface NAReplacePlan {
  type: "naReplace";
  child: LogicalPlan;
  cols: string[];
  replacements: Array<{
    oldValue: string | number | boolean | null;
    newValue: string | number | boolean | null;
  }>;
}

/**
 * Compute Pearson correlation between two columns.
 *
 * - Spark Connect: Relation.Corr (StatCorr)
 */
export interface StatCorrPlan {
  type: "statCorr";
  child: LogicalPlan;
  col1: string;
  col2: string;
  method?: string;
}

/**
 * Compute sample covariance between two columns.
 *
 * - Spark Connect: Relation.Cov (StatCov)
 */
export interface StatCovPlan {
  type: "statCov";
  child: LogicalPlan;
  col1: string;
  col2: string;
}

/**
 * Compute a pair-wise frequency table (contingency table).
 *
 * - Spark Connect: Relation.Crosstab (StatCrosstab)
 */
export interface StatCrosstabPlan {
  type: "statCrosstab";
  child: LogicalPlan;
  col1: string;
  col2: string;
}

/**
 * Find frequent items in columns.
 *
 * - Spark Connect: Relation.FreqItems (StatFreqItems)
 */
export interface StatFreqItemsPlan {
  type: "statFreqItems";
  child: LogicalPlan;
  cols: string[];
  support?: number;
}

/**
 * Compute approximate quantiles of numerical columns.
 *
 * - Spark Connect: Relation.ApproxQuantile (StatApproxQuantile)
 */
export interface StatApproxQuantilePlan {
  type: "statApproxQuantile";
  child: LogicalPlan;
  cols: string[];
  probabilities: number[];
  relativeError: number;
}
