/**
 * ─── LogicalPlan ────────────────────────────────────────────────────────────
 *
 * These types represent the **logical plan tree** that mirrors Spark Catalyst's
 * internal plan representation.  Every DataFrame transformation appends a new
 * node to the tree; the full tree is serialised to protobuf for Spark Connect.
 *
 * @see Spark source (LogicalPlan): sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/logical/LogicalPlan.scala
 * @see Spark source (basic nodes): sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/logical/basicLogicalOperators.scala
 * @see Spark Connect proto (Relation): connector/connect/common/src/main/protobuf/spark/connect/relations.proto
 * @see Spark Connect proto (Expression): connector/connect/common/src/main/protobuf/spark/connect/expressions.proto
 *
 * ─── How this maps to Spark internals ───────────────────────────────────────
 *
 * Spark's Catalyst engine processes plans in phases:
 *
 *   1. Unresolved Logical Plan  ← THIS IS WHAT WE BUILD
 *      Column references are just strings ("age", "dept.name").
 *
 *   2. Resolved Logical Plan (analysis)
 *      The Analyzer resolves column references against the catalog/schema.
 *
 *   3. Optimised Logical Plan (optimization)
 *      Catalyst rules fire: constant folding, predicate pushdown, etc.
 *
 *   4. Physical Plan (planning)
 *      The planner picks execution strategies: SortMergeJoin vs BroadcastHash, etc.
 *
 *   5. Executed Plan (execution)
 *      Code generation (WholeStageCodegen) compiles the plan into JVM bytecode.
 *
 * We only control phase 1.  Everything from phase 2 onward is handled by the
 * JVM-side Spark Connect server.  But getting phase 1 RIGHT is essential —
 * a malformed unresolved plan causes cryptic AnalysisExceptions.
 */

// ─── Expression types ───────────────────────────────────────────────────────
// These model nodes in the expression tree inside filter, select, and agg.

export type Expression =
  | { type: "unresolvedAttribute"; name: string }
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
  | { type: "aggregateFunction"; name: string; arguments: Expression[] };

// ─── Plan node types ────────────────────────────────────────────────────────
// Each type maps to a Spark Connect `Relation` protobuf variant.

export type LogicalPlan = ReadPlan | SqlPlan | FilterPlan | ProjectPlan | AggregatePlan | LimitPlan;

/**
 * Read from a data source.
 * → Spark Connect: Relation.Read { ReadType.DataSource }
 */
export interface ReadPlan {
  type: "read";
  format: string;
  path: string;
  options: Record<string, string>;
}

/**
 * Execute raw SQL.
 * → Spark Connect: Relation.Sql { query }
 */
export interface SqlPlan {
  type: "sql";
  query: string;
}

/**
 * Filter rows by a predicate.
 * → Spark Connect: Relation.Filter { child, condition }
 * → Catalyst: Filter(condition: Expression, child: LogicalPlan)
 */
export interface FilterPlan {
  type: "filter";
  child: LogicalPlan;
  condition: Expression;
}

/**
 * Select / project columns.
 * → Spark Connect: Relation.Project { child, expressions }
 * → Catalyst: Project(projectList: Seq[Expression], child: LogicalPlan)
 */
export interface ProjectPlan {
  type: "project";
  child: LogicalPlan;
  expressions: Expression[];
}

/**
 * Group-by + aggregation.
 * → Spark Connect: Relation.Aggregate { child, grouping_expressions, aggregate_expressions }
 * → Catalyst: Aggregate(groupingExprs, aggregateExprs, child)
 */
export interface AggregatePlan {
  type: "aggregate";
  child: LogicalPlan;
  groupingExpressions: Expression[];
  aggregateExpressions: Expression[];
}

/**
 * Limit the number of returned rows.
 * → Spark Connect: Relation.Limit { child, limit }
 * → Catalyst: GlobalLimit(limitExpr, LocalLimit(limitExpr, child))
 */
export interface LimitPlan {
  type: "limit";
  child: LogicalPlan;
  limit: number;
}
