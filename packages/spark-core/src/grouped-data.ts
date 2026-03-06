/**
 * ─── GroupedData ─────────────────────────────────────────────────────────────
 *
 * Returned by DataFrame.groupBy().  Mirrors `RelationalGroupedDataset` in the
 * JVM Spark API.
 *
 * @see Spark source: sql/core/src/main/scala/org/apache/spark/sql/RelationalGroupedDataset.scala
 * @see Aggregate plan: sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/logical/basicLogicalOperators.scala
 *
 * GroupedData is NOT a DataFrame — it's a transient builder that captures
 * grouping expressions and waits for an aggregation call (agg, count, sum, etc.)
 * to produce the final Aggregate logical plan node.
 *
 * In Catalyst, the Aggregate node looks like:
 *   Aggregate(
 *     groupingExpressions = [col("department")],
 *     aggregateExpressions = [col("department"), sum(col("salary"))],
 *     child = <the source plan>
 *   )
 *
 * The grouping + aggregate expressions are combined into a single plan node,
 * which is why GroupedData must collect both before producing a DataFrame.
 */

import { DataFrame } from "./data-frame.js";
import { Column } from "./column.js";
import type { Expression } from "./plan/logical-plan.js";

export class GroupedData {
  private readonly _df: DataFrame;
  private readonly _groupExprs: Expression[];

  constructor(df: DataFrame, groupExprs: Expression[]) {
    this._df = df;
    this._groupExprs = groupExprs;
  }

  /**
   * Apply one or more aggregate functions.
   *
   * @example
   *   df.groupBy(col("dept")).agg(
   *     functions.sum(col("salary")).alias("total_salary"),
   *     functions.avg(col("age")).alias("avg_age"),
   *   )
   *
   * Each Column passed in should be an aggregate expression (sum, avg, count,
   * min, max).  The grouping columns are implicitly included in the output.
   */
  agg(...exprs: Column[]): DataFrame {
    const aggExprs = exprs.map((e) => e._expr);
    return DataFrame._fromPlan(this._df._session, {
      type: "aggregate",
      child: this._df._plan,
      groupingExpressions: this._groupExprs,
      aggregateExpressions: aggExprs,
    });
  }

  /** Shorthand for agg(count("*")) */
  count(): DataFrame {
    return this.agg(
      new Column({
        type: "aggregateFunction",
        name: "count",
        arguments: [{ type: "literal", value: 1 }],
      }).alias("count"),
    );
  }

  /** Shorthand for agg(sum(column)) */
  sum(...columnNames: string[]): DataFrame {
    const sumExprs = columnNames.map((name) =>
      new Column({
        type: "aggregateFunction",
        name: "sum",
        arguments: [{ type: "unresolvedAttribute", name }],
      }).alias(`sum(${name})`),
    );
    return this.agg(...sumExprs);
  }

  /** Shorthand for agg(avg(column)) */
  avg(...columnNames: string[]): DataFrame {
    const avgExprs = columnNames.map((name) =>
      new Column({
        type: "aggregateFunction",
        name: "avg",
        arguments: [{ type: "unresolvedAttribute", name }],
      }).alias(`avg(${name})`),
    );
    return this.agg(...avgExprs);
  }
}
