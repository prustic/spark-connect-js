/**
 * ─── DataFrame ──────────────────────────────────────────────────────────────
 *
 * The DataFrame is the primary abstraction in Spark's structured API.
 * In the JVM, a DataFrame is an alias for Dataset[Row] — each instance wraps
 * a LogicalPlan that Catalyst will analyse, optimise, and execute.
 *
 * @see Spark source: sql/core/src/main/scala/org/apache/spark/sql/Dataset.scala
 * @see Catalyst plans: sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/logical/LogicalPlan.scala
 *
 * Our TypeScript DataFrame works the same way:
 *   1. Every transformation (filter, select, groupBy) returns a NEW DataFrame
 *      with a new LogicalPlan wrapping the previous one.
 *   2. No computation happens until an **action** (collect, show, count) is
 *      called, triggering plan serialisation → Spark Connect RPC → Arrow
 *      result decoding.
 *
 * ─── Why laziness matters ───────────────────────────────────────────────────
 *
 * Spark's optimizer (Catalyst) needs the FULL plan before it can:
 *   • Push predicates down into the scan (predicate pushdown)
 *   • Prune unused columns (projection pruning)
 *   • Reorder joins (cost-based optimization)
 *   • Fold constant expressions
 *
 * If we eagerly executed each step, none of these optimisations could fire.
 * This is fundamentally different from libraries like lodash or RxJS where
 * lazy chaining is a convenience — here it is a **correctness requirement**
 * for generating efficient Spark jobs.
 */

import type { SparkSession } from "./spark-session.js";
import type { LogicalPlan, SortOrder } from "./plan/logical-plan.js";
import type { Row } from "./types/row.js";
import { Column, col } from "./column.js";
import { GroupedData } from "./grouped-data.js";
import { DataFrameWriter } from "./data-frame-writer.js";

// console is available in Node, Deno, and all browsers, but not in the ES2023 lib.
declare const console: { log(msg: string): void };

export class DataFrame {
  /** @internal */
  readonly _session: SparkSession;
  /** @internal — the logical plan tree this DataFrame represents */
  readonly _plan: LogicalPlan;

  /** @internal — factory used by SparkSession.  Users never call `new DataFrame()`. */
  static _fromPlan(session: SparkSession, plan: LogicalPlan): DataFrame {
    return new DataFrame(session, plan);
  }

  private constructor(session: SparkSession, plan: LogicalPlan) {
    this._session = session;
    this._plan = plan;
  }

  // ── Transformations (lazy — return new DataFrame) ─────────────────────────

  /**
   * Filter rows by a boolean Column expression.
   *
   * Maps to Catalyst's `Filter(condition, child)` logical node.
   * The condition is serialised as a Spark Connect `Expression` protobuf and
   * resolved on the JVM, not evaluated in JS.
   *
   * @example
   *   df.filter(col("age").gt(lit(30)))
   */
  filter(condition: Column): DataFrame {
    return DataFrame._fromPlan(this._session, {
      type: "filter",
      child: this._plan,
      condition: condition._expr,
    });
  }

  /**
   * Alias for filter() — matches PySpark's .where() method.
   */
  where(condition: Column): DataFrame {
    return this.filter(condition);
  }

  /**
   * Project (select) a subset of columns.
   *
   * Maps to Catalyst's `Project(expressions, child)`.
   * Projection pruning means only the selected columns will be read from
   * the data source — critical for Parquet/ORC columnar formats where
   * skipping columns avoids I/O entirely.
   */
  select(...columns: Array<Column | string>): DataFrame {
    const exprs = columns.map((c) => (typeof c === "string" ? col(c)._expr : c._expr));
    return DataFrame._fromPlan(this._session, {
      type: "project",
      child: this._plan,
      expressions: exprs,
    });
  }

  /**
   * Group by one or more columns, returning a GroupedData handle that
   * exposes aggregation methods (agg, count, sum, avg, etc.).
   *
   * Maps to the first half of Catalyst's `Aggregate` node — the grouping
   * expressions.  The aggregation expressions are added when you call
   * .agg() on the returned GroupedData.
   */
  groupBy(...columns: Array<Column | string>): GroupedData {
    const groupExprs = columns.map((c) => (typeof c === "string" ? col(c)._expr : c._expr));
    return new GroupedData(this, groupExprs);
  }

  /**
   * Limit the number of rows.
   *
   * Maps to Catalyst's `LocalLimit` / `GlobalLimit` nodes.
   * On a cluster this still shuffles data to honour the global limit.
   */
  limit(n: number): DataFrame {
    return DataFrame._fromPlan(this._session, {
      type: "limit",
      child: this._plan,
      limit: n,
    });
  }

  /**
   * Sort by one or more columns. Each argument can be:
   *   - A string column name (ascending by default)
   *   - A Column object (ascending by default)
   *
   * For descending sort, use DataFrame.sort() with SortOrder objects
   * or call .orderBy() with a helper.
   *
   * Maps to Catalyst's `Sort(order, global=true, child)`.
   */
  sort(...columns: Array<Column | string>): DataFrame {
    const order: SortOrder[] = columns.map((c) => {
      const expr = typeof c === "string" ? col(c)._expr : c._expr;
      if (typeof c !== "string" && expr.type === "sortOrder") {
        return {
          expression: expr.inner,
          direction: expr.direction,
          nullOrdering: expr.nullOrdering,
        };
      }
      return {
        expression: expr,
        direction: "ascending" as const,
        nullOrdering: "nulls_last" as const,
      };
    });
    return DataFrame._fromPlan(this._session, {
      type: "sort",
      child: this._plan,
      order,
      isGlobal: true,
    });
  }

  /** Alias for sort(). */
  orderBy(...columns: Array<Column | string>): DataFrame {
    return this.sort(...columns);
  }

  /**
   * Join with another DataFrame.
   *
   * @param other - The right side DataFrame
   * @param condition - Join condition (a boolean Column expression)
   * @param joinType - Type of join (default: "inner")
   *
   * Maps to Catalyst's `Join(left, right, joinType, condition)`.
   */
  join(
    other: DataFrame,
    condition?: Column,
    joinType:
      | "inner"
      | "full_outer"
      | "left_outer"
      | "right_outer"
      | "left_semi"
      | "left_anti"
      | "cross" = "inner",
  ): DataFrame {
    return DataFrame._fromPlan(this._session, {
      type: "join",
      left: this._plan,
      right: other._plan,
      condition: condition?._expr,
      joinType,
    });
  }

  /** Alias for join with joinType="cross". */
  crossJoin(other: DataFrame): DataFrame {
    return this.join(other, undefined, "cross");
  }

  /**
   * Drop one or more columns by name.
   *
   * Maps to Spark Connect's `Relation.Drop`.
   */
  drop(...columnNames: string[]): DataFrame {
    return DataFrame._fromPlan(this._session, {
      type: "drop",
      child: this._plan,
      columnNames,
    });
  }

  /**
   * Add or replace a column.
   *
   * @example df.withColumn("doubled", col("value").multiply(lit(2)))
   *
   * Maps to Spark Connect's `Relation.WithColumns`.
   */
  withColumn(name: string, expression: Column): DataFrame {
    return DataFrame._fromPlan(this._session, {
      type: "withColumns",
      child: this._plan,
      aliases: [{ name, expression: expression._expr }],
    });
  }

  /**
   * Add or replace multiple columns at once.
   */
  withColumns(colMap: Record<string, Column>): DataFrame {
    const aliases = Object.entries(colMap).map(([name, c]) => ({
      name,
      expression: c._expr,
    }));
    return DataFrame._fromPlan(this._session, {
      type: "withColumns",
      child: this._plan,
      aliases,
    });
  }

  /**
   * Remove duplicate rows, optionally considering only a subset of columns.
   *
   * Maps to Spark Connect's `Relation.Deduplicate`.
   */
  dropDuplicates(...columnNames: string[]): DataFrame {
    return DataFrame._fromPlan(this._session, {
      type: "deduplicate",
      child: this._plan,
      columnNames: columnNames.length > 0 ? columnNames : undefined,
      allColumnsAsKeys: columnNames.length === 0,
    });
  }

  /** Alias for dropDuplicates() with no arguments. */
  distinct(): DataFrame {
    return this.dropDuplicates();
  }

  /**
   * Skip the first N rows.
   *
   * Maps to Spark Connect's `Relation.Offset`.
   */
  offset(n: number): DataFrame {
    return DataFrame._fromPlan(this._session, {
      type: "offset",
      child: this._plan,
      offset: n,
    });
  }

  // ── Writer ─────────────────────────────────────────────────────────────────

  /** Returns a DataFrameWriter for persisting the contents of this DataFrame. */
  get write(): DataFrameWriter {
    return new DataFrameWriter(this);
  }

  /**
   * Register this DataFrame as a temporary view with the given name.
   * The view is session-scoped and will be dropped when the session ends.
   */
  async createOrReplaceTempView(viewName: string): Promise<void> {
    await this._session._executeCommand({
      type: "createDataframeView",
      plan: this._plan,
      name: viewName,
      isGlobal: false,
      replace: true,
    });
  }

  // ── Actions (eager — trigger plan execution) ──────────────────────────────

  /**
   * Execute the plan and collect ALL result rows into a JS array.
   *
   * This is the most critical code path in the entire client:
   *   1. The LogicalPlan tree is serialised to Spark Connect protobuf.
   *   2. The protobuf is sent over gRPC (via the injected Transport).
   *   3. The server responds with a stream of Arrow IPC record batches.
   *   4. Each batch is decoded from Arrow's columnar format into Row objects.
   *
   * ⚠️  MEMORY WARNING: collect() materialises the ENTIRE result set in the
   * Node.js heap.  For large datasets, prefer .toArrow() (returns raw Arrow
   * tables) or .forEach() (streaming row-by-row processing).
   */
  async collect(): Promise<Row[]> {
    const decoder = this._session._arrowDecoder;
    if (!decoder) {
      throw new Error(
        "No Arrow decoder configured. " +
          "Use @spark-js/node which provides one automatically, " +
          "or pass arrowDecoder in SparkSessionConfig.",
      );
    }

    const chunks: Uint8Array[] = [];
    for await (const batch of this._session._executePlan(this._plan)) {
      chunks.push(batch);
    }

    return decoder(chunks);
  }

  /**
   * Return the number of rows.  Equivalent to `SELECT COUNT(*) FROM ...`.
   * This adds an Aggregate(count) plan node rather than collecting all data.
   */
  async count(): Promise<number> {
    // Simplified: in production this would build a proper aggregate plan.
    const rows = await this.collect();
    return rows.length;
  }

  /**
   * Return the schema of the DataFrame as a plain object.
   * Uses the AnalyzePlan.Schema RPC to resolve column names and types
   * without executing the query.
   */
  async schema(): Promise<Record<string, unknown>> {
    const result = await this._session._analyzePlan({
      type: "schema",
      plan: this._plan,
    });
    return (result.schema as Record<string, unknown>) ?? {};
  }

  /**
   * Return the query execution plan as a string.
   *
   * @param mode - Explain mode: "simple", "extended", "codegen", "cost", "formatted"
   */
  async explain(
    mode: "simple" | "extended" | "codegen" | "cost" | "formatted" = "simple",
  ): Promise<string> {
    const result = await this._session._analyzePlan({
      type: "explain",
      plan: this._plan,
      mode,
    });
    return (result.explainString as string) ?? "";
  }

  /**
   * Print the schema to the console in a tree format.
   * Convenience method that calls schema() and formats the output.
   */
  async printSchema(): Promise<void> {
    const plan = await this.explain("formatted");
    console.log(plan);
  }

  /**
   * Pretty-print the first `numRows` rows to the console as an ASCII table.
   *
   * Mirrors PySpark's `df.show()` behaviour. If `truncate` is true,
   * strings longer than 20 characters are truncated with `...`.
   */
  async show(numRows = 20, truncate = true): Promise<void> {
    const limited = this.limit(numRows);
    const rows = await limited.collect();

    if (rows.length === 0) {
      console.log("(empty DataFrame)");
      return;
    }

    const columns = Object.keys(rows[0]);
    const maxWidth = truncate ? 20 : Infinity;

    const fmt = (val: unknown): string => {
      if (val === null || val === undefined) return "null";
      const s =
        typeof val === "object"
          ? JSON.stringify(val)
          : String(val as string | number | boolean | bigint);
      return s.length > maxWidth ? s.slice(0, maxWidth - 3) + "..." : s;
    };

    // Compute column widths
    const widths = columns.map((col) => {
      const headerLen = col.length;
      const dataLen = rows.reduce((max, row) => Math.max(max, fmt(row[col]).length), 0);
      return Math.max(headerLen, dataLen);
    });

    const sep = "+" + widths.map((w) => "-".repeat(w + 2)).join("+") + "+";
    const fmtRow = (vals: string[]) =>
      "|" + vals.map((v, i) => " " + v.padEnd(widths[i]) + " ").join("|") + "|";

    console.log(sep);
    console.log(fmtRow(columns));
    console.log(sep);
    for (const row of rows) {
      console.log(fmtRow(columns.map((c) => fmt(row[c]))));
    }
    console.log(sep);
  }
}
