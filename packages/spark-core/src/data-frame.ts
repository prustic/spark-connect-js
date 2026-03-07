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
import type { LogicalPlan, Expression, SortOrder } from "./plan/logical-plan.js";
import type { Row } from "./types/row.js";
import { Column, col } from "./column.js";
import { GroupedData } from "./grouped-data.js";
import { DataFrameWriter } from "./data-frame-writer.js";
import { StructType } from "./types/struct.js";

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
   * Rename a single column.
   *
   * Maps to Spark Connect's `Relation.WithColumnsRenamed`.
   */
  withColumnRenamed(existing: string, newName: string): DataFrame {
    return DataFrame._fromPlan(this._session, {
      type: "withColumnsRenamed",
      child: this._plan,
      renames: [{ colName: existing, newColName: newName }],
    });
  }

  /**
   * Rename multiple columns at once.
   *
   * @param colsMap - mapping of { existingName: newName }
   */
  withColumnsRenamed(colsMap: Record<string, string>): DataFrame {
    const renames = Object.entries(colsMap).map(([colName, newColName]) => ({
      colName,
      newColName,
    }));
    return DataFrame._fromPlan(this._session, {
      type: "withColumnsRenamed",
      child: this._plan,
      renames,
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

  // ── Set operations ────────────────────────────────────────────────────────

  /** Return a new DataFrame with rows from both this and other (duplicates kept). */
  union(other: DataFrame): DataFrame {
    return this._setOp(other, "union", true, false);
  }

  /** Alias for union(). */
  unionAll(other: DataFrame): DataFrame {
    return this.union(other);
  }

  /** Union by column name (rather than position), keeping duplicates. */
  unionByName(other: DataFrame, allowMissingColumns = false): DataFrame {
    return this._setOp(other, "union", true, true, allowMissingColumns);
  }

  /** Return rows present in both DataFrames (distinct). */
  intersect(other: DataFrame): DataFrame {
    return this._setOp(other, "intersect", false, false);
  }

  /** Return rows present in both DataFrames (duplicates kept). */
  intersectAll(other: DataFrame): DataFrame {
    return this._setOp(other, "intersect", true, false);
  }

  /** Return rows in this but not in other (distinct). */
  except(other: DataFrame): DataFrame {
    return this._setOp(other, "except", false, false);
  }

  /** Return rows in this but not in other (duplicates kept). */
  exceptAll(other: DataFrame): DataFrame {
    return this._setOp(other, "except", true, false);
  }

  /** @internal */
  private _setOp(
    other: DataFrame,
    opType: "union" | "intersect" | "except",
    isAll: boolean,
    byName: boolean,
    allowMissingColumns = false,
  ): DataFrame {
    return DataFrame._fromPlan(this._session, {
      type: "setOperation",
      left: this._plan,
      right: other._plan,
      opType,
      isAll,
      byName,
      allowMissingColumns,
    });
  }

  // ── Sampling ──────────────────────────────────────────────────────────────

  /** Return a random sample of rows. */
  sample(fraction: number, withReplacement = false, seed?: number): DataFrame {
    return DataFrame._fromPlan(this._session, {
      type: "sample",
      child: this._plan,
      lowerBound: 0.0,
      upperBound: fraction,
      withReplacement,
      seed,
    });
  }

  // ── Null handling ─────────────────────────────────────────────────────────

  /** Replace null values. If cols is empty, applies to all columns. */
  fillna(value: string | number | boolean, cols: string[] = []): DataFrame {
    return DataFrame._fromPlan(this._session, {
      type: "fillNa",
      child: this._plan,
      cols,
      values: [value],
    });
  }

  /** Drop rows with null values. */
  dropna(how: "any" | "all" = "any", cols: string[] = []): DataFrame {
    return DataFrame._fromPlan(this._session, {
      type: "dropNa",
      child: this._plan,
      cols,
      minNonNulls: how === "any" ? undefined : 1,
    });
  }

  // ── Column rename ─────────────────────────────────────────────────────────

  /** Return a new DataFrame with renamed columns (positional). */
  toDF(...columnNames: string[]): DataFrame {
    return DataFrame._fromPlan(this._session, {
      type: "toDF",
      child: this._plan,
      columnNames,
    });
  }

  // ── Aliasing ──────────────────────────────────────────────────────────────

  /**
   * Assign an alias to this DataFrame, useful for self-joins.
   *
   * Maps to Spark Connect's `Relation.SubqueryAlias`.
   */
  alias(name: string): DataFrame {
    return DataFrame._fromPlan(this._session, {
      type: "subqueryAlias",
      child: this._plan,
      alias: name,
    });
  }

  // ── Hints ─────────────────────────────────────────────────────────────────

  /**
   * Attach an optimizer hint to this DataFrame.
   *
   * @example df.hint("broadcast")
   * @example df.join(right.hint("broadcast"), ...)
   */
  hint(name: string, ...parameters: Array<string | number | boolean>): DataFrame {
    const paramExprs = parameters.map((p): Expression => ({ type: "literal", value: p }));
    return DataFrame._fromPlan(this._session, {
      type: "hint",
      child: this._plan,
      name,
      parameters: paramExprs,
    });
  }

  // ── Select with SQL expressions ──────────────────────────────────────────

  /**
   * Select columns using SQL expression strings.
   * Each string is parsed by the server as an expression.
   *
   * @example df.selectExpr("age * 2 as doubled_age", "name")
   */
  selectExpr(...exprs: string[]): DataFrame {
    const sqlExprs = exprs.map(
      (e): Expression => ({
        type: "expressionString",
        expression: e,
      }),
    );
    return DataFrame._fromPlan(this._session, {
      type: "project",
      child: this._plan,
      expressions: sqlExprs,
    });
  }

  // ── Transform ─────────────────────────────────────────────────────────────

  /**
   * Apply a user-defined function to this DataFrame and return the result.
   * This is purely client-side — it just calls `fn(this)`.
   *
   * Enables fluent pipeline composition:
   * @example df.transform(withDoubledAge).transform(withSalaryBand)
   */
  transform<T extends DataFrame>(fn: (df: DataFrame) => T): T {
    return fn(this);
  }

  // ── Sort within partitions ────────────────────────────────────────────────

  /**
   * Sort within each partition (non-global sort).
   *
   * Maps to Spark Connect's `Relation.Sort` with `isGlobal=false`.
   */
  sortWithinPartitions(...columns: Array<Column | string>): DataFrame {
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
      isGlobal: false,
    });
  }

  // ── Statistics ────────────────────────────────────────────────────────────

  /** Compute summary statistics (count, mean, stddev, min, max) for columns. */
  describe(...cols: string[]): DataFrame {
    return DataFrame._fromPlan(this._session, {
      type: "describe",
      child: this._plan,
      cols,
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
   * Node.js heap.  For large datasets, prefer .toLocalIterator() for
   * streaming row-by-row processing, or .forEach() for a callback approach.
   */
  async collect(): Promise<Row[]> {
    const decoder = this._ensureDecoder();

    const chunks: Uint8Array[] = [];
    for await (const batch of this._session._executePlan(this._plan)) {
      chunks.push(batch);
    }

    return decoder(chunks);
  }

  /**
   * Return the number of rows.  Equivalent to `SELECT COUNT(*) FROM ...`.
   *
   * Builds a proper `Aggregate(count(1))` plan so only a single scalar
   * is materialised — the full dataset is never collected into JS memory.
   */
  async count(): Promise<number> {
    const countPlan: LogicalPlan = {
      type: "aggregate",
      child: this._plan,
      groupingExpressions: [],
      aggregateExpressions: [
        {
          type: "alias",
          name: "count",
          inner: {
            type: "unresolvedFunction",
            name: "count",
            arguments: [{ type: "literal", value: 1 }],
          },
        },
      ],
    };

    const decoder = this._ensureDecoder();
    const chunks: Uint8Array[] = [];
    for await (const batch of this._session._executePlan(countPlan)) {
      chunks.push(batch);
    }
    const rows = await decoder(chunks);
    return (rows[0]?.count as number) ?? 0;
  }

  /**
   * Return an async iterator that yields rows one at a time as they arrive
   * from the Spark Connect server.  Each Arrow IPC chunk is decoded on the
   * fly, so only one batch needs to be in memory at a time.
   *
   * Use this instead of collect() for large result sets to avoid OOM.
   *
   * @example
   *   for await (const row of df.toLocalIterator()) {
   *     console.log(row);
   *   }
   */
  async *toLocalIterator(): AsyncIterableIterator<Row> {
    const decoder = this._ensureDecoder();

    for await (const chunk of this._session._executePlan(this._plan)) {
      const rows = await decoder([chunk]);
      for (const row of rows) {
        yield row;
      }
    }
  }

  /**
   * Process each row with a callback as it streams from the server.
   * Rows are decoded and passed to the callback one batch at a time,
   * avoiding full materialisation in memory.
   *
   * @example
   *   await df.forEach((row) => console.log(row.name, row.salary));
   */
  async forEach(fn: (row: Row) => void): Promise<void> {
    for await (const row of this.toLocalIterator()) {
      fn(row);
    }
  }

  /** @internal */
  private _ensureDecoder() {
    const decoder = this._session._arrowDecoder;
    if (!decoder) {
      throw new Error(
        "No Arrow decoder configured. " +
          "Use @spark-js/node which provides one automatically, " +
          "or pass arrowDecoder in SparkSessionConfig.",
      );
    }
    return decoder;
  }

  /**
   * Return the first row as a Row object, or null if the DataFrame is empty.
   */
  async first(): Promise<Row | null> {
    const rows = await this.limit(1).collect();
    return rows[0] ?? null;
  }

  /**
   * Return the first `n` rows as an array (alias for limit + collect).
   */
  async head(n = 1): Promise<Row[]> {
    return this.limit(n).collect();
  }

  /**
   * Return the first `n` rows as an array.
   * Alias for head() — matches PySpark's take() semantics.
   */
  async take(n: number): Promise<Row[]> {
    return this.head(n);
  }

  /**
   * Return the last `n` rows as an array.
   *
   * Maps to Spark Connect's `Relation.Tail`.
   */
  async tail(n: number): Promise<Row[]> {
    const tailDf = DataFrame._fromPlan(this._session, {
      type: "tail",
      child: this._plan,
      limit: n,
    });
    return tailDf.collect();
  }

  /**
   * Return the column names as a string array.
   * Uses the AnalyzePlan.Schema RPC to resolve the schema without executing.
   */
  async columns(): Promise<string[]> {
    const raw = await this.schema();
    const structType = StructType.fromProto(raw);
    return structType.fieldNames;
  }

  /**
   * Return column names and their data types as [name, type] pairs.
   * Uses the AnalyzePlan.Schema RPC.
   */
  async dtypes(): Promise<[string, string][]> {
    const raw = await this.schema();
    const structType = StructType.fromProto(raw);
    return structType.fields.map((f) => [f.name, f.dataType]);
  }

  /**
   * Returns true if the DataFrame has no rows.
   * Uses head(1) to check — stops after the first row.
   */
  async isEmpty(): Promise<boolean> {
    const rows = await this.head(1);
    return rows.length === 0;
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
    const raw = await this.schema();
    const structType = StructType.fromProto(raw);
    console.log(structType.treeString());
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
