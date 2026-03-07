/**
 * ─── Functions ──────────────────────────────────────────────────────────────
 *
 * Built-in functions that mirror PySpark's `pyspark.sql.functions` module.
 *
 * @see PySpark functions: python/pyspark/sql/functions/builtin.py
 * @see Spark built-in functions registry: sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/FunctionRegistry.scala
 *
 * All functions return Column objects wrapping expression AST nodes.
 * They are NEVER evaluated in JavaScript — the server resolves them.
 */

import { Column, col as _col, lit as _lit } from "./column.js";
import type { Expression } from "./plan/logical-plan.js";

// Re-export col and lit so users can import everything from functions
export { col, lit } from "./column.js";

// ─── Helpers ────────────────────────────────────────────────────────────────

type ColOrName = Column | string;

function toCol(c: ColOrName): Column {
  return typeof c === "string" ? _col(c) : c;
}

function toExpr(c: ColOrName): Expression {
  return toCol(c)._expr;
}

function fnExpr(name: string, ...args: Expression[]): Expression {
  return { type: "unresolvedFunction", name, arguments: args };
}

function fn(name: string, ...args: ColOrName[]): Column {
  return new Column(fnExpr(name, ...args.map(toExpr)));
}

// ─── Conditional ────────────────────────────────────────────────────────────

/**
 * Begins a CASE WHEN chain. Use `.when()` / `.otherwise()` on the result.
 *
 * @example
 *   when(col("age").gt(lit(18)), lit("adult"))
 *     .when(col("age").gt(lit(12)), lit("teen"))
 *     .otherwise(lit("child"))
 */
export function when(condition: Column, value: Column): WhenBuilder {
  return new WhenBuilder([{ condition, value }]);
}

export class WhenBuilder {
  /** @internal */
  private readonly _branches: Array<{ condition: Column; value: Column }>;

  constructor(branches: Array<{ condition: Column; value: Column }>) {
    this._branches = branches;
  }

  when(condition: Column, value: Column): WhenBuilder {
    return new WhenBuilder([...this._branches, { condition, value }]);
  }

  otherwise(value: Column): Column {
    // Build: CASE WHEN c1 THEN v1 WHEN c2 THEN v2 ... ELSE default END
    // Spark maps this to the "when" function with alternating condition/value args + else
    const args: Expression[] = [];
    for (const branch of this._branches) {
      args.push(branch.condition._expr, branch.value._expr);
    }
    args.push(value._expr);
    return new Column(fnExpr("when", ...args));
  }

  /** Convert to Column without an otherwise clause (NULL for non-matching rows). */
  toColumn(): Column {
    const args: Expression[] = [];
    for (const branch of this._branches) {
      args.push(branch.condition._expr, branch.value._expr);
    }
    return new Column(fnExpr("when", ...args));
  }
}

// ─── Cast ───────────────────────────────────────────────────────────────────

/**
 * Cast a column to the given data type string (e.g. "string", "int", "double").
 *
 * @example cast(col("id"), "string")
 */
export function cast(column: ColOrName, targetType: string): Column {
  return new Column({ type: "cast", inner: toExpr(column), targetType });
}

// ─── String functions ───────────────────────────────────────────────────────

export function upper(column: ColOrName): Column {
  return fn("upper", column);
}

export function lower(column: ColOrName): Column {
  return fn("lower", column);
}

export function trim(column: ColOrName): Column {
  return fn("trim", column);
}

export function ltrim(column: ColOrName): Column {
  return fn("ltrim", column);
}

export function rtrim(column: ColOrName): Column {
  return fn("rtrim", column);
}

export function length(column: ColOrName): Column {
  return fn("length", column);
}

export function concat(...columns: ColOrName[]): Column {
  return fn("concat", ...columns);
}

export function substring(column: ColOrName, pos: number, len: number): Column {
  return new Column(fnExpr("substring", toExpr(column), _lit(pos)._expr, _lit(len)._expr));
}

export function regexp_replace(column: ColOrName, pattern: string, replacement: string): Column {
  return new Column(
    fnExpr("regexp_replace", toExpr(column), _lit(pattern)._expr, _lit(replacement)._expr),
  );
}

export function contains(column: ColOrName, value: string): Column {
  return new Column(fnExpr("contains", toExpr(column), _lit(value)._expr));
}

export function startswith(column: ColOrName, prefix: string): Column {
  return new Column(fnExpr("startswith", toExpr(column), _lit(prefix)._expr));
}

export function endswith(column: ColOrName, suffix: string): Column {
  return new Column(fnExpr("endswith", toExpr(column), _lit(suffix)._expr));
}

// ─── Date / Timestamp functions ─────────────────────────────────────────────

export function year(column: ColOrName): Column {
  return fn("year", column);
}

export function month(column: ColOrName): Column {
  return fn("month", column);
}

export function dayofmonth(column: ColOrName): Column {
  return fn("dayofmonth", column);
}

export function hour(column: ColOrName): Column {
  return fn("hour", column);
}

export function minute(column: ColOrName): Column {
  return fn("minute", column);
}

export function second(column: ColOrName): Column {
  return fn("second", column);
}

export function current_date(): Column {
  return new Column(fnExpr("current_date"));
}

export function current_timestamp(): Column {
  return new Column(fnExpr("current_timestamp"));
}

export function datediff(end: ColOrName, start: ColOrName): Column {
  return fn("datediff", end, start);
}

export function date_add(column: ColOrName, days: number): Column {
  return new Column(fnExpr("date_add", toExpr(column), _lit(days)._expr));
}

export function date_sub(column: ColOrName, days: number): Column {
  return new Column(fnExpr("date_sub", toExpr(column), _lit(days)._expr));
}

// ─── Math functions ─────────────────────────────────────────────────────────

export function abs(column: ColOrName): Column {
  return fn("abs", column);
}

export function round(column: ColOrName, scale = 0): Column {
  return new Column(fnExpr("round", toExpr(column), _lit(scale)._expr));
}

export function ceil(column: ColOrName): Column {
  return fn("ceil", column);
}

export function floor(column: ColOrName): Column {
  return fn("floor", column);
}

export function sqrt(column: ColOrName): Column {
  return fn("sqrt", column);
}

export function pow(column: ColOrName, exponent: number): Column {
  return new Column(fnExpr("power", toExpr(column), _lit(exponent)._expr));
}

export function log(column: ColOrName): Column {
  return fn("ln", column);
}

export function exp(column: ColOrName): Column {
  return fn("exp", column);
}

// ─── Aggregate functions ────────────────────────────────────────────────────

export function count(column: ColOrName): Column {
  return fn("count", column);
}

export function sum(column: ColOrName): Column {
  return fn("sum", column);
}

export function avg(column: ColOrName): Column {
  return fn("avg", column);
}

export function min(column: ColOrName): Column {
  return fn("min", column);
}

export function max(column: ColOrName): Column {
  return fn("max", column);
}

export function countDistinct(column: ColOrName, ...more: ColOrName[]): Column {
  return new Column({
    type: "unresolvedFunction",
    name: "count",
    arguments: [column, ...more].map(toExpr),
    isDistinct: true,
  });
}

// ─── Collection / Null functions ────────────────────────────────────────────

export function coalesce(...columns: ColOrName[]): Column {
  return fn("coalesce", ...columns);
}

export function isnull(column: ColOrName): Column {
  return fn("isnull", column);
}

export function isnan(column: ColOrName): Column {
  return fn("isnan", column);
}

// ─── Misc ───────────────────────────────────────────────────────────────────

export function struct(...columns: ColOrName[]): Column {
  return fn("struct", ...columns);
}

export function array(...columns: ColOrName[]): Column {
  return fn("array", ...columns);
}

export function explode(column: ColOrName): Column {
  return fn("explode", column);
}

export function size(column: ColOrName): Column {
  return fn("size", column);
}

// ─── Window functions ───────────────────────────────────────────────────────

/** Returns the row number within a window partition (1-based). */
export function row_number(): Column {
  return new Column(fnExpr("row_number"));
}

/** Returns the rank within a window partition (gaps on ties). */
export function rank(): Column {
  return new Column(fnExpr("rank"));
}

/** Returns the rank within a window partition (no gaps on ties). */
export function dense_rank(): Column {
  return new Column(fnExpr("dense_rank"));
}

/** Returns the value N rows before the current row in a window partition. */
export function lag(column: ColOrName, offset = 1, defaultValue?: ColOrName): Column {
  const args = [toExpr(column), _lit(offset)._expr];
  if (defaultValue !== undefined) args.push(toExpr(defaultValue));
  return new Column(fnExpr("lag", ...args));
}

/** Returns the value N rows after the current row in a window partition. */
export function lead(column: ColOrName, offset = 1, defaultValue?: ColOrName): Column {
  const args = [toExpr(column), _lit(offset)._expr];
  if (defaultValue !== undefined) args.push(toExpr(defaultValue));
  return new Column(fnExpr("lead", ...args));
}

/** Divides the ordered window partition into n buckets. */
export function ntile(n: number): Column {
  return new Column(fnExpr("ntile", _lit(n)._expr));
}
