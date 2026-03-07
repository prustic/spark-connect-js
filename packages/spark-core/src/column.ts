/**
 * ─── Column ─────────────────────────────────────────────────────────────────
 *
 * Represents a column expression in Spark's expression tree.
 *
 * @see Spark source: sql/core/src/main/scala/org/apache/spark/sql/Column.scala
 * @see Catalyst expressions: sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/
 *
 * In the JVM, Column wraps a Catalyst Expression.  Every operation on a Column
 * (>, <, ===, +, alias) produces a NEW expression tree node, never mutating
 * the original.  The full tree is serialised as a Spark Connect `Expression`
 * protobuf when the plan is sent to the server.
 *
 * Critical design constraint:
 *   These expressions are NEVER evaluated in JavaScript.  They are
 *   descriptions of computation that the JVM will execute.  We are building
 *   an AST, not running a query engine.
 */

import type { Expression } from "./plan/logical-plan.js";
import type { WindowSpec } from "./window.js";

export class Column {
  /** @internal — the raw expression tree node */
  readonly _expr: Expression;

  constructor(expr: Expression) {
    this._expr = expr;
  }

  // ── Comparison operators ──────────────────────────────────────────────────
  // Each returns a new Column wrapping a comparison expression node.

  gt(other: Column): Column {
    return new Column({ type: "gt", left: this._expr, right: other._expr });
  }

  lt(other: Column): Column {
    return new Column({ type: "lt", left: this._expr, right: other._expr });
  }

  eq(other: Column): Column {
    return new Column({ type: "eq", left: this._expr, right: other._expr });
  }

  neq(other: Column): Column {
    return new Column({ type: "neq", left: this._expr, right: other._expr });
  }

  gte(other: Column): Column {
    return new Column({ type: "gte", left: this._expr, right: other._expr });
  }

  lte(other: Column): Column {
    return new Column({ type: "lte", left: this._expr, right: other._expr });
  }

  // ── Logical operators ─────────────────────────────────────────────────────

  and(other: Column): Column {
    return new Column({ type: "and", left: this._expr, right: other._expr });
  }

  or(other: Column): Column {
    return new Column({ type: "or", left: this._expr, right: other._expr });
  }

  // ── Arithmetic ────────────────────────────────────────────────────────────

  plus(other: Column): Column {
    return new Column({ type: "add", left: this._expr, right: other._expr });
  }

  minus(other: Column): Column {
    return new Column({ type: "subtract", left: this._expr, right: other._expr });
  }

  multiply(other: Column): Column {
    return new Column({ type: "multiply", left: this._expr, right: other._expr });
  }

  divide(other: Column): Column {
    return new Column({ type: "divide", left: this._expr, right: other._expr });
  }

  // ── Aliasing ──────────────────────────────────────────────────────────────

  /**
   * Rename this column expression.  Maps to Catalyst's `Alias(expr, name)`.
   */
  alias(name: string): Column {
    return new Column({ type: "alias", inner: this._expr, name });
  }

  as(name: string): Column {
    return this.alias(name);
  }

  // ── Cast ──────────────────────────────────────────────────────────────────

  /** Cast this column to the given type string (e.g. "string", "int", "double"). */
  cast(targetType: string): Column {
    return new Column({ type: "cast", inner: this._expr, targetType });
  }

  // ── Sort ordering ──────────────────────────────────────────────────────────

  /** Mark this column as ascending sort order. */
  asc(): Column {
    return new Column({
      type: "sortOrder",
      inner: this._expr,
      direction: "ascending",
      nullOrdering: "nulls_last",
    });
  }

  /** Mark this column as descending sort order. */
  desc(): Column {
    return new Column({
      type: "sortOrder",
      inner: this._expr,
      direction: "descending",
      nullOrdering: "nulls_last",
    });
  }

  /** Ascending sort, nulls first. */
  asc_nulls_first(): Column {
    return new Column({
      type: "sortOrder",
      inner: this._expr,
      direction: "ascending",
      nullOrdering: "nulls_first",
    });
  }

  /** Ascending sort, nulls last. */
  asc_nulls_last(): Column {
    return this.asc();
  }

  /** Descending sort, nulls first. */
  desc_nulls_first(): Column {
    return new Column({
      type: "sortOrder",
      inner: this._expr,
      direction: "descending",
      nullOrdering: "nulls_first",
    });
  }

  /** Descending sort, nulls last. */
  desc_nulls_last(): Column {
    return this.desc();
  }

  // ── Null checks ───────────────────────────────────────────────────────────

  /** Test whether this column is null. */
  isNull(): Column {
    return new Column({ type: "unresolvedFunction", name: "isnull", arguments: [this._expr] });
  }

  /** Test whether this column is not null. */
  isNotNull(): Column {
    return new Column({ type: "unresolvedFunction", name: "isnotnull", arguments: [this._expr] });
  }

  /** Test whether this column value is NaN. */
  isNaN(): Column {
    return new Column({ type: "unresolvedFunction", name: "isnan", arguments: [this._expr] });
  }

  // ── Null-safe equality ────────────────────────────────────────────────────

  /** Null-safe equality comparison (returns true when both sides are null). */
  eqNullSafe(other: Column): Column {
    return new Column({
      type: "unresolvedFunction",
      name: "<=>",
      arguments: [this._expr, other._expr],
    });
  }

  // ── Bitwise operators ─────────────────────────────────────────────────────

  /** Bitwise AND. */
  bitwiseAND(other: Column): Column {
    return new Column({
      type: "unresolvedFunction",
      name: "&",
      arguments: [this._expr, other._expr],
    });
  }

  /** Bitwise OR. */
  bitwiseOR(other: Column): Column {
    return new Column({
      type: "unresolvedFunction",
      name: "|",
      arguments: [this._expr, other._expr],
    });
  }

  /** Bitwise XOR. */
  bitwiseXOR(other: Column): Column {
    return new Column({
      type: "unresolvedFunction",
      name: "^",
      arguments: [this._expr, other._expr],
    });
  }

  // ── Substring ─────────────────────────────────────────────────────────────

  /** Extract a substring (1-based position). */
  substr(startPos: number, length: number): Column {
    return new Column({
      type: "unresolvedFunction",
      name: "substring",
      arguments: [
        this._expr,
        { type: "literal", value: startPos },
        { type: "literal", value: length },
      ],
    });
  }

  // ── Struct / Map / Array field access ─────────────────────────────────────

  /** Access a field in a StructType column by name. */
  getField(fieldName: string): Column {
    return new Column({
      type: "unresolvedFunction",
      name: "get_field",
      arguments: [this._expr, { type: "literal", value: fieldName }],
    });
  }

  /** Access an element in an ArrayType or MapType column by key/index. */
  getItem(key: number | string): Column {
    return new Column({
      type: "unresolvedFunction",
      name: "get",
      arguments: [this._expr, { type: "literal", value: key }],
    });
  }

  /** Add or replace a field in a StructType column. */
  withField(fieldName: string, col: Column): Column {
    return new Column({
      type: "unresolvedFunction",
      name: "with_field",
      arguments: [this._expr, { type: "literal", value: fieldName }, col._expr],
    });
  }

  /** Drop field(s) from a StructType column. */
  dropFields(...fieldNames: string[]): Column {
    const args: Expression[] = [
      this._expr,
      ...fieldNames.map((f): Expression => ({ type: "literal", value: f })),
    ];
    return new Column({
      type: "unresolvedFunction",
      name: "drop_fields",
      arguments: args,
    });
  }

  // ── Membership / range ────────────────────────────────────────────────────

  /** Test whether this column's value is in the given list. */
  isin(...values: Array<string | number | boolean | bigint | null>): Column {
    const args: Expression[] = [
      this._expr,
      ...values.map((v): Expression => ({ type: "literal", value: v })),
    ];
    return new Column({ type: "unresolvedFunction", name: "in", arguments: args });
  }

  /** Test whether this column's value is between lower and upper (inclusive). */
  between(lower: Column, upper: Column): Column {
    return this.gte(lower).and(this.lte(upper));
  }

  // ── String matching ───────────────────────────────────────────────────────

  /** SQL LIKE pattern match. */
  like(pattern: string): Column {
    return new Column({
      type: "unresolvedFunction",
      name: "like",
      arguments: [this._expr, { type: "literal", value: pattern }],
    });
  }

  /** Case-insensitive LIKE pattern match. */
  ilike(pattern: string): Column {
    return new Column({
      type: "unresolvedFunction",
      name: "ilike",
      arguments: [this._expr, { type: "literal", value: pattern }],
    });
  }

  /** SQL RLIKE (regex) pattern match. */
  rlike(pattern: string): Column {
    return new Column({
      type: "unresolvedFunction",
      name: "rlike",
      arguments: [this._expr, { type: "literal", value: pattern }],
    });
  }

  /** Test whether this string column starts with the given prefix. */
  startsWith(prefix: string): Column {
    return new Column({
      type: "unresolvedFunction",
      name: "startswith",
      arguments: [this._expr, { type: "literal", value: prefix }],
    });
  }

  /** Test whether this string column ends with the given suffix. */
  endsWith(suffix: string): Column {
    return new Column({
      type: "unresolvedFunction",
      name: "endswith",
      arguments: [this._expr, { type: "literal", value: suffix }],
    });
  }

  /** Test whether this string column contains the given substring. */
  contains(substr: string): Column {
    return new Column({
      type: "unresolvedFunction",
      name: "contains",
      arguments: [this._expr, { type: "literal", value: substr }],
    });
  }

  // ── Window ────────────────────────────────────────────────────────────────

  /** Apply a window specification to this (window function) column. */
  over(windowSpec: WindowSpec): Column {
    return new Column({
      type: "window",
      windowFunction: this._expr,
      partitionSpec: windowSpec._partitionSpec,
      orderSpec: windowSpec._orderSpec,
      frameSpec: windowSpec._frameSpec,
    });
  }
}

// ─── Convenience factories ──────────────────────────────────────────────────
// These mirror PySpark's `from pyspark.sql.functions import col, lit`

/**
 * Reference an unresolved column by name.
 *
 * "Unresolved" means the server will resolve it against the schema at
 * analysis time.  If the column doesn't exist, the JVM throws
 * AnalysisException — we can't catch that at compile time.
 */
export function col(name: string): Column {
  return new Column({ type: "unresolvedAttribute", name });
}

/**
 * Create a literal value expression.
 *
 * ⚠️  Type mapping caveat:
 *   JS `number` is always IEEE-754 float64.  If you pass an integer literal
 *   that Spark expects as LongType, precision can be silently lost for values
 *   > Number.MAX_SAFE_INTEGER (2^53 - 1).  For large integers, use BigInt
 *   literals and ensure the plan builder maps them to Spark's LongType.
 */
export function lit(value: string | number | boolean | bigint | null): Column {
  return new Column({ type: "literal", value });
}
