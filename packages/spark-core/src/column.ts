import type { Expression } from "./plan/logical-plan.js";
import type { WindowSpec } from "./window.js";

/**
 * A reference to a column expression, typically obtained from {@link col} or
 * from methods on a {@link DataFrame} such as `df.col("name")`.
 *
 * Every method on `Column` returns a **new** `Column` that wraps a new
 * expression tree node; the original is never mutated. The combined tree is
 * serialised to a Spark Connect `Expression` protobuf when the plan is sent
 * to the server.
 *
 * @example Build a filter predicate
 * ```ts
 * import { col, lit } from "@spark-connect-js/core";
 *
 * const predicate = col("age").gte(lit(18)).and(col("country").eq(lit("NL")));
 * const adults = df.filter(predicate);
 * ```
 *
 * @example Build a sort key
 * ```ts
 * const sorted = df.orderBy(col("score").desc_nulls_last());
 * ```
 *
 * @see [Spark source: Column.scala](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/Column.scala)
 */
export class Column {
  /** @internal The raw expression tree node */
  readonly _expr: Expression;

  constructor(expr: Expression) {
    this._expr = expr;
  }

  // Comparison operators
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

  // Logical operators

  and(other: Column): Column {
    return new Column({ type: "and", left: this._expr, right: other._expr });
  }

  or(other: Column): Column {
    return new Column({ type: "or", left: this._expr, right: other._expr });
  }

  // Arithmetic

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

  // Aliasing

  /**
   * Rename this column expression.  Maps to Catalyst's `Alias(expr, name)`.
   */
  alias(name: string): Column {
    return new Column({ type: "alias", inner: this._expr, name });
  }

  as(name: string): Column {
    return this.alias(name);
  }

  // Cast

  /** Cast this column to the given type string (e.g. "string", "int", "double"). */
  cast(targetType: string): Column {
    return new Column({ type: "cast", inner: this._expr, targetType });
  }

  // Sort ordering

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

  // Null checks

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

  // Null-safe equality

  /** Null-safe equality comparison (returns true when both sides are null). */
  eqNullSafe(other: Column): Column {
    return new Column({
      type: "unresolvedFunction",
      name: "<=>",
      arguments: [this._expr, other._expr],
    });
  }

  // Bitwise operators

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

  // Substring

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

  // Struct / Map / Array field access

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

  // Membership / range

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

  // String matching

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

  // Window

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

// Convenience factories
// These mirror PySpark's `from pyspark.sql.functions import col, lit`

/**
 * Reference a column by name.
 *
 * The name is left unresolved client-side and bound against the DataFrame
 * schema on the server at analysis time. If the column does not exist, the
 * server raises `AnalysisException` when the plan is executed.
 *
 * @example
 * ```ts
 * import { col, lit } from "@spark-connect-js/core";
 *
 * df.filter(col("country").eq(lit("NL")));
 * ```
 *
 * @param name - Column name; may be a simple identifier or a dotted path
 *   (for example `"address.city"`) to reference a nested struct field.
 */
export function col(name: string): Column {
  return new Column({ type: "unresolvedAttribute", name });
}

/**
 * Wrap a JavaScript value as a literal column expression.
 *
 * @example
 * ```ts
 * df.withColumn("flag", lit(true));
 * df.filter(col("age").gte(lit(18)));
 * ```
 *
 * @remarks
 * **Integer precision.** JavaScript `number` is IEEE-754 float64. Integer
 * values above `Number.MAX_SAFE_INTEGER` (2^53 - 1) lose precision. Pass
 * `bigint` literals when you need `LongType` semantics on the server.
 */
export function lit(value: string | number | boolean | bigint | null): Column {
  return new Column({ type: "literal", value });
}
