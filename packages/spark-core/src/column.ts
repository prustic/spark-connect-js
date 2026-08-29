import type { Expression } from "./plan/logical-plan.js";
import type { WindowSpec } from "./window.js";
import { InvalidInputError } from "./errors.js";

/**
 * Either a {@link Column} or a primitive literal that {@link lit} can wrap.
 * Methods on `Column` that accept `ColOrLiteral` auto-wrap primitives, so
 * `col("age").gt(18)` is equivalent to `col("age").gt(lit(18))`.
 */
export type ColOrLiteral = Column | string | number | boolean | bigint | null;

/**
 * Wrap a `ColOrLiteral` into a `Column`. Column values pass through.
 * Primitives get wrapped in `lit()`. Used by `Column` methods and by
 * DSL functions that accept either a Column or a primitive literal.
 *
 * @internal
 */
export function liftCol(v: ColOrLiteral): Column {
  return v instanceof Column ? v : lit(v);
}

// Local to avoid a cycle with functions/_helpers.ts, which imports Column.
function fnOf(name: string, ...args: Expression[]): Expression {
  return { type: "unresolvedFunction", name, arguments: args, isDistinct: false };
}

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
  // Primitive arguments are auto-wrapped via `lit(...)`.

  gt(other: ColOrLiteral): Column {
    return new Column({ type: "gt", left: this._expr, right: liftCol(other)._expr });
  }

  lt(other: ColOrLiteral): Column {
    return new Column({ type: "lt", left: this._expr, right: liftCol(other)._expr });
  }

  eq(other: ColOrLiteral): Column {
    return new Column({ type: "eq", left: this._expr, right: liftCol(other)._expr });
  }

  neq(other: ColOrLiteral): Column {
    return new Column({ type: "neq", left: this._expr, right: liftCol(other)._expr });
  }

  gte(other: ColOrLiteral): Column {
    return new Column({ type: "gte", left: this._expr, right: liftCol(other)._expr });
  }

  lte(other: ColOrLiteral): Column {
    return new Column({ type: "lte", left: this._expr, right: liftCol(other)._expr });
  }

  // Logical operators

  and(other: ColOrLiteral): Column {
    return new Column({ type: "and", left: this._expr, right: liftCol(other)._expr });
  }

  or(other: ColOrLiteral): Column {
    return new Column({ type: "or", left: this._expr, right: liftCol(other)._expr });
  }

  // Arithmetic

  plus(other: ColOrLiteral): Column {
    return new Column({ type: "add", left: this._expr, right: liftCol(other)._expr });
  }

  minus(other: ColOrLiteral): Column {
    return new Column({ type: "subtract", left: this._expr, right: liftCol(other)._expr });
  }

  multiply(other: ColOrLiteral): Column {
    return new Column({ type: "multiply", left: this._expr, right: liftCol(other)._expr });
  }

  divide(other: ColOrLiteral): Column {
    return new Column({ type: "divide", left: this._expr, right: liftCol(other)._expr });
  }

  /** Modulo. Equivalent to PySpark's `%` operator. */
  mod(other: ColOrLiteral): Column {
    return new Column(fnOf("%", this._expr, liftCol(other)._expr));
  }

  /** Raise to a power. Equivalent to PySpark's `**` operator. */
  pow(other: ColOrLiteral): Column {
    return new Column(fnOf("power", this._expr, liftCol(other)._expr));
  }

  /** Logical negation. Equivalent to PySpark's `~` operator. */
  not(): Column {
    return new Column(fnOf("not", this._expr));
  }

  /** Arithmetic negation. Equivalent to PySpark's unary `-` operator. */
  negate(): Column {
    return new Column(fnOf("negative", this._expr));
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

  /**
   * Rename this column expression.
   *
   * @see {@link alias} — this is an alias of it, for PySpark parity.
   */
  name(alias: string): Column {
    return this.alias(alias);
  }

  // Cast

  /** Cast this column to the given type string (e.g. "string", "int", "double"). */
  cast(targetType: string): Column {
    return new Column({ type: "cast", inner: this._expr, targetType });
  }

  /**
   * Cast this column to the given type string, yielding `NULL` instead of
   * failing when a value cannot be converted.
   */
  try_cast(targetType: string): Column {
    return new Column({ type: "cast", inner: this._expr, targetType, evalMode: "try" });
  }

  /**
   * Cast this column to the given type string.
   *
   * @see {@link cast} — this is an alias of it, for PySpark parity.
   */
  astype(targetType: string): Column {
    return this.cast(targetType);
  }

  // Conditionals

  /**
   * Add a condition to a CASE WHEN chain. Only valid on a column returned by
   * the `when` function or by another `when` call, and only before
   * {@link otherwise} closes the chain.
   *
   * @throws `InvalidInputError` when this column is not an open CASE WHEN chain.
   *
   * @example
   * ```ts
   * when(col("age").gt(18), lit("adult"))
   *   .when(col("age").gt(12), lit("teen"))
   *   .otherwise(lit("child"));
   * ```
   */
  when(condition: Column, value: ColOrLiteral): Column {
    const chain = this._openCaseWhen("when");

    return new Column({
      type: "caseWhen",
      branches: [...chain.branches, { condition: condition._expr, value: liftCol(value)._expr }],
    });
  }

  /**
   * Close a CASE WHEN chain with the value for non-matching rows. Without it
   * the chain yields `NULL` for those rows.
   *
   * @throws `InvalidInputError` when this column is not an open CASE WHEN chain.
   */
  otherwise(value: ColOrLiteral): Column {
    const chain = this._openCaseWhen("otherwise");

    return new Column({
      type: "caseWhen",
      branches: [...chain.branches],
      elseValue: liftCol(value)._expr,
    });
  }

  // Mirrors PySpark's INVALID_WHEN_USAGE: the chain must be open, so neither a
  // plain column nor one already closed by otherwise() accepts more branches.
  private _openCaseWhen(method: string): Extract<Expression, { type: "caseWhen" }> {
    if (this._expr.type !== "caseWhen") {
      throw new InvalidInputError(
        `${method}() can only be applied on a column previously generated by the when() function.`,
      );
    }
    if (this._expr.elseValue !== undefined) {
      throw new InvalidInputError(
        `${method}() cannot be applied once otherwise() is applied to the chain.`,
      );
    }

    return this._expr;
  }

  /**
   * Apply a transformation function to this column and return its result.
   * A composition helper: `f(this)`, evaluated client-side.
   */
  transform(f: (column: Column) => Column): Column {
    return f(this);
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
  eqNullSafe(other: ColOrLiteral): Column {
    return new Column({
      type: "unresolvedFunction",
      name: "<=>",
      arguments: [this._expr, liftCol(other)._expr],
    });
  }

  // Bitwise operators

  /** Bitwise AND. */
  bitwiseAND(other: ColOrLiteral): Column {
    return new Column({
      type: "unresolvedFunction",
      name: "&",
      arguments: [this._expr, liftCol(other)._expr],
    });
  }

  /** Bitwise OR. */
  bitwiseOR(other: ColOrLiteral): Column {
    return new Column({
      type: "unresolvedFunction",
      name: "|",
      arguments: [this._expr, liftCol(other)._expr],
    });
  }

  /** Bitwise XOR. */
  bitwiseXOR(other: ColOrLiteral): Column {
    return new Column({
      type: "unresolvedFunction",
      name: "^",
      arguments: [this._expr, liftCol(other)._expr],
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
  between(lower: ColOrLiteral, upper: ColOrLiteral): Column {
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
 * Wrap a JavaScript value as a literal column expression. `lit(null)` produces
 * a `NullType` null literal that can be re-typed with `.cast(...)`. Passing
 * `undefined` throws `InvalidInputError` (JS `undefined` has no Spark analog).
 *
 * @example
 * ```ts
 * df.withColumn("flag", lit(true));
 * df.filter(col("age").gte(lit(18)));
 * df.withColumn("name", lit(null).cast("string"));
 * ```
 *
 * @remarks
 * **Integer precision.** JavaScript `number` is IEEE-754 float64. Integer
 * values above `Number.MAX_SAFE_INTEGER` (2^53 - 1) lose precision. Pass
 * `bigint` literals when you need `LongType` semantics on the server.
 */
export function lit(value: string | number | boolean | bigint | null): Column {
  if (value === undefined) {
    throw new InvalidInputError(
      "lit(undefined) is not a valid literal. Pass null for a NULL literal " +
        "(and .cast(type) it if you need a specific type), or pass a concrete value.",
    );
  }
  return new Column({ type: "literal", value });
}
