/**
 * Conditional functions: when/otherwise, cast, coalesce, null-handling, predicates.
 */

import { Column, fnExpr, toExpr, type ColOrName, fn, _liftCol } from "./_helpers.js";
import { toCondition, type ColOrLiteral } from "../column.js";
import { InvalidInputError } from "../errors.js";

// when / otherwise

import type { Expression } from "../plan/logical-plan.js";

/**
 * Begins a CASE WHEN chain. Returns a {@link WhenBuilder}. Terminate the
 * chain with {@link WhenBuilder.otherwise} for an explicit default, or
 * {@link WhenBuilder.toColumn} for a `NULL` default.
 *
 * @example
 *   when(col("age").gt(lit(18)), lit("adult"))
 *     .when(col("age").gt(lit(12)), lit("teen"))
 *     .otherwise(lit("child"))
 */
export function when(condition: Column | string, value: ColOrLiteral): WhenBuilder {
  const cond = toCondition(condition, "when()");
  if (cond === undefined) {
    throw new InvalidInputError("when() requires a condition.");
  }

  return new WhenBuilder([{ condition: cond._expr, value: _liftCol(value)._expr }]);
}

/**
 * An open CASE WHEN chain. It is a {@link Column}, so it can be used directly
 * wherever a column is expected (non-matching rows yield `NULL`), or closed
 * with {@link Column.otherwise} for an explicit default.
 */
export class WhenBuilder extends Column {
  private readonly _branches: readonly { condition: Expression; value: Expression }[];

  /** @internal Obtained via {@link when}. */
  constructor(branches: { condition: Expression; value: Expression }[]) {
    super({ type: "caseWhen", branches: [...branches] });
    this._branches = [...branches];
  }

  override when(condition: Column | string, value: ColOrLiteral): WhenBuilder {
    const cond = toCondition(condition, "when()");
    if (cond === undefined) {
      throw new InvalidInputError("when() requires a condition.");
    }

    return new WhenBuilder([
      ...this._branches,
      { condition: cond._expr, value: _liftCol(value)._expr },
    ]);
  }

  /**
   * Convert to a plain Column with a `NULL` default.
   *
   * @deprecated A chain is already a Column; use it directly.
   */
  toColumn(): Column {
    return new Column(this._expr);
  }
}

// Cast

/**
 * Cast a column to the given data type string (e.g. "string", "int", "double").
 *
 * @example cast(col("id"), "string")
 */
export function cast(column: ColOrName, targetType: string): Column {
  return new Column({ type: "cast", inner: toExpr(column), targetType });
}

// Null-handling

/** Returns the first column that is not null, or null if all inputs are null. */
export function coalesce(...columns: ColOrName[]): Column {
  return fn("coalesce", ...columns);
}

/** Returns true if the column is null. */
export function isnull(column: ColOrName): Column {
  return fn("isnull", column);
}

/** Returns true if the column is NaN. */
export function isnan(column: ColOrName): Column {
  return fn("isnan", column);
}

/** Returns true if the column is not null. */
export function isnotnull(column: ColOrName): Column {
  return fn("isnotnull", column);
}

/** Returns col1 if it is not NaN, or col2 if col1 is NaN. */
export function nanvl(col1: ColOrName, col2: ColOrName): Column {
  return fn("nanvl", col1, col2);
}

/** Returns col2 if col1 is null, or col1 otherwise. */
export function ifnull(col1: ColOrName, col2: ColOrName): Column {
  return fn("ifnull", col1, col2);
}

/** Alias for {@link ifnull}. */
export const nvl = ifnull;

/** Returns col2 if col1 is not null, or col3 if col1 is null. */
export function nvl2(col1: ColOrName, col2: ColOrName, col3: ColOrName): Column {
  return fn("nvl2", col1, col2, col3);
}

/** Returns null if col1 equals col2, or col1 otherwise. */
export function nullif(col1: ColOrName, col2: ColOrName): Column {
  return fn("nullif", col1, col2);
}

// Expression / misc predicates

/** Parses a SQL expression string into a Column. */
export function expr(expression: string): Column {
  return new Column({ type: "expressionString", expression });
}

/** Returns a monotonically increasing 64-bit integer. Not consecutive across partitions. */
export function monotonically_increasing_id(): Column {
  return new Column(fnExpr("monotonically_increasing_id"));
}

/** Returns the partition ID for each row. */
export function spark_partition_id(): Column {
  return new Column(fnExpr("spark_partition_id"));
}

/** Returns the runtime data type of the column as a string. */
export function typeof_(column: ColOrName): Column {
  return fn("typeof", column);
}

/** Returns a universally unique identifier (UUID) string. */
export function uuid(): Column {
  return new Column(fnExpr("uuid"));
}

/** Marks a DataFrame as small enough for a broadcast join. */
export function broadcast(df: Column): Column {
  // broadcast is typically used at the DataFrame level via hint("broadcast"),
  // but as a function it just passes through the column
  return df;
}
