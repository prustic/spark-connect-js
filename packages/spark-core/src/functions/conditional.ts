/**
 * Conditional functions: when/otherwise, cast, coalesce, null-handling, predicates.
 */

import { Column, fnExpr, toExpr, type ColOrName, fn } from "./_helpers.js";

// when / otherwise

import type { Expression } from "../plan/logical-plan.js";

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
