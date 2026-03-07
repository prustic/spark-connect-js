/**
 * Conditional functions: when/otherwise, cast, coalesce.
 */

import { Column, fnExpr, toExpr, type ColOrName, fn } from "./_helpers.js";

// ─── when / otherwise ───────────────────────────────────────────────────────

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

// ─── Cast ───────────────────────────────────────────────────────────────────

/**
 * Cast a column to the given data type string (e.g. "string", "int", "double").
 *
 * @example cast(col("id"), "string")
 */
export function cast(column: ColOrName, targetType: string): Column {
  return new Column({ type: "cast", inner: toExpr(column), targetType });
}

// ─── Null-handling ──────────────────────────────────────────────────────────

export function coalesce(...columns: ColOrName[]): Column {
  return fn("coalesce", ...columns);
}

export function isnull(column: ColOrName): Column {
  return fn("isnull", column);
}

export function isnan(column: ColOrName): Column {
  return fn("isnan", column);
}
