/**
 * Shared helpers for function modules.
 * @internal Not part of the public API.
 */

import { Column, col as _col, lit as _lit, liftCol as _liftCol } from "../column.js";
import type { Expression } from "../plan/logical-plan.js";

export type ColOrName = Column | string;

export function toCol(c: ColOrName): Column {
  return typeof c === "string" ? _col(c) : c;
}

export function toExpr(c: ColOrName): Expression {
  return toCol(c)._expr;
}

/**
 * An argument PySpark types `Union[ColumnOrName, int]`: a number is a literal,
 * a string names a column.
 *
 * @internal
 */
export function toNumberOrExpr(v: ColOrName | number): Expression {
  return typeof v === "number" ? _lit(v)._expr : toExpr(v);
}

/**
 * A seed for a random function called without one. Picked on the client, as
 * PySpark and Scala do, so every execution of the plan draws the same values.
 *
 * @internal
 */
export function randomSeed(): Expression {
  const high = BigInt(Math.floor(Math.random() * 2 ** 31));
  const low = BigInt(Math.floor(Math.random() * 2 ** 32));

  return _lit((high << 32n) | low)._expr;
}

export function fnExpr(name: string, ...args: Expression[]): Expression {
  return { type: "unresolvedFunction", name, arguments: args };
}

export function fn(name: string, ...args: ColOrName[]): Column {
  return new Column(fnExpr(name, ...args.map(toExpr)));
}

// Re-export for convenience in function modules
export { Column, _col, _lit, _liftCol };
