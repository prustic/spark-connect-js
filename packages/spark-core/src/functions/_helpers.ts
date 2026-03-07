/**
 * Shared helpers for function modules.
 * @internal — not part of the public API.
 */

import { Column, col as _col, lit as _lit } from "../column.js";
import type { Expression } from "../plan/logical-plan.js";

export type ColOrName = Column | string;

export function toCol(c: ColOrName): Column {
  return typeof c === "string" ? _col(c) : c;
}

export function toExpr(c: ColOrName): Expression {
  return toCol(c)._expr;
}

export function fnExpr(name: string, ...args: Expression[]): Expression {
  return { type: "unresolvedFunction", name, arguments: args };
}

export function fn(name: string, ...args: ColOrName[]): Column {
  return new Column(fnExpr(name, ...args.map(toExpr)));
}

// Re-export for convenience in function modules
export { Column, _col, _lit };
