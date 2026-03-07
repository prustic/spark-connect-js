/**
 * String functions.
 */

import { Column, fnExpr, toExpr, _lit, type ColOrName, fn } from "./_helpers.js";

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
