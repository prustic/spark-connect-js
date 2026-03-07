/**
 * Math functions.
 */

import { Column, fnExpr, toExpr, _lit, type ColOrName, fn } from "./_helpers.js";

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
