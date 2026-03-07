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

export function bround(column: ColOrName, scale = 0): Column {
  return new Column(fnExpr("bround", toExpr(column), _lit(scale)._expr));
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

export function cbrt(column: ColOrName): Column {
  return fn("cbrt", column);
}

export function pow(column: ColOrName, exponent: number): Column {
  return new Column(fnExpr("power", toExpr(column), _lit(exponent)._expr));
}

export function log(column: ColOrName): Column {
  return fn("ln", column);
}

export function log2(column: ColOrName): Column {
  return fn("log2", column);
}

export function log10(column: ColOrName): Column {
  return fn("log10", column);
}

export function log1p(column: ColOrName): Column {
  return fn("log1p", column);
}

export function exp(column: ColOrName): Column {
  return fn("exp", column);
}

export function expm1(column: ColOrName): Column {
  return fn("expm1", column);
}

export function sin(column: ColOrName): Column {
  return fn("sin", column);
}

export function cos(column: ColOrName): Column {
  return fn("cos", column);
}

export function tan(column: ColOrName): Column {
  return fn("tan", column);
}

export function asin(column: ColOrName): Column {
  return fn("asin", column);
}

export function acos(column: ColOrName): Column {
  return fn("acos", column);
}

export function atan(column: ColOrName): Column {
  return fn("atan", column);
}

export function atan2(y: ColOrName, x: ColOrName): Column {
  return fn("atan2", y, x);
}

export function sinh(column: ColOrName): Column {
  return fn("sinh", column);
}

export function cosh(column: ColOrName): Column {
  return fn("cosh", column);
}

export function tanh(column: ColOrName): Column {
  return fn("tanh", column);
}

export function signum(column: ColOrName): Column {
  return fn("signum", column);
}

export const sign = signum;

export function degrees(column: ColOrName): Column {
  return fn("degrees", column);
}

export function radians(column: ColOrName): Column {
  return fn("radians", column);
}

export function factorial(column: ColOrName): Column {
  return fn("factorial", column);
}

export function greatest(...columns: ColOrName[]): Column {
  return fn("greatest", ...columns);
}

export function least(...columns: ColOrName[]): Column {
  return fn("least", ...columns);
}

export function hex(column: ColOrName): Column {
  return fn("hex", column);
}

export function unhex(column: ColOrName): Column {
  return fn("unhex", column);
}

export function bin(column: ColOrName): Column {
  return fn("bin", column);
}

export function rand(seed?: number): Column {
  return seed !== undefined
    ? new Column(fnExpr("rand", _lit(seed)._expr))
    : new Column(fnExpr("rand"));
}

export function randn(seed?: number): Column {
  return seed !== undefined
    ? new Column(fnExpr("randn", _lit(seed)._expr))
    : new Column(fnExpr("randn"));
}

export function pmod(dividend: ColOrName, divisor: ColOrName): Column {
  return fn("pmod", dividend, divisor);
}

export function rint(column: ColOrName): Column {
  return fn("rint", column);
}

export function hypot(a: ColOrName, b: ColOrName): Column {
  return fn("hypot", a, b);
}

export function negate(column: ColOrName): Column {
  return fn("negative", column);
}

export const negative = negate;

export function positive(column: ColOrName): Column {
  return fn("positive", column);
}

export function e(): Column {
  return new Column(fnExpr("e"));
}

export function pi(): Column {
  return new Column(fnExpr("pi"));
}
