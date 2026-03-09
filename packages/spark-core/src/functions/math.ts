/**
 * Math functions.
 */

import { Column, fnExpr, toExpr, _lit, type ColOrName, fn } from "./_helpers.js";

/** Computes the absolute value of a numeric value. */
export function abs(column: ColOrName): Column {
  return fn("abs", column);
}

/** Rounds the value of a column to the given number of decimal places using HALF_UP rounding mode. */
export function round(column: ColOrName, scale = 0): Column {
  return new Column(fnExpr("round", toExpr(column), _lit(scale)._expr));
}

/** Rounds the value of a column to the given number of decimal places using HALF_EVEN rounding mode (Banker's rounding). */
export function bround(column: ColOrName, scale = 0): Column {
  return new Column(fnExpr("bround", toExpr(column), _lit(scale)._expr));
}

/** Computes the ceiling of the given value. */
export function ceil(column: ColOrName): Column {
  return fn("ceil", column);
}

/** Computes the floor of the given value. */
export function floor(column: ColOrName): Column {
  return fn("floor", column);
}

/** Computes the square root of the specified float value. */
export function sqrt(column: ColOrName): Column {
  return fn("sqrt", column);
}

/** Computes the cube root of the given value. */
export function cbrt(column: ColOrName): Column {
  return fn("cbrt", column);
}

/** Returns the value of the first argument raised to the power of the second argument. */
export function pow(column: ColOrName, exponent: number): Column {
  return new Column(fnExpr("power", toExpr(column), _lit(exponent)._expr));
}

/** Computes the natural logarithm of the given value. */
export function log(column: ColOrName): Column {
  return fn("ln", column);
}

/** Computes the logarithm of the given value in base 2. */
export function log2(column: ColOrName): Column {
  return fn("log2", column);
}

/** Computes the logarithm of the given value in base 10. */
export function log10(column: ColOrName): Column {
  return fn("log10", column);
}

/** Computes the natural logarithm of the given value plus one. */
export function log1p(column: ColOrName): Column {
  return fn("log1p", column);
}

/** Computes the exponential of the given value. */
export function exp(column: ColOrName): Column {
  return fn("exp", column);
}

/** Computes the exponential of the given value minus one. */
export function expm1(column: ColOrName): Column {
  return fn("expm1", column);
}

/** Computes the sine of the given value. */
export function sin(column: ColOrName): Column {
  return fn("sin", column);
}

/** Computes the cosine of the given value. */
export function cos(column: ColOrName): Column {
  return fn("cos", column);
}

/** Computes the tangent of the given value. */
export function tan(column: ColOrName): Column {
  return fn("tan", column);
}

/** Computes the inverse sine (arc sine) of the given value. */
export function asin(column: ColOrName): Column {
  return fn("asin", column);
}

/** Computes the inverse cosine (arc cosine) of the given value. */
export function acos(column: ColOrName): Column {
  return fn("acos", column);
}

/** Computes the inverse tangent (arc tangent) of the given value. */
export function atan(column: ColOrName): Column {
  return fn("atan", column);
}

/** Computes the angle theta from the conversion of rectangular coordinates (x, y) to polar coordinates (r, theta). */
export function atan2(y: ColOrName, x: ColOrName): Column {
  return fn("atan2", y, x);
}

/** Computes the hyperbolic sine of the given value. */
export function sinh(column: ColOrName): Column {
  return fn("sinh", column);
}

/** Computes the hyperbolic cosine of the given value. */
export function cosh(column: ColOrName): Column {
  return fn("cosh", column);
}

/** Computes the hyperbolic tangent of the given value. */
export function tanh(column: ColOrName): Column {
  return fn("tanh", column);
}

/** Computes the signum of the given value. */
export function signum(column: ColOrName): Column {
  return fn("signum", column);
}

/** Alias for {@link signum}. */
export const sign = signum;

/** Converts an angle measured in radians to an approximately equivalent angle measured in degrees. */
export function degrees(column: ColOrName): Column {
  return fn("degrees", column);
}

/** Converts an angle measured in degrees to an approximately equivalent angle measured in radians. */
export function radians(column: ColOrName): Column {
  return fn("radians", column);
}

/** Computes the factorial of the given value. */
export function factorial(column: ColOrName): Column {
  return fn("factorial", column);
}

/** Returns the greatest value of the list of column names, skipping null values. */
export function greatest(...columns: ColOrName[]): Column {
  return fn("greatest", ...columns);
}

/** Returns the least value of the list of column names, skipping null values. */
export function least(...columns: ColOrName[]): Column {
  return fn("least", ...columns);
}

/** Computes hex value of the given column. */
export function hex(column: ColOrName): Column {
  return fn("hex", column);
}

/** Inverse of hex. Interprets each pair of characters as a hexadecimal number and converts to the byte representation. */
export function unhex(column: ColOrName): Column {
  return fn("unhex", column);
}

/** Returns the string representation of the binary value of the given long column. */
export function bin(column: ColOrName): Column {
  return fn("bin", column);
}

/** Generates a random column with independent and identically distributed (i.i.d.) samples uniformly distributed in [0.0, 1.0). */
export function rand(seed?: number): Column {
  return seed !== undefined
    ? new Column(fnExpr("rand", _lit(seed)._expr))
    : new Column(fnExpr("rand"));
}

/** Generates a column with independent and identically distributed (i.i.d.) samples from the standard normal distribution. */
export function randn(seed?: number): Column {
  return seed !== undefined
    ? new Column(fnExpr("randn", _lit(seed)._expr))
    : new Column(fnExpr("randn"));
}

/** Returns the positive value of dividend mod divisor. */
export function pmod(dividend: ColOrName, divisor: ColOrName): Column {
  return fn("pmod", dividend, divisor);
}

/** Returns the double value that is closest in value to the argument and is equal to a mathematical integer. */
export function rint(column: ColOrName): Column {
  return fn("rint", column);
}

/** Computes sqrt(a^2 + b^2) without intermediate overflow or underflow. */
export function hypot(a: ColOrName, b: ColOrName): Column {
  return fn("hypot", a, b);
}

/** Returns the negation of the value. */
export function negate(column: ColOrName): Column {
  return fn("negative", column);
}

/** Alias for {@link negate}. */
export const negative = negate;

/** Returns the value. */
export function positive(column: ColOrName): Column {
  return fn("positive", column);
}

/** Returns Euler's number. */
export function e(): Column {
  return new Column(fnExpr("e"));
}

/** Returns pi. */
export function pi(): Column {
  return new Column(fnExpr("pi"));
}
