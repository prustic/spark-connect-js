/**
 * Bitwise functions.
 */

import { Column, fnExpr, toExpr, _lit, type ColOrName, fn } from "./_helpers.js";

/** Computes bitwise NOT of a number. */
export function bitwise_not(column: ColOrName): Column {
  return fn("~", column);
}

/** Returns the number of bits that are set in the argument. */
export function bit_count(column: ColOrName): Column {
  return fn("bit_count", column);
}

/** Returns the value of the bit (0 or 1) at the specified position. */
export function bit_get(column: ColOrName, pos: ColOrName): Column {
  return fn("bit_get", column, pos);
}

/** Alias for {@link bit_get}. */
export const getbit = bit_get;

/** Shifts the given value numBits left. */
export function shiftleft(column: ColOrName, numBits: number): Column {
  return new Column(fnExpr("shiftleft", toExpr(column), _lit(numBits)._expr));
}

/** (Signed) shifts the given value numBits right. */
export function shiftright(column: ColOrName, numBits: number): Column {
  return new Column(fnExpr("shiftright", toExpr(column), _lit(numBits)._expr));
}

/** Unsigned shifts the given value numBits right. */
export function shiftrightunsigned(column: ColOrName, numBits: number): Column {
  return new Column(fnExpr("shiftrightunsigned", toExpr(column), _lit(numBits)._expr));
}
