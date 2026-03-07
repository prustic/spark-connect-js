/**
 * Bitwise functions.
 */

import { Column, fnExpr, toExpr, _lit, type ColOrName, fn } from "./_helpers.js";

export function bitwise_not(column: ColOrName): Column {
  return fn("~", column);
}

export function bit_count(column: ColOrName): Column {
  return fn("bit_count", column);
}

export function bit_get(column: ColOrName, pos: ColOrName): Column {
  return fn("bit_get", column, pos);
}

export const getbit = bit_get;

export function shiftleft(column: ColOrName, numBits: number): Column {
  return new Column(fnExpr("shiftleft", toExpr(column), _lit(numBits)._expr));
}

export function shiftright(column: ColOrName, numBits: number): Column {
  return new Column(fnExpr("shiftright", toExpr(column), _lit(numBits)._expr));
}

export function shiftrightunsigned(column: ColOrName, numBits: number): Column {
  return new Column(fnExpr("shiftrightunsigned", toExpr(column), _lit(numBits)._expr));
}
