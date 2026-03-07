/**
 * Hash functions.
 */

import { type ColOrName, fn } from "./_helpers.js";
import { Column, fnExpr, toExpr, _lit } from "./_helpers.js";

export function md5(column: ColOrName): Column {
  return fn("md5", column);
}

export function sha1(column: ColOrName): Column {
  return fn("sha1", column);
}

export function sha2(column: ColOrName, numBits: number): Column {
  return new Column(fnExpr("sha2", toExpr(column), _lit(numBits)._expr));
}

export function hash(...columns: ColOrName[]): Column {
  return fn("hash", ...columns);
}

export function xxhash64(...columns: ColOrName[]): Column {
  return fn("xxhash64", ...columns);
}

export function crc32(column: ColOrName): Column {
  return fn("crc32", column);
}
