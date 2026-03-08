/**
 * Hash functions.
 */

import { type ColOrName, fn } from "./_helpers.js";
import { Column, fnExpr, toExpr, _lit } from "./_helpers.js";

/** Calculates the MD5 digest of a binary column and returns the value as a 32 character hex string. */
export function md5(column: ColOrName): Column {
  return fn("md5", column);
}

/** Returns the hex string result of SHA-1. */
export function sha1(column: ColOrName): Column {
  return fn("sha1", column);
}

/** Returns the hex string result of SHA-2 family of hash functions (SHA-224, SHA-256, SHA-384, and SHA-512). */
export function sha2(column: ColOrName, numBits: number): Column {
  return new Column(fnExpr("sha2", toExpr(column), _lit(numBits)._expr));
}

/** Calculates the hash code of given columns, and returns the result as an int column. */
export function hash(...columns: ColOrName[]): Column {
  return fn("hash", ...columns);
}

/** Calculates the hash code of given columns using the 64-bit variant of the xxHash algorithm, and returns the result as a long column. */
export function xxhash64(...columns: ColOrName[]): Column {
  return fn("xxhash64", ...columns);
}

/** Calculates the cyclic redundancy check value (CRC-32) of a binary column and returns the value as a bigint. */
export function crc32(column: ColOrName): Column {
  return fn("crc32", column);
}
