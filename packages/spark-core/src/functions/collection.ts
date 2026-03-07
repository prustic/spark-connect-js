/**
 * Collection / struct / array / misc functions.
 */

import { type ColOrName, fn } from "./_helpers.js";
import type { Column } from "../column.js";

export function struct(...columns: ColOrName[]): Column {
  return fn("struct", ...columns);
}

export function array(...columns: ColOrName[]): Column {
  return fn("array", ...columns);
}

export function explode(column: ColOrName): Column {
  return fn("explode", column);
}

export function size(column: ColOrName): Column {
  return fn("size", column);
}
