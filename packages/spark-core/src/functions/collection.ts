/**
 * Collection / struct / array / map functions.
 */

import {
  Column,
  fnExpr,
  toExpr,
  toNumberOrExpr,
  _lit,
  _liftCol,
  type ColOrName,
  fn,
} from "./_helpers.js";
import type { ColOrLiteral } from "../column.js";

// Struct

/** Creates a new struct column from a set of columns. */
export function struct(...columns: ColOrName[]): Column {
  return fn("struct", ...columns);
}

// Array functions

/** Creates a new array column from the given columns. */
export function array(...columns: ColOrName[]): Column {
  return fn("array", ...columns);
}

/**
 * Returns true if the array contains `value`. As in PySpark, a string `value`
 * is a literal. Pass a Column to compare against another column.
 */
export function array_contains(column: ColOrName, value: ColOrLiteral): Column {
  return new Column(fnExpr("array_contains", toExpr(column), _liftCol(value)._expr));
}

/** Removes duplicate values from the array. */
export function array_distinct(column: ColOrName): Column {
  return fn("array_distinct", column);
}

/** Returns the union of two arrays without duplicates. */
export function array_union(col1: ColOrName, col2: ColOrName): Column {
  return fn("array_union", col1, col2);
}

/** Returns the intersection of two arrays without duplicates. */
export function array_intersect(col1: ColOrName, col2: ColOrName): Column {
  return fn("array_intersect", col1, col2);
}

/** Returns the elements in col1 but not in col2, without duplicates. */
export function array_except(col1: ColOrName, col2: ColOrName): Column {
  return fn("array_except", col1, col2);
}

/** Concatenates the elements of the given array using the delimiter. */
export function array_join(column: ColOrName, delimiter: string, nullReplacement?: string): Column {
  const args = [toExpr(column), _lit(delimiter)._expr];
  if (nullReplacement !== undefined) args.push(_lit(nullReplacement)._expr);
  return new Column(fnExpr("array_join", ...args));
}

/** Sorts the input array in ascending order. The elements must be orderable. */
export function array_sort(column: ColOrName): Column {
  return fn("array_sort", column);
}

/** Sorts the input array in ascending or descending order. */
export function sort_array(column: ColOrName, asc = true): Column {
  return new Column(fnExpr("sort_array", toExpr(column), _lit(asc)._expr));
}

/** Returns the maximum value of the array. */
export function array_max(column: ColOrName): Column {
  return fn("array_max", column);
}

/** Returns the minimum value of the array. */
export function array_min(column: ColOrName): Column {
  return fn("array_min", column);
}

/**
 * Returns the 1-based position of the first element equal to `value`, or 0.
 * As in PySpark, a string `value` is a literal.
 */
export function array_position(column: ColOrName, value: ColOrLiteral): Column {
  return new Column(fnExpr("array_position", toExpr(column), _liftCol(value)._expr));
}

/**
 * Removes every element equal to `element` from the array. As in PySpark, a
 * string `element` is a literal.
 */
export function array_remove(column: ColOrName, element: ColOrLiteral): Column {
  return new Column(fnExpr("array_remove", toExpr(column), _liftCol(element)._expr));
}

/** Creates an array containing the given element repeated count times. */
export function array_repeat(column: ColOrName, count: ColOrName | number): Column {
  return new Column(fnExpr("array_repeat", toExpr(column), toNumberOrExpr(count)));
}

/** Removes null values from the array. */
export function array_compact(column: ColOrName): Column {
  return fn("array_compact", column);
}

/** Returns true if the two arrays contain any common non-null element. */
export function arrays_overlap(col1: ColOrName, col2: ColOrName): Column {
  return fn("arrays_overlap", col1, col2);
}

/** Returns a merged array of structs from the given arrays. */
export function arrays_zip(...columns: ColOrName[]): Column {
  return fn("arrays_zip", ...columns);
}

/** Creates a single array from an array of arrays. */
export function flatten(column: ColOrName): Column {
  return fn("flatten", column);
}

/** Generates a sequence of integers from start to stop (inclusive), with an optional step. */
export function sequence(start: ColOrName, stop: ColOrName, step?: ColOrName): Column {
  if (step !== undefined) return fn("sequence", start, stop, step);
  return fn("sequence", start, stop);
}

/**
 * Returns the element at the given 1-based index of an array, or the value for
 * the given key of a map. As in PySpark and Scala, a string or number `index`
 * is a literal key. Pass a Column to look up by another column's value.
 */
export function element_at(column: ColOrName, index: ColOrLiteral): Column {
  return new Column(fnExpr("element_at", toExpr(column), _liftCol(index)._expr));
}

/** Returns the element at the given (0-based) index in the array. */
export function get(column: ColOrName, index: ColOrName | number): Column {
  return new Column(fnExpr("get", toExpr(column), toNumberOrExpr(index)));
}

/** Returns a portion of the array starting at the given index with the given length. */
export function slice(
  column: ColOrName,
  start: ColOrName | number,
  length: ColOrName | number,
): Column {
  return new Column(fnExpr("slice", toExpr(column), toNumberOrExpr(start), toNumberOrExpr(length)));
}

// Explode variants

/** Creates a new row for each element in the given array or map column. */
export function explode(column: ColOrName): Column {
  return fn("explode", column);
}

/** Creates a new row for each element in the given array or map column, including null and empty values. */
export function explode_outer(column: ColOrName): Column {
  return fn("explode_outer", column);
}

/** Creates a new row for each element with position in the given array or map column. */
export function posexplode(column: ColOrName): Column {
  return fn("posexplode", column);
}

/** Creates a new row for each element with position in the given array or map column, including null and empty values. */
export function posexplode_outer(column: ColOrName): Column {
  return fn("posexplode_outer", column);
}

/** Explodes an array of structs into a table. */
export function inline(column: ColOrName): Column {
  return fn("inline", column);
}

/** Explodes an array of structs into a table, including null and empty values. */
export function inline_outer(column: ColOrName): Column {
  return fn("inline_outer", column);
}

/** Separates the expressions into n groups, filling from left to right. */
export function stack(n: number, ...columns: ColOrName[]): Column {
  return new Column(fnExpr("stack", _lit(n)._expr, ...columns.map(toExpr)));
}

// Size

/** Returns the length of the array or map stored in the column. */
export function size(column: ColOrName): Column {
  return fn("size", column);
}

// Map functions

/** Creates a new map column from an even number of input columns (alternating key, value). */
export function create_map(...columns: ColOrName[]): Column {
  return fn("map", ...columns);
}

/** Returns an unordered array containing the keys of the map. */
export function map_keys(column: ColOrName): Column {
  return fn("map_keys", column);
}

/** Returns an unordered array containing the values of the map. */
export function map_values(column: ColOrName): Column {
  return fn("map_values", column);
}

/** Returns an unordered array of all entries in the given map. */
export function map_entries(column: ColOrName): Column {
  return fn("map_entries", column);
}

/** Creates a map from an array of entries (key-value struct pairs). */
export function map_from_entries(column: ColOrName): Column {
  return fn("map_from_entries", column);
}

/** Creates a new map from two arrays (keys and values). */
export function map_from_arrays(keys: ColOrName, values: ColOrName): Column {
  return fn("map_from_arrays", keys, values);
}

/** Merges all the given maps into a single map. */
export function map_concat(...columns: ColOrName[]): Column {
  return fn("map_concat", ...columns);
}
