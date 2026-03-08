/**
 * Collection / struct / array / map functions.
 */

import { Column, fnExpr, toExpr, _lit, type ColOrName, fn } from "./_helpers.js";

// Struct

export function struct(...columns: ColOrName[]): Column {
  return fn("struct", ...columns);
}

// Array functions

export function array(...columns: ColOrName[]): Column {
  return fn("array", ...columns);
}

export function array_contains(column: ColOrName, value: ColOrName): Column {
  return fn("array_contains", column, value);
}

export function array_distinct(column: ColOrName): Column {
  return fn("array_distinct", column);
}

export function array_union(col1: ColOrName, col2: ColOrName): Column {
  return fn("array_union", col1, col2);
}

export function array_intersect(col1: ColOrName, col2: ColOrName): Column {
  return fn("array_intersect", col1, col2);
}

export function array_except(col1: ColOrName, col2: ColOrName): Column {
  return fn("array_except", col1, col2);
}

export function array_join(column: ColOrName, delimiter: string, nullReplacement?: string): Column {
  const args = [toExpr(column), _lit(delimiter)._expr];
  if (nullReplacement !== undefined) args.push(_lit(nullReplacement)._expr);
  return new Column(fnExpr("array_join", ...args));
}

export function array_sort(column: ColOrName): Column {
  return fn("array_sort", column);
}

export function sort_array(column: ColOrName, asc = true): Column {
  return new Column(fnExpr("sort_array", toExpr(column), _lit(asc)._expr));
}

export function array_max(column: ColOrName): Column {
  return fn("array_max", column);
}

export function array_min(column: ColOrName): Column {
  return fn("array_min", column);
}

export function array_position(column: ColOrName, value: ColOrName): Column {
  return fn("array_position", column, value);
}

export function array_remove(column: ColOrName, element: ColOrName): Column {
  return fn("array_remove", column, element);
}

export function array_repeat(column: ColOrName, count: number): Column {
  return new Column(fnExpr("array_repeat", toExpr(column), _lit(count)._expr));
}

export function array_compact(column: ColOrName): Column {
  return fn("array_compact", column);
}

export function arrays_overlap(col1: ColOrName, col2: ColOrName): Column {
  return fn("arrays_overlap", col1, col2);
}

export function arrays_zip(...columns: ColOrName[]): Column {
  return fn("arrays_zip", ...columns);
}

export function flatten(column: ColOrName): Column {
  return fn("flatten", column);
}

export function sequence(start: ColOrName, stop: ColOrName, step?: ColOrName): Column {
  if (step !== undefined) return fn("sequence", start, stop, step);
  return fn("sequence", start, stop);
}

export function element_at(column: ColOrName, index: ColOrName): Column {
  return fn("element_at", column, index);
}

export function get(column: ColOrName, index: number): Column {
  return new Column(fnExpr("get", toExpr(column), _lit(index)._expr));
}

export function slice(column: ColOrName, start: number, length: number): Column {
  return new Column(fnExpr("slice", toExpr(column), _lit(start)._expr, _lit(length)._expr));
}

// Explode variants

export function explode(column: ColOrName): Column {
  return fn("explode", column);
}

export function explode_outer(column: ColOrName): Column {
  return fn("explode_outer", column);
}

export function posexplode(column: ColOrName): Column {
  return fn("posexplode", column);
}

export function posexplode_outer(column: ColOrName): Column {
  return fn("posexplode_outer", column);
}

export function inline(column: ColOrName): Column {
  return fn("inline", column);
}

export function inline_outer(column: ColOrName): Column {
  return fn("inline_outer", column);
}

export function stack(n: number, ...columns: ColOrName[]): Column {
  return new Column(fnExpr("stack", _lit(n)._expr, ...columns.map(toExpr)));
}

// Size

export function size(column: ColOrName): Column {
  return fn("size", column);
}

// Map functions

export function create_map(...columns: ColOrName[]): Column {
  return fn("map", ...columns);
}

export function map_keys(column: ColOrName): Column {
  return fn("map_keys", column);
}

export function map_values(column: ColOrName): Column {
  return fn("map_values", column);
}

export function map_entries(column: ColOrName): Column {
  return fn("map_entries", column);
}

export function map_from_entries(column: ColOrName): Column {
  return fn("map_from_entries", column);
}

export function map_from_arrays(keys: ColOrName, values: ColOrName): Column {
  return fn("map_from_arrays", keys, values);
}

export function map_concat(...columns: ColOrName[]): Column {
  return fn("map_concat", ...columns);
}
