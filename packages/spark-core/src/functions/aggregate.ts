/**
 * Aggregate functions.
 */

import { Column, fnExpr, toExpr, _lit, type ColOrName, fn } from "./_helpers.js";

export function count(column: ColOrName): Column {
  return fn("count", column);
}

export function sum(column: ColOrName): Column {
  return fn("sum", column);
}

export function avg(column: ColOrName): Column {
  return fn("avg", column);
}

export const mean = avg;

export function min(column: ColOrName): Column {
  return fn("min", column);
}

export function max(column: ColOrName): Column {
  return fn("max", column);
}

export function countDistinct(column: ColOrName, ...more: ColOrName[]): Column {
  return new Column({
    type: "unresolvedFunction",
    name: "count",
    arguments: [column, ...more].map(toExpr),
    isDistinct: true,
  });
}

export function sum_distinct(column: ColOrName): Column {
  return new Column({
    type: "unresolvedFunction",
    name: "sum",
    arguments: [toExpr(column)],
    isDistinct: true,
  });
}

export function first(column: ColOrName, ignorenulls = false): Column {
  return new Column(fnExpr("first", toExpr(column), _lit(ignorenulls)._expr));
}

export const first_value = first;

export function last(column: ColOrName, ignorenulls = false): Column {
  return new Column(fnExpr("last", toExpr(column), _lit(ignorenulls)._expr));
}

export const last_value = last;

export function collect_list(column: ColOrName): Column {
  return fn("collect_list", column);
}

export function collect_set(column: ColOrName): Column {
  return fn("collect_set", column);
}

export function array_agg(column: ColOrName): Column {
  return fn("array_agg", column);
}

export function stddev(column: ColOrName): Column {
  return fn("stddev", column);
}

export function stddev_pop(column: ColOrName): Column {
  return fn("stddev_pop", column);
}

export function stddev_samp(column: ColOrName): Column {
  return fn("stddev_samp", column);
}

export function var_pop(column: ColOrName): Column {
  return fn("var_pop", column);
}

export function var_samp(column: ColOrName): Column {
  return fn("var_samp", column);
}

export const variance = var_samp;

export function corr(col1: ColOrName, col2: ColOrName): Column {
  return fn("corr", col1, col2);
}

export function covar_pop(col1: ColOrName, col2: ColOrName): Column {
  return fn("covar_pop", col1, col2);
}

export function covar_samp(col1: ColOrName, col2: ColOrName): Column {
  return fn("covar_samp", col1, col2);
}

export function approx_count_distinct(column: ColOrName, rsd?: number): Column {
  if (rsd !== undefined) {
    return new Column(fnExpr("approx_count_distinct", toExpr(column), _lit(rsd)._expr));
  }
  return fn("approx_count_distinct", column);
}

export function skewness(column: ColOrName): Column {
  return fn("skewness", column);
}

export function kurtosis(column: ColOrName): Column {
  return fn("kurtosis", column);
}

export function product(column: ColOrName): Column {
  return fn("product", column);
}

export function any_value(column: ColOrName, ignorenulls = false): Column {
  return new Column(fnExpr("any_value", toExpr(column), _lit(ignorenulls)._expr));
}

export function count_if(column: ColOrName): Column {
  return fn("count_if", column);
}

export function max_by(column: ColOrName, ord: ColOrName): Column {
  return fn("max_by", column, ord);
}

export function min_by(column: ColOrName, ord: ColOrName): Column {
  return fn("min_by", column, ord);
}

export function percentile_approx(
  column: ColOrName,
  percentage: number | number[],
  accuracy = 10000,
): Column {
  const pctExpr = Array.isArray(percentage)
    ? fnExpr("array", ...percentage.map((p) => _lit(p)._expr))
    : _lit(percentage)._expr;
  return new Column(fnExpr("percentile_approx", toExpr(column), pctExpr, _lit(accuracy)._expr));
}

export function median(column: ColOrName): Column {
  return fn("median", column);
}

export function mode(column: ColOrName): Column {
  return fn("mode", column);
}

export function grouping(column: ColOrName): Column {
  return fn("grouping", column);
}

export function grouping_id(...columns: ColOrName[]): Column {
  return fn("grouping_id", ...columns);
}

export function bool_and(column: ColOrName): Column {
  return fn("bool_and", column);
}

export const every = bool_and;

export function bool_or(column: ColOrName): Column {
  return fn("bool_or", column);
}

export const some = bool_or;

export function bit_and(column: ColOrName): Column {
  return fn("bit_and", column);
}

export function bit_or(column: ColOrName): Column {
  return fn("bit_or", column);
}

export function bit_xor(column: ColOrName): Column {
  return fn("bit_xor", column);
}
