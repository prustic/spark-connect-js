/**
 * Aggregate functions.
 */

import { Column, fnExpr, toExpr, _lit, type ColOrName, fn } from "./_helpers.js";

/** Returns the number of items in a group. */
export function count(column: ColOrName): Column {
  return fn("count", column);
}

/** Returns the sum of all values in the expression. */
export function sum(column: ColOrName): Column {
  return fn("sum", column);
}

/** Returns the average of the values in a group. */
export function avg(column: ColOrName): Column {
  return fn("avg", column);
}

/** Alias for {@link avg}. */
export const mean = avg;

/** Returns the minimum value of the expression in a group. */
export function min(column: ColOrName): Column {
  return fn("min", column);
}

/** Returns the maximum value of the expression in a group. */
export function max(column: ColOrName): Column {
  return fn("max", column);
}

/** Returns the number of distinct values in a group. */
export function countDistinct(column: ColOrName, ...more: ColOrName[]): Column {
  return new Column({
    type: "unresolvedFunction",
    name: "count",
    arguments: [column, ...more].map(toExpr),
    isDistinct: true,
  });
}

/** Returns the sum of distinct values in the expression. */
export function sum_distinct(column: ColOrName): Column {
  return new Column({
    type: "unresolvedFunction",
    name: "sum",
    arguments: [toExpr(column)],
    isDistinct: true,
  });
}

/** Returns the first value in a group. */
export function first(column: ColOrName, ignorenulls = false): Column {
  return new Column(fnExpr("first", toExpr(column), _lit(ignorenulls)._expr));
}

/** Alias for {@link first}. */
export const first_value = first;

/** Returns the last value in a group. */
export function last(column: ColOrName, ignorenulls = false): Column {
  return new Column(fnExpr("last", toExpr(column), _lit(ignorenulls)._expr));
}

/** Alias for {@link last}. */
export const last_value = last;

/** Returns a list of objects with duplicates. */
export function collect_list(column: ColOrName): Column {
  return fn("collect_list", column);
}

/** Returns a set of objects with duplicate elements eliminated. */
export function collect_set(column: ColOrName): Column {
  return fn("collect_set", column);
}

/** Returns a list of objects with duplicates. */
export function array_agg(column: ColOrName): Column {
  return fn("array_agg", column);
}

/** Returns the sample standard deviation of the expression in a group (alias for stddev_samp). */
export function stddev(column: ColOrName): Column {
  return fn("stddev", column);
}

/** Returns the population standard deviation of the expression in a group. */
export function stddev_pop(column: ColOrName): Column {
  return fn("stddev_pop", column);
}

/** Returns the unbiased sample standard deviation of the expression in a group. */
export function stddev_samp(column: ColOrName): Column {
  return fn("stddev_samp", column);
}

/** Returns the population variance of the values in a group. */
export function var_pop(column: ColOrName): Column {
  return fn("var_pop", column);
}

/** Returns the unbiased sample variance of the values in a group. */
export function var_samp(column: ColOrName): Column {
  return fn("var_samp", column);
}

/** Alias for {@link var_samp}. */
export const variance = var_samp;

/** Returns the Pearson Correlation Coefficient for two columns. */
export function corr(col1: ColOrName, col2: ColOrName): Column {
  return fn("corr", col1, col2);
}

/** Returns the population covariance for two columns. */
export function covar_pop(col1: ColOrName, col2: ColOrName): Column {
  return fn("covar_pop", col1, col2);
}

/** Returns the sample covariance for two columns. */
export function covar_samp(col1: ColOrName, col2: ColOrName): Column {
  return fn("covar_samp", col1, col2);
}

/** Returns the approximate number of distinct items in a group. */
export function approx_count_distinct(column: ColOrName, rsd?: number): Column {
  if (rsd !== undefined) {
    return new Column(fnExpr("approx_count_distinct", toExpr(column), _lit(rsd)._expr));
  }
  return fn("approx_count_distinct", column);
}

/** Returns the skewness of the values in a group. */
export function skewness(column: ColOrName): Column {
  return fn("skewness", column);
}

/** Returns the kurtosis of the values in a group. */
export function kurtosis(column: ColOrName): Column {
  return fn("kurtosis", column);
}

/** Returns the product of the values in a group. */
export function product(column: ColOrName): Column {
  return fn("product", column);
}

/** Returns some value of the group. The result is non-deterministic. */
export function any_value(column: ColOrName, ignorenulls = false): Column {
  return new Column(fnExpr("any_value", toExpr(column), _lit(ignorenulls)._expr));
}

/** Returns the number of true values for the column. */
export function count_if(column: ColOrName): Column {
  return fn("count_if", column);
}

/** Returns the value associated with the maximum value of ord. */
export function max_by(column: ColOrName, ord: ColOrName): Column {
  return fn("max_by", column, ord);
}

/** Returns the value associated with the minimum value of ord. */
export function min_by(column: ColOrName, ord: ColOrName): Column {
  return fn("min_by", column, ord);
}

/** Returns the approximate percentile of the numeric column. */
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

/** Returns the median of the values in a group. */
export function median(column: ColOrName): Column {
  return fn("median", column);
}

/** Returns the most frequent value in a group. */
export function mode(column: ColOrName): Column {
  return fn("mode", column);
}

/** Indicates whether a specified column in a GROUP BY list is aggregated or not. Returns 1 for aggregated or 0 for not aggregated. */
export function grouping(column: ColOrName): Column {
  return fn("grouping", column);
}

/** Returns the level of grouping as an integer bitmap. */
export function grouping_id(...columns: ColOrName[]): Column {
  return fn("grouping_id", ...columns);
}

/** Returns true if all values of the column are true. */
export function bool_and(column: ColOrName): Column {
  return fn("bool_and", column);
}

/** Alias for {@link bool_and}. */
export const every = bool_and;

/** Returns true if at least one value of the column is true. */
export function bool_or(column: ColOrName): Column {
  return fn("bool_or", column);
}

/** Alias for {@link bool_or}. */
export const some = bool_or;

/** Returns the bitwise AND of all non-null input values. */
export function bit_and(column: ColOrName): Column {
  return fn("bit_and", column);
}

/** Returns the bitwise OR of all non-null input values. */
export function bit_or(column: ColOrName): Column {
  return fn("bit_or", column);
}

/** Returns the bitwise XOR of all non-null input values. */
export function bit_xor(column: ColOrName): Column {
  return fn("bit_xor", column);
}
