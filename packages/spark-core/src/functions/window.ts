/**
 * Window functions.
 */

import { Column, fnExpr, toExpr, _lit, type ColOrName } from "./_helpers.js";

/** Returns the row number within a window partition (1-based). */
export function row_number(): Column {
  return new Column(fnExpr("row_number"));
}

/** Returns the rank within a window partition (gaps on ties). */
export function rank(): Column {
  return new Column(fnExpr("rank"));
}

/** Returns the rank within a window partition (no gaps on ties). */
export function dense_rank(): Column {
  return new Column(fnExpr("dense_rank"));
}

/** Returns the cumulative distribution of values within a window partition. */
export function cume_dist(): Column {
  return new Column(fnExpr("cume_dist"));
}

/** Returns the relative rank (percentile) within a window partition. */
export function percent_rank(): Column {
  return new Column(fnExpr("percent_rank"));
}

/** Returns the value of the nth row in a window frame (1-based). */
export function nth_value(column: ColOrName, offset: number, ignoreNulls = false): Column {
  return new Column(
    fnExpr("nth_value", toExpr(column), _lit(offset)._expr, _lit(ignoreNulls)._expr),
  );
}

/** Returns the value N rows before the current row in a window partition. */
export function lag(column: ColOrName, offset = 1, defaultValue?: ColOrName): Column {
  const args = [toExpr(column), _lit(offset)._expr];
  if (defaultValue !== undefined) args.push(toExpr(defaultValue));
  return new Column(fnExpr("lag", ...args));
}

/** Returns the value N rows after the current row in a window partition. */
export function lead(column: ColOrName, offset = 1, defaultValue?: ColOrName): Column {
  const args = [toExpr(column), _lit(offset)._expr];
  if (defaultValue !== undefined) args.push(toExpr(defaultValue));
  return new Column(fnExpr("lead", ...args));
}

/** Divides the ordered window partition into n buckets. */
export function ntile(n: number): Column {
  return new Column(fnExpr("ntile", _lit(n)._expr));
}
