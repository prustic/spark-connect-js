/**
 * Date / Timestamp functions.
 */

import { Column, fnExpr, toExpr, _lit, type ColOrName, fn } from "./_helpers.js";

export function year(column: ColOrName): Column {
  return fn("year", column);
}

export function month(column: ColOrName): Column {
  return fn("month", column);
}

export function dayofmonth(column: ColOrName): Column {
  return fn("dayofmonth", column);
}

export function hour(column: ColOrName): Column {
  return fn("hour", column);
}

export function minute(column: ColOrName): Column {
  return fn("minute", column);
}

export function second(column: ColOrName): Column {
  return fn("second", column);
}

export function current_date(): Column {
  return new Column(fnExpr("current_date"));
}

export function current_timestamp(): Column {
  return new Column(fnExpr("current_timestamp"));
}

export function datediff(end: ColOrName, start: ColOrName): Column {
  return fn("datediff", end, start);
}

export function date_add(column: ColOrName, days: number): Column {
  return new Column(fnExpr("date_add", toExpr(column), _lit(days)._expr));
}

export function date_sub(column: ColOrName, days: number): Column {
  return new Column(fnExpr("date_sub", toExpr(column), _lit(days)._expr));
}
