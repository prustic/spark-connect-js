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

export function dayofweek(column: ColOrName): Column {
  return fn("dayofweek", column);
}

export function dayofyear(column: ColOrName): Column {
  return fn("dayofyear", column);
}

export function weekofyear(column: ColOrName): Column {
  return fn("weekofyear", column);
}

export function quarter(column: ColOrName): Column {
  return fn("quarter", column);
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

export function months_between(end: ColOrName, start: ColOrName, roundOff = true): Column {
  return new Column(fnExpr("months_between", toExpr(end), toExpr(start), _lit(roundOff)._expr));
}

export function next_day(column: ColOrName, dayOfWeek: string): Column {
  return new Column(fnExpr("next_day", toExpr(column), _lit(dayOfWeek)._expr));
}

export function last_day(column: ColOrName): Column {
  return fn("last_day", column);
}

export function add_months(column: ColOrName, months: number): Column {
  return new Column(fnExpr("add_months", toExpr(column), _lit(months)._expr));
}

export function date_format(column: ColOrName, format: string): Column {
  return new Column(fnExpr("date_format", toExpr(column), _lit(format)._expr));
}

export function to_date(column: ColOrName, format?: string): Column {
  const args = [toExpr(column)];
  if (format !== undefined) args.push(_lit(format)._expr);
  return new Column(fnExpr("to_date", ...args));
}

export function to_timestamp(column: ColOrName, format?: string): Column {
  const args = [toExpr(column)];
  if (format !== undefined) args.push(_lit(format)._expr);
  return new Column(fnExpr("to_timestamp", ...args));
}

export function from_unixtime(column: ColOrName, format = "yyyy-MM-dd HH:mm:ss"): Column {
  return new Column(fnExpr("from_unixtime", toExpr(column), _lit(format)._expr));
}

export function unix_timestamp(column?: ColOrName, format = "yyyy-MM-dd HH:mm:ss"): Column {
  if (column === undefined) return new Column(fnExpr("unix_timestamp"));
  return new Column(fnExpr("unix_timestamp", toExpr(column), _lit(format)._expr));
}

export function date_trunc(format: string, column: ColOrName): Column {
  return new Column(fnExpr("date_trunc", _lit(format)._expr, toExpr(column)));
}

export function trunc(column: ColOrName, format: string): Column {
  return new Column(fnExpr("trunc", toExpr(column), _lit(format)._expr));
}

export function extract(field: string, column: ColOrName): Column {
  return new Column(fnExpr("extract", _lit(field)._expr, toExpr(column)));
}

export const date_part = extract;
