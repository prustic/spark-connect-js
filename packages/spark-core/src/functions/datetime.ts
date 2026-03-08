/**
 * Date / Timestamp functions.
 */

import { Column, fnExpr, toExpr, _lit, type ColOrName, fn } from "./_helpers.js";

/** Extracts the year as an integer from a given date/timestamp/string. */
export function year(column: ColOrName): Column {
  return fn("year", column);
}

/** Extracts the month as an integer from a given date/timestamp/string. */
export function month(column: ColOrName): Column {
  return fn("month", column);
}

/** Extracts the day of the month as an integer from a given date/timestamp/string. */
export function dayofmonth(column: ColOrName): Column {
  return fn("dayofmonth", column);
}

/** Extracts the day of the week as an integer (1 = Sunday, 7 = Saturday) from a given date/timestamp/string. */
export function dayofweek(column: ColOrName): Column {
  return fn("dayofweek", column);
}

/** Extracts the day of the year as an integer from a given date/timestamp/string. */
export function dayofyear(column: ColOrName): Column {
  return fn("dayofyear", column);
}

/** Extracts the week number as an integer from a given date/timestamp/string. */
export function weekofyear(column: ColOrName): Column {
  return fn("weekofyear", column);
}

/** Extracts the quarter as an integer from a given date/timestamp/string. */
export function quarter(column: ColOrName): Column {
  return fn("quarter", column);
}

/** Extracts the hours as an integer from a given date/timestamp/string. */
export function hour(column: ColOrName): Column {
  return fn("hour", column);
}

/** Extracts the minutes as an integer from a given date/timestamp/string. */
export function minute(column: ColOrName): Column {
  return fn("minute", column);
}

/** Extracts the seconds as an integer from a given date/timestamp/string. */
export function second(column: ColOrName): Column {
  return fn("second", column);
}

/** Returns the current date at the start of query evaluation as a date column. */
export function current_date(): Column {
  return new Column(fnExpr("current_date"));
}

/** Returns the current timestamp at the start of query evaluation as a timestamp column. */
export function current_timestamp(): Column {
  return new Column(fnExpr("current_timestamp"));
}

/** Returns the number of days from start to end. */
export function datediff(end: ColOrName, start: ColOrName): Column {
  return fn("datediff", end, start);
}

/** Returns the date that is the given number of days after the start date. */
export function date_add(column: ColOrName, days: number): Column {
  return new Column(fnExpr("date_add", toExpr(column), _lit(days)._expr));
}

/** Returns the date that is the given number of days before the start date. */
export function date_sub(column: ColOrName, days: number): Column {
  return new Column(fnExpr("date_sub", toExpr(column), _lit(days)._expr));
}

/** Returns number of months between dates end and start. */
export function months_between(end: ColOrName, start: ColOrName, roundOff = true): Column {
  return new Column(fnExpr("months_between", toExpr(end), toExpr(start), _lit(roundOff)._expr));
}

/** Returns the first date which is later than the given date and named day of the week. */
export function next_day(column: ColOrName, dayOfWeek: string): Column {
  return new Column(fnExpr("next_day", toExpr(column), _lit(dayOfWeek)._expr));
}

/** Returns the last day of the month which the given date belongs to. */
export function last_day(column: ColOrName): Column {
  return fn("last_day", column);
}

/** Returns the date that is the given number of months after the start date. */
export function add_months(column: ColOrName, months: number): Column {
  return new Column(fnExpr("add_months", toExpr(column), _lit(months)._expr));
}

/** Converts a date/timestamp/string to a string formatted with the given date time pattern. */
export function date_format(column: ColOrName, format: string): Column {
  return new Column(fnExpr("date_format", toExpr(column), _lit(format)._expr));
}

/** Converts a string column to a date column with an optional format. */
export function to_date(column: ColOrName, format?: string): Column {
  const args = [toExpr(column)];
  if (format !== undefined) args.push(_lit(format)._expr);
  return new Column(fnExpr("to_date", ...args));
}

/** Converts a string column to a timestamp column with an optional format. */
export function to_timestamp(column: ColOrName, format?: string): Column {
  const args = [toExpr(column)];
  if (format !== undefined) args.push(_lit(format)._expr);
  return new Column(fnExpr("to_timestamp", ...args));
}

/** Converts the number of seconds from Unix epoch to a string representation of the timestamp using the given format. */
export function from_unixtime(column: ColOrName, format = "yyyy-MM-dd HH:mm:ss"): Column {
  return new Column(fnExpr("from_unixtime", toExpr(column), _lit(format)._expr));
}

/** Converts a given timestamp or string to the number of seconds since the Unix epoch. */
export function unix_timestamp(column?: ColOrName, format = "yyyy-MM-dd HH:mm:ss"): Column {
  if (column === undefined) return new Column(fnExpr("unix_timestamp"));
  return new Column(fnExpr("unix_timestamp", toExpr(column), _lit(format)._expr));
}

/** Truncates a timestamp to the specified format (e.g. "year", "month", "day", "hour"). */
export function date_trunc(format: string, column: ColOrName): Column {
  return new Column(fnExpr("date_trunc", _lit(format)._expr, toExpr(column)));
}

/** Truncates a date to the specified format (e.g. "year", "month"). */
export function trunc(column: ColOrName, format: string): Column {
  return new Column(fnExpr("trunc", toExpr(column), _lit(format)._expr));
}

/** Extracts a part of the date/timestamp or interval source (e.g. "year", "month", "day"). */
export function extract(field: string, column: ColOrName): Column {
  return new Column(fnExpr("extract", _lit(field)._expr, toExpr(column)));
}

/** Alias for {@link extract}. */
export const date_part = extract;
