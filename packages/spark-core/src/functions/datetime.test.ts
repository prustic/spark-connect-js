import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  year, month, dayofmonth, dayofweek, dayofyear, weekofyear, quarter,
  hour, minute, second, current_date, current_timestamp, datediff,
  date_add, date_sub, months_between, next_day, last_day, add_months,
  date_format, to_date, to_timestamp, from_unixtime, unix_timestamp,
  date_trunc, trunc, extract, date_part,
} from "./datetime.js";

import type { Column } from "../column.js";

function assertFn(c: Column, name: string, argCount?: number) {
  assert.equal(c._expr.type, "unresolvedFunction");
  if (c._expr.type === "unresolvedFunction") {
    assert.equal(c._expr.name, name);
    if (argCount !== undefined) assert.equal(c._expr.arguments.length, argCount);
  }
}

describe("date/timestamp functions", () => {
  it("extraction: year/month/dayofmonth/dayofweek/dayofyear/weekofyear/quarter", () => {
    assertFn(year("d"), "year", 1);
    assertFn(month("d"), "month", 1);
    assertFn(dayofmonth("d"), "dayofmonth", 1);
    assertFn(dayofweek("d"), "dayofweek", 1);
    assertFn(dayofyear("d"), "dayofyear", 1);
    assertFn(weekofyear("d"), "weekofyear", 1);
    assertFn(quarter("d"), "quarter", 1);
  });

  it("time extraction: hour/minute/second", () => {
    assertFn(hour("t"), "hour", 1);
    assertFn(minute("t"), "minute", 1);
    assertFn(second("t"), "second", 1);
  });

  it("current_date/current_timestamp are zero-arg", () => {
    assertFn(current_date(), "current_date", 0);
    assertFn(current_timestamp(), "current_timestamp", 0);
  });

  it("datediff/date_add/date_sub take two args", () => {
    assertFn(datediff("end", "start"), "datediff", 2);
    assertFn(date_add("d", 5), "date_add", 2);
    assertFn(date_sub("d", 5), "date_sub", 2);
  });

  it("months_between takes col+col+roundOff", () => {
    assertFn(months_between("d1", "d2"), "months_between", 3);
  });

  it("next_day/last_day", () => {
    const nd = next_day("d", "Mon");
    assert.equal(nd._expr.type, "unresolvedFunction");
    assertFn(last_day("d"), "last_day", 1);
  });

  it("add_months takes col + months", () => {
    const r = add_months("d", 3);
    assert.equal(r._expr.type, "unresolvedFunction");
  });

  it("date_format takes col + format", () => {
    const r = date_format("d", "yyyy-MM-dd");
    assert.equal(r._expr.type, "unresolvedFunction");
  });

  it("to_date with optional format", () => {
    assertFn(to_date("x"), "to_date", 1);
    const r = to_date("x", "yyyy-MM-dd");
    assert.equal(r._expr.type, "unresolvedFunction");
  });

  it("to_timestamp with optional format", () => {
    assertFn(to_timestamp("x"), "to_timestamp", 1);
  });

  it("from_unixtime/unix_timestamp", () => {
    const r = from_unixtime("ts");
    assert.equal(r._expr.type, "unresolvedFunction");
    assertFn(unix_timestamp(), "unix_timestamp", 0);
  });

  it("date_trunc/trunc", () => {
    const r1 = date_trunc("month", "d");
    assert.equal(r1._expr.type, "unresolvedFunction");
    const r2 = trunc("d", "month");
    assert.equal(r2._expr.type, "unresolvedFunction");
  });

  it("extract/date_part alias", () => {
    const r1 = extract("year", "d");
    assert.equal(r1._expr.type, "unresolvedFunction");
    const r2 = date_part("year", "d");
    assert.equal(r2._expr.type, "unresolvedFunction");
  });
});
