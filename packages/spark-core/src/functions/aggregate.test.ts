import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  count, sum, avg, mean, min, max, countDistinct, sum_distinct,
  first, first_value, last, last_value, collect_list, collect_set, array_agg,
  stddev, stddev_pop, stddev_samp, var_pop, var_samp, variance,
  corr, covar_pop, covar_samp, approx_count_distinct,
  skewness, kurtosis, product, any_value, count_if,
  max_by, min_by, percentile_approx, median, mode,
  grouping, grouping_id, bool_and, every, bool_or, some,
  bit_and, bit_or, bit_xor,
} from "./aggregate.js";

import type { Column } from "../column.js";

function assertFn(c: Column, name: string, argCount?: number) {
  assert.equal(c._expr.type, "unresolvedFunction");
  if (c._expr.type === "unresolvedFunction") {
    assert.equal(c._expr.name, name);
    if (argCount !== undefined) assert.equal(c._expr.arguments.length, argCount);
  }
}

describe("aggregate functions", () => {
  it("count/sum/avg/mean/min/max create unresolvedFunction", () => {
    assertFn(count("*"), "count", 1);
    assertFn(sum("x"), "sum", 1);
    assertFn(avg("x"), "avg", 1);
    assertFn(mean("x"), "avg", 1);
    assertFn(min("x"), "min", 1);
    assertFn(max("x"), "max", 1);
  });

  it("countDistinct/sum_distinct set isDistinct", () => {
    const cd = countDistinct("x");
    if (cd._expr.type === "unresolvedFunction") {
      assert.equal(cd._expr.isDistinct, true);
    }
    const sd = sum_distinct("x");
    if (sd._expr.type === "unresolvedFunction") {
      assert.equal(sd._expr.isDistinct, true);
    }
  });

  it("first/first_value/last/last_value", () => {
    assertFn(first("x"), "first", 2); // col + ignorenulls default
    assertFn(first_value("x"), "first", 2);
    assertFn(last("x"), "last", 2);
    assertFn(last_value("x"), "last", 2);
  });

  it("collect_list/collect_set/array_agg", () => {
    assertFn(collect_list("x"), "collect_list", 1);
    assertFn(collect_set("x"), "collect_set", 1);
    assertFn(array_agg("x"), "array_agg", 1);
  });

  it("stddev/stddev_pop/stddev_samp", () => {
    assertFn(stddev("x"), "stddev", 1);
    assertFn(stddev_pop("x"), "stddev_pop", 1);
    assertFn(stddev_samp("x"), "stddev_samp", 1);
  });

  it("var_pop/var_samp/variance", () => {
    assertFn(var_pop("x"), "var_pop", 1);
    assertFn(var_samp("x"), "var_samp", 1);
    assertFn(variance("x"), "var_samp", 1);
  });

  it("corr/covar_pop/covar_samp take two args", () => {
    assertFn(corr("x", "y"), "corr", 2);
    assertFn(covar_pop("x", "y"), "covar_pop", 2);
    assertFn(covar_samp("x", "y"), "covar_samp", 2);
  });

  it("approx_count_distinct with optional rsd", () => {
    assertFn(approx_count_distinct("x"), "approx_count_distinct", 1);
    const r = approx_count_distinct("x", 0.05);
    if (r._expr.type === "unresolvedFunction") {
      assert.equal(r._expr.arguments.length, 2);
    }
  });

  it("skewness/kurtosis/product", () => {
    assertFn(skewness("x"), "skewness", 1);
    assertFn(kurtosis("x"), "kurtosis", 1);
    assertFn(product("x"), "product", 1);
  });

  it("any_value/count_if", () => {
    assertFn(any_value("x"), "any_value", 2); // col + ignorenulls default
    assertFn(count_if("x"), "count_if", 1);
  });

  it("max_by/min_by take two args", () => {
    assertFn(max_by("x", "y"), "max_by", 2);
    assertFn(min_by("x", "y"), "min_by", 2);
  });

  it("percentile_approx/median/mode", () => {
    const r = percentile_approx("x", 0.5, 10000);
    assert.equal(r._expr.type, "unresolvedFunction");
    assertFn(median("x"), "median", 1);
    assertFn(mode("x"), "mode", 1);
  });

  it("grouping/grouping_id", () => {
    assertFn(grouping("x"), "grouping", 1);
    const gi = grouping_id("a", "b");
    if (gi._expr.type === "unresolvedFunction") {
      assert.equal(gi._expr.name, "grouping_id");
      assert.equal(gi._expr.arguments.length, 2);
    }
  });

  it("bool_and/every/bool_or/some", () => {
    assertFn(bool_and("x"), "bool_and", 1);
    assertFn(every("x"), "bool_and", 1);
    assertFn(bool_or("x"), "bool_or", 1);
    assertFn(some("x"), "bool_or", 1);
  });

  it("bit_and/bit_or/bit_xor", () => {
    assertFn(bit_and("x"), "bit_and", 1);
    assertFn(bit_or("x"), "bit_or", 1);
    assertFn(bit_xor("x"), "bit_xor", 1);
  });
});
