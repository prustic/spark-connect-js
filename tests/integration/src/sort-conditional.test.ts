import { describe, it, after } from "node:test";
import assert from "node:assert/strict";
import {
  col,
  lit,
  asc,
  desc,
  asc_nulls_first,
  desc_nulls_last,
  coalesce,
  isnull,
  isnan,
  nanvl,
  ifnull,
  typeof_,
  monotonically_increasing_id,
  expr,
} from "@spark-connect-js/node";
import { spark, stopSession } from "./setup.js";

describe("sort functions", () => {
  after(stopSession);

  const sortData = () =>
    spark().sql(`
      SELECT * FROM VALUES
        ('Alice', 90000),
        ('Bob',   NULL),
        ('Carol', 70000),
        ('Dave',  NULL),
        ('Eve',   95000)
      AS data(name, salary)
    `);

  it("asc / desc", async () => {
    const ascRows = await sortData().sort(asc("name")).collect();
    assert.equal(ascRows[0]["name"], "Alice");

    const descRows = await sortData().sort(desc("salary")).collect();
    assert.equal(descRows[0]["salary"], 95000);
  });

  it("asc_nulls_first / desc_nulls_last", async () => {
    const ascNF = await sortData().sort(asc_nulls_first("salary")).collect();
    assert.equal(ascNF[0]["salary"], null);

    const descNL = await sortData().sort(desc_nulls_last("salary")).collect();
    assert.equal(descNL[descNL.length - 1]["salary"], null);
  });
});

describe("conditional / utility functions", () => {
  after(stopSession);

  it("coalesce / isnull / isnan / nanvl / ifnull", async () => {
    const df = spark().sql(`
      SELECT * FROM VALUES
        (1,    NULL,  3),
        (NULL, 20,    NULL),
        (5,    CAST('NaN' AS DOUBLE), 7)
      AS data(a, b, c)
    `);

    const rows = await df
      .withColumn("coal", coalesce(col("a"), col("b"), col("c")))
      .withColumn("a_null", isnull(col("a")))
      .withColumn("b_nan", isnan(col("b")))
      .withColumn("b_safe", nanvl(col("b"), lit(0)))
      .withColumn("a_or_c", ifnull(col("a"), col("c")))
      .collect();

    // Row 0: coalesce(1, null, 3) = 1
    assert.equal(rows[0]["coal"], 1);
    assert.equal(rows[0]["a_null"], false);

    // Row 1: coalesce(null, 20, null) = 20
    assert.equal(rows[1]["coal"], 20);
    assert.equal(rows[1]["a_null"], true);

    // Row 2: isnan(NaN) = true, nanvl(NaN, 0) = 0
    assert.equal(rows[2]["b_nan"], true);
    assert.equal(rows[2]["b_safe"], 0);
  });

  it("typeof / monotonically_increasing_id / expr", async () => {
    const employees = spark().sql(`
      SELECT * FROM VALUES
        ('Alice', 90000),
        ('Bob',   85000)
      AS employees(name, salary)
    `);

    const rows = await employees
      .withColumn("type_sal", typeof_(col("salary")))
      .withColumn("mono_id", monotonically_increasing_id())
      .withColumn("expr_calc", expr("salary * 2"))
      .collect();

    assert.equal(rows.length, 2);
    assert.equal(typeof rows[0]["type_sal"], "string");
    assert.equal(typeof rows[0]["mono_id"], "number");
    assert.equal(rows[0]["expr_calc"], 180000);
  });
});
