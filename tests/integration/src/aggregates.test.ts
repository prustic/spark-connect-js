import { describe, it, after } from "node:test";
import assert from "node:assert/strict";
import {
  col,
  count,
  sum,
  avg,
  min,
  max,
  round,
  stddev,
  variance,
  corr,
  collect_list,
  collect_set,
  first,
  last,
  countDistinct,
  approx_count_distinct,
  monotonically_increasing_id,
} from "@spark-connect-js/node";
import { spark, stopSession } from "./setup.js";

const employees = () =>
  spark().sql(`
    SELECT * FROM VALUES
      ('Alice', 'Engineering', 90000),
      ('Bob',   'Engineering', 85000),
      ('Carol', 'Marketing',   70000),
      ('Dave',  'Marketing',   72000),
      ('Eve',   'Engineering', 95000)
    AS employees(name, department, salary)
  `);

describe("expanded aggregates", () => {
  after(stopSession);

  it("stddev / variance / corr", async () => {
    const empWithId = employees().withColumn("emp_id", monotonically_increasing_id());
    const rows = await empWithId
      .groupBy("department")
      .agg(
        count(col("name")).as("n"),
        min(col("salary")).as("min_sal"),
        max(col("salary")).as("max_sal"),
        round(stddev(col("salary")), 2).as("stddev_sal"),
        round(variance(col("salary")), 2).as("var_sal"),
        round(corr(col("salary"), col("emp_id")), 4).as("corr_sal_id"),
      )
      .collect();

    assert.equal(rows.length, 2);
    const eng = rows.find((r) => r["department"] === "Engineering");
    assert.equal(eng?.["n"], 3);
    assert.equal(eng?.["min_sal"], 85000);
    assert.equal(eng?.["max_sal"], 95000);
  });

  it("collect_list / collect_set", async () => {
    const rows = await employees()
      .groupBy("department")
      .agg(collect_list(col("name")).as("name_list"), collect_set(col("name")).as("name_set"))
      .collect();

    assert.equal(rows.length, 2);
    const eng = rows.find((r) => r["department"] === "Engineering");
    // TODO: cast needed because Row is Record<string, unknown> (see roadmap M3)
    assert.equal((eng?.["name_list"] as unknown[]).length, 3);
  });

  it("first / last / countDistinct / approx_count_distinct", async () => {
    const rows = await employees()
      .groupBy("department")
      .agg(
        first(col("name")).as("first_name"),
        last(col("name")).as("last_name"),
        countDistinct(col("salary")).as("distinct_salaries"),
        approx_count_distinct(col("salary")).as("approx_distinct"),
      )
      .collect();

    assert.equal(rows.length, 2);
    const eng = rows.find((r) => r["department"] === "Engineering");
    assert.equal(eng?.["distinct_salaries"], 3);
  });

  it("sum + round + avg", async () => {
    const rows = await employees()
      .groupBy("department")
      .agg(sum(col("salary")).as("total"), round(avg(col("salary")), 0).as("avg_sal"))
      .collect();

    const eng = rows.find((r) => r["department"] === "Engineering");
    assert.equal(eng?.["total"], 270000);
  });
});
