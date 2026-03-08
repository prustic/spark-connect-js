import { describe, it, after } from "node:test";
import assert from "node:assert/strict";
import {
  col,
  lit,
  array,
  array_contains,
  array_distinct,
  array_sort,
  array_union,
  array_intersect,
  array_except,
  size,
  explode,
  struct,
  create_map,
  map_keys,
  map_values,
} from "@spark-connect-js/node";
import { spark, stopSession } from "./setup.js";

describe("collection / array / map functions", () => {
  after(stopSession);

  it("array ops: contains, distinct, sort, union, intersect, except, size", async () => {
    const df = spark().sql(`
      SELECT * FROM VALUES
        (ARRAY(1, 2, 3, 2), ARRAY(2, 3, 4)),
        (ARRAY(5, 5, 6),    ARRAY(6, 7, 8)),
        (ARRAY(9, 10),      ARRAY(10, 11, 12))
      AS data(a, b)
    `);

    const rows = await df
      .withColumn("has_2", array_contains(col("a"), lit(2)))
      .withColumn("a_dist", array_distinct(col("a")))
      .withColumn("a_sorted", array_sort(col("a")))
      .withColumn("a_union_b", array_union(col("a"), col("b")))
      .withColumn("a_inter_b", array_intersect(col("a"), col("b")))
      .withColumn("a_except_b", array_except(col("a"), col("b")))
      .withColumn("a_size", size(col("a")))
      .collect();

    assert.equal(rows[0]["has_2"], true);
    assert.equal(rows[0]["a_size"], 4);
    assert.equal(rows[1]["has_2"], false);
  });

  it("array() constructor + explode", async () => {
    const df = spark().sql(`
      SELECT * FROM VALUES (1, 10), (2, 20), (3, 30) AS data(x, y)
    `);
    const rows = await df
      .withColumn("arr", array(col("x"), col("y")))
      .select(col("x"), col("arr"), explode(col("arr")).as("element"))
      .collect();

    // Each row explodes into 2 rows (x, y), so 3 * 2 = 6
    assert.equal(rows.length, 6);
  });

  it("struct / create_map / map_keys / map_values", async () => {
    const employees = spark().sql(`
      SELECT * FROM VALUES
        ('Alice', 'Engineering', 90000),
        ('Bob',   'Engineering', 85000)
      AS employees(name, department, salary)
    `);

    const rows = await employees
      .withColumn("info", struct("name", "department"))
      .withColumn("salary_map", create_map(lit("salary"), col("salary")))
      .withColumn("keys", map_keys(create_map(lit("salary"), col("salary"))))
      .withColumn("vals", map_values(create_map(lit("salary"), col("salary"))))
      .collect();

    assert.equal(rows.length, 2);
    assert.ok(rows[0]["info"] !== null);
    assert.deepEqual(rows[0]["keys"], ["salary"]);
  });
});
