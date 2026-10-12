import { describe, it, after } from "node:test";
import assert from "node:assert/strict";
import {
  col,
  lit,
  element_at,
  array_contains,
  array_position,
  array_remove,
  lag,
  lead,
  date_add,
  substring,
  extract,
  Window,
} from "@spark-connect-js/node";
import { spark, stopSession } from "./setup.js";

// Each case runs through the client and as the same SQL, so a string read as
// the wrong kind (column versus literal) shows up as a mismatch.
describe("Function arguments mean what they mean in PySpark", () => {
  after(stopSession);

  it("element_at reads a string key as a literal even when a column shares its name", async () => {
    const source = "SELECT * FROM VALUES (map('k', 7), 'nope'), (map('k', 8), 'nope') AS t(m, k)";
    const ours = await spark()
      .sql(source)
      .select(element_at(col("m"), "k").alias("v"))
      .collect();
    const sql = await spark().sql(`SELECT element_at(m, 'k') AS v FROM (${source})`).collect();

    assert.deepEqual(ours, sql);
    assert.deepEqual(
      ours.map((r) => r["v"]),
      [7, 8],
    );
  });

  it("array_contains, array_position, and array_remove read a string value as a literal", async () => {
    const source = "SELECT array('a', 'b', 'a') AS arr, 'b' AS a";
    const ours = await spark()
      .sql(source)
      .select(
        array_contains(col("arr"), "a").alias("has"),
        array_position(col("arr"), "a").alias("pos"),
        array_remove(col("arr"), "a").alias("rest"),
      )
      .collect();
    const sql = await spark()
      .sql(
        `SELECT array_contains(arr, 'a') AS has, array_position(arr, 'a') AS pos,
                array_remove(arr, 'a') AS rest FROM (${source})`,
      )
      .collect();

    assert.deepEqual(ours, sql);
  });

  it("lag and lead take a literal default", async () => {
    const source = "SELECT * FROM VALUES (1, 10), (2, 20) AS t(id, v)";
    const w = Window.orderBy(col("id"));
    const ours = await spark()
      .sql(source)
      .select(
        lag(col("v"), 1, 0).over(w).alias("prev"),
        lead(col("v"), 1, -1).over(w).alias("next"),
      )
      .collect();
    const sql = await spark()
      .sql(
        `SELECT lag(v, 1, 0) OVER (ORDER BY id) AS prev, lead(v, 1, -1) OVER (ORDER BY id) AS next
         FROM (${source})`,
      )
      .collect();

    assert.deepEqual(ours, sql);
  });

  it("counts and positions accept a column, and extract accepts a Column field", async () => {
    const source =
      "SELECT DATE'2026-01-10' AS d, 3 AS n, 'abcdef' AS s, TIMESTAMP'2026-05-01 00:00:00' AS ts";
    const ours = await spark()
      .sql(source)
      .select(
        date_add(col("d"), "n").alias("later"),
        substring(col("s"), "n", 2).alias("sub"),
        extract(lit("YEAR"), col("ts")).alias("y"),
      )
      .collect();
    const sql = await spark()
      .sql(
        `SELECT date_add(d, n) AS later, substring(s, n, 2) AS sub, extract(YEAR FROM ts) AS y
         FROM (${source})`,
      )
      .collect();

    assert.deepEqual(ours, sql);
  });
});
