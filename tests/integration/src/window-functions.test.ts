import { describe, it, after } from "node:test";
import assert from "node:assert/strict";
import {
  col,
  row_number,
  rank,
  dense_rank,
  lag,
  lead,
  ntile,
  cume_dist,
  percent_rank,
  sum,
  Window,
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

describe("window functions", () => {
  after(stopSession);

  const w = () => Window.partitionBy("department").orderBy(col("salary").desc());

  it("row_number / rank / dense_rank", async () => {
    const rows = await employees()
      .withColumn("row_num", row_number().over(w()))
      .withColumn("rnk", rank().over(w()))
      .withColumn("dense_rnk", dense_rank().over(w()))
      .select("name", "department", "salary", "row_num", "rnk", "dense_rnk")
      .collect();

    const eve = rows.find((r) => r["name"] === "Eve");
    assert.equal(eve?.["row_num"], 1);
    assert.equal(eve?.["rnk"], 1);
    assert.equal(eve?.["dense_rnk"], 1);
  });

  it("lag / lead", async () => {
    const rows = await employees()
      .withColumn("prev_salary", lag("salary", 1).over(w()))
      .withColumn("next_salary", lead("salary", 1).over(w()))
      .select("name", "department", "salary", "prev_salary", "next_salary")
      .collect();

    // Eve is first in Engineering (desc), so lag is null
    const eve = rows.find((r) => r["name"] === "Eve");
    assert.equal(eve?.["prev_salary"], null);
  });

  it("ntile", async () => {
    const rows = await employees()
      .withColumn("tile", ntile(2).over(w()))
      .select("name", "department", "tile")
      .collect();

    const tiles = rows.map((r) => r["tile"]);
    assert.ok(tiles.every((t) => t === 1 || t === 2));
  });

  it("cume_dist / percent_rank", async () => {
    const rows = await employees()
      .withColumn("cume", cume_dist().over(w()))
      .withColumn("pct_rank", percent_rank().over(w()))
      .select("name", "department", "salary", "cume", "pct_rank")
      .collect();

    // TODO: casts needed because Row is Record<string, unknown> (see roadmap M3)
    for (const r of rows) {
      assert.ok((r["cume"] as number) > 0 && (r["cume"] as number) <= 1);
      assert.ok((r["pct_rank"] as number) >= 0 && (r["pct_rank"] as number) <= 1);
    }
  });

  it("window with rows frame", async () => {
    const wRows = Window.partitionBy("department").orderBy(col("salary").desc()).rowsBetween(-1, 1);

    const rows = await employees().withColumn("neighbor_sum", sum("salary").over(wRows)).collect();
    assert.equal(rows.length, 5);
    // Each row's neighbor_sum should be the sum of at most 3 neighbors
    assert.ok(rows.every((r) => (r["neighbor_sum"] as number) > 0));
  });
});
