import { describe, it, after } from "node:test";
import assert from "node:assert/strict";
import {
  col,
  lit,
  abs,
  ceil,
  floor,
  sqrt,
  pow,
  log,
  log2,
  log10,
  sin,
  cos,
  degrees,
  radians,
  factorial,
  greatest,
  least,
  hex,
  unhex,
  rand,
  e,
  pi,
  round,
} from "@spark-connect-js/node";
import { spark, stopSession } from "./setup.js";

describe("math functions", () => {
  after(stopSession);

  it("abs / ceil / floor", async () => {
    const rows = await spark()
      .range(1, 4)
      .withColumn("abs_neg", abs(col("id").multiply(lit(-1))))
      .withColumn("ceil_div", ceil(col("id").divide(lit(3))))
      .withColumn("floor_div", floor(col("id").divide(lit(3))))
      .collect();
    assert.equal(rows[0]["abs_neg"], 1);
    assert.equal(rows[0]["ceil_div"], 1);
    assert.equal(rows[0]["floor_div"], 0);
  });

  it("sqrt / pow", async () => {
    const rows = await spark()
      .range(1, 5)
      .withColumn("sqrt_id", round(sqrt(col("id")), 4))
      .withColumn("pow_2", pow(col("id"), 2))
      .collect();
    assert.equal(rows[3]["pow_2"], 16); // 4^2
  });

  it("log / log2 / log10", async () => {
    const rows = await spark()
      .range(1, 4)
      .withColumn("log_id", log(col("id")))
      .withColumn("log2_id", log2(col("id")))
      .withColumn("log10_id", log10(col("id")))
      .collect();
    // log(1) = 0
    assert.equal(rows[0]["log_id"], 0);
    assert.equal(rows[0]["log2_id"], 0);
    assert.equal(rows[0]["log10_id"], 0);
  });

  it("sin / cos / degrees / radians", async () => {
    const rows = await spark()
      .range(1, 3)
      .withColumn("sin_id", sin(col("id")))
      .withColumn("cos_id", cos(col("id")))
      .withColumn("deg", degrees(col("id")))
      .withColumn("rad", radians(lit(180)))
      .collect();
    assert.equal(rows.length, 2);
    // radians(180) should be close to pi
    // TODO: cast needed because Row is Record<string, unknown> (see roadmap M3)
    assert.ok(Math.abs((rows[0]["rad"] as number) - Math.PI) < 0.0001);
  });

  it("factorial", async () => {
    const rows = await spark()
      .range(1, 6)
      .withColumn("fact", factorial(col("id")))
      .collect();
    assert.equal(rows[4]["fact"], 120); // 5! = 120
  });

  it("greatest / least", async () => {
    const rows = await spark()
      .range(1, 4)
      .withColumn("a", col("id").multiply(lit(3)))
      .withColumn("b", col("id").multiply(lit(7)))
      .withColumn("max_ab", greatest("a", "b"))
      .withColumn("min_ab", least("a", "b"))
      .collect();
    assert.equal(rows[0]["max_ab"], 7); // max(3, 7)
    assert.equal(rows[0]["min_ab"], 3); // min(3, 7)
  });

  it("hex / unhex", async () => {
    const rows = await spark()
      .range(1, 4)
      .withColumn("hex_id", hex(col("id")))
      .withColumn("unhex_ff", unhex(lit("FF")))
      .collect();
    assert.equal(rows[0]["hex_id"], "1");
  });

  it("rand", async () => {
    const rows = await spark().range(3).withColumn("random", rand()).collect();
    // TODO: cast needed because Row is Record<string, unknown> (see roadmap M3)
    for (const r of rows) {
      const val = r["random"] as number;
      assert.ok(val >= 0 && val < 1);
    }
  });

  it("e / pi constants", async () => {
    const rows = await spark()
      .range(1)
      .withColumn("euler", e())
      .withColumn("pi_val", pi())
      .collect();
    // TODO: casts needed because Row is Record<string, unknown> (see roadmap M3)
    assert.ok(Math.abs((rows[0]["euler"] as number) - Math.E) < 0.0001);
    assert.ok(Math.abs((rows[0]["pi_val"] as number) - Math.PI) < 0.0001);
  });
});
