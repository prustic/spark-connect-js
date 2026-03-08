import { describe, it, after } from "node:test";
import assert from "node:assert/strict";
import {
  col,
  to_date,
  to_timestamp,
  year,
  month,
  dayofweek,
  hour,
  date_add,
  date_sub,
  datediff,
  months_between,
  date_format,
  unix_timestamp,
  from_unixtime,
  current_date,
  current_timestamp,
} from "@spark-connect-js/node";
import { spark, stopSession } from "./setup.js";

const dtData = () =>
  spark()
    .sql(
      `
      SELECT * FROM VALUES
        ('2024-01-15', '2024-06-30 14:30:00'),
        ('2024-03-20', '2024-12-25 09:15:00'),
        ('2024-07-04', '2024-07-04 00:00:00')
      AS data(date_str, ts_str)
    `,
    )
    .withColumn("dt", to_date(col("date_str")))
    .withColumn("ts", to_timestamp(col("ts_str")));

describe("datetime functions", () => {
  after(stopSession);

  it("year / month / dayofweek / hour", async () => {
    const rows = await dtData()
      .withColumn("yr", year(col("dt")))
      .withColumn("mon", month(col("dt")))
      .withColumn("dow", dayofweek(col("dt")))
      .withColumn("hr", hour(col("ts")))
      .collect();

    assert.equal(rows[0]["yr"], 2024);
    assert.equal(rows[0]["mon"], 1);
    assert.equal(rows[1]["hr"], 9);
  });

  it("date_format", async () => {
    const rows = await dtData()
      .withColumn("formatted", date_format(col("dt"), "MMMM dd, yyyy"))
      .collect();
    assert.equal(rows[0]["formatted"], "January 15, 2024");
  });

  it("date_add / date_sub / datediff / months_between", async () => {
    const rows = await dtData()
      .withColumn("plus_7d", date_add(col("dt"), 7))
      .withColumn("minus_3d", date_sub(col("dt"), 3))
      .withColumn("diff_days", datediff(col("ts"), col("dt")))
      .withColumn("diff_months", months_between(col("ts"), col("dt")))
      .collect();

    assert.equal(rows.length, 3);
    // For 2024-07-04 row: ts and dt are same date, so diff_days = 0
    assert.equal(rows[2]["diff_days"], 0);
  });

  it("unix_timestamp / from_unixtime", async () => {
    const rows = await dtData()
      .withColumn("epoch", unix_timestamp(col("ts")))
      .withColumn("back_str", from_unixtime(unix_timestamp(col("ts")), "yyyy/MM/dd"))
      .collect();

    assert.equal(typeof rows[0]["epoch"], "number");
    // TODO: cast needed because Row is Record<string, unknown> (see roadmap M3)
    assert.ok((rows[0]["back_str"] as string).startsWith("2024/"));
  });

  it("current_date / current_timestamp", async () => {
    const rows = await spark()
      .range(1)
      .withColumn("today", current_date())
      .withColumn("now", current_timestamp())
      .collect();

    assert.ok(rows[0]["today"] !== null);
    assert.ok(rows[0]["now"] !== null);
  });
});
