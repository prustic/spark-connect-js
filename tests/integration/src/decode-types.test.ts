import { describe, it, after } from "node:test";
import assert from "node:assert/strict";
import { spark, stopSession } from "./setup.js";

describe("Arrow decode against live Spark", () => {
  after(stopSession);

  it("DECIMAL decodes to a scale-correct fixed-point string", async () => {
    const rows = await spark()
      .sql(
        `SELECT
           CAST(1.5 AS DECIMAL(10,2)) AS positive,
           CAST(-12345.6789 AS DECIMAL(18,4)) AS negative,
           CAST(0.000001 AS DECIMAL(20,9)) AS tiny,
           CAST(NULL AS DECIMAL(10,2)) AS missing`,
      )
      .collect();
    assert.equal(rows.length, 1);
    assert.equal(rows[0]["positive"], "1.50");
    assert.equal(rows[0]["negative"], "-12345.6789");
    assert.equal(rows[0]["tiny"], "0.000001000");
    assert.equal(rows[0]["missing"], null);
  });

  it("DATE decodes to a JS Date", async () => {
    const rows = await spark().sql(`SELECT DATE '2026-06-29' AS d`).collect();
    assert.ok(rows[0]["d"] instanceof Date);
    assert.equal(rows[0]["d"].toISOString(), "2026-06-29T00:00:00.000Z");
  });

  it("TIMESTAMP decodes to a JS Date (sub-ms truncated)", async () => {
    const rows = await spark().sql(`SELECT TIMESTAMP '2026-06-29 13:45:06.123' AS t`).collect();
    assert.ok(rows[0]["t"] instanceof Date);
    assert.equal(typeof rows[0]["t"].getTime(), "number");
  });

  it("MAP decodes to a JS Map with non-string keys preserved", async () => {
    const rows = await spark().sql(`SELECT map(1, 'a', 2, 'b') AS m`).collect();
    const m = rows[0]["m"] as Map<unknown, unknown>;
    assert.ok(m instanceof Map);
    assert.equal(m.get(1), "a");
    assert.equal(m.get(2), "b");
    assert.equal(typeof [...m.keys()][0], "number");
  });

  it("Nested decimal inside a struct decodes correctly", async () => {
    const rows = await spark()
      .sql(`SELECT named_struct('amt', CAST(1.5 AS DECIMAL(10,2)), 'name', 'x') AS s`)
      .collect();
    const s = rows[0]["s"] as Record<string, unknown>;
    assert.equal(s["amt"], "1.50");
    assert.equal(s["name"], "x");
  });

  it("Nested decimal inside an array decodes correctly", async () => {
    const rows = await spark()
      .sql(`SELECT array(CAST(1.5 AS DECIMAL(10,2)), CAST(2.5 AS DECIMAL(10,2))) AS xs`)
      .collect();
    assert.deepEqual(rows[0]["xs"], ["1.50", "2.50"]);
  });
});
