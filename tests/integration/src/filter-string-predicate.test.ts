import { describe, it, after } from "node:test";
import assert from "node:assert/strict";
import { spark, stopSession } from "./setup.js";

describe("filter() / where() with a SQL predicate string", () => {
  after(stopSession);

  it("filter(sqlString) parses and matches rows", async () => {
    const df = spark().sql(
      `SELECT * FROM VALUES ('EU', 30), ('US', 25), ('EU', 20) AS people(region, age)`,
    );
    const rows = await df.filter("region = 'EU' AND age >= 25").collect();
    assert.equal(rows.length, 1);
    assert.equal(rows[0]["region"], "EU");
    assert.equal(rows[0]["age"], 30);
  });

  it("where(sqlString) parses and matches rows", async () => {
    const df = spark().sql(`SELECT * FROM VALUES (1), (2), (3) AS t(x)`);
    const rows = await df.where("x >= 2").collect();
    assert.equal(rows.length, 2);
  });
});
