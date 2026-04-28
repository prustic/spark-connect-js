import { describe, it, after } from "node:test";
import assert from "node:assert/strict";
import { spark, stopSession } from "./setup.js";

describe("SparkSession.version", () => {
  after(stopSession);

  it("returns the connected server's Spark version", async () => {
    const v = await spark().version();
    assert.match(v, /^\d+\.\d+\.\d+/);
  });
});

describe("SparkSession.conf", () => {
  after(stopSession);

  // One test that exercises the full RuntimeConfig surface against a real
  // server. `spark.sql.shuffle.partitions` is a documented modifiable key
  // present on every Spark distribution.
  it("set / get / getAll / isModifiable / unset round-trip", async () => {
    const conf = spark().conf;
    const key = "spark.sql.shuffle.partitions";

    assert.equal(await conf.isModifiable(key), true);

    await conf.set(key, "16");
    assert.equal(await conf.get(key), "16");

    const all = await conf.getAll();
    assert.equal(all[key], "16");

    await conf.unset(key);
    // After unset the entry returns to its server-side default; what matters
    // is that our session-level "16" override is gone.
    assert.notEqual(await conf.get(key), "16");
  });
});
