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

    const original = await conf.get(key);
    // Pick a value guaranteed to differ from whatever the server has set.
    const override = original === "17" ? "23" : "17";

    await conf.set(key, override);
    assert.equal(await conf.get(key), override);

    const all = await conf.getAll();
    assert.equal(all[key], override);

    await conf.unset(key);
    // After unset, the entry returns to whatever the server had before.
    assert.equal(await conf.get(key), original);
  });
});
