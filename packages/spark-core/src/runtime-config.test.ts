import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { RuntimeConfig } from "./runtime-config.js";
import type { SparkSession } from "./spark-session.js";

/**
 * Stub session that records `_config` calls and replays a scripted response.
 */
function makeRecorder(response: Record<string, unknown> = { pairs: [], warnings: [] }): {
  conf: RuntimeConfig;
  calls: Record<string, unknown>[];
} {
  const calls: Record<string, unknown>[] = [];
  const session = {
    _config: async (op: Record<string, unknown>) => {
      calls.push(op);
      return response;
    },
  } as unknown as SparkSession;
  return { conf: new RuntimeConfig(session), calls };
}

describe("RuntimeConfig.set", () => {
  it("sends a Set op with one key-value pair", async () => {
    const { conf, calls } = makeRecorder();
    await conf.set("spark.sql.shuffle.partitions", "16");
    assert.deepStrictEqual(calls, [{ op: "set", pairs: [["spark.sql.shuffle.partitions", "16"]] }]);
  });
});

describe("RuntimeConfig.get", () => {
  it("returns the value when the server replies with a pair", async () => {
    const { conf, calls } = makeRecorder({
      pairs: [["spark.sql.shuffle.partitions", "200"]],
      warnings: [],
    });
    const value = await conf.get("spark.sql.shuffle.partitions");
    assert.equal(value, "200");
    assert.deepStrictEqual(calls, [{ op: "get", keys: ["spark.sql.shuffle.partitions"] }]);
  });

  it("returns undefined when the server replies with no pair", async () => {
    const { conf } = makeRecorder({ pairs: [], warnings: [] });
    const value = await conf.get("spark.unknown.key");
    assert.equal(value, undefined);
  });
});

describe("RuntimeConfig.unset", () => {
  it("sends an Unset op", async () => {
    const { conf, calls } = makeRecorder();
    await conf.unset("spark.sql.shuffle.partitions");
    assert.deepStrictEqual(calls, [{ op: "unset", keys: ["spark.sql.shuffle.partitions"] }]);
  });
});

describe("RuntimeConfig.getAll", () => {
  it("sends a GetAll op without prefix", async () => {
    const { conf, calls } = makeRecorder({
      pairs: [
        ["spark.sql.shuffle.partitions", "200"],
        ["spark.executor.memory", "4g"],
      ],
      warnings: [],
    });
    const all = await conf.getAll();
    assert.deepStrictEqual(all, {
      "spark.sql.shuffle.partitions": "200",
      "spark.executor.memory": "4g",
    });
    assert.deepStrictEqual(calls, [{ op: "getAll" }]);
  });

  it("passes prefix when given", async () => {
    const { conf, calls } = makeRecorder({ pairs: [], warnings: [] });
    await conf.getAll("spark.sql");
    assert.deepStrictEqual(calls, [{ op: "getAll", prefix: "spark.sql" }]);
  });

  it("drops pairs whose value is undefined", async () => {
    const { conf } = makeRecorder({
      pairs: [
        ["a", "1"],
        ["b", undefined],
      ],
      warnings: [],
    });
    const all = await conf.getAll();
    assert.deepStrictEqual(all, { a: "1" });
  });
});

describe("RuntimeConfig.isModifiable", () => {
  it("returns true when server replies 'true'", async () => {
    const { conf, calls } = makeRecorder({ pairs: [["k", "true"]], warnings: [] });
    assert.equal(await conf.isModifiable("k"), true);
    assert.deepStrictEqual(calls, [{ op: "isModifiable", keys: ["k"] }]);
  });

  it("returns false otherwise", async () => {
    const { conf } = makeRecorder({ pairs: [["k", "false"]], warnings: [] });
    assert.equal(await conf.isModifiable("k"), false);
  });
});
