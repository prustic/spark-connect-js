import { describe, it, after } from "node:test";
import assert from "node:assert/strict";
import { tableFromArrays, tableToIPC } from "apache-arrow";
import { InvalidInputError } from "@spark-connect-js/node";
import { spark, stopSession } from "./setup.js";

describe("createDataFrame([]): plain-rows overload", () => {
  after(stopSession);

  it("round-trips a simple string+int table", async () => {
    const df = spark().createDataFrame([
      { name: "alice", age: 30 },
      { name: "bob", age: 25 },
    ]);
    const rows = await df.collect();
    assert.equal(rows.length, 2);
    // String columns arrive as their actual values, not dictionary indices.
    assert.equal(rows[0]["name"], "alice");
    assert.equal(rows[1]["name"], "bob");
    assert.equal(rows[0]["age"], 30);
    assert.equal(rows[1]["age"], 25);
  });

  it("preserves nulls in a mixed-null column", async () => {
    const df = spark().createDataFrame([{ v: 1 }, { v: null }, { v: 3 }]);
    const rows = await df.collect();
    assert.equal(rows[0]["v"], 1);
    assert.equal(rows[1]["v"], null);
    assert.equal(rows[2]["v"], 3);
  });

  it("handles multiple rows with the same string values (no dictionary corruption)", async () => {
    // Regression for the dictionary-encoding bug: repeated strings must not
    // collapse into shared integer indices on the server.
    const df = spark().createDataFrame([
      { tag: "alpha" },
      { tag: "beta" },
      { tag: "alpha" },
      { tag: "beta" },
    ]);
    const rows = await df.collect();
    assert.deepEqual(
      rows.map((r) => r["tag"]),
      ["alpha", "beta", "alpha", "beta"],
    );
  });
});

describe("createDataFrame(bytes): Arrow IPC input validation", () => {
  after(stopSession);

  it("rejects the Arrow file format with a clear error", () => {
    // ARROW1\0\0 magic prefix.
    const fileBytes = new Uint8Array([0x41, 0x52, 0x52, 0x4f, 0x57, 0x31, 0x00, 0x00, 0x00]);
    assert.throws(() => spark().createDataFrame(fileBytes), InvalidInputError);
  });

  it("accepts pre-built stream-format Arrow bytes", async () => {
    // Build via apache-arrow directly. Note that apache-arrow's default
    // tableFromArrays uses Dictionary<Int32, Utf8> for strings; ints are
    // materialized, so this test uses an int column.
    const table = tableFromArrays({ id: [10, 20, 30] });
    const bytes = tableToIPC(table, "stream");
    const df = spark().createDataFrame(bytes);
    const rows = await df.collect();
    assert.equal(rows.length, 3);
    assert.equal(rows[0]["id"], 10);
    assert.equal(rows[2]["id"], 30);
  });
});
