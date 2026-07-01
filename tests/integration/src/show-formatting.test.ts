import { describe, it, after } from "node:test";
import assert from "node:assert/strict";
import { spark, stopSession } from "./setup.js";

/**
 * Capture console.log output during the callback. show() writes each table
 * line via console.log, so capturing gives us the full rendered table.
 */
async function captureShow(run: () => Promise<void>): Promise<string> {
  const original = console.log;
  const lines: string[] = [];
  console.log = (msg: string) => {
    lines.push(msg);
  };
  try {
    await run();
  } finally {
    console.log = original;
  }
  return lines.join("\n");
}

describe("show() Spark-style formatting", () => {
  after(stopSession);

  it("renders DATE columns without quotes or T/Z markers", async () => {
    const df = spark().sql(`SELECT DATE '2026-06-29' AS d`);
    const out = await captureShow(() => df.show());
    assert.match(out, /2026-06-29/);
    assert.doesNotMatch(out, /"2026-06-29/);
    assert.doesNotMatch(out, /T\d\d:\d\d:\d\d/);
    assert.doesNotMatch(out, /Z\b/);
  });

  it("renders TIMESTAMP columns as space-separated with trailing zeros stripped", async () => {
    const df = spark().sql(`SELECT TIMESTAMP '2026-06-29 13:45:06.5' AS t`);
    // The full timestamp is 21 chars, so run without truncation.
    const out = await captureShow(() => df.show(20, false));
    assert.match(out, /2026-06-29 13:45:06\.5/);
    assert.doesNotMatch(out, /"2026-06-29/);
  });

  it("renders DECIMAL columns as fixed-point strings without quotes", async () => {
    const df = spark().sql(`SELECT CAST(1.5 AS DECIMAL(10,2)) AS amt`);
    const out = await captureShow(() => df.show());
    assert.match(out, /1\.50/);
    assert.doesNotMatch(out, /"1\.50"/);
    assert.doesNotMatch(out, /"150"/);
  });

  it("renders MAP columns in Spark arrow notation", async () => {
    const df = spark().sql(`SELECT map(1, 'a', 2, 'b') AS m`);
    const out = await captureShow(() => df.show());
    assert.match(out, /1 -> a/);
    assert.match(out, /2 -> b/);
    assert.doesNotMatch(out, /"1":/);
  });

  it("renders STRUCT columns as brace-comma values", async () => {
    const df = spark().sql(`SELECT named_struct('name', 'Alice', 'age', 30) AS s`);
    const out = await captureShow(() => df.show());
    assert.match(out, /\{Alice, 30\}/);
    assert.doesNotMatch(out, /"name":"Alice"/);
  });

  it("renders ARRAY columns with element-wise recursion", async () => {
    const df = spark().sql(`SELECT array(DATE '2026-06-29', DATE '2026-07-01') AS xs`);
    // truncate=false so the full array survives for the regex check.
    const out = await captureShow(() => df.show(20, false));
    assert.match(out, /\[2026-06-29, 2026-07-01\]/);
  });
});
