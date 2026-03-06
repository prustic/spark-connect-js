import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { ArrowDecoder } from "./arrow-decoder.js";
import { tableFromArrays, tableToIPC } from "apache-arrow";

/** Create an Arrow IPC stream buffer from column data. */
function makeArrowChunk(columns: Record<string, unknown[]>): Uint8Array {
  const table = tableFromArrays(columns);
  return tableToIPC(table, "stream");
}

describe("ArrowDecoder.decode()", () => {
  it("returns empty array for no chunks", async () => {
    const rows = await ArrowDecoder.decode([]);
    assert.deepStrictEqual(rows, []);
  });

  it("decodes a single chunk with integer data", async () => {
    const chunk = makeArrowChunk({ id: [1, 2, 3] });
    const rows = await ArrowDecoder.decode([chunk]);
    assert.equal(rows.length, 3);
    assert.equal(rows[0]["id"], 1);
    assert.equal(rows[2]["id"], 3);
  });

  it("decodes multiple independent chunks", async () => {
    const chunk1 = makeArrowChunk({ id: [1, 2] });
    const chunk2 = makeArrowChunk({ id: [3, 4] });
    const rows = await ArrowDecoder.decode([chunk1, chunk2]);
    assert.equal(rows.length, 4);
    assert.equal(rows[0]["id"], 1);
    assert.equal(rows[3]["id"], 4);
  });

  it("decodes string columns", async () => {
    const chunk = makeArrowChunk({ name: ["Alice", "Bob"] });
    const rows = await ArrowDecoder.decode([chunk]);
    assert.equal(rows[0]["name"], "Alice");
    assert.equal(rows[1]["name"], "Bob");
  });

  it("decodes multiple columns", async () => {
    const chunk = makeArrowChunk({
      id: [1, 2],
      name: ["Alice", "Bob"],
    });
    const rows = await ArrowDecoder.decode([chunk]);
    assert.equal(rows.length, 2);
    assert.equal(rows[0]["id"], 1);
    assert.equal(rows[0]["name"], "Alice");
  });

  it("decodes boolean columns", async () => {
    const chunk = makeArrowChunk({ active: [true, false, true] });
    const rows = await ArrowDecoder.decode([chunk]);
    assert.equal(rows[0]["active"], true);
    assert.equal(rows[1]["active"], false);
  });

  it("decodes float columns", async () => {
    const chunk = makeArrowChunk({ score: [1.5, 2.7, 3.14] });
    const rows = await ArrowDecoder.decode([chunk]);
    assert.ok(Math.abs((rows[0]["score"] as number) - 1.5) < 0.01);
  });
});
