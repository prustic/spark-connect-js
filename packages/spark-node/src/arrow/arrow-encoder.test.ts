import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { ArrowEncoder } from "./arrow-encoder.js";
import { ArrowDecoder } from "./arrow-decoder.js";
import { RecordBatchReader, Type, type DataType } from "apache-arrow";
import { InvalidInputError } from "@spark-connect-js/core";

describe("ArrowEncoder.encode()", () => {
  it("round-trips a simple string column via the decoder", async () => {
    const bytes = ArrowEncoder.encode([{ name: "alice" }, { name: "bob" }]);
    const rows = await ArrowDecoder.decode([bytes]);
    assert.deepEqual(rows, [{ name: "alice" }, { name: "bob" }]);
  });

  it("emits Utf8 rather than Dictionary for string columns", () => {
    const bytes = ArrowEncoder.encode([{ name: "alice" }, { name: "bob" }]);
    const reader = RecordBatchReader.from(bytes);
    for (const batch of reader) {
      const type = batch.schema.fields[0].type as DataType;
      assert.equal(type.typeId, Type.Utf8);
      // Confirm we did not fall into the Dictionary<Int32, Utf8> default
      assert.equal(type.constructor.name, "Utf8");
    }
  });

  it("round-trips integer numbers as Int32", async () => {
    const bytes = ArrowEncoder.encode([{ id: 1 }, { id: 2 }, { id: 3 }]);
    const rows = await ArrowDecoder.decode([bytes]);
    assert.deepEqual(rows, [{ id: 1 }, { id: 2 }, { id: 3 }]);
  });

  it("round-trips float numbers as Float64", async () => {
    const bytes = ArrowEncoder.encode([{ score: 1.5 }, { score: 2.75 }]);
    const rows = await ArrowDecoder.decode([bytes]);
    assert.equal(rows[0]["score"], 1.5);
    assert.equal(rows[1]["score"], 2.75);
  });

  it("round-trips booleans", async () => {
    const bytes = ArrowEncoder.encode([{ active: true }, { active: false }]);
    const rows = await ArrowDecoder.decode([bytes]);
    assert.equal(rows[0]["active"], true);
    assert.equal(rows[1]["active"], false);
  });

  it("round-trips bigints via Int64", async () => {
    const bytes = ArrowEncoder.encode([{ big: 9_000_000_000n }]);
    const rows = await ArrowDecoder.decode([bytes]);
    // Long decode policy is parked. Safe-range values return number.
    assert.equal(rows[0]["big"], 9_000_000_000);
  });

  it("round-trips Dates via TimestampMillisecond", async () => {
    const d = new Date("2026-06-29T13:45:06.500Z");
    const bytes = ArrowEncoder.encode([{ t: d }]);
    const rows = await ArrowDecoder.decode([bytes]);
    assert.ok(rows[0]["t"] instanceof Date);
    assert.equal(rows[0]["t"].toISOString(), d.toISOString());
  });

  it("preserves null values in a typed column", async () => {
    const bytes = ArrowEncoder.encode([{ id: 1 }, { id: null }, { id: 3 }]);
    const rows = await ArrowDecoder.decode([bytes]);
    assert.equal(rows[0]["id"], 1);
    assert.equal(rows[1]["id"], null);
    assert.equal(rows[2]["id"], 3);
  });

  it("encodes multiple columns of mixed types together", async () => {
    const bytes = ArrowEncoder.encode([
      { id: 1, name: "alice", active: true },
      { id: 2, name: "bob", active: false },
    ]);
    const rows = await ArrowDecoder.decode([bytes]);
    assert.deepEqual(rows, [
      { id: 1, name: "alice", active: true },
      { id: 2, name: "bob", active: false },
    ]);
  });

  it("throws InvalidInputError on empty input", () => {
    assert.throws(() => ArrowEncoder.encode([]), InvalidInputError);
  });

  it("throws InvalidInputError on rows with no fields", () => {
    assert.throws(() => ArrowEncoder.encode([{}]), InvalidInputError);
  });

  it("throws a clear error for unsupported value types", () => {
    assert.throws(
      () => ArrowEncoder.encode([{ tuple: [1, 2, 3] }]),
      (err: unknown) => {
        if (!(err instanceof InvalidInputError)) return false;
        assert.match(err.message, /unsupported/);
        assert.match(err.message, /tuple/);
        return true;
      },
    );
  });

  it("throws a clear error when a column mixes categories", () => {
    assert.throws(
      () => ArrowEncoder.encode([{ v: 1 }, { v: "hello" }]),
      (err: unknown) => {
        if (!(err instanceof InvalidInputError)) return false;
        assert.match(err.message, /column "v"/);
        assert.match(err.message, /mixes number and string/);
        assert.match(err.message, /row 1/);
        return true;
      },
    );
  });
});
