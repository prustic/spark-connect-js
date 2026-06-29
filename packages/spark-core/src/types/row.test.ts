import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { row, type Row } from "./row.js";
import { InvalidInputError } from "../errors.js";

describe("row runtime accessors", () => {
  const r: Row = {
    intCol: 42,
    longCol: 9_000_000_000n,
    doubleCol: 3.14,
    stringCol: "hello",
    boolCol: true,
    binaryCol: new Uint8Array([1, 2, 3]),
    dateCol: new Date("2026-01-01"),
    nullCol: null,
  };

  describe("getInt", () => {
    it("returns the value when the column is an integer number", () => {
      assert.equal(row.getInt(r, "intCol"), 42);
    });

    it("returns null when the value is null", () => {
      assert.equal(row.getInt(r, "nullCol"), null);
    });

    it("throws on a non-integer number", () => {
      assert.throws(() => row.getInt(r, "doubleCol"), InvalidInputError);
    });

    it("throws on a bigint (suggesting getLong)", () => {
      assert.throws(() => row.getInt(r, "longCol"), /expected integer, got bigint/);
    });

    it("throws on a missing column", () => {
      assert.throws(() => row.getInt(r, "nope"), /Column "nope" not found/);
    });
  });

  describe("getLong", () => {
    it("returns the value when the column is a bigint", () => {
      assert.equal(row.getLong(r, "longCol"), 9_000_000_000n);
    });

    it("returns null when the value is null", () => {
      assert.equal(row.getLong(r, "nullCol"), null);
    });

    it("throws on a number (suggesting getInt or getDouble)", () => {
      assert.throws(() => row.getLong(r, "intCol"), /expected bigint, got number/);
    });
  });

  describe("getDouble", () => {
    it("returns the value when the column is any number", () => {
      assert.equal(row.getDouble(r, "doubleCol"), 3.14);
      assert.equal(row.getDouble(r, "intCol"), 42);
    });

    it("returns null when the value is null", () => {
      assert.equal(row.getDouble(r, "nullCol"), null);
    });

    it("throws on a bigint", () => {
      assert.throws(() => row.getDouble(r, "longCol"), /expected number, got bigint/);
    });
  });

  describe("getString", () => {
    it("returns the value when the column is a string", () => {
      assert.equal(row.getString(r, "stringCol"), "hello");
    });

    it("returns null when the value is null", () => {
      assert.equal(row.getString(r, "nullCol"), null);
    });

    it("throws on a number", () => {
      assert.throws(() => row.getString(r, "intCol"), /expected string, got number/);
    });
  });

  describe("getBoolean", () => {
    it("returns the value when the column is a boolean", () => {
      assert.equal(row.getBoolean(r, "boolCol"), true);
    });

    it("throws on a number", () => {
      assert.throws(() => row.getBoolean(r, "intCol"), /expected boolean, got number/);
    });
  });

  describe("getBinary", () => {
    it("returns the value when the column is a Uint8Array", () => {
      assert.deepEqual(row.getBinary(r, "binaryCol"), new Uint8Array([1, 2, 3]));
    });

    it("throws on a string", () => {
      assert.throws(() => row.getBinary(r, "stringCol"), InvalidInputError);
    });
  });

  describe("getDate", () => {
    it("returns the value when the column is a Date", () => {
      const d = row.getDate(r, "dateCol");
      assert.ok(d instanceof Date);
      assert.equal(d?.toISOString(), "2026-01-01T00:00:00.000Z");
    });

    it("throws on a string", () => {
      assert.throws(() => row.getDate(r, "stringCol"), /expected Date, got string/);
    });
  });

  it("missing-column error lists available columns", () => {
    try {
      row.getInt(r, "nope");
      assert.fail("expected throw");
    } catch (err) {
      assert.ok(err instanceof InvalidInputError);
      assert.match(err.message, /Available: intCol, longCol/);
    }
  });

  it("treats prototype-inherited properties as not found", () => {
    // `"toString" in r` is true via prototype, but it's not an own property.
    // Plain `in` would mistakenly accept it and return Object.prototype.toString.
    assert.throws(() => row.getString(r, "toString"), /Column "toString" not found/);
    assert.throws(() => row.getString(r, "hasOwnProperty"), /not found/);
  });

  it("rejects null/undefined rows with a domain error", () => {
    assert.throws(() => row.getInt(null as unknown as Row, "col"), /null\/undefined row/);
    assert.throws(() => row.getInt(undefined as unknown as Row, "col"), /null\/undefined row/);
  });
});
