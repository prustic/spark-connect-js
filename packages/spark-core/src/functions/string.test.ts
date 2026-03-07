import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { col } from "../column.js";
import { upper, lower, trim, length, concat, substring } from "./string.js";

describe("string functions", () => {
  it("upper() creates unresolvedFunction", () => {
    const result = upper("name");
    assert.equal(result._expr.type, "unresolvedFunction");
    if (result._expr.type === "unresolvedFunction") {
      assert.equal(result._expr.name, "upper");
    }
  });

  it("lower() creates unresolvedFunction", () => {
    const result = lower(col("name"));
    if (result._expr.type === "unresolvedFunction") {
      assert.equal(result._expr.name, "lower");
    }
  });

  it("trim() creates unresolvedFunction", () => {
    assert.equal(trim("name")._expr.type, "unresolvedFunction");
  });

  it("length() creates unresolvedFunction", () => {
    const result = length("name");
    if (result._expr.type === "unresolvedFunction") {
      assert.equal(result._expr.name, "length");
    }
  });

  it("concat() handles multiple columns", () => {
    const result = concat("first", "last");
    if (result._expr.type === "unresolvedFunction") {
      assert.equal(result._expr.name, "concat");
      assert.equal(result._expr.arguments.length, 2);
    }
  });

  it("substring() passes position and length", () => {
    const result = substring("name", 1, 3);
    if (result._expr.type === "unresolvedFunction") {
      assert.equal(result._expr.name, "substring");
      assert.equal(result._expr.arguments.length, 3);
    }
  });
});
