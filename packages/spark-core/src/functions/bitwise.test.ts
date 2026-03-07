import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  bitwise_not,
  bit_count,
  bit_get,
  getbit,
  shiftleft,
  shiftright,
  shiftrightunsigned,
} from "./bitwise.js";

import type { Column } from "../column.js";

function assertFn(c: Column, name: string, argCount?: number) {
  assert.equal(c._expr.type, "unresolvedFunction");
  if (c._expr.type === "unresolvedFunction") {
    assert.equal(c._expr.name, name);
    if (argCount !== undefined) assert.equal(c._expr.arguments.length, argCount);
  }
}

describe("bitwise functions", () => {
  it("bitwise_not wraps ~", () => {
    assertFn(bitwise_not("x"), "~", 1);
  });

  it("bit_count is unary", () => {
    assertFn(bit_count("x"), "bit_count", 1);
  });

  it("bit_get/getbit take col and pos", () => {
    assertFn(bit_get("x", "pos"), "bit_get", 2);
    assertFn(getbit("x", "pos"), "bit_get", 2);
  });

  it("shiftleft/shiftright/shiftrightunsigned take col and numBits", () => {
    const r1 = shiftleft("x", 2);
    assert.equal(r1._expr.type, "unresolvedFunction");
    if (r1._expr.type === "unresolvedFunction") {
      assert.equal(r1._expr.arguments.length, 2);
    }

    const r2 = shiftright("x", 3);
    assert.equal(r2._expr.type, "unresolvedFunction");

    const r3 = shiftrightunsigned("x", 1);
    assert.equal(r3._expr.type, "unresolvedFunction");
  });
});
