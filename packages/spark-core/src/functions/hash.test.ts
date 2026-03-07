import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { md5, sha1, sha2, hash, xxhash64, crc32 } from "./hash.js";

import type { Column } from "../column.js";

function assertFn(c: Column, name: string, argCount?: number) {
  assert.equal(c._expr.type, "unresolvedFunction");
  if (c._expr.type === "unresolvedFunction") {
    assert.equal(c._expr.name, name);
    if (argCount !== undefined) assert.equal(c._expr.arguments.length, argCount);
  }
}

describe("hash functions", () => {
  it("md5/sha1/crc32 are unary", () => {
    assertFn(md5("x"), "md5", 1);
    assertFn(sha1("x"), "sha1", 1);
    assertFn(crc32("x"), "crc32", 1);
  });

  it("sha2 takes col and numBits", () => {
    const r = sha2("x", 256);
    assert.equal(r._expr.type, "unresolvedFunction");
    if (r._expr.type === "unresolvedFunction") {
      assert.equal(r._expr.name, "sha2");
      assert.equal(r._expr.arguments.length, 2);
    }
  });

  it("hash/xxhash64 are variadic", () => {
    assertFn(hash("a", "b", "c"), "hash", 3);
    assertFn(xxhash64("a", "b"), "xxhash64", 2);
  });
});
