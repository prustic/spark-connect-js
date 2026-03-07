import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { col } from "../column.js";
import {
  upper,
  lower,
  trim,
  ltrim,
  rtrim,
  btrim,
  length,
  char_length,
  character_length,
  octet_length,
  bit_length,
  concat,
  concat_ws,
  substring,
  regexp_replace,
  contains,
  startswith,
  endswith,
  split,
  initcap,
  lpad,
  rpad,
  repeat,
  reverse,
  instr,
  locate,
  translate,
  ascii,
  format_number,
  format_string,
  base64,
  unbase64,
  soundex,
  levenshtein,
  overlay,
  left,
  right,
  decode,
  encode,
} from "./string.js";

import type { Column } from "../column.js";

function assertFn(c: Column, name: string, argCount?: number) {
  assert.equal(c._expr.type, "unresolvedFunction");
  if (c._expr.type === "unresolvedFunction") {
    assert.equal(c._expr.name, name);
    if (argCount !== undefined) assert.equal(c._expr.arguments.length, argCount);
  }
}

describe("string functions", () => {
  it("upper/lower/trim/ltrim/rtrim/btrim", () => {
    assertFn(upper("name"), "upper", 1);
    assertFn(lower(col("name")), "lower", 1);
    assertFn(trim("name"), "trim", 1);
    assertFn(ltrim("name"), "ltrim", 1);
    assertFn(rtrim("name"), "rtrim", 1);
    assertFn(btrim("name"), "btrim", 1);
  });

  it("length/char_length/character_length/octet_length/bit_length", () => {
    assertFn(length("x"), "length", 1);
    assertFn(char_length("x"), "char_length", 1);
    assertFn(character_length("x"), "char_length", 1);
    assertFn(octet_length("x"), "octet_length", 1);
    assertFn(bit_length("x"), "bit_length", 1);
  });

  it("concat handles multiple columns", () => {
    assertFn(concat("first", "last"), "concat", 2);
  });

  it("concat_ws joins with separator", () => {
    const r = concat_ws(",", "a", "b", "c");
    assert.equal(r._expr.type, "unresolvedFunction");
    if (r._expr.type === "unresolvedFunction") {
      assert.equal(r._expr.name, "concat_ws");
    }
  });

  it("substring passes position and length", () => {
    assertFn(substring("name", 1, 3), "substring", 3);
  });

  it("regexp_replace/contains/startswith/endswith", () => {
    assertFn(regexp_replace("x", "a", "b"), "regexp_replace", 3);
    assertFn(contains("x", "y"), "contains", 2);
    assertFn(startswith("x", "y"), "startswith", 2);
    assertFn(endswith("x", "y"), "endswith", 2);
  });

  it("split with limit", () => {
    const r = split("x", ",", 3);
    assert.equal(r._expr.type, "unresolvedFunction");
    if (r._expr.type === "unresolvedFunction") {
      assert.equal(r._expr.name, "split");
    }
  });

  it("initcap/reverse/ascii/soundex", () => {
    assertFn(initcap("x"), "initcap", 1);
    assertFn(reverse("x"), "reverse", 1);
    assertFn(ascii("x"), "ascii", 1);
    assertFn(soundex("x"), "soundex", 1);
  });

  it("lpad/rpad with length and pad string", () => {
    assertFn(lpad("x", 10, " "), "lpad", 3);
    assertFn(rpad("x", 10, " "), "rpad", 3);
  });

  it("repeat creates unresolvedFunction", () => {
    assertFn(repeat("x", 3), "repeat", 2);
  });

  it("instr/locate/translate", () => {
    assertFn(instr("x", "y"), "instr", 2);
    assertFn(locate("sub", "x"), "locate", 3); // includes default pos=1
    assertFn(translate("x", "abc", "xyz"), "translate", 3);
  });

  it("format_number/format_string", () => {
    const r1 = format_number("x", 2);
    assert.equal(r1._expr.type, "unresolvedFunction");
    const r2 = format_string("%s-%s", "a", "b");
    assert.equal(r2._expr.type, "unresolvedFunction");
  });

  it("base64/unbase64", () => {
    assertFn(base64("x"), "base64", 1);
    assertFn(unbase64("x"), "unbase64", 1);
  });

  it("levenshtein takes two args", () => {
    assertFn(levenshtein("a", "b"), "levenshtein", 2);
  });

  it("overlay takes col pos len", () => {
    const r = overlay("x", "rep", 1, 3);
    assert.equal(r._expr.type, "unresolvedFunction");
    if (r._expr.type === "unresolvedFunction") {
      assert.equal(r._expr.name, "overlay");
    }
  });

  it("left/right take col and length", () => {
    assertFn(left("x", 3), "left", 2);
    assertFn(right("x", 3), "right", 2);
  });

  it("decode/encode", () => {
    assertFn(decode("x", "UTF-8"), "decode", 2);
    assertFn(encode("x", "UTF-8"), "encode", 2);
  });
});
