import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { col, lit } from "../column.js";
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
  regexp_extract,
  regexp_extract_all,
  regexp_like,
  regexp_count,
  regexp_substr,
  regexp_instr,
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
  ucase,
  lcase,
  rlike,
  regexp,
  like,
  ilike,
  replace,
  position,
  find_in_set,
  printf,
  substr,
  substring_index,
  split_part,
  elt,
  char,
  chr,
  collate,
  collation,
  mask,
  randstr,
  sentences,
  str_to_map,
  to_binary,
  try_to_binary,
  to_char,
  to_varchar,
  to_number,
  try_to_number,
  is_valid_utf8,
  make_valid_utf8,
  validate_utf8,
  try_validate_utf8,
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

  it("regexp extraction and matching family", () => {
    assertFn(regexp_extract("x", "(a)(b)", 1), "regexp_extract", 3);
    assertFn(regexp_extract_all("x", "(a)"), "regexp_extract_all", 2);
    assertFn(regexp_extract_all("x", "(a)", 2), "regexp_extract_all", 3);
    assertFn(regexp_like("x", "a+"), "regexp_like", 2);
    assertFn(regexp_count("x", "a"), "regexp_count", 2);
    assertFn(regexp_substr("x", "a+"), "regexp_substr", 2);
    assertFn(regexp_instr("x", "a+"), "regexp_instr", 2);
    assertFn(regexp_instr("x", "(a)", 1), "regexp_instr", 3);
  });

  it("reads a bare string pattern or value as a column name, as PySpark does", () => {
    const cases: [string, Column][] = [
      ["regexp_extract_all", regexp_extract_all("s", "p")],
      ["regexp_like", regexp_like("s", "p")],
      ["regexp_count", regexp_count("s", "p")],
      ["regexp_substr", regexp_substr("s", "p")],
      ["regexp_instr", regexp_instr("s", "p")],
      ["contains", contains("s", "p")],
      ["startswith", startswith("s", "p")],
      ["endswith", endswith("s", "p")],
    ];
    for (const [name, result] of cases) {
      assert.ok(result._expr.type === "unresolvedFunction", name);
      assert.deepStrictEqual(
        result._expr.arguments[1],
        { type: "unresolvedAttribute", name: "p" },
        name,
      );
    }
  });

  it("takes a literal pattern or value through lit()", () => {
    for (const result of [regexp_like("s", lit("a+")), contains("s", lit("a"))]) {
      assert.ok(result._expr.type === "unresolvedFunction");
      assert.equal(result._expr.arguments[1].type, "literal");
    }
  });

  it("regexp family accepts a Column pattern", () => {
    for (const result of [
      regexp_extract_all("s", col("pat")),
      regexp_like("s", col("pat")),
      regexp_count("s", col("pat")),
      regexp_substr("s", col("pat")),
      regexp_instr("s", col("pat")),
    ]) {
      assert.equal(result._expr.type, "unresolvedFunction");
      if (result._expr.type === "unresolvedFunction") {
        const patternExpr = result._expr.arguments[1];
        assert.equal(patternExpr.type, "unresolvedAttribute");
      }
    }
  });

  it("regexp_replace with a Column pattern forwards the expression as-is", () => {
    const result = regexp_replace("s", col("pat"), "X");
    if (result._expr.type === "unresolvedFunction") {
      const patternExpr = result._expr.arguments[1];
      assert.equal(patternExpr.type, "unresolvedAttribute");
      if (patternExpr.type === "unresolvedAttribute") {
        assert.equal(patternExpr.name, "pat");
      }
    }
  });

  it("regexp_replace with a Column replacement forwards the expression as-is", () => {
    const result = regexp_replace("s", "a", col("repl"));
    if (result._expr.type === "unresolvedFunction") {
      const replacementExpr = result._expr.arguments[2];
      assert.equal(replacementExpr.type, "unresolvedAttribute");
      if (replacementExpr.type === "unresolvedAttribute") {
        assert.equal(replacementExpr.name, "repl");
      }
    }
  });

  it("regexp_replace with string pattern/replacement wraps them in lit()", () => {
    const result = regexp_replace("s", "a", "b");
    if (result._expr.type === "unresolvedFunction") {
      const patternExpr = result._expr.arguments[1];
      const replacementExpr = result._expr.arguments[2];
      assert.equal(patternExpr.type, "literal");
      assert.equal(replacementExpr.type, "literal");
    }
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

describe("string functions added for PySpark parity", () => {
  it("send the function name PySpark sends", () => {
    const cases: [Column, string, number][] = [
      [ucase("s"), "ucase", 1],
      [lcase("s"), "lcase", 1],
      [rlike("s", "p"), "rlike", 2],
      [regexp("s", "p"), "regexp", 2],
      [like("s", "p"), "like", 2],
      [like("s", "p", "e"), "like", 3],
      [ilike("s", "p"), "ilike", 2],
      [ilike("s", "p", "e"), "ilike", 3],
      [replace("s", "a"), "replace", 2],
      [replace("s", "a", "b"), "replace", 3],
      [position("a", "s"), "position", 2],
      [position("a", "s", "n"), "position", 3],
      [find_in_set("s", "list"), "find_in_set", 2],
      [printf("f", "a", "b"), "printf", 3],
      [substr("s", "p"), "substr", 2],
      [substr("s", "p", "l"), "substr", 3],
      [split_part("s", "d", "n"), "split_part", 3],
      [elt("n", "a", "b"), "elt", 3],
      [char("n"), "char", 1],
      [chr("n"), "chr", 1],
      [collation("s"), "collation", 1],
      [to_binary("s"), "to_binary", 1],
      [to_binary("s", "f"), "to_binary", 2],
      [try_to_binary("s", "f"), "try_to_binary", 2],
      [to_char("n", "f"), "to_char", 2],
      [to_varchar("n", "f"), "to_varchar", 2],
      [to_number("s", "f"), "to_number", 2],
      [try_to_number("s", "f"), "try_to_number", 2],
      [is_valid_utf8("s"), "is_valid_utf8", 1],
      [make_valid_utf8("s"), "make_valid_utf8", 1],
      [validate_utf8("s"), "validate_utf8", 1],
      [try_validate_utf8("s"), "try_validate_utf8", 1],
    ];
    for (const [result, name, argCount] of cases) {
      assertFn(result, name, argCount);
    }
  });

  it("reads a bare string pattern, format, or delimiter as a column name, as PySpark does", () => {
    const cases: [string, Column, number][] = [
      ["like", like("s", "p"), 1],
      ["ilike", ilike("s", "p"), 1],
      ["rlike", rlike("s", "p"), 1],
      ["replace", replace("s", "p"), 1],
      ["split_part", split_part("s", "p", "n"), 1],
      ["printf", printf("p", "a"), 0],
      ["to_char", to_char("s", "p"), 1],
      ["mask", mask("s", "p"), 1],
      ["str_to_map", str_to_map("s", "p"), 1],
    ];
    for (const [name, result, index] of cases) {
      assert.ok(result._expr.type === "unresolvedFunction", name);
      assert.deepStrictEqual(
        result._expr.arguments[index],
        { type: "unresolvedAttribute", name: "p" },
        name,
      );
    }
  });

  it("takes collate's collation and substring_index's delimiter and count as literals", () => {
    const collated = collate("s", "UTF8_LCASE");
    const indexed = substring_index("s", ".", 2);
    assert.ok(collated._expr.type === "unresolvedFunction");
    assert.ok(indexed._expr.type === "unresolvedFunction");
    assert.deepStrictEqual(collated._expr.arguments[1], { type: "literal", value: "UTF8_LCASE" });
    assert.deepStrictEqual(indexed._expr.arguments.slice(1), [
      { type: "literal", value: "." },
      { type: "literal", value: 2 },
    ]);
  });

  it("fills PySpark's defaults for mask, sentences, and str_to_map", () => {
    const literals = (c: Column) =>
      c._expr.type === "unresolvedFunction"
        ? c._expr.arguments.slice(1).map((a) => (a.type === "literal" ? a.value : a.type))
        : [];
    assert.deepStrictEqual(literals(mask("s")), ["X", "x", "n", null]);
    assert.deepStrictEqual(literals(mask("s", lit("U"))), ["U", "x", "n", null]);
    assert.deepStrictEqual(literals(sentences("s")), ["", ""]);
    assert.deepStrictEqual(literals(str_to_map("s")), [",", ":"]);
  });

  it("randstr picks a seed on the client when none is given", () => {
    const result = randstr(5);
    assert.ok(result._expr.type === "unresolvedFunction");
    assert.deepStrictEqual(result._expr.arguments[0], { type: "literal", value: 5 });
    const seed = result._expr.arguments[1];
    assert.ok(seed?.type === "literal" && typeof seed.value === "bigint");
  });
});
