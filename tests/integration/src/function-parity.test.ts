import { describe, it, after } from "node:test";
import assert from "node:assert/strict";
import {
  lit,
  type Column,
  acosh,
  asinh,
  atanh,
  cot,
  csc,
  sec,
  ceil,
  ceiling,
  floor,
  conv,
  ln,
  power,
  random,
  rand,
  randn,
  uniform,
  width_bucket,
  try_add,
  try_subtract,
  try_multiply,
  try_divide,
  try_mod,
  equal_null,
  zeroifnull,
  nullifzero,
  char,
  chr,
  collate,
  collation,
  elt,
  find_in_set,
  lcase,
  ucase,
  mask,
  position,
  printf,
  randstr,
  regexp,
  like,
  ilike,
  rlike,
  replace,
  sentences,
  split_part,
  substr,
  substring_index,
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
  sha,
  str_to_map,
} from "@spark-connect-js/node";
import { spark, stopSession } from "./setup.js";

const SOURCE = `SELECT * FROM VALUES
  (1.5, 0.5, 3, 0, 72, 'Hello World 42', 'H%', 'a,b,c', 'a:1,b:2', '1F', '$12.50', CAST(NULL AS INT)),
  (2.0, -0.25, 5, 2, 97, 'abc DEF 7', 'a%', 'x,y', 'k:v', 'FF', '$3.00', 4)
  AS t(x, h, n, z, code, s, pat, list, kv, hx, money, nn)`;

// Each function runs through the client and as the same SQL on the server.
// Columns are left unaliased, so the generated names, which carry the function
// name the client sent, are compared along with the values.
const CASES: [Column, string][] = [
  [acosh("x"), "acosh(x)"],
  [asinh("x"), "asinh(x)"],
  [atanh("h"), "atanh(h)"],
  [cot("x"), "cot(x)"],
  [csc("x"), "csc(x)"],
  [sec("x"), "sec(x)"],
  [ceiling("x"), "ceiling(x)"],
  [ceiling("x", 1), "ceiling(x, 1)"],
  [ceil("x", 1), "ceil(x, 1)"],
  [floor("x", lit(1)), "floor(x, 1)"],
  [conv("hx", 16, 2), "conv(hx, 16, 2)"],
  [ln("x"), "ln(x)"],
  [power("x", 2), "power(x, 2)"],
  [random(7), "rand(7)"],
  [uniform(0, 10, 7), "uniform(0, 10, 7)"],
  [width_bucket("x", lit(0), lit(10), 5), "width_bucket(x, 0, 10, 5)"],
  [width_bucket("x", lit(0), lit(10), "n"), "width_bucket(x, 0, 10, n)"],
  [try_add("n", "z"), "try_add(n, z)"],
  [try_subtract("n", "z"), "try_subtract(n, z)"],
  [try_multiply("n", "z"), "try_multiply(n, z)"],
  [try_divide("n", "z"), "try_divide(n, z)"],
  [try_mod("n", "z"), "try_mod(n, z)"],
  [equal_null("nn", "z"), "equal_null(nn, z)"],
  [zeroifnull("nn"), "zeroifnull(nn)"],
  [nullifzero("z"), "nullifzero(z)"],
  [char("code"), "char(code)"],
  [chr("code"), "chr(code)"],
  [collate("s", "UTF8_LCASE"), "collate(s, 'UTF8_LCASE')"],
  [collation(collate("s", "UTF8_LCASE")), "collation(collate(s, 'UTF8_LCASE'))"],
  [elt(lit(2), "s", "list"), "elt(2, s, list)"],
  [find_in_set(lit("b"), "list"), "find_in_set('b', list)"],
  [lcase("s"), "lcase(s)"],
  [ucase("s"), "ucase(s)"],
  [mask("s"), "mask(s)"],
  [mask("s", lit("U"), lit("l")), "mask(s, 'U', 'l')"],
  [position(lit("o"), "s"), "position('o', s)"],
  [position(lit("o"), "s", lit(6)), "position('o', s, 6)"],
  [printf(lit("%s-%d"), "s", "n"), "printf('%s-%d', s, n)"],
  [randstr(5, 7), "randstr(5, 7)"],
  [regexp("s", lit("^H")), "regexp(s, '^H')"],
  [rlike("s", lit("[0-9]+")), "rlike(s, '[0-9]+')"],
  [like("s", lit("H%")), "like(s, 'H%')"],
  [like("s", "pat"), "like(s, pat)"],
  [like("s", lit("H!%"), lit("!")), "like(s, 'H!%', '!')"],
  [ilike("s", lit("h%")), "ilike(s, 'h%')"],
  [replace("s", lit("o"), lit("0")), "replace(s, 'o', '0')"],
  [replace("s", lit("l")), "replace(s, 'l')"],
  [sentences("s"), "sentences(s)"],
  [split_part("list", lit(","), lit(2)), "split_part(list, ',', 2)"],
  [substr("s", lit(2), lit(3)), "substr(s, 2, 3)"],
  [substr("s", "n"), "substr(s, n)"],
  [substring_index("list", ",", 2), "substring_index(list, ',', 2)"],
  [to_binary("hx"), "to_binary(hx)"],
  [to_binary("s", lit("utf-8")), "to_binary(s, 'utf-8')"],
  [try_to_binary("s", lit("hex")), "try_to_binary(s, 'hex')"],
  [to_char("n", lit("999")), "to_char(n, '999')"],
  [to_varchar("n", lit("999")), "to_varchar(n, '999')"],
  [to_number("money", lit("$99.99")), "to_number(money, '$99.99')"],
  [try_to_number("s", lit("99")), "try_to_number(s, '99')"],
  [is_valid_utf8("s"), "is_valid_utf8(s)"],
  [make_valid_utf8("s"), "make_valid_utf8(s)"],
  [validate_utf8("s"), "validate_utf8(s)"],
  [try_validate_utf8("s"), "try_validate_utf8(s)"],
  [sha("s"), "sha(s)"],
  [str_to_map("kv"), "str_to_map(kv)"],
  [str_to_map("kv", lit(","), lit(":")), "str_to_map(kv, ',', ':')"],
];

describe("Functions match the same SQL on the server", () => {
  after(stopSession);

  for (const [ours, sql] of CASES) {
    it(sql, async () => {
      const expected = await spark().sql(`SELECT ${sql} FROM (${SOURCE})`).collect();
      const actual = await spark().sql(SOURCE).select(ours).collect();

      assert.deepEqual(actual, expected);
    });
  }

  it("a random function without a seed draws the same values on every action", async () => {
    const df = spark().sql(SOURCE).select(rand(), randn(), uniform(0, 100), randstr(8));

    assert.deepEqual(await df.collect(), await df.collect());
  });
});
