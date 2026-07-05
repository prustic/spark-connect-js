import { describe, it, after } from "node:test";
import assert from "node:assert/strict";
import {
  col,
  lower,
  trim,
  length,
  initcap,
  reverse,
  concat_ws,
  split,
  lpad,
  rpad,
  substring,
  instr,
  translate,
  regexp_replace,
  regexp_extract,
  regexp_extract_all,
  regexp_like,
  regexp_count,
  regexp_substr,
  regexp_instr,
  soundex,
  base64,
  repeat,
} from "@spark-connect-js/node";
import { spark, stopSession } from "./setup.js";

const strData = () =>
  spark().sql(`
    SELECT * FROM VALUES
      ('Hello World', 'spark'),
      ('  trim me  ', 'java'),
      ('fooBarBaz',   'scala')
    AS data(text, lang)
  `);

describe("string functions", () => {
  after(stopSession);

  it("lower / trim / length / initcap / reverse", async () => {
    const rows = await strData()
      .withColumn("lower_text", lower(col("text")))
      .withColumn("trimmed", trim(col("text")))
      .withColumn("len", length(col("text")))
      .withColumn("initcapped", initcap(col("text")))
      .withColumn("reversed", reverse(col("text")))
      .collect();

    assert.equal(rows[0]["lower_text"], "hello world");
    assert.equal(rows[1]["trimmed"], "trim me");
    assert.equal(rows[0]["reversed"], "dlroW olleH");
  });

  it("concat_ws / split / lpad / rpad / substring", async () => {
    const rows = await strData()
      .withColumn("merged", concat_ws("-", col("text"), col("lang")))
      .withColumn("words", split(col("text"), " "))
      .withColumn("lpadded", lpad(col("lang"), 10, "*"))
      .withColumn("rpadded", rpad(col("lang"), 10, "*"))
      .withColumn("sub", substring(col("text"), 1, 5))
      .collect();

    assert.equal(rows[0]["merged"], "Hello World-spark");
    assert.equal(rows[0]["lpadded"], "*****spark");
    assert.equal(rows[0]["rpadded"], "spark*****");
    assert.equal(rows[0]["sub"], "Hello");
  });

  it("instr / translate / regexp_replace / soundex", async () => {
    const rows = await strData()
      .withColumn("pos_o", instr(col("text"), "o"))
      .withColumn("translated", translate(col("text"), "aeiou", "AEIOU"))
      .withColumn("replaced", regexp_replace(col("text"), "[A-Z]", "_"))
      .withColumn("snd", soundex(col("text")))
      .collect();

    // 'o' first appears at position 5 in "Hello World"
    assert.equal(rows[0]["pos_o"], 5);
  });

  it("base64 / repeat", async () => {
    const rows = await strData()
      .withColumn("b64", base64(col("lang")))
      .withColumn("repeated", repeat(col("lang"), 3))
      .collect();

    assert.equal(typeof rows[0]["b64"], "string");
    // TODO: cast needed because Row is Record<string, unknown> (see roadmap M3)
    assert.ok((rows[0]["b64"] as string).length > 0);
    assert.equal(rows[0]["repeated"], "sparksparkspark");
  });

  it("repeat produces correct output", async () => {
    const rows = await spark()
      .sql("SELECT 'ab' AS s")
      .withColumn("rep", repeat(col("s"), 3))
      .collect();
    assert.equal(rows[0]["rep"], "ababab");
  });

  it("regexp extraction and matching family", async () => {
    const rows = await spark()
      .sql("SELECT 'user-42, user-7' AS s")
      .withColumn("first_id", regexp_extract(col("s"), "user-(\\d+)", 1))
      .withColumn("all_ids", regexp_extract_all(col("s"), "user-(\\d+)"))
      .withColumn("is_match", regexp_like(col("s"), "user-\\d+"))
      .withColumn("matches", regexp_count(col("s"), "user-\\d+"))
      .withColumn("first_sub", regexp_substr(col("s"), "user-\\d+"))
      .withColumn("pos", regexp_instr(col("s"), "user-\\d+"))
      .collect();

    assert.equal(rows[0]["first_id"], "42");
    assert.deepStrictEqual(rows[0]["all_ids"], ["42", "7"]);
    assert.equal(rows[0]["is_match"], true);
    assert.equal(rows[0]["matches"], 2);
    assert.equal(rows[0]["first_sub"], "user-42");
    assert.equal(rows[0]["pos"], 1);
  });

  it("regexp family no-match behavior", async () => {
    const rows = await spark()
      .sql("SELECT 'nothing here' AS s")
      .withColumn("extracted", regexp_extract(col("s"), "user-(\\d+)", 1))
      .withColumn("is_match", regexp_like(col("s"), "user-\\d+"))
      .withColumn("matches", regexp_count(col("s"), "user-\\d+"))
      .withColumn("first_sub", regexp_substr(col("s"), "user-\\d+"))
      .withColumn("pos", regexp_instr(col("s"), "user-\\d+"))
      .collect();

    assert.equal(rows[0]["extracted"], "");
    assert.equal(rows[0]["is_match"], false);
    assert.equal(rows[0]["matches"], 0);
    assert.equal(rows[0]["first_sub"], null);
    assert.equal(rows[0]["pos"], 0);
  });

  it("regexp family accepts a per-row Column pattern", async () => {
    const rows = await spark()
      .sql("SELECT 'abc-123' AS s, '\\\\d+' AS pat")
      .withColumn("matched", regexp_substr(col("s"), col("pat")))
      .collect();
    assert.equal(rows[0]["matched"], "123");
  });

  it("regexp_extract_all and regexp_instr take an explicit group index", async () => {
    const rows = await spark()
      .sql("SELECT '12-34, 56-78' AS s")
      .withColumn("second_groups", regexp_extract_all(col("s"), "(\\d+)-(\\d+)", 2))
      .withColumn("pos", regexp_instr(col("s"), "(\\d+)-(\\d+)", 1))
      .collect();

    assert.deepStrictEqual(rows[0]["second_groups"], ["34", "78"]);
    assert.equal(rows[0]["pos"], 1);
  });
});
