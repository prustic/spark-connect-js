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
});
