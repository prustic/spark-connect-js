import { describe, it, after } from "node:test";
import assert from "node:assert/strict";
import {
  col,
  lit,
  md5,
  sha1,
  sha2,
  hash,
  xxhash64,
  crc32,
  to_json,
  get_json_object,
  json_array_length,
  json_object_keys,
  struct,
  bitwise_not,
  shiftleft,
  shiftright,
} from "@spark-connect-js/node";
import { spark, stopSession } from "./setup.js";

describe("hash functions", () => {
  after(stopSession);

  it("md5 / sha1 / sha2 / hash / xxhash64 / crc32", async () => {
    const employees = spark().sql(`
      SELECT * FROM VALUES ('Alice'), ('Bob'), ('Carol') AS data(name)
    `);

    const rows = await employees
      .withColumn("md5_name", md5(col("name")))
      .withColumn("sha1_name", sha1(col("name")))
      .withColumn("sha2_name", sha2(col("name"), 256))
      .withColumn("hash_name", hash(col("name")))
      .withColumn("xxh64_name", xxhash64(col("name")))
      .withColumn("crc32_name", crc32(col("name")))
      .collect();

    assert.equal(rows.length, 3);
    // TODO: casts needed because Row is Record<string, unknown> (see roadmap M3)
    // md5 returns 32-char hex string
    assert.equal((rows[0]["md5_name"] as string).length, 32);
    // sha1 returns 40-char hex string
    assert.equal((rows[0]["sha1_name"] as string).length, 40);
    // sha2(256) returns 64-char hex string
    assert.equal((rows[0]["sha2_name"] as string).length, 64);
    // hash and xxhash64 return integers
    assert.equal(typeof rows[0]["hash_name"], "number");
  });
});

describe("JSON functions", () => {
  after(stopSession);

  it("to_json / get_json_object / json_array_length / json_object_keys", async () => {
    const df = spark().sql(`
      SELECT * FROM VALUES
        ('{"name":"Alice","age":30}'),
        ('{"name":"Bob","age":25}'),
        ('[1,2,3]')
      AS data(json_str)
    `);

    const rows = await df
      .withColumn("as_struct", to_json(struct(lit("hello").as("greeting"))))
      .withColumn("get_name", get_json_object(col("json_str"), "$.name"))
      .withColumn("arr_len", json_array_length(lit("[1,2,3]")))
      .withColumn("obj_keys", json_object_keys(col("json_str")))
      .collect();

    assert.equal(rows[0]["get_name"], "Alice");
    assert.equal(rows[1]["get_name"], "Bob");
    assert.equal(rows[0]["arr_len"], 3);
  });
});

describe("bitwise functions", () => {
  after(stopSession);

  it("bitwise_not / shiftleft / shiftright", async () => {
    const rows = await spark()
      .range(1, 5)
      .withColumn("not_id", bitwise_not(col("id")))
      .withColumn("shl_2", shiftleft(col("id"), 2))
      .withColumn("shr_1", shiftright(col("id"), 1))
      .collect();

    assert.equal(rows.length, 4);
    // shiftleft(1, 2) = 4
    assert.equal(rows[0]["shl_2"], 4);
    // shiftright(2, 1) = 1
    assert.equal(rows[1]["shr_1"], 1);
    // bitwise_not(1) = -2
    assert.equal(rows[0]["not_id"], -2);
  });
});
