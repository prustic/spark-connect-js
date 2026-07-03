import { describe, it, after } from "node:test";
import assert from "node:assert/strict";
import { InvalidInputError, lit } from "@spark-connect-js/node";
import { spark, stopSession } from "./setup.js";

describe("lit() safety", () => {
  after(stopSession);

  it("lit(null) round-trips without server error", async () => {
    const rows = await spark().range(1).withColumn("n", lit(null)).collect();
    assert.equal(rows[0]["n"], null);
  });

  it("lit(null).cast(string) produces a typed null column", async () => {
    const rows = await spark().range(1).withColumn("s", lit(null).cast("string")).collect();
    assert.equal(rows[0]["s"], null);
  });

  it("lit(null).cast(int) produces a typed null column", async () => {
    const rows = await spark().range(1).withColumn("i", lit(null).cast("int")).collect();
    assert.equal(rows[0]["i"], null);
  });

  it("lit(undefined) throws InvalidInputError before any RPC", () => {
    assert.throws(() => lit(undefined as unknown as null), InvalidInputError);
  });
});
