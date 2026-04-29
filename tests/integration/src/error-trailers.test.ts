import { describe, it, after } from "node:test";
import assert from "node:assert/strict";
import { SparkConnectError } from "@spark-connect-js/node";
import { spark, stopSession } from "./setup.js";

describe("SparkConnectError trailer decoding", () => {
  after(stopSession);

  it("populates errorClass and sqlState for an UNRESOLVED_COLUMN failure", async () => {
    let caught: unknown;
    try {
      await spark().sql("SELECT no_such_column FROM VALUES (1) AS t(id)").collect();
    } catch (err) {
      caught = err;
    }

    assert.ok(caught instanceof SparkConnectError, "expected SparkConnectError");
    const e = caught;
    assert.ok(
      typeof e.errorClass === "string" && e.errorClass.startsWith("UNRESOLVED_COLUMN"),
      `expected errorClass to start with UNRESOLVED_COLUMN, got "${String(e.errorClass)}"`,
    );
    assert.ok(typeof e.sqlState === "string" && e.sqlState.length > 0, "expected sqlState");
  });
});
