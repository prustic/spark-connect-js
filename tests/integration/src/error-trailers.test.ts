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

    if (!(caught instanceof SparkConnectError)) {
      throw new Error(`expected SparkConnectError, got ${String(caught)}`);
    }
    assert.ok(
      typeof caught.errorClass === "string" && caught.errorClass.startsWith("UNRESOLVED_COLUMN"),
      `expected errorClass to start with UNRESOLVED_COLUMN, got "${String(caught.errorClass)}"`,
    );
    assert.ok(
      typeof caught.sqlState === "string" && caught.sqlState.length > 0,
      "expected sqlState",
    );
  });

  it("populates errorTypeHierarchy via FetchErrorDetails", async () => {
    let caught: unknown;
    try {
      await spark().sql("SELECT another_missing FROM VALUES (1) AS t(id)").collect();
    } catch (err) {
      caught = err;
    }

    if (!(caught instanceof SparkConnectError)) {
      throw new Error(`expected SparkConnectError, got ${String(caught)}`);
    }
    assert.ok(
      Array.isArray(caught.errorTypeHierarchy) && caught.errorTypeHierarchy.length > 0,
      "expected non-empty errorTypeHierarchy from FetchErrorDetails",
    );
    assert.ok(
      caught.errorTypeHierarchy.some((c: string) => c.includes("AnalysisException")),
      `expected AnalysisException in errorTypeHierarchy, got ${JSON.stringify(caught.errorTypeHierarchy)}`,
    );
  });
});
