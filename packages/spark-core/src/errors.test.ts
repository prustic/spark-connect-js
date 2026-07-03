import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  SparkConnectError,
  SparkClientError,
  InvalidConfigError,
  InvalidInputError,
  UnsupportedOperationError,
  GrpcStatusCode,
  isSessionInvalidated,
} from "./errors.js";

describe("SparkConnectError", () => {
  it("sets name, code, message", () => {
    const err = new SparkConnectError("connection failed", { code: GrpcStatusCode.UNAVAILABLE });
    assert.equal(err.name, "SparkConnectError");
    assert.equal(err.code, 14);
    assert.equal(err.message, "connection failed");
    assert.ok(err instanceof Error);
  });

  it("carries errorClass and sqlState", () => {
    const err = new SparkConnectError("not found", {
      code: GrpcStatusCode.NOT_FOUND,
      errorClass: "TABLE_OR_VIEW_NOT_FOUND",
      sqlState: "42P01",
    });
    assert.equal(err.errorClass, "TABLE_OR_VIEW_NOT_FOUND");
    assert.equal(err.sqlState, "42P01");
  });

  it("preserves cause chain", () => {
    const cause = new Error("network");
    const err = new SparkConnectError("wrapped", {
      code: GrpcStatusCode.UNKNOWN,
      cause,
    });
    assert.equal(err.cause, cause);
  });
});

describe("SparkClientError hierarchy", () => {
  it("InvalidConfigError extends SparkClientError and Error", () => {
    const err = new InvalidConfigError("missing transport");
    assert.ok(err instanceof InvalidConfigError);
    assert.ok(err instanceof SparkClientError);
    assert.ok(err instanceof Error);
    assert.equal(err.name, "InvalidConfigError");
    assert.equal(err.message, "missing transport");
  });

  it("InvalidInputError extends SparkClientError and Error", () => {
    const err = new InvalidInputError("bad schema");
    assert.ok(err instanceof InvalidInputError);
    assert.ok(err instanceof SparkClientError);
    assert.ok(err instanceof Error);
    assert.equal(err.name, "InvalidInputError");
    assert.equal(err.message, "bad schema");
  });

  it("UnsupportedOperationError extends SparkClientError and Error", () => {
    const err = new UnsupportedOperationError("no analyzePlan");
    assert.ok(err instanceof UnsupportedOperationError);
    assert.ok(err instanceof SparkClientError);
    assert.ok(err instanceof Error);
    assert.equal(err.name, "UnsupportedOperationError");
    assert.equal(err.message, "no analyzePlan");
  });

  it("SparkClientError preserves cause", () => {
    const cause = new Error("root");
    const err = new InvalidConfigError("wrapped", { cause });
    assert.equal(err.cause, cause);
  });

  it("SparkClientError is distinct from SparkConnectError", () => {
    const client = new InvalidConfigError("client issue");
    const server = new SparkConnectError("server issue", { code: 2 });
    assert.ok(!(client instanceof SparkConnectError));
    assert.ok(!(server instanceof SparkClientError));
  });
});

describe("isSessionInvalidated", () => {
  for (const errorClass of [
    "INVALID_HANDLE.SESSION_NOT_FOUND",
    "INVALID_HANDLE.SESSION_CHANGED",
    "INVALID_HANDLE.SESSION_CLOSED",
    "INVALID_HANDLE.OPERATION_NOT_FOUND",
  ]) {
    it(`returns true for ${errorClass}`, () => {
      const err = new SparkConnectError("gone", { code: GrpcStatusCode.NOT_FOUND, errorClass });
      assert.equal(isSessionInvalidated(err), true);
    });
  }

  it("returns false for unrelated Spark errors", () => {
    const err = new SparkConnectError("missing table", {
      code: GrpcStatusCode.NOT_FOUND,
      errorClass: "TABLE_OR_VIEW_NOT_FOUND",
    });
    assert.equal(isSessionInvalidated(err), false);
  });

  it("returns false when errorClass is undefined", () => {
    const err = new SparkConnectError("no class", { code: GrpcStatusCode.INTERNAL });
    assert.equal(isSessionInvalidated(err), false);
  });

  it("returns false for non-SparkConnectError values", () => {
    assert.equal(isSessionInvalidated(new Error("plain")), false);
    assert.equal(isSessionInvalidated(null), false);
    assert.equal(isSessionInvalidated(undefined), false);
    assert.equal(isSessionInvalidated("string"), false);
    assert.equal(isSessionInvalidated({ errorClass: "INVALID_HANDLE.SESSION_NOT_FOUND" }), false);
  });
});
