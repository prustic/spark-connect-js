import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { InvalidConfigError } from "@spark-connect-js/core";
import { GrpcTransport } from "./grpc-transport.js";

describe("GrpcTransport — credentials selection", () => {
  it("rejects token without TLS", () => {
    assert.throws(
      () =>
        new GrpcTransport({
          host: "example.com",
          port: 443,
          token: "abc",
          // useSsl deliberately omitted (falsy) to assert rejection
        }),
      InvalidConfigError,
    );
  });

  it("rejects token with explicit useSsl=false", () => {
    assert.throws(
      () =>
        new GrpcTransport({
          host: "example.com",
          port: 443,
          token: "abc",
          useSsl: false,
        }),
      InvalidConfigError,
    );
  });

  it("constructs cleanly with insecure transport (no auth)", () => {
    new GrpcTransport({ host: "localhost", port: 15002 });
  });

  it("constructs cleanly with TLS, no token", () => {
    new GrpcTransport({ host: "example.com", port: 443, useSsl: true });
  });

  it("constructs cleanly with TLS + bearer token", () => {
    new GrpcTransport({
      host: "example.com",
      port: 443,
      useSsl: true,
      token: "bearer-abc",
    });
  });

  it("accepts a free-form metadata bag", () => {
    new GrpcTransport({
      host: "example.com",
      port: 443,
      useSsl: true,
      metadata: { "x-databricks-cluster-id": "0123-456789-abcdefgh" },
    });
  });
});
