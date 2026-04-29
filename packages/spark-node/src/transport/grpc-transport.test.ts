import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { InvalidConfigError } from "@spark-connect-js/core";
import { GrpcTransport } from "./grpc-transport.js";
import type { ExecutePlanRequest } from "@spark-connect-js/connect";

describe("GrpcTransport: credentials selection", () => {
  it("rejects token without TLS", () => {
    assert.throws(
      () =>
        new GrpcTransport({
          host: "example.com",
          port: 443,
          token: "abc",
          // useSsl is omitted (falsy) so the constructor must reject the
          // combination instead of silently downgrading to insecure.
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

describe("GrpcTransport: ExecutePlanRequest shape", () => {
  type RequestBuilder = (
    sessionId: string,
    operationId: string,
    options: undefined,
    plan: { case: "root"; value: object } | { case: "command"; value: object },
  ) => ExecutePlanRequest;

  it("sets reattach_options.reattachable=true on the initial request", () => {
    const transport = new GrpcTransport({ host: "localhost", port: 15002 });
    const build = (transport as unknown as { _buildExecutePlanRequest: RequestBuilder })
      ._buildExecutePlanRequest;
    const req = build.call(transport, "session-id", "op-id", undefined, {
      case: "root",
      value: {},
    });

    assert.equal(req.requestOptions.length, 1);
    const opt = req.requestOptions[0].requestOption;
    assert.equal(opt.case, "reattachOptions");
    if (opt.case === "reattachOptions") {
      assert.equal(opt.value.reattachable, true);
    }
  });

  it("carries the operation_id and session_id onto the request", () => {
    const transport = new GrpcTransport({ host: "localhost", port: 15002 });
    const build = (transport as unknown as { _buildExecutePlanRequest: RequestBuilder })
      ._buildExecutePlanRequest;
    const req = build.call(transport, "session-id", "op-id", undefined, {
      case: "root",
      value: {},
    });
    assert.equal(req.operationId, "op-id");
    assert.equal(req.sessionId, "session-id");
  });

  it("enables reattach for command-plan requests too", () => {
    const transport = new GrpcTransport({ host: "localhost", port: 15002 });
    const build = (transport as unknown as { _buildExecutePlanRequest: RequestBuilder })
      ._buildExecutePlanRequest;
    const req = build.call(transport, "session-id", "op-id", undefined, {
      case: "command",
      value: {},
    });
    const opt = req.requestOptions[0].requestOption;
    assert.equal(opt.case, "reattachOptions");
    if (opt.case === "reattachOptions") {
      assert.equal(opt.value.reattachable, true);
    }
  });
});
