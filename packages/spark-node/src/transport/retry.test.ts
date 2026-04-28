import { describe, it } from "node:test";
import assert from "node:assert/strict";
import * as grpc from "@grpc/grpc-js";
import {
  DEFAULT_RETRY_POLICY,
  computeBackoff,
  isRetryable,
  withRetry,
  type RetryPolicy,
} from "./retry.js";

describe("isRetryable", () => {
  it("returns true for UNAVAILABLE", () => {
    assert.equal(isRetryable({ code: grpc.status.UNAVAILABLE }), true);
  });

  it("returns true for INTERNAL with INVALID_CURSOR.DISCONNECTED in the message", () => {
    assert.equal(
      isRetryable({
        code: grpc.status.INTERNAL,
        message: "Server error: INVALID_CURSOR.DISCONNECTED happened",
      }),
      true,
    );
  });

  it("returns false for plain INTERNAL without the cursor marker", () => {
    assert.equal(isRetryable({ code: grpc.status.INTERNAL, message: "boom" }), false);
  });

  it("returns false for non-retryable codes", () => {
    assert.equal(isRetryable({ code: grpc.status.INVALID_ARGUMENT }), false);
    assert.equal(isRetryable({ code: grpc.status.PERMISSION_DENIED }), false);
    assert.equal(isRetryable({ code: grpc.status.NOT_FOUND }), false);
  });

  it("returns false for plain Errors with no code", () => {
    assert.equal(isRetryable(new Error("nope")), false);
  });
});

describe("computeBackoff", () => {
  const noJitter: RetryPolicy = { ...DEFAULT_RETRY_POLICY, jitterMs: 0 };

  it("scales exponentially before the cap", () => {
    assert.equal(computeBackoff(0, noJitter), 50);
    assert.equal(computeBackoff(1, noJitter), 200);
    assert.equal(computeBackoff(2, noJitter), 800);
    assert.equal(computeBackoff(3, noJitter), 3200);
  });

  it("caps at maxBackoffMs", () => {
    assert.equal(computeBackoff(20, noJitter), noJitter.maxBackoffMs);
  });

  it("adds jitter in [0, jitterMs] when jitterMs > 0", () => {
    const policy: RetryPolicy = { ...DEFAULT_RETRY_POLICY, jitterMs: 100 };
    for (let i = 0; i < 30; i++) {
      const b = computeBackoff(0, policy);
      assert.ok(b >= 50 && b <= 150, `backoff ${b} outside [50, 150]`);
    }
  });
});

describe("withRetry", () => {
  const fastPolicy: RetryPolicy = {
    maxRetries: 3,
    initialBackoffMs: 1,
    maxBackoffMs: 1,
    backoffMultiplier: 1,
    jitterMs: 0,
  };
  const noSleep = (): Promise<void> => Promise.resolve();

  function grpcError(code: number, message = ""): Error & { code: number } {
    const e = new Error(message) as Error & { code: number };
    e.code = code;
    return e;
  }

  it("returns the value from a successful first attempt", async () => {
    let attempts = 0;
    const result = await withRetry(
      async () => {
        attempts++;
        return "ok";
      },
      fastPolicy,
      noSleep,
    );
    assert.equal(result, "ok");
    assert.equal(attempts, 1);
  });

  it("retries on UNAVAILABLE and eventually succeeds", async () => {
    let attempts = 0;
    const result = await withRetry(
      async () => {
        attempts++;
        if (attempts < 3) {
          throw grpcError(grpc.status.UNAVAILABLE, "try again");
        }
        return "ok";
      },
      fastPolicy,
      noSleep,
    );
    assert.equal(result, "ok");
    assert.equal(attempts, 3);
  });

  it("throws immediately on non-retryable errors", async () => {
    let attempts = 0;
    await assert.rejects(
      withRetry(
        async () => {
          attempts++;
          throw grpcError(grpc.status.INVALID_ARGUMENT, "bad");
        },
        fastPolicy,
        noSleep,
      ),
    );
    assert.equal(attempts, 1);
  });

  it("gives up and throws after maxRetries retryable failures", async () => {
    let attempts = 0;
    await assert.rejects(
      withRetry(
        async () => {
          attempts++;
          throw grpcError(grpc.status.UNAVAILABLE, "down");
        },
        fastPolicy,
        noSleep,
      ),
    );
    // initial attempt + maxRetries
    assert.equal(attempts, fastPolicy.maxRetries + 1);
  });
});
