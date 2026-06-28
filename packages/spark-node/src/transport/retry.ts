/**
 * Retry policy for unary Spark Connect RPCs.
 *
 * Defaults match `pyspark.sql.connect.client.retries.DefaultPolicy`:
 *   max_retries = 15
 *   initial_backoff = 50 ms
 *   max_backoff = 60_000 ms
 *   backoff_multiplier = 4.0
 *   jitter = 500 ms
 *
 * Retryable conditions, also mirrored from PySpark's `can_retry()`:
 *   - gRPC code UNAVAILABLE (14)
 *   - gRPC code INTERNAL (13) when the message contains
 *     "INVALID_CURSOR.DISCONNECTED" (server preemption signal)
 *
 * Server-provided retry delay via `RetryInfo` (a trailer extension) is not
 * yet honoured here; that lands together with error-trailer decoding.
 * Until then we use the policy backoff for everything.
 *
 * @see python/pyspark/sql/connect/client/retries.py
 */

import { GrpcStatusCode } from "@spark-connect-js/core";
import * as grpc from "@grpc/grpc-js";

export interface RetryPolicy {
  /** Maximum number of attempts after the initial try. Default 15. */
  maxRetries: number;
  /** Backoff before the first retry. Default 50 ms. */
  initialBackoffMs: number;
  /** Backoff cap. Default 60 s. */
  maxBackoffMs: number;
  /** Multiplier applied between retries. Default 4. */
  backoffMultiplier: number;
  /** Random ms added to each backoff to spread thundering herds. Default 500. */
  jitterMs: number;
}

export const DEFAULT_RETRY_POLICY: RetryPolicy = {
  maxRetries: 15,
  initialBackoffMs: 50,
  maxBackoffMs: 60_000,
  backoffMultiplier: 4,
  jitterMs: 500,
};

const RETRYABLE_CURSOR = "INVALID_CURSOR.DISCONNECTED";

/**
 * Decide whether `err` is a transient condition we should retry. The error
 * argument can be a raw gRPC `ServiceError` or a wrapped {@link SparkConnectError}.
 */
export function isRetryable(err: unknown): boolean {
  const e = err as { code?: number; details?: string; message?: string };
  const grpcCode = e.code;
  const text = e.details ?? e.message ?? "";

  if (grpcCode === grpc.status.UNAVAILABLE || grpcCode === GrpcStatusCode.UNAVAILABLE) {
    return true;
  }
  if (
    (grpcCode === grpc.status.INTERNAL || grpcCode === GrpcStatusCode.INTERNAL) &&
    text.includes(RETRYABLE_CURSOR)
  ) {
    return true;
  }
  return false;
}

/**
 * Run `fn` with retry on transient failures. Throws the *last* error thrown
 * by `fn` once `maxRetries` is exhausted; if a non-retryable error appears
 * earlier, throws that immediately.
 */
export async function withRetry<T>(
  fn: () => Promise<T>,
  policy: RetryPolicy = DEFAULT_RETRY_POLICY,
  sleep: (ms: number) => Promise<void> = defaultSleep,
): Promise<T> {
  let attempt = 0;
  while (true) {
    try {
      return await fn();
    } catch (err) {
      if (!isRetryable(err) || attempt >= policy.maxRetries) {
        throw err;
      }
      const backoff = computeBackoff(attempt, policy);
      attempt++;
      await sleep(backoff);
    }
  }
}

/** Exponential backoff capped at `maxBackoffMs`, plus jitter. */
export function computeBackoff(attempt: number, policy: RetryPolicy): number {
  const exp = policy.initialBackoffMs * Math.pow(policy.backoffMultiplier, attempt);
  const capped = Math.min(exp, policy.maxBackoffMs);
  const jitter = policy.jitterMs > 0 ? Math.random() * policy.jitterMs : 0;
  return capped + jitter;
}

function defaultSleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
