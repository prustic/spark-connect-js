/**
 * ReattachExecute iterator for server-streaming Spark Connect RPCs.
 *
 * Once an `ExecutePlan` stream has produced data, retrying from scratch is
 * unsafe because we'd duplicate already-yielded chunks (and re-execute the
 * plan on the server). Spark Connect provides `ReattachExecute` for exactly
 * this case: the client tracks the last received `responseId`, and on a
 * mid-stream gRPC drop opens `ReattachExecute(operation_id, last_response_id)`
 * which resumes the response stream from after that point.
 *
 * This module owns the resume loop. It is parametric over the two stream
 * openers (initial and reattach) so it can be unit-tested without a real
 * gRPC client.
 *
 * Equivalent to PySpark's `ExecutePlanResponseReattachableIterator` and the
 * Scala client's `ExecutePlanResponseReattachableIterator`.
 *
 * @see python/pyspark/sql/connect/client/reattach.py
 * @see connector/connect/common/src/main/scala/org/apache/spark/sql/connect/client/ExecutePlanResponseReattachableIterator.scala
 */

import { SparkConnectError } from "@spark-connect-js/core";
import type { ExecutePlanResponse } from "@spark-connect-js/connect";
import { computeBackoff, isRetryable, type RetryPolicy } from "./retry.js";

export interface ReattachIterationOptions {
  /** Open the initial server-streaming `ExecutePlan` call. */
  initial: () => AsyncIterable<ExecutePlanResponse>;
  /** Open a `ReattachExecute` call resuming after `lastResponseId`. */
  reattach: (lastResponseId: string | undefined) => AsyncIterable<ExecutePlanResponse>;
  /** Retry policy controlling reattach attempt budget and backoff. */
  retryPolicy: RetryPolicy;
  /** Sleep function; injectable for tests. */
  sleep: (ms: number) => Promise<void>;
  /**
   * Wrap raw gRPC errors into the project's typed error hierarchy. The wrap
   * happens only for terminal failures (non-retryable, or budget exhausted).
   */
  wrapError?: (err: unknown) => unknown;
}

/**
 * Iterate `ExecutePlanResponse` values across the initial stream and any
 * required reattaches. Yields the Arrow IPC bytes for each `arrowBatch`
 * response. Terminates on `resultComplete` or natural stream end.
 */
export async function* iterateWithReattach(
  opts: ReattachIterationOptions,
): AsyncIterable<Uint8Array> {
  const { initial, reattach, retryPolicy, sleep, wrapError = identity } = opts;
  let lastResponseId: string | undefined;
  let attempt = 0;
  let stream = initial();

  while (true) {
    try {
      for await (const response of stream) {
        if (response.responseId.length > 0) {
          lastResponseId = response.responseId;
        }
        if (
          response.responseType.case === "arrowBatch" &&
          response.responseType.value.data.length > 0
        ) {
          yield response.responseType.value.data;
        }
        if (response.responseType.case === "resultComplete") {
          return;
        }
      }
      return;
    } catch (err) {
      if (!isRetryable(err) || attempt >= retryPolicy.maxRetries) {
        throw wrapError(err);
      }
      const backoff = computeBackoff(attempt, retryPolicy);
      attempt++;
      await sleep(backoff);
      stream = reattach(lastResponseId);
    }
  }
}

function identity(err: unknown): unknown {
  // Default wrapError preserves SparkConnectError as-is and lets raw errors
  // through unchanged; transports override with their own wrap function.
  if (err instanceof SparkConnectError) return err;
  return err;
}
