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

import type { ExecutePlanResponse } from "@spark-connect-js/connect";
import { SparkConnectError, GrpcStatusCode } from "@spark-connect-js/core";
import { computeBackoff, DEFAULT_RETRY_POLICY, isRetryable, type RetryPolicy } from "./retry.js";

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
   * Returns a Promise so the wrapper can issue follow-up RPCs (e.g.
   * `FetchErrorDetails` to enrich the error with the server's chain).
   */
  wrapError?: (err: unknown) => Promise<unknown>;
  /**
   * Called once per response before any data is yielded. Lets the caller
   * capture per-response state (e.g. `serverSideSessionId` for stale-session
   * detection) without inspecting the raw stream itself.
   */
  onResponse?: (response: ExecutePlanResponse) => void;
}

/**
 * Iterate `ExecutePlanResponse` values across the initial stream and any
 * required reattaches. Yields the Arrow IPC bytes for each `arrowBatch`
 * response. Terminates on `resultComplete` or natural stream end.
 */
export async function* iterateWithReattach(
  opts: ReattachIterationOptions,
): AsyncIterable<Uint8Array> {
  const { initial, reattach, retryPolicy, sleep, wrapError = passThrough, onResponse } = opts;
  let lastResponseId: string | undefined;
  let attempt = 0;
  let consecutiveNoProgress = 0;
  let stream = initial();
  const noProgressCap =
    retryPolicy.maxConsecutiveNoProgressReattaches ??
    DEFAULT_RETRY_POLICY.maxConsecutiveNoProgressReattaches ??
    3;

  while (true) {
    let yieldedThisStream = false;
    try {
      for await (const response of stream) {
        onResponse?.(response);
        if (response.responseId.length > 0) {
          lastResponseId = response.responseId;
        }
        if (
          response.responseType.case === "arrowBatch" &&
          response.responseType.value.data.length > 0
        ) {
          yieldedThisStream = true;
          yield response.responseType.value.data;
        }
        if (response.responseType.case === "resultComplete") {
          return;
        }
      }
      return;
    } catch (err) {
      if (!isRetryable(err) || attempt >= retryPolicy.maxRetries) {
        throw await wrapError(err);
      }
      if (yieldedThisStream) {
        consecutiveNoProgress = 0;
        attempt = 0;
      } else {
        consecutiveNoProgress++;
      }
      if (noProgressCap > 0 && consecutiveNoProgress >= noProgressCap) {
        throw await wrapError(
          new SparkConnectError(
            `Spark Connect made no progress across ${String(consecutiveNoProgress)} ` +
              `consecutive stream attempts (initial and reattaches). No Arrow batches ` +
              `or resultComplete were delivered. Check network conditions between the ` +
              `client and the Connect endpoint.`,
            {
              code: GrpcStatusCode.DEADLINE_EXCEEDED,
              errorClass: "REATTACH_NO_PROGRESS",
              cause: err,
            },
          ),
        );
      }
      const backoff = computeBackoff(attempt, retryPolicy);
      attempt++;
      await sleep(backoff);
      stream = reattach(lastResponseId);
    }
  }
}

/**
 * Default `wrapError`: leaves the error unchanged. Transports pass their own
 * wrapper to convert raw gRPC errors into the project's typed hierarchy.
 */
function passThrough(err: unknown): Promise<unknown> {
  return Promise.resolve(err);
}
