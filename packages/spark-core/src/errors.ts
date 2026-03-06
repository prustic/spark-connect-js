/**
 * ─── SparkConnectError ──────────────────────────────────────────────────────
 *
 * Structured error type for Spark Connect failures. Wraps the underlying
 * transport error (gRPC status, network issue, etc.) with Spark-specific
 * context so callers can programmatically handle different failure modes.
 *
 * @see Spark error classes: common/utils/src/main/resources/error/error-conditions.json
 * @see gRPC status codes: https://grpc.github.io/grpc/core/md_doc_statuscodes.html
 * @see FetchErrorDetails RPC: connector/connect/common/src/main/protobuf/spark/connect/base.proto
 */

/**
 * Maps gRPC status codes to human-readable names.
 * Only includes codes commonly seen from Spark Connect.
 */
export const GrpcStatusCode = {
  OK: 0,
  CANCELLED: 1,
  UNKNOWN: 2,
  INVALID_ARGUMENT: 3,
  DEADLINE_EXCEEDED: 4,
  NOT_FOUND: 5,
  ALREADY_EXISTS: 6,
  PERMISSION_DENIED: 7,
  RESOURCE_EXHAUSTED: 8,
  FAILED_PRECONDITION: 9,
  ABORTED: 10,
  UNAVAILABLE: 14,
  INTERNAL: 13,
  UNAUTHENTICATED: 16,
} as const;

export type GrpcStatusCode = (typeof GrpcStatusCode)[keyof typeof GrpcStatusCode];

export class SparkConnectError extends Error {
  /** gRPC status code (0 = OK, 14 = UNAVAILABLE, etc.) */
  readonly code: number;

  /** Spark error class if available (e.g. "TABLE_OR_VIEW_NOT_FOUND") */
  readonly errorClass?: string;

  /** SQL state code if available (e.g. "42P01") */
  readonly sqlState?: string;

  constructor(
    message: string,
    options: {
      code: number;
      cause?: unknown;
      errorClass?: string;
      sqlState?: string;
    },
  ) {
    super(message, { cause: options.cause });
    this.name = "SparkConnectError";
    this.code = options.code;
    this.errorClass = options.errorClass;
    this.sqlState = options.sqlState;
  }
}
