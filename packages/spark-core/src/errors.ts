/**
 * @packageDocumentation
 *
 * Error types for `@spark-connect-js/core`.
 *
 * Two error families:
 *
 * - {@link SparkConnectError}: server-side or transport failures. Wraps gRPC
 *   status codes and Spark error classes from the JVM server.
 * - {@link SparkClientError}: client-side failures raised before any RPC is
 *   made. Concrete subclasses: {@link InvalidConfigError},
 *   {@link InvalidInputError}, {@link UnsupportedOperationError}.
 *
 * @see [Spark error class catalogue](https://github.com/apache/spark/blob/master/common/utils/src/main/resources/error/error-conditions.json)
 * @see [gRPC status codes](https://grpc.github.io/grpc/core/md_doc_statuscodes.html)
 */

// ---------------------------------------------------------------------------
// gRPC status codes
// ---------------------------------------------------------------------------

/**
 * Well-known gRPC status codes commonly seen from Spark Connect.
 *
 * Use this object for comparisons against {@link SparkConnectError.code}
 * instead of magic numbers.
 *
 * @example
 * ```ts
 * try {
 *   await df.collect();
 * } catch (err) {
 *   if (err instanceof SparkConnectError && err.code === GrpcStatusCode.UNAVAILABLE) {
 *     // retry or surface a degraded-mode error
 *   }
 * }
 * ```
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

// ---------------------------------------------------------------------------
// Server-side / transport error
// ---------------------------------------------------------------------------

/**
 * An error returned by the Spark Connect server or raised at the gRPC
 * transport layer.
 *
 * Carries the gRPC `code`, and when available the Spark `errorClass`
 * (for example `"TABLE_OR_VIEW_NOT_FOUND"`) and SQL state code.
 *
 * `.message` already includes the `[errorClass]` prefix when the class is
 * known. Print it verbatim instead of re-prefixing.
 *
 * @example
 * ```ts
 * try {
 *   await spark.sql("SELECT * FROM missing").collect();
 * } catch (err) {
 *   if (err instanceof SparkConnectError && err.errorClass === "TABLE_OR_VIEW_NOT_FOUND") {
 *     // handle known error class
 *   }
 * }
 * ```
 */
export class SparkConnectError extends Error {
  /** gRPC status code (0 = OK, 14 = UNAVAILABLE, etc.) */
  readonly code: number;

  /** Spark error class if available (e.g. "TABLE_OR_VIEW_NOT_FOUND") */
  readonly errorClass?: string;

  /** SQL state code if available (e.g. "42P01") */
  readonly sqlState?: string;

  /** Server-supplied error message parameters keyed by name. */
  readonly messageParameters?: Record<string, string>;

  /**
   * Fully qualified class names of the server-side exception and its parent
   * classes, root-most first. Populated when the server returns rich error
   * details via `FetchErrorDetails`.
   */
  readonly errorTypeHierarchy?: readonly string[];

  /**
   * Server-side stack trace as JVM frame strings, populated only when the
   * server has `spark.sql.connect.serverStacktrace.enabled=true`. Production
   * deployments usually leave this disabled, so the field is empty in most
   * cases.
   */
  readonly serverStackTrace?: readonly string[];

  constructor(
    message: string,
    options: {
      code: number;
      cause?: unknown;
      errorClass?: string;
      sqlState?: string;
      messageParameters?: Record<string, string>;
      errorTypeHierarchy?: readonly string[];
      serverStackTrace?: readonly string[];
    },
  ) {
    super(message, { cause: options.cause });
    this.name = "SparkConnectError";
    this.code = options.code;
    this.errorClass = options.errorClass;
    this.sqlState = options.sqlState;
    this.messageParameters = options.messageParameters;
    this.errorTypeHierarchy = options.errorTypeHierarchy;
    this.serverStackTrace = options.serverStackTrace;
  }
}

// ---------------------------------------------------------------------------
// Client-side errors
// ---------------------------------------------------------------------------

/**
 * Base class for errors raised by the client before any RPC is made.
 * Callers can `catch (e) { if (e instanceof SparkClientError) ... }` to
 * distinguish client-side issues from server-side failures.
 */
export class SparkClientError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = "SparkClientError";
  }
}

/**
 * Raised when the session or builder is misconfigured (missing transport,
 * missing remote URL, missing Arrow decoder, etc.).
 */
export class InvalidConfigError extends SparkClientError {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = "InvalidConfigError";
  }
}

/**
 * Raised when a public API method receives an invalid argument (bad schema
 * DDL, non-positive bucket count, empty column list, etc.).
 */
export class InvalidInputError extends SparkClientError {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = "InvalidInputError";
  }
}

/**
 * Raised when the caller invokes a capability that the current transport or
 * configuration does not support, e.g. executeCommand on a read-only
 * transport, or an unrecognized plan/expression type.
 */
export class UnsupportedOperationError extends SparkClientError {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = "UnsupportedOperationError";
  }
}

// ---------------------------------------------------------------------------
// Predicates
// ---------------------------------------------------------------------------

/**
 * True when `err` indicates the server-side session backing this client is no
 * longer valid. Happens after a server restart, an explicit session close from
 * the server, or a mid-session handoff to a different server. Recover by
 * building a fresh `SparkSession`.
 *
 * @example
 * ```ts
 * try {
 *   await df.collect();
 * } catch (err) {
 *   if (isSessionInvalidated(err)) {
 *     spark = SparkSessionBuilder.remote(REMOTE).getOrCreate();
 *     return retry();
 *   }
 *   throw err;
 * }
 * ```
 */
export function isSessionInvalidated(err: unknown): boolean {
  if (!(err instanceof SparkConnectError)) return false;
  return err.errorClass?.startsWith("INVALID_HANDLE.") ?? false;
}
