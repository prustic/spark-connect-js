/**
 * Spark Connect connection string parser.
 *
 * Format (from `sql/connect/docs/client-connection-string.md` in apache/spark):
 *
 *   sc://host[:port][/[;param=value[;param=value]*]]
 *
 *   - default port is 15002
 *   - parameter names are case-sensitive
 *   - parameter values are percent-decoded
 *   - the path component before the leading `;` must be empty (just `/`)
 *
 * Reserved parameters (everything else becomes a gRPC metadata header):
 *
 *   token                  - bearer token; presence implicitly enables SSL
 *   use_ssl                - "true" / "false"
 *   user_id                - server-side user identity
 *   user_agent             - client identifier (we append a canonical suffix)
 *   session_id             - UUID to reuse an existing server session
 *   grpc_max_message_size  - bytes; defaults to 128 MiB elsewhere
 *
 * @see https://github.com/apache/spark/blob/master/sql/connect/docs/client-connection-string.md
 * @see python/pyspark/sql/connect/client/core.py (DefaultChannelBuilder)
 * @see connector/connect/common/src/main/scala/org/apache/spark/sql/connect/client (Scala client)
 */

import { InvalidConfigError } from "@spark-connect-js/core";

const DEFAULT_PORT = 15002;
const SCHEME = "sc://";

const RESERVED = new Set([
  "token",
  "use_ssl",
  "user_id",
  "user_agent",
  "session_id",
  "grpc_max_message_size",
]);

export interface ParsedConnectionString {
  host: string;
  port: number;
  useSsl: boolean;
  token?: string;
  userId?: string;
  userAgent?: string;
  sessionId?: string;
  grpcMaxMessageSize?: number;
  /** Non-reserved params, passed through as gRPC metadata on every RPC. */
  headers: Record<string, string>;
}

/**
 * Parse a Spark Connect connection string.
 *
 * For backward compatibility, a bare `host:port` (no `sc://`) is accepted and
 * produces a parse with no params. Production code should always use the
 * `sc://` form so that auth params and metadata can attach.
 */
export function parseConnectionString(remote: string): ParsedConnectionString {
  if (typeof remote !== "string" || remote.length === 0) {
    throw new InvalidConfigError("Spark Connect URL must be a non-empty string.");
  }

  if (!remote.startsWith(SCHEME)) {
    return parseHostPort(remote);
  }

  const body = remote.slice(SCHEME.length);
  const slashIdx = body.indexOf("/");
  const hostPort = slashIdx === -1 ? body : body.slice(0, slashIdx);
  const tail = slashIdx === -1 ? "" : body.slice(slashIdx + 1);

  const parsed = parseHostPort(hostPort);
  const state: ParseState = { useSslExplicit: false };
  applyParams(parsed, tail, state);
  validate(parsed, state, remote);
  return parsed;
}

/**
 * Mutable parsing state that doesn't belong on the result. Tracks whether
 * `use_ssl` was set explicitly so we can distinguish from `token=`'s
 * implicit `useSsl=true` and reject contradictory combinations.
 */
interface ParseState {
  useSslExplicit: boolean;
}

function parseHostPort(input: string): ParsedConnectionString {
  if (input.length === 0) {
    throw new InvalidConfigError("Spark Connect URL is missing host.");
  }

  // IPv6 literal: [::1]:15002
  if (input.startsWith("[")) {
    const close = input.indexOf("]");
    if (close === -1) {
      throw new InvalidConfigError(`Invalid IPv6 host in Spark Connect URL: "${input}"`);
    }
    const host = input.slice(1, close);
    const rest = input.slice(close + 1);
    let port = DEFAULT_PORT;
    if (rest.startsWith(":")) {
      port = parsePort(rest.slice(1), input);
    } else if (rest.length > 0) {
      throw new InvalidConfigError(`Invalid characters after IPv6 host: "${input}"`);
    }
    return { host, port, useSsl: false, headers: {} };
  }

  const colonIdx = input.lastIndexOf(":");
  if (colonIdx === -1) {
    return { host: input, port: DEFAULT_PORT, useSsl: false, headers: {} };
  }
  const host = input.slice(0, colonIdx);
  if (host.length === 0) {
    throw new InvalidConfigError(`Spark Connect URL is missing host: "${input}"`);
  }
  const port = parsePort(input.slice(colonIdx + 1), input);
  return { host, port, useSsl: false, headers: {} };
}

function parsePort(s: string, full: string): number {
  if (!/^\d+$/.test(s)) {
    throw new InvalidConfigError(`Invalid port in Spark Connect URL: "${full}"`);
  }
  const port = Number.parseInt(s, 10);
  if (port < 1 || port > 65535) {
    throw new InvalidConfigError(`Port out of range in Spark Connect URL: "${full}"`);
  }
  return port;
}

function applyParams(out: ParsedConnectionString, tail: string, state: ParseState): void {
  if (tail.length === 0) return;
  if (!tail.startsWith(";")) {
    // Spec: path component must be empty. We tolerate `/` alone, but anything
    // before the first `;` is invalid.
    throw new InvalidConfigError(
      `Spark Connect URL has a non-empty path component (expected leading ';' for params): "${tail}"`,
    );
  }

  for (const piece of tail.slice(1).split(";")) {
    if (piece.length === 0) continue;
    const eq = piece.indexOf("=");
    if (eq === -1) {
      throw new InvalidConfigError(`Spark Connect URL parameter is missing "=": "${piece}"`);
    }
    const key = piece.slice(0, eq);
    const value = decode(piece.slice(eq + 1));
    if (key.length === 0) {
      throw new InvalidConfigError(`Spark Connect URL parameter has empty name: "${piece}"`);
    }
    applyParam(out, state, key, value);
  }
}

function applyParam(
  out: ParsedConnectionString,
  state: ParseState,
  key: string,
  value: string,
): void {
  switch (key) {
    case "token":
      out.token = value;
      // Token implies SSL per spec, but never override an explicit `use_ssl`.
      // The contradictory combination (token + use_ssl=false) is caught in `validate`.
      if (!state.useSslExplicit) {
        out.useSsl = true;
      }
      break;
    case "use_ssl":
      out.useSsl = parseBool(value, key);
      state.useSslExplicit = true;
      break;
    case "user_id":
      out.userId = value;
      break;
    case "user_agent":
      out.userAgent = value;
      break;
    case "session_id":
      out.sessionId = value;
      break;
    case "grpc_max_message_size": {
      if (!/^\d+$/.test(value)) {
        throw new InvalidConfigError(
          `grpc_max_message_size must be a positive integer: "${value}"`,
        );
      }
      const size = Number.parseInt(value, 10);
      if (!Number.isSafeInteger(size) || size <= 0) {
        throw new InvalidConfigError(
          `grpc_max_message_size must be a positive safe integer: "${value}"`,
        );
      }
      out.grpcMaxMessageSize = size;
      break;
    }
    default:
      if (RESERVED.has(key)) {
        // Should be unreachable; keeps the `RESERVED` set the source of truth.
        throw new InvalidConfigError(`Unhandled reserved parameter: "${key}"`);
      }
      out.headers[key] = value;
  }
}

function parseBool(value: string, key: string): boolean {
  if (value === "true") return true;
  if (value === "false") return false;
  throw new InvalidConfigError(
    `Spark Connect URL parameter "${key}" must be "true" or "false", got "${value}"`,
  );
}

function decode(s: string): string {
  try {
    return decodeURIComponent(s);
  } catch {
    throw new InvalidConfigError(`Invalid percent-encoding in Spark Connect URL value: "${s}"`);
  }
}

function validate(parsed: ParsedConnectionString, state: ParseState, raw: string): void {
  if (parsed.sessionId !== undefined && !isUuid(parsed.sessionId)) {
    throw new InvalidConfigError(
      `Spark Connect "session_id" must be a valid UUID, got "${parsed.sessionId}" (in "${raw}")`,
    );
  }
  if (parsed.token !== undefined && state.useSslExplicit && !parsed.useSsl) {
    throw new InvalidConfigError(
      `Spark Connect URL has token=... with use_ssl=false; the spec says setting token enables SSL, ` +
        `so this combination is contradictory. Either drop use_ssl=false or remove the token. ` +
        `(in "${raw}")`,
    );
  }
}

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
function isUuid(s: string): boolean {
  return UUID_RE.test(s);
}
