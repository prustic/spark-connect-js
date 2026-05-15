/**
 * GrpcTransport
 *
 * Concrete implementation of @spark-connect-js/core's Transport interface using
 * @grpc/grpc-js to communicate with the Spark Connect gRPC service.
 *
 * @see [Spark Connect proto: base.proto](https://github.com/apache/spark/blob/master/sql/connect/common/src/main/protobuf/spark/connect/base.proto)
 * @see [Spark source: SparkConnectService.scala](https://github.com/apache/spark/blob/master/sql/connect/server/src/main/scala/org/apache/spark/sql/connect/service/SparkConnectService.scala)
 * @see [Spark source: ExecuteGrpcResponseSender.scala](https://github.com/apache/spark/blob/master/sql/connect/server/src/main/scala/org/apache/spark/sql/connect/execution/ExecuteGrpcResponseSender.scala)
 *
 * Uses @bufbuild/protobuf for message serialization and @grpc/grpc-js for
 * the HTTP/2 transport. Messages are created using the generated schemas
 * from @spark-connect-js/connect and serialized to binary protobuf on the wire.
 */

import { InvalidConfigError, UnsupportedOperationError } from "@spark-connect-js/core";
import * as grpc from "@grpc/grpc-js";
import { randomUUID } from "node:crypto";
import { create, toBinary, fromBinary } from "@bufbuild/protobuf";
import {
  ExecutePlanRequestSchema,
  ExecutePlanResponseSchema,
  PlanSchema,
  UserContextSchema,
  type UserContext,
  ReleaseSessionRequestSchema,
  ReleaseSessionResponseSchema,
  AnalyzePlanRequestSchema,
  AnalyzePlanResponseSchema,
  AnalyzePlanRequest_SchemaSchema,
  AnalyzePlanRequest_ExplainSchema,
  AnalyzePlanRequest_PersistSchema,
  AnalyzePlanRequest_UnpersistSchema,
  AnalyzePlanRequest_GetStorageLevelSchema,
  AnalyzePlanRequest_SameSemanticsSchema,
  AnalyzePlanRequest_SemanticHashSchema,
  AnalyzePlanRequest_SparkVersionSchema,
  ConfigRequestSchema,
  ConfigResponseSchema,
  ConfigRequest_OperationSchema,
  ConfigRequest_SetSchema,
  ConfigRequest_GetSchema,
  ConfigRequest_GetAllSchema,
  ConfigRequest_UnsetSchema,
  ConfigRequest_IsModifiableSchema,
  InterruptRequestSchema,
  InterruptResponseSchema,
  InterruptRequest_InterruptType,
  KeyValueSchema,
  StorageLevelSchema,
  CommandSchema,
  WriteOperationSchema,
  WriteOperation_SaveMode,
  WriteOperation_SaveTableSchema,
  WriteOperation_SaveTable_TableSaveMethod,
  WriteOperation_BucketBySchema,
  WriteOperationV2Schema,
  WriteOperationV2_Mode,
  CreateDataFrameViewCommandSchema,
  WriteStreamOperationStartSchema,
  StreamingQueryInstanceIdSchema,
  StreamingQueryCommandSchema,
  StreamingQueryCommand_ExplainCommandSchema,
  StreamingQueryCommand_AwaitTerminationCommandSchema,
  type WriteStreamOperationStart,
  type WriteStreamOperationStartResult,
  type StreamingQueryCommand,
  type StreamingQueryCommandResult,
  CommonInlineUserDefinedFunctionSchema,
  JavaUDFSchema,
  DataTypeSchema,
  DataType_UnparsedSchema,
  ReattachOptionsSchema,
  ExecutePlanRequest_RequestOptionSchema,
  ReattachExecuteRequestSchema,
  StatusSchema,
  ErrorInfoSchema,
  FetchErrorDetailsRequestSchema,
  FetchErrorDetailsResponseSchema,
  type Status,
  type ErrorInfo,
  type FetchErrorDetailsRequest,
  type FetchErrorDetailsResponse,
  type WriteOperation,
  type ExecutePlanRequest,
  type ExecutePlanResponse,
  type ReattachExecuteRequest,
  type ReleaseSessionRequest,
  type ReleaseSessionResponse,
  type AnalyzePlanRequest,
  type AnalyzePlanResponse,
  type ConfigRequest,
  type ConfigRequest_Operation,
  type ConfigResponse,
  type InterruptRequest,
  type InterruptResponse,
} from "@spark-connect-js/connect";
import type { ExecuteOptions, Transport } from "@spark-connect-js/core";
import type { LogicalPlan } from "@spark-connect-js/core";
import { SparkConnectError } from "@spark-connect-js/core";
import { buildRelation, buildExpression } from "../proto/proto-builder.js";
import { DEFAULT_RETRY_POLICY, withRetry, type RetryPolicy } from "./retry.js";
import { iterateWithReattach } from "./reattach.js";

/** Default gRPC max message size: Arrow batches commonly exceed 4 MB. */
const DEFAULT_MAX_MESSAGE_SIZE = 128 * 1024 * 1024;

/**
 * Version string used in `clientType` (server-side User-Agent equivalent).
 * Must be kept in lockstep with `package.json#version`; changesets bumps
 * the manifest at release, this constant gets bumped in the same commit.
 * Hardcoded rather than imported from package.json to avoid
 * `resolveJsonModule` gymnastics across the workspace.
 */
const SPARK_JS_VERSION = "0.3.0";

export interface GrpcTransportOptions {
  host: string;
  port: number;
  /** Connect over TLS. Implied by `token`. */
  useSsl?: boolean;
  /** Bearer token. Requires `useSsl` (token-over-insecure is rejected). */
  token?: string;
  /** Spark user identity for `UserContext.userId`. */
  userId?: string;
  /** User-provided client identifier; a canonical suffix is appended. */
  userAgent?: string;
  /** Free-form gRPC metadata attached to every RPC. */
  metadata?: Record<string, string>;
  /** Explicit ChannelCredentials override; bypasses `useSsl` / `token`. */
  channelCredentials?: grpc.ChannelCredentials;
  /** Override the 128 MiB default for both send and receive. */
  grpcMaxMessageSize?: number;
  /**
   * Retry policy for unary RPCs (analyzePlan, releaseSession, config,
   * interrupt). Server-streaming RPCs use ReattachExecute instead.
   * Defaults to {@link DEFAULT_RETRY_POLICY}, which mirrors PySpark's
   * `DefaultPolicy`.
   */
  retryPolicy?: RetryPolicy;
}

/**
 * gRPC-based transport for Spark Connect.
 *
 * @example
 *   const transport = new GrpcTransport({ host: "localhost", port: 15002 });
 *   const spark = SparkSession.builder()
 *     .remote("sc://localhost:15002")
 *     .transport(transport)
 *     .getOrCreate();
 */
export class GrpcTransport implements Transport {
  private readonly endpoint: string;
  private readonly credentials: grpc.ChannelCredentials;
  private readonly channelOptions: Record<string, number>;
  private readonly metadata: grpc.Metadata;
  private readonly userContext: UserContext;
  private readonly clientType: string;
  private readonly retryPolicy: RetryPolicy;
  private client: grpc.Client | null = null;
  /**
   * Server-side session IDs observed in responses, keyed by client session ID.
   * Echoed back on subsequent requests so the server can detect a stale
   * session (e.g. after a server restart) and reject the call early.
   */
  private readonly observedServerSideSessionIds = new Map<string, string>();

  constructor(options: GrpcTransportOptions) {
    if (!options.host) {
      throw new InvalidConfigError("GrpcTransport requires a non-empty host.");
    }
    if (!Number.isInteger(options.port) || options.port < 1 || options.port > 65535) {
      throw new InvalidConfigError(
        `GrpcTransport requires a valid port (1-65535), got ${String(options.port)}.`,
      );
    }
    this.endpoint = `${options.host}:${options.port}`;
    this.credentials = buildCredentials(options);
    this.channelOptions = buildChannelOptions(options.grpcMaxMessageSize);
    this.metadata = buildMetadata(options.metadata);
    this.userContext = create(UserContextSchema, {
      userId: options.userId ?? "spark-js",
    });
    this.clientType = buildClientType(options.userAgent);
    this.retryPolicy = options.retryPolicy ?? DEFAULT_RETRY_POLICY;
  }

  /** Send a logical plan to Spark Connect and yield Arrow IPC batches. */
  async *executePlan(
    sessionId: string,
    plan: LogicalPlan,
    options?: ExecuteOptions,
  ): AsyncIterable<Uint8Array> {
    const operationId = newOperationId();
    const relation = buildRelation(plan);
    const request = this._buildExecutePlanRequest(sessionId, operationId, options, {
      case: "root",
      value: relation,
    });
    yield* this._streamWithReattach(sessionId, operationId, request);
  }

  /** Close the gRPC channel. */
  close(): void {
    if (this.client) {
      this.client.close();
      this.client = null;
    }
  }

  /** Execute a command (write, createView, etc.) via ExecutePlan RPC. */
  async executeCommand(
    sessionId: string,
    command: Record<string, unknown>,
    options?: ExecuteOptions,
  ): Promise<void> {
    const operationId = newOperationId();
    const commandProto = buildCommandProto(command);
    const request = this._buildExecutePlanRequest(sessionId, operationId, options, {
      case: "command",
      value: commandProto,
    });
    // Drain the stream; commands don't yield Arrow batches but reattach
    // still applies if the connection drops mid-execution.
    for await (const _ of this._streamWithReattach(sessionId, operationId, request)) {
      // discard
    }
  }

  /**
   * Execute a command and collect non-Arrow result responses (streaming
   * commands return `WriteStreamOperationStartResult` /
   * `StreamingQueryCommandResult` instead of Arrow batches).
   */
  async executeCommandResponses(
    sessionId: string,
    command: Record<string, unknown>,
    options?: ExecuteOptions,
  ): Promise<Record<string, unknown>[]> {
    const operationId = newOperationId();
    const commandProto = buildCommandProto(command);
    const request = this._buildExecutePlanRequest(sessionId, operationId, options, {
      case: "command",
      value: commandProto,
    });
    const responses: Record<string, unknown>[] = [];
    const capture = (response: ExecutePlanResponse): void => {
      const decoded = decodeCommandResponse(response);
      if (decoded !== undefined) responses.push(decoded);
    };
    for await (const _ of this._streamWithReattach(sessionId, operationId, request, capture)) {
      // Drain the stream so the onResponse hook fires for every result frame.
      // Streaming commands carry their result in non-Arrow response frames
      // captured above; nothing is yielded from this iterator.
    }
    return responses;
  }

  /** @internal Build an ExecutePlanRequest with reattach enabled. */
  private _buildExecutePlanRequest(
    sessionId: string,
    operationId: string,
    options: ExecuteOptions | undefined,
    plan:
      | { case: "root"; value: ReturnType<typeof buildRelation> }
      | { case: "command"; value: ReturnType<typeof buildCommandProto> },
  ): ExecutePlanRequest {
    return create(ExecutePlanRequestSchema, {
      sessionId,
      userContext: this.userContext,
      operationId,
      plan: create(PlanSchema, { opType: plan }),
      clientType: this.clientType,
      tags: options?.tags ? Array.from(options.tags) : [],
      ...this._observedSessionFor(sessionId),
      // Reattach must be requested on the initial ExecutePlan; if the
      // server-streaming connection drops, we use the operationId and the
      // last received responseId to pick up where we left off.
      requestOptions: [
        create(ExecutePlanRequest_RequestOptionSchema, {
          requestOption: {
            case: "reattachOptions",
            value: create(ReattachOptionsSchema, { reattachable: true }),
          },
        }),
      ],
    });
  }

  /**
   * @internal Spread a `clientObservedServerSideSessionId` field if we've
   * captured one for this session; otherwise return an empty object so the
   * field is omitted from the request.
   */
  private _observedSessionFor(sessionId: string): { clientObservedServerSideSessionId?: string } {
    const observed = this.observedServerSideSessionIds.get(sessionId);
    return observed !== undefined ? { clientObservedServerSideSessionId: observed } : {};
  }

  /**
   * @internal Capture the server-side session id from a response so we can
   * echo it back on subsequent requests as a stale-session detector.
   */
  private _captureServerSession(
    sessionId: string,
    response: { serverSideSessionId?: string } | undefined,
  ): void {
    const id = response?.serverSideSessionId;
    if (id !== undefined && id.length > 0) {
      this.observedServerSideSessionIds.set(sessionId, id);
    }
  }

  /** @internal */
  private _streamWithReattach(
    sessionId: string,
    operationId: string,
    request: ExecutePlanRequest,
    onResponse?: (response: ExecutePlanResponse) => void,
  ): AsyncIterable<Uint8Array> {
    const client = this._getClient();
    return iterateWithReattach({
      initial: () => this._openExecutePlanStream(client, request),
      reattach: (lastResponseId: string | undefined) =>
        this._openReattachStream(client, sessionId, operationId, lastResponseId),
      retryPolicy: this.retryPolicy,
      sleep,
      wrapError: (err) => this._wrapError(sessionId, err),
      onResponse: (response) => {
        this._captureServerSession(sessionId, response);
        onResponse?.(response);
      },
    });
  }

  private _openExecutePlanStream(
    client: grpc.Client,
    request: ExecutePlanRequest,
  ): AsyncIterable<ExecutePlanResponse> {
    const serialize = (value: ExecutePlanRequest): Buffer =>
      Buffer.from(toBinary(ExecutePlanRequestSchema, value));
    const deserialize = (bytes: Buffer): ExecutePlanResponse =>
      fromBinary(ExecutePlanResponseSchema, bytes);
    return client.makeServerStreamRequest<ExecutePlanRequest, ExecutePlanResponse>(
      "/spark.connect.SparkConnectService/ExecutePlan",
      serialize,
      deserialize,
      request,
      this.metadata,
    );
  }

  private _openReattachStream(
    client: grpc.Client,
    sessionId: string,
    operationId: string,
    lastResponseId: string | undefined,
  ): AsyncIterable<ExecutePlanResponse> {
    const request = create(ReattachExecuteRequestSchema, {
      sessionId,
      userContext: this.userContext,
      clientType: this.clientType,
      operationId,
      ...this._observedSessionFor(sessionId),
      ...(lastResponseId !== undefined ? { lastResponseId } : {}),
    });
    const serialize = (value: ReattachExecuteRequest): Buffer =>
      Buffer.from(toBinary(ReattachExecuteRequestSchema, value));
    const deserialize = (bytes: Buffer): ExecutePlanResponse =>
      fromBinary(ExecutePlanResponseSchema, bytes);
    return client.makeServerStreamRequest<ReattachExecuteRequest, ExecutePlanResponse>(
      "/spark.connect.SparkConnectService/ReattachExecute",
      serialize,
      deserialize,
      request,
      this.metadata,
    );
  }

  /** Release the session on the server. */
  releaseSession(sessionId: string): Promise<void> {
    const client = this._getClient();

    const request = create(ReleaseSessionRequestSchema, {
      sessionId,
      userContext: this.userContext,
      clientType: this.clientType,
      ...this._observedSessionFor(sessionId),
    });

    const serialize = (value: ReleaseSessionRequest): Buffer =>
      Buffer.from(toBinary(ReleaseSessionRequestSchema, value));
    const deserialize = (bytes: Buffer): ReleaseSessionResponse =>
      fromBinary(ReleaseSessionResponseSchema, bytes);

    return withRetry(
      () =>
        new Promise<void>((resolve, reject) => {
          client.makeUnaryRequest<ReleaseSessionRequest, ReleaseSessionResponse>(
            "/spark.connect.SparkConnectService/ReleaseSession",
            serialize,
            deserialize,
            request,
            this.metadata,
            (err: grpc.ServiceError | null) => {
              if (err) {
                this._wrapError(sessionId, err).then(reject, reject);
              } else {
                this.observedServerSideSessionIds.delete(sessionId);
                resolve();
              }
            },
          );
        }),
      this.retryPolicy,
    );
  }

  /** Send an AnalyzePlan request (unary RPC). */
  analyzePlan(
    sessionId: string,
    request: Record<string, unknown>,
  ): Promise<Record<string, unknown>> {
    const client = this._getClient();

    const analyzeRequest = buildAnalyzePlanRequest(
      sessionId,
      request,
      this.userContext,
      this.clientType,
      this.observedServerSideSessionIds.get(sessionId),
    );

    const serialize = (value: AnalyzePlanRequest): Buffer =>
      Buffer.from(toBinary(AnalyzePlanRequestSchema, value));
    const deserialize = (bytes: Buffer): AnalyzePlanResponse =>
      fromBinary(AnalyzePlanResponseSchema, bytes);

    return withRetry(
      () =>
        new Promise<Record<string, unknown>>((resolve, reject) => {
          client.makeUnaryRequest<AnalyzePlanRequest, AnalyzePlanResponse>(
            "/spark.connect.SparkConnectService/AnalyzePlan",
            serialize,
            deserialize,
            analyzeRequest,
            this.metadata,
            (err: grpc.ServiceError | null, response?: AnalyzePlanResponse) => {
              if (err) {
                this._wrapError(sessionId, err).then(reject, reject);
              } else {
                this._captureServerSession(sessionId, response);
                resolve(extractAnalyzeResult(response!));
              }
            },
          );
        }),
      this.retryPolicy,
    );
  }

  /** Interrupt running operations. Returns server-reported interrupted IDs. */
  interrupt(sessionId: string, request: Record<string, unknown>): Promise<string[]> {
    const client = this._getClient();

    const interruptRequest = create(InterruptRequestSchema, {
      sessionId,
      userContext: this.userContext,
      clientType: this.clientType,
      ...this._observedSessionFor(sessionId),
      ...buildInterruptOneOf(request),
    });

    const serialize = (value: InterruptRequest): Buffer =>
      Buffer.from(toBinary(InterruptRequestSchema, value));
    const deserialize = (bytes: Buffer): InterruptResponse =>
      fromBinary(InterruptResponseSchema, bytes);

    return withRetry(
      () =>
        new Promise<string[]>((resolve, reject) => {
          client.makeUnaryRequest<InterruptRequest, InterruptResponse>(
            "/spark.connect.SparkConnectService/Interrupt",
            serialize,
            deserialize,
            interruptRequest,
            this.metadata,
            (err: grpc.ServiceError | null, response?: InterruptResponse) => {
              if (err) {
                this._wrapError(sessionId, err).then(reject, reject);
              } else {
                this._captureServerSession(sessionId, response);
                resolve(response!.interruptedIds);
              }
            },
          );
        }),
      this.retryPolicy,
    );
  }

  /** Read or write Spark runtime configuration via the Config RPC. */
  config(sessionId: string, operation: Record<string, unknown>): Promise<Record<string, unknown>> {
    const client = this._getClient();

    const configRequest = create(ConfigRequestSchema, {
      sessionId,
      userContext: this.userContext,
      clientType: this.clientType,
      ...this._observedSessionFor(sessionId),
      operation: buildConfigOperation(operation),
    });

    const serialize = (value: ConfigRequest): Buffer =>
      Buffer.from(toBinary(ConfigRequestSchema, value));
    const deserialize = (bytes: Buffer): ConfigResponse => fromBinary(ConfigResponseSchema, bytes);

    return withRetry(
      () =>
        new Promise<Record<string, unknown>>((resolve, reject) => {
          client.makeUnaryRequest<ConfigRequest, ConfigResponse>(
            "/spark.connect.SparkConnectService/Config",
            serialize,
            deserialize,
            configRequest,
            this.metadata,
            (err: grpc.ServiceError | null, response?: ConfigResponse) => {
              if (err) {
                this._wrapError(sessionId, err).then(reject, reject);
              } else {
                this._captureServerSession(sessionId, response);
                resolve(extractConfigResult(response!));
              }
            },
          );
        }),
      this.retryPolicy,
    );
  }

  /**
   * @internal Wrap a raw gRPC error and, when the inline trailer carries an
   * `errorId`, fetch the rich error chain via `FetchErrorDetails`. If the
   * fetch fails, fall back to the inline-only wrap.
   */
  private async _wrapError(sessionId: string, err: unknown): Promise<SparkConnectError> {
    if (err instanceof SparkConnectError) return err;
    const grpcErr = err as RawGrpcError;
    const errorInfo = grpcErr.metadata ? extractErrorInfo(grpcErr.metadata) : undefined;
    const base = buildBasicSparkError(err, errorInfo);
    const errorId = errorInfo?.metadata.errorId;
    if (errorId === undefined || errorId.length === 0) return base;
    try {
      const response = await this._fetchErrorDetails(sessionId, errorId);
      this._captureServerSession(sessionId, response);
      return enrichFromFetchResponse(base, response);
    } catch {
      return base;
    }
  }

  private _fetchErrorDetails(
    sessionId: string,
    errorId: string,
  ): Promise<FetchErrorDetailsResponse> {
    const client = this._getClient();
    const request = create(FetchErrorDetailsRequestSchema, {
      sessionId,
      userContext: this.userContext,
      clientType: this.clientType,
      ...this._observedSessionFor(sessionId),
      errorId,
    });
    const serialize = (value: FetchErrorDetailsRequest): Buffer =>
      Buffer.from(toBinary(FetchErrorDetailsRequestSchema, value));
    const deserialize = (bytes: Buffer): FetchErrorDetailsResponse =>
      fromBinary(FetchErrorDetailsResponseSchema, bytes);
    return new Promise<FetchErrorDetailsResponse>((resolve, reject) => {
      client.makeUnaryRequest<FetchErrorDetailsRequest, FetchErrorDetailsResponse>(
        "/spark.connect.SparkConnectService/FetchErrorDetails",
        serialize,
        deserialize,
        request,
        this.metadata,
        (err: grpc.ServiceError | null, response?: FetchErrorDetailsResponse) => {
          if (err) {
            reject(err);
          } else {
            resolve(response!);
          }
        },
      );
    });
  }

  private _getClient(): grpc.Client {
    if (!this.client) {
      this.client = new grpc.Client(this.endpoint, this.credentials, this.channelOptions);
    }
    return this.client;
  }
}

// Construction helpers

function buildCredentials(opts: GrpcTransportOptions): grpc.ChannelCredentials {
  if (opts.channelCredentials !== undefined) {
    return opts.channelCredentials;
  }
  if (opts.token !== undefined && opts.useSsl !== true) {
    throw new InvalidConfigError(
      "Spark Connect token authentication requires TLS. Either drop useSsl=false " +
        "(in a connection string, drop use_ssl=false) or remove the token. " +
        "Token-over-insecure transports are rejected to avoid leaking credentials.",
    );
  }
  if (opts.token !== undefined) {
    const token = opts.token;
    const callCreds = grpc.credentials.createFromMetadataGenerator((_params, callback) => {
      const md = new grpc.Metadata();
      md.add("authorization", `Bearer ${token}`);
      callback(null, md);
    });
    return grpc.credentials.combineChannelCredentials(grpc.credentials.createSsl(), callCreds);
  }
  if (opts.useSsl === true) {
    return grpc.credentials.createSsl();
  }
  return grpc.credentials.createInsecure();
}

function buildChannelOptions(grpcMaxMessageSize: number | undefined): Record<string, number> {
  const max = grpcMaxMessageSize ?? DEFAULT_MAX_MESSAGE_SIZE;
  return {
    "grpc.max_receive_message_length": max,
    "grpc.max_send_message_length": max,
    // Keepalive to detect dead connections through load balancers
    "grpc.keepalive_time_ms": 30_000,
    "grpc.keepalive_timeout_ms": 10_000,
  };
}

function buildMetadata(headers: Record<string, string> | undefined): grpc.Metadata {
  const md = new grpc.Metadata();
  if (headers !== undefined) {
    for (const [k, v] of Object.entries(headers)) {
      md.add(k, v);
    }
  }
  return md;
}

/** UUIDv4 attached to every ExecutePlanRequest as `operation_id`. */
function newOperationId(): string {
  return randomUUID();
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Synthesize the `clientType` field on Spark Connect requests, equivalent to
 * a User-Agent. The canonical suffix identifies the client library and
 * runtime so server logs can attribute traffic.
 */
function buildClientType(userAgent: string | undefined): string {
  const suffix = `spark-connect-js/${SPARK_JS_VERSION} (node ${process.versions.node}; ${process.platform})`;
  if (userAgent !== undefined && userAgent.length > 0) {
    return `${userAgent} ${suffix}`;
  }
  return suffix;
}

// Config helpers

/**
 * Map a core-side `_config` operation record onto the proto
 * `ConfigRequest.Operation` oneof. The shapes here mirror PySpark's
 * `RuntimeConfig` calls and the variants that exist in `ConfigRequest`.
 */
function buildConfigOperation(op: Record<string, unknown>): ConfigRequest_Operation {
  const kind = op.op as string;
  switch (kind) {
    case "set": {
      const pairs = (op.pairs as [string, string][]).map(([key, value]) =>
        create(KeyValueSchema, { key, value }),
      );
      return create(ConfigRequest_OperationSchema, {
        opType: { case: "set", value: create(ConfigRequest_SetSchema, { pairs }) },
      });
    }
    case "get":
      return create(ConfigRequest_OperationSchema, {
        opType: {
          case: "get",
          value: create(ConfigRequest_GetSchema, { keys: op.keys as string[] }),
        },
      });
    case "getAll":
      return create(ConfigRequest_OperationSchema, {
        opType: {
          case: "getAll",
          value: create(ConfigRequest_GetAllSchema, {
            ...(op.prefix !== undefined ? { prefix: op.prefix as string } : {}),
          }),
        },
      });
    case "unset":
      return create(ConfigRequest_OperationSchema, {
        opType: {
          case: "unset",
          value: create(ConfigRequest_UnsetSchema, { keys: op.keys as string[] }),
        },
      });
    case "isModifiable":
      return create(ConfigRequest_OperationSchema, {
        opType: {
          case: "isModifiable",
          value: create(ConfigRequest_IsModifiableSchema, { keys: op.keys as string[] }),
        },
      });
    default:
      throw new UnsupportedOperationError(`Unsupported config op: ${kind}`);
  }
}

/**
 * Map a core-side `_interrupt` request onto the proto fields. Returns a
 * partial spread for `create(InterruptRequestSchema, { ... })`.
 */
function buildInterruptOneOf(req: Record<string, unknown>): {
  interruptType: InterruptRequest_InterruptType;
  interrupt?: InterruptRequest["interrupt"];
} {
  const type = req.type as string;
  switch (type) {
    case "all":
      return { interruptType: InterruptRequest_InterruptType.ALL };
    case "tag":
      return {
        interruptType: InterruptRequest_InterruptType.TAG,
        interrupt: { case: "operationTag", value: req.tag as string },
      };
    case "operationId":
      return {
        interruptType: InterruptRequest_InterruptType.OPERATION_ID,
        interrupt: { case: "operationId", value: req.operationId as string },
      };
    default:
      throw new UnsupportedOperationError(`Unsupported interrupt type: ${type}`);
  }
}

function extractConfigResult(response: ConfigResponse): Record<string, unknown> {
  return {
    pairs: response.pairs.map((kv) => [kv.key, kv.value]),
    warnings: response.warnings,
  };
}

// Streaming command response decoding

/**
 * Decode a streaming-command response payload into a JSON-like shape the
 * core package can interpret. Returns `undefined` for response types that
 * are not streaming results (Arrow batches, `resultComplete`, etc.).
 *
 * @internal Exported for unit testing; not part of the package's public API
 * (not re-exported from `index.ts`).
 */
export function decodeCommandResponse(
  response: ExecutePlanResponse,
): Record<string, unknown> | undefined {
  const r = response.responseType;
  if (r.case === "writeStreamOperationStartResult") {
    return decodeWriteStreamStartResult(r.value);
  }
  if (r.case === "streamingQueryCommandResult") {
    return decodeStreamingQueryCommandResult(r.value);
  }
  return undefined;
}

function decodeWriteStreamStartResult(
  result: WriteStreamOperationStartResult,
): Record<string, unknown> {
  return {
    type: "writeStreamOperationStartResult",
    queryId: result.queryId ? { id: result.queryId.id, runId: result.queryId.runId } : undefined,
    name: result.name,
    ...(result.queryStartedEventJson !== undefined
      ? { queryStartedEventJson: result.queryStartedEventJson }
      : {}),
  };
}

function decodeStreamingQueryCommandResult(
  result: StreamingQueryCommandResult,
): Record<string, unknown> {
  const queryId = result.queryId
    ? { id: result.queryId.id, runId: result.queryId.runId }
    : undefined;
  const r = result.resultType;
  switch (r.case) {
    case "status":
      return {
        type: "streamingQueryCommandResult",
        queryId,
        resultType: "status",
        status: {
          statusMessage: r.value.statusMessage,
          isDataAvailable: r.value.isDataAvailable,
          isTriggerActive: r.value.isTriggerActive,
          isActive: r.value.isActive,
        },
      };
    case "recentProgress":
      return {
        type: "streamingQueryCommandResult",
        queryId,
        resultType: "recentProgress",
        recentProgressJson: r.value.recentProgressJson,
      };
    case "explain":
      return {
        type: "streamingQueryCommandResult",
        queryId,
        resultType: "explain",
        explain: r.value.result,
      };
    case "exception": {
      const { exceptionMessage, errorClass, stackTrace } = r.value;
      return {
        type: "streamingQueryCommandResult",
        queryId,
        resultType: "exception",
        exception: {
          ...(exceptionMessage !== undefined && { exceptionMessage }),
          ...(errorClass !== undefined && { errorClass }),
          ...(stackTrace !== undefined && { stackTrace }),
        },
      };
    }
    case "awaitTermination":
      return {
        type: "streamingQueryCommandResult",
        queryId,
        resultType: "awaitTermination",
        terminated: r.value.terminated,
      };
    default:
      // No result_type set (stop / processAllAvailable acks); just the queryId.
      return { type: "streamingQueryCommandResult", queryId };
  }
}

// Error wrapping

/** Human-readable gRPC status code names. */
const STATUS_NAMES: Record<number, string> = {
  0: "OK",
  1: "CANCELLED",
  2: "UNKNOWN",
  3: "INVALID_ARGUMENT",
  4: "DEADLINE_EXCEEDED",
  5: "NOT_FOUND",
  7: "PERMISSION_DENIED",
  8: "RESOURCE_EXHAUSTED",
  9: "FAILED_PRECONDITION",
  10: "ABORTED",
  13: "INTERNAL",
  14: "UNAVAILABLE",
  16: "UNAUTHENTICATED",
};

interface RawGrpcError {
  code?: number;
  details?: string;
  message?: string;
  metadata?: grpc.Metadata;
}

function buildBasicSparkError(err: unknown, errorInfo: ErrorInfo | undefined): SparkConnectError {
  const grpcErr = err as RawGrpcError;
  const code = grpcErr.code ?? 2; // UNKNOWN
  const statusName = STATUS_NAMES[code] ?? `STATUS_${code}`;
  const details = grpcErr.details ?? grpcErr.message ?? "Unknown gRPC error";
  return new SparkConnectError(`[${statusName}] ${details}`, {
    code,
    cause: err,
    errorClass: errorInfo?.metadata.errorClass,
    sqlState: errorInfo?.metadata.sqlState,
    messageParameters: errorInfo ? extractMessageParameters(errorInfo.metadata) : undefined,
  });
}

function enrichFromFetchResponse(
  base: SparkConnectError,
  response: FetchErrorDetailsResponse,
): SparkConnectError {
  const root = response.errors[response.rootErrorIdx ?? 0];
  if (!root) return base;
  return new SparkConnectError(root.message || base.message, {
    code: base.code,
    cause: base.cause,
    errorClass: root.sparkThrowable?.errorClass ?? base.errorClass,
    sqlState: root.sparkThrowable?.sqlState ?? base.sqlState,
    messageParameters: root.sparkThrowable?.messageParameters ?? base.messageParameters,
    errorTypeHierarchy: root.errorTypeHierarchy,
    serverStackTrace: root.stackTrace.map(formatStackFrame),
  });
}

function formatStackFrame(frame: {
  declaringClass: string;
  methodName: string;
  fileName?: string;
  lineNumber: number;
}): string {
  const location = frame.fileName ? `${frame.fileName}:${frame.lineNumber}` : "Unknown Source";
  return `${frame.declaringClass}.${frame.methodName}(${location})`;
}

/**
 * Strip reserved metadata keys so `messageParameters` carries only the
 * format-string parameters the user can substitute into the error template.
 * Mirrors PySpark's reserved set in `pyspark/errors/exceptions/connect.py`.
 */
const RESERVED_METADATA_KEYS = new Set([
  "errorClass",
  "sqlState",
  "errorId",
  "fragment",
  "breakingChange",
]);

function extractMessageParameters(metadata: Record<string, string>): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(metadata)) {
    if (!RESERVED_METADATA_KEYS.has(k)) out[k] = v;
  }
  return out;
}

/**
 * Decode the `grpc-status-details-bin` trailer into the Spark `ErrorInfo`.
 * The Spark Connect server packs one `google.rpc.ErrorInfo` into
 * `google.rpc.Status.details[]`; that ErrorInfo carries `errorClass`,
 * `sqlState`, message parameters, and an `errorId` for richer fetches.
 */
function extractErrorInfo(metadata: grpc.Metadata): ErrorInfo | undefined {
  const entries = metadata.get("grpc-status-details-bin");
  if (entries.length === 0) return undefined;
  const raw = entries[0];
  const bytes = typeof raw === "string" ? Buffer.from(raw, "base64") : raw;
  let status: Status;
  try {
    status = fromBinary(StatusSchema, bytes);
  } catch {
    return undefined;
  }
  for (const detail of status.details) {
    if (detail.typeUrl.endsWith("/google.rpc.ErrorInfo")) {
      try {
        return fromBinary(ErrorInfoSchema, detail.value);
      } catch {
        continue;
      }
    }
  }
  return undefined;
}

// Command building

import type { Command } from "@spark-connect-js/connect";

const SAVE_MODE_MAP: Record<string, WriteOperation_SaveMode> = {
  append: WriteOperation_SaveMode.APPEND,
  overwrite: WriteOperation_SaveMode.OVERWRITE,
  error: WriteOperation_SaveMode.ERROR_IF_EXISTS,
  ignore: WriteOperation_SaveMode.IGNORE,
};

const WRITE_V2_MODE_MAP: Record<string, WriteOperationV2_Mode> = {
  create: WriteOperationV2_Mode.CREATE,
  replace: WriteOperationV2_Mode.REPLACE,
  createOrReplace: WriteOperationV2_Mode.CREATE_OR_REPLACE,
  append: WriteOperationV2_Mode.APPEND,
  overwrite: WriteOperationV2_Mode.OVERWRITE,
  overwritePartitions: WriteOperationV2_Mode.OVERWRITE_PARTITIONS,
};

/**
 * Map a core-side command record onto a Spark Connect `Command` protobuf.
 *
 * @internal Exported for unit testing; not part of the package's public API
 * (not re-exported from `index.ts`).
 */
export function buildCommandProto(command: Record<string, unknown>): Command {
  const type = command.type as string;

  if (type === "writeOperation") {
    const plan = command.plan as import("@spark-connect-js/core").LogicalPlan;
    const relation = buildRelation(plan);
    const saveType = command.saveType as { case: string; value: unknown };
    const mode =
      SAVE_MODE_MAP[(command.mode as string) ?? "error"] ?? WriteOperation_SaveMode.ERROR_IF_EXISTS;

    let saveTypeProto: WriteOperation["saveType"];
    if (saveType.case === "path") {
      saveTypeProto = { case: "path", value: saveType.value as string };
    } else if (saveType.case === "table") {
      const tableInfo = saveType.value as { tableName: string; saveMethod: string };
      saveTypeProto = {
        case: "table",
        value: create(WriteOperation_SaveTableSchema, {
          tableName: tableInfo.tableName,
          saveMethod:
            tableInfo.saveMethod === "insertInto"
              ? WriteOperation_SaveTable_TableSaveMethod.INSERT_INTO
              : WriteOperation_SaveTable_TableSaveMethod.SAVE_AS_TABLE,
        }),
      };
    } else {
      saveTypeProto = { case: undefined, value: undefined };
    }

    const writeOp = create(WriteOperationSchema, {
      input: relation,
      source: command.source as string,
      mode,
      saveType: saveTypeProto,
      options: (command.options as Record<string, string>) ?? {},
      partitioningColumns: (command.partitioningColumns as string[]) ?? [],
      sortColumnNames: (command.sortColumnNames as string[]) ?? [],
    });

    const bucket = command.bucketBy as { numBuckets: number; columnNames: string[] } | undefined;
    if (bucket) {
      writeOp.bucketBy = create(WriteOperation_BucketBySchema, {
        numBuckets: bucket.numBuckets,
        bucketColumnNames: bucket.columnNames,
      });
    }

    return create(CommandSchema, {
      commandType: {
        case: "writeOperation",
        value: writeOp,
      },
    });
  }

  if (type === "createDataframeView") {
    const plan = command.plan as import("@spark-connect-js/core").LogicalPlan;
    const relation = buildRelation(plan);
    return create(CommandSchema, {
      commandType: {
        case: "createDataframeView",
        value: create(CreateDataFrameViewCommandSchema, {
          input: relation,
          name: command.name as string,
          isGlobal: (command.isGlobal as boolean) ?? false,
          replace: (command.replace as boolean) ?? true,
        }),
      },
    });
  }

  if (type === "writeOperationV2") {
    const plan = command.plan as import("@spark-connect-js/core").LogicalPlan;
    const relation = buildRelation(plan);
    const modeStr = command.mode as string;
    const mode = WRITE_V2_MODE_MAP[modeStr];
    if (mode === undefined) {
      throw new UnsupportedOperationError(`Unknown writeOperationV2 mode: ${modeStr}`);
    }

    const partitioningExprs = (
      (command.partitioningColumns as import("@spark-connect-js/core").Expression[]) ?? []
    ).map((e) => buildExpression(e));

    const writeV2 = create(WriteOperationV2Schema, {
      input: relation,
      tableName: command.tableName as string,
      provider: (command.provider as string) ?? undefined,
      mode,
      options: (command.options as Record<string, string>) ?? {},
      tableProperties: (command.tableProperties as Record<string, string>) ?? {},
      partitioningColumns: partitioningExprs,
      clusteringColumns: (command.clusteringColumns as string[]) ?? [],
    });

    if (command.overwriteCondition) {
      writeV2.overwriteCondition = buildExpression(
        command.overwriteCondition as import("@spark-connect-js/core").Expression,
      );
    }

    return create(CommandSchema, {
      commandType: {
        case: "writeOperationV2",
        value: writeV2,
      },
    });
  }

  if (type === "writeStreamOperationStart") {
    return buildWriteStreamStartCommand(command);
  }

  if (type === "streamingQueryCommand") {
    return buildStreamingQueryCommand(command);
  }

  if (type === "registerFunction") {
    const javaUdf = create(JavaUDFSchema, {
      className: command.className as string,
      aggregate: (command.aggregate as boolean) ?? false,
      ...(command.returnType !== undefined
        ? {
            outputType: create(DataTypeSchema, {
              kind: {
                case: "unparsed",
                value: create(DataType_UnparsedSchema, {
                  dataTypeString: command.returnType as string,
                }),
              },
            }),
          }
        : {}),
    });
    const udf = create(CommonInlineUserDefinedFunctionSchema, {
      functionName: command.functionName as string,
      deterministic: true,
      arguments: [],
      isDistinct: false,
      function: { case: "javaUdf", value: javaUdf },
    });
    return create(CommandSchema, {
      commandType: { case: "registerFunction", value: udf },
    });
  }

  throw new UnsupportedOperationError(`Unsupported command type: ${type}`);
}

// Streaming command building

// Mirrors the public `Trigger` discriminated union from
// `@spark-connect-js/core`. Defined here (rather than imported) so the
// transport stays decoupled from streaming-specific exports.
type TriggerSpec =
  | { kind: "processingTime"; interval: string }
  | { kind: "availableNow" }
  | { kind: "once" }
  | { kind: "continuous"; interval: string };

interface SinkSpec {
  kind: "path" | "table";
  value: string;
}

function buildTriggerProto(trigger: TriggerSpec | undefined): WriteStreamOperationStart["trigger"] {
  if (trigger === undefined) return { case: undefined, value: undefined };
  switch (trigger.kind) {
    case "processingTime":
      return { case: "processingTimeInterval", value: trigger.interval };
    case "availableNow":
      return { case: "availableNow", value: true };
    case "once":
      return { case: "once", value: true };
    case "continuous":
      return { case: "continuousCheckpointInterval", value: trigger.interval };
  }
}

function buildSinkProto(sink: SinkSpec | undefined): WriteStreamOperationStart["sinkDestination"] {
  if (sink === undefined) return { case: undefined, value: undefined };
  return sink.kind === "path"
    ? { case: "path", value: sink.value }
    : { case: "tableName", value: sink.value };
}

function buildWriteStreamStartCommand(command: Record<string, unknown>): Command {
  const plan = command.plan as LogicalPlan;
  const start = create(WriteStreamOperationStartSchema, {
    input: buildRelation(plan),
    format: (command.format as string) ?? "",
    options: (command.options as Record<string, string>) ?? {},
    partitioningColumnNames: (command.partitioningColumnNames as string[]) ?? [],
    trigger: buildTriggerProto(command.trigger as TriggerSpec | undefined),
    outputMode: (command.outputMode as string) ?? "",
    queryName: (command.queryName as string) ?? "",
    sinkDestination: buildSinkProto(command.sink as SinkSpec | undefined),
  });
  return create(CommandSchema, {
    commandType: { case: "writeStreamOperationStart", value: start },
  });
}

function buildStreamingQueryOpProto(
  op: string,
  command: Record<string, unknown>,
): StreamingQueryCommand["command"] {
  switch (op) {
    case "status":
      return { case: "status", value: true };
    case "lastProgress":
      return { case: "lastProgress", value: true };
    case "recentProgress":
      return { case: "recentProgress", value: true };
    case "stop":
      return { case: "stop", value: true };
    case "processAllAvailable":
      return { case: "processAllAvailable", value: true };
    case "exception":
      return { case: "exception", value: true };
    case "explain":
      return {
        case: "explain",
        value: create(StreamingQueryCommand_ExplainCommandSchema, {
          extended: (command.extended as boolean) ?? false,
        }),
      };
    case "awaitTermination": {
      const timeoutMs = command.timeoutMs as number | undefined;
      return {
        case: "awaitTermination",
        value: create(StreamingQueryCommand_AwaitTerminationCommandSchema, {
          ...(timeoutMs !== undefined && { timeoutMs: BigInt(timeoutMs) }),
        }),
      };
    }
    default:
      throw new UnsupportedOperationError(`Unsupported streamingQueryCommand op: ${op}`);
  }
}

function buildStreamingQueryCommand(command: Record<string, unknown>): Command {
  const queryId = command.queryId as { id: string; runId: string };
  const op = command.op as string;
  return create(CommandSchema, {
    commandType: {
      case: "streamingQueryCommand",
      value: create(StreamingQueryCommandSchema, {
        queryId: create(StreamingQueryInstanceIdSchema, {
          id: queryId.id,
          runId: queryId.runId,
        }),
        command: buildStreamingQueryOpProto(op, command),
      }),
    },
  });
}

// AnalyzePlan request/response building

function buildAnalyzePlanRequest(
  sessionId: string,
  request: Record<string, unknown>,
  userContext: UserContext,
  clientType: string,
  observedServerSideSessionId: string | undefined,
): AnalyzePlanRequest {
  const type = request.type as string;
  const plan = request.plan as import("@spark-connect-js/core").LogicalPlan | undefined;
  const relation = plan ? buildRelation(plan) : undefined;

  const base = {
    sessionId,
    userContext,
    clientType,
    ...(observedServerSideSessionId !== undefined
      ? { clientObservedServerSideSessionId: observedServerSideSessionId }
      : {}),
  };

  if (type === "schema") {
    return create(AnalyzePlanRequestSchema, {
      ...base,
      analyze: {
        case: "schema",
        value: create(AnalyzePlanRequest_SchemaSchema, {
          plan: create(PlanSchema, {
            opType: { case: "root", value: relation! },
          }),
        }),
      },
    });
  }

  if (type === "explain") {
    // Look up the explain mode enum value
    const modeStr = (request.mode as string) ?? "simple";
    const modeValues: Record<string, number> = {
      simple: 1,
      extended: 2,
      codegen: 3,
      cost: 4,
      formatted: 5,
    };

    return create(AnalyzePlanRequestSchema, {
      ...base,
      analyze: {
        case: "explain",
        value: create(AnalyzePlanRequest_ExplainSchema, {
          plan: create(PlanSchema, {
            opType: { case: "root", value: relation! },
          }),
          explainMode: modeValues[modeStr] ?? 1,
        }),
      },
    });
  }

  if (type === "persist") {
    const sl = request.storageLevel as
      | {
          useDisk: boolean;
          useMemory: boolean;
          useOffHeap: boolean;
          deserialized: boolean;
          replication: number;
        }
      | undefined;
    return create(AnalyzePlanRequestSchema, {
      ...base,
      analyze: {
        case: "persist",
        value: create(AnalyzePlanRequest_PersistSchema, {
          relation: relation!,
          storageLevel: sl
            ? create(StorageLevelSchema, {
                useDisk: sl.useDisk,
                useMemory: sl.useMemory,
                useOffHeap: sl.useOffHeap,
                deserialized: sl.deserialized,
                replication: sl.replication,
              })
            : undefined,
        }),
      },
    });
  }

  if (type === "unpersist") {
    return create(AnalyzePlanRequestSchema, {
      ...base,
      analyze: {
        case: "unpersist",
        value: create(AnalyzePlanRequest_UnpersistSchema, {
          relation: relation!,
          blocking: (request.blocking as boolean) ?? false,
        }),
      },
    });
  }

  if (type === "getStorageLevel") {
    return create(AnalyzePlanRequestSchema, {
      ...base,
      analyze: {
        case: "getStorageLevel",
        value: create(AnalyzePlanRequest_GetStorageLevelSchema, {
          relation: relation!,
        }),
      },
    });
  }

  if (type === "sameSemantics") {
    const otherPlan = request.otherPlan as import("@spark-connect-js/core").LogicalPlan;
    const otherRelation = buildRelation(otherPlan);
    return create(AnalyzePlanRequestSchema, {
      ...base,
      analyze: {
        case: "sameSemantics",
        value: create(AnalyzePlanRequest_SameSemanticsSchema, {
          targetPlan: create(PlanSchema, {
            opType: { case: "root", value: relation! },
          }),
          otherPlan: create(PlanSchema, {
            opType: { case: "root", value: otherRelation },
          }),
        }),
      },
    });
  }

  if (type === "semanticHash") {
    return create(AnalyzePlanRequestSchema, {
      ...base,
      analyze: {
        case: "semanticHash",
        value: create(AnalyzePlanRequest_SemanticHashSchema, {
          plan: create(PlanSchema, {
            opType: { case: "root", value: relation! },
          }),
        }),
      },
    });
  }

  if (type === "sparkVersion") {
    return create(AnalyzePlanRequestSchema, {
      ...base,
      analyze: {
        case: "sparkVersion",
        value: create(AnalyzePlanRequest_SparkVersionSchema, {}),
      },
    });
  }

  throw new UnsupportedOperationError(`Unsupported analyze type: ${type}`);
}

function extractAnalyzeResult(response: AnalyzePlanResponse): Record<string, unknown> {
  const result = response.result;
  if (!result || result.case === undefined) {
    return {};
  }

  switch (result.case) {
    case "schema":
      return { type: "schema", schema: result.value.schema };
    case "explain":
      return { type: "explain", explainString: result.value.explainString };
    case "persist":
      return { type: "persist" };
    case "unpersist":
      return { type: "unpersist" };
    case "getStorageLevel": {
      const sl = result.value.storageLevel;
      return {
        type: "getStorageLevel",
        storageLevel: sl
          ? {
              useDisk: sl.useDisk,
              useMemory: sl.useMemory,
              useOffHeap: sl.useOffHeap,
              deserialized: sl.deserialized,
              replication: sl.replication,
            }
          : undefined,
      };
    }
    case "sameSemantics":
      return { type: "sameSemantics", result: result.value.result };
    case "semanticHash":
      return { type: "semanticHash", result: result.value.result };
    case "sparkVersion":
      return { type: "sparkVersion", version: result.value.version };
    default:
      return { type: result.case };
  }
}
