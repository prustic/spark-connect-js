/**
 * GrpcTransport
 *
 * Concrete implementation of @spark-connect-js/core's Transport interface using
 * @grpc/grpc-js to communicate with the Spark Connect gRPC service.
 *
 * @see Spark Connect service proto: sql/connect/common/src/main/protobuf/spark/connect/base.proto
 * @see Spark Connect server: sql/connect/server/src/main/scala/org/apache/spark/sql/connect/service/SparkConnectService.scala
 * @see ExecutePlan handler: sql/connect/server/src/main/scala/org/apache/spark/sql/connect/execution/ExecuteGrpcResponseSender.scala
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
  CommonInlineUserDefinedFunctionSchema,
  JavaUDFSchema,
  DataTypeSchema,
  DataType_UnparsedSchema,
  type WriteOperation,
  type ExecutePlanRequest,
  type ExecutePlanResponse,
  type ReleaseSessionRequest,
  type ReleaseSessionResponse,
  type AnalyzePlanRequest,
  type AnalyzePlanResponse,
  type ConfigRequest,
  type ConfigRequest_Operation,
  type ConfigResponse,
} from "@spark-connect-js/connect";
import type { Transport } from "@spark-connect-js/core";
import type { LogicalPlan } from "@spark-connect-js/core";
import { SparkConnectError } from "@spark-connect-js/core";
import { buildRelation, buildExpression } from "../proto/proto-builder.js";

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
  private client: grpc.Client | null = null;

  constructor(options: GrpcTransportOptions) {
    this.endpoint = `${options.host}:${options.port}`;
    this.credentials = buildCredentials(options);
    this.channelOptions = buildChannelOptions(options.grpcMaxMessageSize);
    this.metadata = buildMetadata(options.metadata);
    this.userContext = create(UserContextSchema, {
      userId: options.userId ?? "spark-js",
    });
    this.clientType = buildClientType(options.userAgent);
  }

  /** Send a logical plan to Spark Connect and yield Arrow IPC batches. */
  async *executePlan(sessionId: string, plan: LogicalPlan): AsyncIterable<Uint8Array> {
    const client = this._getClient();

    // Convert our LogicalPlan to a typed Spark Connect Relation protobuf
    const relation = buildRelation(plan);

    // Build the ExecutePlanRequest protobuf message. The operation_id is a
    // client-generated UUIDv4 the server echoes on every response and that
    // identifies the operation for reattach and interrupt RPCs.
    const request = create(ExecutePlanRequestSchema, {
      sessionId,
      userContext: this.userContext,
      operationId: newOperationId(),
      plan: create(PlanSchema, {
        opType: { case: "root", value: relation },
      }),
      clientType: this.clientType,
    });

    // Server-streaming RPC call
    const serialize = (value: ExecutePlanRequest): Buffer =>
      Buffer.from(toBinary(ExecutePlanRequestSchema, value));
    const deserialize = (bytes: Buffer): ExecutePlanResponse =>
      fromBinary(ExecutePlanResponseSchema, bytes);

    const stream = client.makeServerStreamRequest<ExecutePlanRequest, ExecutePlanResponse>(
      "/spark.connect.SparkConnectService/ExecutePlan",
      serialize,
      deserialize,
      request,
      this.metadata,
    );

    // Consume the gRPC server stream
    try {
      for await (const _response of stream) {
        const response = _response as ExecutePlanResponse;
        if (
          response.responseType.case === "arrowBatch" &&
          response.responseType.value.data.length > 0
        ) {
          yield response.responseType.value.data;
        }

        if (response.responseType.case === "resultComplete") {
          break;
        }
      }
    } catch (err: unknown) {
      throw wrapGrpcError(err);
    }
  }

  /** Close the gRPC channel. */
  close(): void {
    if (this.client) {
      this.client.close();
      this.client = null;
    }
  }

  /** Execute a command (write, createView, etc.) via ExecutePlan RPC. */
  async executeCommand(sessionId: string, command: Record<string, unknown>): Promise<void> {
    const client = this._getClient();

    const commandProto = buildCommandProto(command);

    const request = create(ExecutePlanRequestSchema, {
      sessionId,
      userContext: this.userContext,
      operationId: newOperationId(),
      plan: create(PlanSchema, {
        opType: { case: "command", value: commandProto },
      }),
      clientType: this.clientType,
    });

    const serialize = (value: ExecutePlanRequest): Buffer =>
      Buffer.from(toBinary(ExecutePlanRequestSchema, value));
    const deserialize = (bytes: Buffer): ExecutePlanResponse =>
      fromBinary(ExecutePlanResponseSchema, bytes);

    const stream = client.makeServerStreamRequest<ExecutePlanRequest, ExecutePlanResponse>(
      "/spark.connect.SparkConnectService/ExecutePlan",
      serialize,
      deserialize,
      request,
      this.metadata,
    );

    // Consume the stream (commands may return resultComplete)
    try {
      for await (const _response of stream) {
        const response = _response as ExecutePlanResponse;
        if (response.responseType.case === "resultComplete") {
          break;
        }
      }
    } catch (err: unknown) {
      throw wrapGrpcError(err);
    }
  }

  /** Release the session on the server. */
  releaseSession(sessionId: string): Promise<void> {
    const client = this._getClient();

    const request = create(ReleaseSessionRequestSchema, {
      sessionId,
      userContext: this.userContext,
      clientType: this.clientType,
    });

    const serialize = (value: ReleaseSessionRequest): Buffer =>
      Buffer.from(toBinary(ReleaseSessionRequestSchema, value));
    const deserialize = (bytes: Buffer): ReleaseSessionResponse =>
      fromBinary(ReleaseSessionResponseSchema, bytes);

    return new Promise<void>((resolve, reject) => {
      client.makeUnaryRequest<ReleaseSessionRequest, ReleaseSessionResponse>(
        "/spark.connect.SparkConnectService/ReleaseSession",
        serialize,
        deserialize,
        request,
        this.metadata,
        (err: grpc.ServiceError | null) => {
          if (err) {
            reject(wrapGrpcError(err));
          } else {
            resolve();
          }
        },
      );
    });
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
    );

    const serialize = (value: AnalyzePlanRequest): Buffer =>
      Buffer.from(toBinary(AnalyzePlanRequestSchema, value));
    const deserialize = (bytes: Buffer): AnalyzePlanResponse =>
      fromBinary(AnalyzePlanResponseSchema, bytes);

    return new Promise<Record<string, unknown>>((resolve, reject) => {
      client.makeUnaryRequest<AnalyzePlanRequest, AnalyzePlanResponse>(
        "/spark.connect.SparkConnectService/AnalyzePlan",
        serialize,
        deserialize,
        analyzeRequest,
        this.metadata,
        (err: grpc.ServiceError | null, response?: AnalyzePlanResponse) => {
          if (err) {
            reject(wrapGrpcError(err));
          } else {
            resolve(extractAnalyzeResult(response!));
          }
        },
      );
    });
  }

  /** Read or write Spark runtime configuration via the Config RPC. */
  config(sessionId: string, operation: Record<string, unknown>): Promise<Record<string, unknown>> {
    const client = this._getClient();

    const configRequest = create(ConfigRequestSchema, {
      sessionId,
      userContext: this.userContext,
      clientType: this.clientType,
      operation: buildConfigOperation(operation),
    });

    const serialize = (value: ConfigRequest): Buffer =>
      Buffer.from(toBinary(ConfigRequestSchema, value));
    const deserialize = (bytes: Buffer): ConfigResponse => fromBinary(ConfigResponseSchema, bytes);

    return new Promise<Record<string, unknown>>((resolve, reject) => {
      client.makeUnaryRequest<ConfigRequest, ConfigResponse>(
        "/spark.connect.SparkConnectService/Config",
        serialize,
        deserialize,
        configRequest,
        this.metadata,
        (err: grpc.ServiceError | null, response?: ConfigResponse) => {
          if (err) {
            reject(wrapGrpcError(err));
          } else {
            resolve(extractConfigResult(response!));
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
  if (opts.token !== undefined) {
    if (opts.useSsl !== true) {
      throw new InvalidConfigError(
        "Spark Connect token authentication requires use_ssl=true. " +
          "Token-over-insecure transports are rejected to avoid leaking credentials.",
      );
    }
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

function extractConfigResult(response: ConfigResponse): Record<string, unknown> {
  return {
    pairs: response.pairs.map((kv) => [kv.key, kv.value]),
    warnings: response.warnings,
  };
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

function wrapGrpcError(err: unknown): SparkConnectError {
  if (err instanceof SparkConnectError) return err;

  // gRPC errors from @grpc/grpc-js have `code`, `details`, and `metadata` props
  const grpcErr = err as { code?: number; details?: string; message?: string };
  const code = grpcErr.code ?? 2; // UNKNOWN
  const statusName = STATUS_NAMES[code] ?? `STATUS_${code}`;
  const details = grpcErr.details ?? grpcErr.message ?? "Unknown gRPC error";

  return new SparkConnectError(`[${statusName}] ${details}`, {
    code,
    cause: err,
  });
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

function buildCommandProto(command: Record<string, unknown>): Command {
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

// AnalyzePlan request/response building

function buildAnalyzePlanRequest(
  sessionId: string,
  request: Record<string, unknown>,
  userContext: UserContext,
  clientType: string,
): AnalyzePlanRequest {
  const type = request.type as string;
  const plan = request.plan as import("@spark-connect-js/core").LogicalPlan | undefined;
  const relation = plan ? buildRelation(plan) : undefined;

  const base = {
    sessionId,
    userContext,
    clientType,
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
