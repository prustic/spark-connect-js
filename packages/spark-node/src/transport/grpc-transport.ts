/**
 * ─── GrpcTransport ──────────────────────────────────────────────────────────
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

import * as grpc from "@grpc/grpc-js";
import { create, toBinary, fromBinary } from "@bufbuild/protobuf";
import {
  ExecutePlanRequestSchema,
  ExecutePlanResponseSchema,
  PlanSchema,
  UserContextSchema,
  ReleaseSessionRequestSchema,
  ReleaseSessionResponseSchema,
  AnalyzePlanRequestSchema,
  AnalyzePlanResponseSchema,
  AnalyzePlanRequest_SchemaSchema,
  AnalyzePlanRequest_ExplainSchema,
  CommandSchema,
  WriteOperationSchema,
  WriteOperation_SaveMode,
  WriteOperation_SaveTableSchema,
  WriteOperation_SaveTable_TableSaveMethod,
  CreateDataFrameViewCommandSchema,
  type WriteOperation,
  type ExecutePlanRequest,
  type ExecutePlanResponse,
  type ReleaseSessionRequest,
  type ReleaseSessionResponse,
  type AnalyzePlanRequest,
  type AnalyzePlanResponse,
} from "@spark-connect-js/connect";
import type { Transport } from "@spark-connect-js/core";
import type { LogicalPlan } from "@spark-connect-js/core";
import { SparkConnectError } from "@spark-connect-js/core";
import { buildRelation } from "../proto/proto-builder.js";

/** gRPC channel options tuned for Spark Connect workloads. */
const CHANNEL_OPTIONS: Record<string, number> = {
  // Arrow batches can be 64MB+; default 4MB limit is too small
  "grpc.max_receive_message_length": 128 * 1024 * 1024,
  "grpc.max_send_message_length": 128 * 1024 * 1024,
  // Keepalive to detect dead connections through load balancers
  "grpc.keepalive_time_ms": 30_000,
  "grpc.keepalive_timeout_ms": 10_000,
};

/**
 * gRPC-based transport for Spark Connect.
 *
 * Usage:
 *   const transport = new GrpcTransport("localhost:15002");
 *   const spark = SparkSession.builder()
 *     .remote("sc://localhost:15002")
 *     .transport(transport)
 *     .getOrCreate();
 */
export class GrpcTransport implements Transport {
  private readonly endpoint: string;
  private readonly credentials: grpc.ChannelCredentials;
  private client: grpc.Client | null = null;

  constructor(endpoint: string, credentials?: grpc.ChannelCredentials) {
    this.endpoint = endpoint;
    this.credentials = credentials ?? grpc.credentials.createInsecure();
  }

  /**
   * Send a logical plan to Spark Connect and yield Arrow IPC batches.
   *
   * The flow:
   *   1. Convert LogicalPlan tree → Spark Connect Relation protobuf
   *   2. Wrap in ExecutePlanRequest with session_id and user_context
   *   3. Call SparkConnectService.ExecutePlan (server-streaming RPC)
   *   4. For each ExecutePlanResponse in the stream:
   *      - If it contains arrow_batch.data → yield the Uint8Array
   *      - If it contains result_complete → break
   *
   * @yields Uint8Array chunks of Arrow IPC stream data
   */
  async *executePlan(sessionId: string, plan: LogicalPlan): AsyncIterable<Uint8Array> {
    const client = this._getClient();

    // Convert our LogicalPlan to a typed Spark Connect Relation protobuf
    const relation = buildRelation(plan);

    // Build the ExecutePlanRequest protobuf message
    const request = create(ExecutePlanRequestSchema, {
      sessionId,
      userContext: create(UserContextSchema, {
        userId: "spark-js",
      }),
      plan: create(PlanSchema, {
        opType: { case: "root", value: relation },
      }),
      clientType: "spark-js-node",
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

  /**
   * Execute a command (write, createView, etc.) via ExecutePlan RPC.
   * Commands use Plan.command instead of Plan.root.
   */
  async executeCommand(sessionId: string, command: Record<string, unknown>): Promise<void> {
    const client = this._getClient();

    const commandProto = buildCommandProto(command);

    const request = create(ExecutePlanRequestSchema, {
      sessionId,
      userContext: create(UserContextSchema, { userId: "spark-js" }),
      plan: create(PlanSchema, {
        opType: { case: "command", value: commandProto },
      }),
      clientType: "spark-js-node",
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

  /**
   * Release the session on the server (frees server-side state like temp views).
   *
   * @see Spark Connect: SparkConnectService.ReleaseSession RPC
   */
  releaseSession(sessionId: string): Promise<void> {
    const client = this._getClient();

    const request = create(ReleaseSessionRequestSchema, {
      sessionId,
      userContext: create(UserContextSchema, {
        userId: "spark-js",
      }),
      clientType: "spark-js-node",
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

  /**
   * Send an AnalyzePlan request (unary RPC).
   *
   * Accepts a plain descriptor from spark-core and builds the proper
   * AnalyzePlanRequest protobuf message.
   *
   * @see Spark Connect: SparkConnectService.AnalyzePlan RPC
   */
  analyzePlan(
    sessionId: string,
    request: Record<string, unknown>,
  ): Promise<Record<string, unknown>> {
    const client = this._getClient();

    const analyzeRequest = buildAnalyzePlanRequest(sessionId, request);

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

  private _getClient(): grpc.Client {
    if (!this.client) {
      this.client = new grpc.Client(this.endpoint, this.credentials, CHANNEL_OPTIONS);
    }
    return this.client;
  }
}

// ─── Error wrapping ─────────────────────────────────────────────────────────

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

// ─── Command building ───────────────────────────────────────────────────────

import type { Command } from "@spark-connect-js/connect";

const SAVE_MODE_MAP: Record<string, WriteOperation_SaveMode> = {
  append: WriteOperation_SaveMode.APPEND,
  overwrite: WriteOperation_SaveMode.OVERWRITE,
  error: WriteOperation_SaveMode.ERROR_IF_EXISTS,
  ignore: WriteOperation_SaveMode.IGNORE,
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

    return create(CommandSchema, {
      commandType: {
        case: "writeOperation",
        value: create(WriteOperationSchema, {
          input: relation,
          source: command.source as string,
          mode,
          saveType: saveTypeProto,
          options: (command.options as Record<string, string>) ?? {},
          partitioningColumns: (command.partitioningColumns as string[]) ?? [],
          sortColumnNames: (command.sortColumnNames as string[]) ?? [],
        }),
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

  throw new Error(`Unsupported command type: ${type}`);
}

// ─── AnalyzePlan request/response building ──────────────────────────────────

function buildAnalyzePlanRequest(
  sessionId: string,
  request: Record<string, unknown>,
): AnalyzePlanRequest {
  const type = request.type as string;
  const plan = request.plan as import("@spark-connect-js/core").LogicalPlan | undefined;
  const relation = plan ? buildRelation(plan) : undefined;

  const base = {
    sessionId,
    userContext: create(UserContextSchema, { userId: "spark-js" }),
    clientType: "spark-js-node",
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

  throw new Error(`Unsupported analyze type: ${type}`);
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
    default:
      return { type: result.case };
  }
}
