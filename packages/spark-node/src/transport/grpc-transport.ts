/**
 * ─── GrpcTransport ──────────────────────────────────────────────────────────
 *
 * Concrete implementation of @spark-js/core's Transport interface using
 * @grpc/grpc-js to communicate with the Spark Connect gRPC service.
 *
 * @see Spark Connect service proto: sql/connect/common/src/main/protobuf/spark/connect/base.proto
 * @see Spark Connect server: sql/connect/server/src/main/scala/org/apache/spark/sql/connect/service/SparkConnectService.scala
 * @see ExecutePlan handler: sql/connect/server/src/main/scala/org/apache/spark/sql/connect/execution/ExecuteGrpcResponseSender.scala
 *
 * Uses @bufbuild/protobuf for message serialization and @grpc/grpc-js for
 * the HTTP/2 transport. Messages are created using the generated schemas
 * from @spark-js/connect and serialized to binary protobuf on the wire.
 */

import * as grpc from "@grpc/grpc-js";
import { create, toBinary, fromBinary } from "@bufbuild/protobuf";
import {
  ExecutePlanRequestSchema,
  ExecutePlanResponseSchema,
  PlanSchema,
  UserContextSchema,
  type ExecutePlanRequest,
  type ExecutePlanResponse,
} from "@spark-js/connect";
import type { Transport } from "@spark-js/core";
import type { LogicalPlan } from "@spark-js/core";
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
  }

  /** Close the gRPC channel. */
  close(): void {
    if (this.client) {
      this.client.close();
      this.client = null;
    }
  }

  private _getClient(): grpc.Client {
    if (!this.client) {
      this.client = new grpc.Client(this.endpoint, this.credentials, CHANNEL_OPTIONS);
    }
    return this.client;
  }
}
