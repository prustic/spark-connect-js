import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { create } from "@bufbuild/protobuf";
import {
  ExecutePlanResponseSchema,
  StreamingQueryInstanceIdSchema,
  WriteStreamOperationStartResultSchema,
  StreamingQueryCommandResultSchema,
  StreamingQueryCommandResult_StatusResultSchema,
  StreamingQueryCommandResult_RecentProgressResultSchema,
  StreamingQueryCommandResult_ExplainResultSchema,
  StreamingQueryCommandResult_ExceptionResultSchema,
  StreamingQueryCommandResult_AwaitTerminationResultSchema,
  StreamingQueryManagerCommandResultSchema,
  StreamingQueryManagerCommandResult_ActiveResultSchema,
  StreamingQueryManagerCommandResult_StreamingQueryInstanceSchema,
  StreamingQueryManagerCommandResult_AwaitAnyTerminationResultSchema,
  StreamingQueryListenerEventsResultSchema,
  StreamingQueryListenerEventSchema,
  StreamingQueryEventType,
} from "@spark-connect-js/connect";
import type { LogicalPlan } from "@spark-connect-js/core";
import { UnsupportedOperationError } from "@spark-connect-js/core";
import { buildCommandProto, decodeCommandResponse } from "./grpc-transport.js";

const RATE_PLAN: LogicalPlan = {
  type: "read",
  format: "rate",
  path: "",
  options: {},
  isStreaming: true,
};

describe("buildCommandProto: writeStreamOperationStart", () => {
  it("maps the base fields and an empty trigger/sink oneof", () => {
    const cmd = buildCommandProto({
      type: "writeStreamOperationStart",
      plan: RATE_PLAN,
      format: "memory",
      options: { checkpointLocation: "/tmp/c" },
      partitioningColumnNames: ["y", "m"],
      outputMode: "append",
      queryName: "q1",
    });
    assert.equal(cmd.commandType.case, "writeStreamOperationStart");
    if (cmd.commandType.case !== "writeStreamOperationStart") return;
    const v = cmd.commandType.value;
    assert.equal(v.format, "memory");
    assert.deepEqual(v.options, { checkpointLocation: "/tmp/c" });
    assert.deepEqual(v.partitioningColumnNames, ["y", "m"]);
    assert.equal(v.outputMode, "append");
    assert.equal(v.queryName, "q1");
    assert.ok(v.input);
    assert.equal(v.trigger.case, undefined);
    assert.equal(v.sinkDestination.case, undefined);
  });

  for (const [spec, expectedCase, expectedValue] of [
    [{ kind: "processingTime", interval: "5 seconds" }, "processingTimeInterval", "5 seconds"],
    [{ kind: "availableNow" }, "availableNow", true],
    [{ kind: "once" }, "once", true],
    [{ kind: "continuous", interval: "1 second" }, "continuousCheckpointInterval", "1 second"],
  ] as const) {
    it(`maps trigger ${spec.kind} → ${expectedCase}`, () => {
      const cmd = buildCommandProto({
        type: "writeStreamOperationStart",
        plan: RATE_PLAN,
        format: "memory",
        trigger: spec,
      });
      assert.equal(cmd.commandType.case, "writeStreamOperationStart");
      if (cmd.commandType.case !== "writeStreamOperationStart") return;
      assert.equal(cmd.commandType.value.trigger.case, expectedCase);
      assert.equal(cmd.commandType.value.trigger.value, expectedValue);
    });
  }

  it("maps a path sink to the `path` oneof case", () => {
    const cmd = buildCommandProto({
      type: "writeStreamOperationStart",
      plan: RATE_PLAN,
      format: "parquet",
      sink: { kind: "path", value: "/out" },
    });
    assert.equal(cmd.commandType.case, "writeStreamOperationStart");
    if (cmd.commandType.case !== "writeStreamOperationStart") return;
    assert.deepEqual(cmd.commandType.value.sinkDestination, { case: "path", value: "/out" });
  });

  it("maps a table sink to the `tableName` oneof case", () => {
    const cmd = buildCommandProto({
      type: "writeStreamOperationStart",
      plan: RATE_PLAN,
      format: "parquet",
      sink: { kind: "table", value: "events" },
    });
    assert.equal(cmd.commandType.case, "writeStreamOperationStart");
    if (cmd.commandType.case !== "writeStreamOperationStart") return;
    assert.deepEqual(cmd.commandType.value.sinkDestination, {
      case: "tableName",
      value: "events",
    });
  });
});

describe("buildCommandProto: streamingQueryCommand", () => {
  const queryId = { id: "id-a", runId: "run-b" };

  for (const op of [
    "status",
    "lastProgress",
    "recentProgress",
    "stop",
    "processAllAvailable",
    "exception",
  ] as const) {
    it(`maps op=${op} to a boolean command case`, () => {
      const cmd = buildCommandProto({ type: "streamingQueryCommand", queryId, op });
      assert.equal(cmd.commandType.case, "streamingQueryCommand");
      if (cmd.commandType.case !== "streamingQueryCommand") return;
      const v = cmd.commandType.value;
      assert.deepEqual(v.queryId, create(StreamingQueryInstanceIdSchema, queryId));
      assert.equal(v.command.case, op);
      assert.equal(v.command.value, true);
    });
  }

  it("maps op=explain with the extended flag", () => {
    const cmd = buildCommandProto({
      type: "streamingQueryCommand",
      queryId,
      op: "explain",
      extended: true,
    });
    assert.equal(cmd.commandType.case, "streamingQueryCommand");
    if (cmd.commandType.case !== "streamingQueryCommand") return;
    const c = cmd.commandType.value.command;
    assert.equal(c.case, "explain");
    if (c.case !== "explain") return;
    assert.equal(c.value.extended, true);
  });

  it("maps op=awaitTermination with timeoutMs as a bigint", () => {
    const cmd = buildCommandProto({
      type: "streamingQueryCommand",
      queryId,
      op: "awaitTermination",
      timeoutMs: 5000,
    });
    assert.equal(cmd.commandType.case, "streamingQueryCommand");
    if (cmd.commandType.case !== "streamingQueryCommand") return;
    const c = cmd.commandType.value.command;
    assert.equal(c.case, "awaitTermination");
    if (c.case !== "awaitTermination") return;
    assert.equal(c.value.timeoutMs, 5000n);
  });

  it("maps op=awaitTermination without timeoutMs (unset bigint)", () => {
    const cmd = buildCommandProto({
      type: "streamingQueryCommand",
      queryId,
      op: "awaitTermination",
    });
    assert.equal(cmd.commandType.case, "streamingQueryCommand");
    if (cmd.commandType.case !== "streamingQueryCommand") return;
    const c = cmd.commandType.value.command;
    assert.equal(c.case, "awaitTermination");
    if (c.case !== "awaitTermination") return;
    assert.equal(c.value.timeoutMs, undefined);
  });

  it("rejects a non-integer awaitTermination timeoutMs (defense-in-depth)", () => {
    assert.throws(
      () =>
        buildCommandProto({
          type: "streamingQueryCommand",
          queryId,
          op: "awaitTermination",
          timeoutMs: 1000.5,
        }),
      UnsupportedOperationError,
    );
  });

  it("throws UnsupportedOperationError on an unknown op", () => {
    assert.throws(
      () => buildCommandProto({ type: "streamingQueryCommand", queryId, op: "bogus" }),
      UnsupportedOperationError,
    );
  });
});

describe("decodeCommandResponse", () => {
  const queryId = create(StreamingQueryInstanceIdSchema, { id: "id-a", runId: "run-b" });

  it("returns undefined for non-streaming responses (arrow batch)", () => {
    const resp = create(ExecutePlanResponseSchema, {
      responseType: { case: "arrowBatch", value: { rowCount: 0n, data: new Uint8Array() } },
    });
    assert.equal(decodeCommandResponse(resp), undefined);
  });

  it("decodes writeStreamOperationStartResult", () => {
    const resp = create(ExecutePlanResponseSchema, {
      responseType: {
        case: "writeStreamOperationStartResult",
        value: create(WriteStreamOperationStartResultSchema, {
          queryId,
          name: "q1",
          queryStartedEventJson: '{"id":"x"}',
        }),
      },
    });
    assert.deepEqual(decodeCommandResponse(resp), {
      type: "writeStreamOperationStartResult",
      queryId: { id: "id-a", runId: "run-b" },
      name: "q1",
      queryStartedEventJson: '{"id":"x"}',
    });
  });

  it("decodes the status result", () => {
    const resp = create(ExecutePlanResponseSchema, {
      responseType: {
        case: "streamingQueryCommandResult",
        value: create(StreamingQueryCommandResultSchema, {
          queryId,
          resultType: {
            case: "status",
            value: create(StreamingQueryCommandResult_StatusResultSchema, {
              statusMessage: "running",
              isDataAvailable: true,
              isTriggerActive: false,
              isActive: true,
            }),
          },
        }),
      },
    });
    assert.deepEqual(decodeCommandResponse(resp), {
      type: "streamingQueryCommandResult",
      queryId: { id: "id-a", runId: "run-b" },
      resultType: "status",
      status: {
        statusMessage: "running",
        isDataAvailable: true,
        isTriggerActive: false,
        isActive: true,
      },
    });
  });

  it("decodes the recentProgress result", () => {
    const resp = create(ExecutePlanResponseSchema, {
      responseType: {
        case: "streamingQueryCommandResult",
        value: create(StreamingQueryCommandResultSchema, {
          queryId,
          resultType: {
            case: "recentProgress",
            value: create(StreamingQueryCommandResult_RecentProgressResultSchema, {
              recentProgressJson: ['{"batchId":0}', '{"batchId":1}'],
            }),
          },
        }),
      },
    });
    assert.deepEqual(decodeCommandResponse(resp), {
      type: "streamingQueryCommandResult",
      queryId: { id: "id-a", runId: "run-b" },
      resultType: "recentProgress",
      recentProgressJson: ['{"batchId":0}', '{"batchId":1}'],
    });
  });

  it("decodes the explain result", () => {
    const resp = create(ExecutePlanResponseSchema, {
      responseType: {
        case: "streamingQueryCommandResult",
        value: create(StreamingQueryCommandResultSchema, {
          queryId,
          resultType: {
            case: "explain",
            value: create(StreamingQueryCommandResult_ExplainResultSchema, {
              result: "== Physical Plan ==",
            }),
          },
        }),
      },
    });
    assert.deepEqual(decodeCommandResponse(resp), {
      type: "streamingQueryCommandResult",
      queryId: { id: "id-a", runId: "run-b" },
      resultType: "explain",
      explain: "== Physical Plan ==",
    });
  });

  it("decodes the exception result (only set fields)", () => {
    const resp = create(ExecutePlanResponseSchema, {
      responseType: {
        case: "streamingQueryCommandResult",
        value: create(StreamingQueryCommandResultSchema, {
          queryId,
          resultType: {
            case: "exception",
            value: create(StreamingQueryCommandResult_ExceptionResultSchema, {
              exceptionMessage: "boom",
              errorClass: "STREAM_FAILED",
            }),
          },
        }),
      },
    });
    assert.deepEqual(decodeCommandResponse(resp), {
      type: "streamingQueryCommandResult",
      queryId: { id: "id-a", runId: "run-b" },
      resultType: "exception",
      exception: { exceptionMessage: "boom", errorClass: "STREAM_FAILED" },
    });
  });

  it("decodes the awaitTermination result", () => {
    const resp = create(ExecutePlanResponseSchema, {
      responseType: {
        case: "streamingQueryCommandResult",
        value: create(StreamingQueryCommandResultSchema, {
          queryId,
          resultType: {
            case: "awaitTermination",
            value: create(StreamingQueryCommandResult_AwaitTerminationResultSchema, {
              terminated: true,
            }),
          },
        }),
      },
    });
    assert.deepEqual(decodeCommandResponse(resp), {
      type: "streamingQueryCommandResult",
      queryId: { id: "id-a", runId: "run-b" },
      resultType: "awaitTermination",
      terminated: true,
    });
  });

  it("decodes the missing-resultType ack (stop / processAllAvailable)", () => {
    const resp = create(ExecutePlanResponseSchema, {
      responseType: {
        case: "streamingQueryCommandResult",
        value: create(StreamingQueryCommandResultSchema, { queryId }),
      },
    });
    assert.deepEqual(decodeCommandResponse(resp), {
      type: "streamingQueryCommandResult",
      queryId: { id: "id-a", runId: "run-b" },
    });
  });
});

describe("buildCommandProto: streamingQueryManagerCommand", () => {
  for (const op of ["active", "resetTerminated", "listListeners"] as const) {
    it(`maps op=${op} to a boolean command case`, () => {
      const cmd = buildCommandProto({ type: "streamingQueryManagerCommand", op });
      assert.equal(cmd.commandType.case, "streamingQueryManagerCommand");
      if (cmd.commandType.case !== "streamingQueryManagerCommand") return;
      const c = cmd.commandType.value.command;
      assert.equal(c.case, op);
      assert.equal(c.value, true);
    });
  }

  it("maps op=getQuery to the getQuery string case", () => {
    const cmd = buildCommandProto({
      type: "streamingQueryManagerCommand",
      op: "getQuery",
      id: "id-a",
    });
    assert.equal(cmd.commandType.case, "streamingQueryManagerCommand");
    if (cmd.commandType.case !== "streamingQueryManagerCommand") return;
    const c = cmd.commandType.value.command;
    assert.equal(c.case, "getQuery");
    assert.equal(c.value, "id-a");
  });

  it("maps op=awaitAnyTermination with timeoutMs as a bigint", () => {
    const cmd = buildCommandProto({
      type: "streamingQueryManagerCommand",
      op: "awaitAnyTermination",
      timeoutMs: 5000,
    });
    assert.equal(cmd.commandType.case, "streamingQueryManagerCommand");
    if (cmd.commandType.case !== "streamingQueryManagerCommand") return;
    const c = cmd.commandType.value.command;
    assert.equal(c.case, "awaitAnyTermination");
    if (c.case !== "awaitAnyTermination") return;
    assert.equal(c.value.timeoutMs, 5000n);
  });

  it("maps op=awaitAnyTermination without timeoutMs (unset bigint)", () => {
    const cmd = buildCommandProto({
      type: "streamingQueryManagerCommand",
      op: "awaitAnyTermination",
    });
    assert.equal(cmd.commandType.case, "streamingQueryManagerCommand");
    if (cmd.commandType.case !== "streamingQueryManagerCommand") return;
    const c = cmd.commandType.value.command;
    assert.equal(c.case, "awaitAnyTermination");
    if (c.case !== "awaitAnyTermination") return;
    assert.equal(c.value.timeoutMs, undefined);
  });

  it("rejects a non-integer awaitAnyTermination timeoutMs (defense-in-depth)", () => {
    assert.throws(
      () =>
        buildCommandProto({
          type: "streamingQueryManagerCommand",
          op: "awaitAnyTermination",
          timeoutMs: 1000.5,
        }),
      UnsupportedOperationError,
    );
  });

  it("throws UnsupportedOperationError on an unknown manager op", () => {
    assert.throws(
      () => buildCommandProto({ type: "streamingQueryManagerCommand", op: "bogus" }),
      UnsupportedOperationError,
    );
  });
});

describe("decodeCommandResponse: streamingQueryManagerCommandResult", () => {
  const instance = create(StreamingQueryManagerCommandResult_StreamingQueryInstanceSchema, {
    id: create(StreamingQueryInstanceIdSchema, { id: "id-a", runId: "run-b" }),
    name: "q1",
  });

  it("decodes the active result", () => {
    const resp = create(ExecutePlanResponseSchema, {
      responseType: {
        case: "streamingQueryManagerCommandResult",
        value: create(StreamingQueryManagerCommandResultSchema, {
          resultType: {
            case: "active",
            value: create(StreamingQueryManagerCommandResult_ActiveResultSchema, {
              activeQueries: [instance],
            }),
          },
        }),
      },
    });
    assert.deepEqual(decodeCommandResponse(resp), {
      type: "streamingQueryManagerCommandResult",
      resultType: "active",
      activeQueries: [{ id: "id-a", runId: "run-b", name: "q1" }],
    });
  });

  it("decodes the query (get) result", () => {
    const resp = create(ExecutePlanResponseSchema, {
      responseType: {
        case: "streamingQueryManagerCommandResult",
        value: create(StreamingQueryManagerCommandResultSchema, {
          resultType: { case: "query", value: instance },
        }),
      },
    });
    assert.deepEqual(decodeCommandResponse(resp), {
      type: "streamingQueryManagerCommandResult",
      resultType: "query",
      query: { id: "id-a", runId: "run-b", name: "q1" },
    });
  });

  it("decodes a missing query (get-by-id miss) as resultType=query with undefined query", () => {
    const resp = create(ExecutePlanResponseSchema, {
      responseType: {
        case: "streamingQueryManagerCommandResult",
        value: create(StreamingQueryManagerCommandResultSchema, {
          resultType: {
            case: "query",
            value: create(StreamingQueryManagerCommandResult_StreamingQueryInstanceSchema, {}),
          },
        }),
      },
    });
    assert.deepEqual(decodeCommandResponse(resp), {
      type: "streamingQueryManagerCommandResult",
      resultType: "query",
      query: undefined,
    });
  });

  it("decodes the awaitAnyTermination result", () => {
    const resp = create(ExecutePlanResponseSchema, {
      responseType: {
        case: "streamingQueryManagerCommandResult",
        value: create(StreamingQueryManagerCommandResultSchema, {
          resultType: {
            case: "awaitAnyTermination",
            value: create(StreamingQueryManagerCommandResult_AwaitAnyTerminationResultSchema, {
              terminated: true,
            }),
          },
        }),
      },
    });
    assert.deepEqual(decodeCommandResponse(resp), {
      type: "streamingQueryManagerCommandResult",
      resultType: "awaitAnyTermination",
      terminated: true,
    });
  });

  it("decodes the resetTerminated ack", () => {
    const resp = create(ExecutePlanResponseSchema, {
      responseType: {
        case: "streamingQueryManagerCommandResult",
        value: create(StreamingQueryManagerCommandResultSchema, {
          resultType: { case: "resetTerminated", value: true },
        }),
      },
    });
    assert.deepEqual(decodeCommandResponse(resp), {
      type: "streamingQueryManagerCommandResult",
      resultType: "resetTerminated",
    });
  });
});

describe("buildCommandProto: streamingQueryListenerBusCommand", () => {
  it("maps op=addListenerBusListener", () => {
    const cmd = buildCommandProto({
      type: "streamingQueryListenerBusCommand",
      op: "addListenerBusListener",
    });
    assert.equal(cmd.commandType.case, "streamingQueryListenerBusCommand");
    if (cmd.commandType.case !== "streamingQueryListenerBusCommand") return;
    const c = cmd.commandType.value.command;
    assert.equal(c.case, "addListenerBusListener");
    assert.equal(c.value, true);
  });

  it("maps op=removeListenerBusListener", () => {
    const cmd = buildCommandProto({
      type: "streamingQueryListenerBusCommand",
      op: "removeListenerBusListener",
    });
    assert.equal(cmd.commandType.case, "streamingQueryListenerBusCommand");
    if (cmd.commandType.case !== "streamingQueryListenerBusCommand") return;
    const c = cmd.commandType.value.command;
    assert.equal(c.case, "removeListenerBusListener");
    assert.equal(c.value, true);
  });

  it("throws UnsupportedOperationError on an unknown bus op", () => {
    assert.throws(
      () => buildCommandProto({ type: "streamingQueryListenerBusCommand", op: "bogus" }),
      UnsupportedOperationError,
    );
  });
});

describe("decodeCommandResponse: streamingQueryListenerEventsResult", () => {
  it("decodes the registration ack with no events", () => {
    const resp = create(ExecutePlanResponseSchema, {
      responseType: {
        case: "streamingQueryListenerEventsResult",
        value: create(StreamingQueryListenerEventsResultSchema, {
          events: [],
          listenerBusListenerAdded: true,
        }),
      },
    });
    assert.deepEqual(decodeCommandResponse(resp), {
      type: "streamingQueryListenerEventsResult",
      events: [],
      listenerBusListenerAdded: true,
    });
  });

  it("decodes a batch of events with event-type names", () => {
    const resp = create(ExecutePlanResponseSchema, {
      responseType: {
        case: "streamingQueryListenerEventsResult",
        value: create(StreamingQueryListenerEventsResultSchema, {
          events: [
            create(StreamingQueryListenerEventSchema, {
              eventType: StreamingQueryEventType.QUERY_PROGRESS_EVENT,
              eventJson: '{"batchId":0}',
            }),
            create(StreamingQueryListenerEventSchema, {
              eventType: StreamingQueryEventType.QUERY_IDLE_EVENT,
              eventJson: '{"id":"id-a"}',
            }),
            create(StreamingQueryListenerEventSchema, {
              eventType: StreamingQueryEventType.QUERY_TERMINATED_EVENT,
              eventJson: '{"id":"id-a"}',
            }),
          ],
        }),
      },
    });
    assert.deepEqual(decodeCommandResponse(resp), {
      type: "streamingQueryListenerEventsResult",
      events: [
        { eventType: "progress", eventJson: '{"batchId":0}' },
        { eventType: "idle", eventJson: '{"id":"id-a"}' },
        { eventType: "terminated", eventJson: '{"id":"id-a"}' },
      ],
    });
  });
});
