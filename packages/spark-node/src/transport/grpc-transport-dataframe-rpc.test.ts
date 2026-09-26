import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { create } from "@bufbuild/protobuf";
import {
  ExecutePlanResponseSchema,
  UserContextSchema,
  type AnalyzePlanResponse,
  AnalyzePlanResponseSchema,
} from "@spark-connect-js/connect";
import type { LogicalPlan } from "@spark-connect-js/core";
import { MEMORY_ONLY } from "@spark-connect-js/core";
import {
  buildAnalyzePlanRequest,
  buildCommandProto,
  decodeCommandResponse,
  extractAnalyzeResult,
} from "./grpc-transport.js";

const PLAN: LogicalPlan = { type: "sql", query: "SELECT 1" };
const USER = create(UserContextSchema, { userId: "u" });

describe("buildAnalyzePlanRequest: isLocal, isStreaming, inputFiles", () => {
  for (const type of ["isLocal", "isStreaming", "inputFiles"] as const) {
    it(`wraps the relation as a root plan for ${type}`, () => {
      const req = buildAnalyzePlanRequest("s", { type, plan: PLAN }, USER, "js", undefined);
      assert.equal(req.analyze.case, type);
      const value = req.analyze.value as { plan?: { opType: { case: string } } };
      assert.equal(value.plan?.opType.case, "root");
    });
  }
});

describe("extractAnalyzeResult: isLocal, isStreaming, inputFiles", () => {
  function response(result: AnalyzePlanResponse["result"]): AnalyzePlanResponse {
    return create(AnalyzePlanResponseSchema, { result });
  }

  it("returns the flag or the file list as the result", () => {
    const local = extractAnalyzeResult(
      response({
        case: "isLocal",
        value: { $typeName: "spark.connect.AnalyzePlanResponse.IsLocal", isLocal: true },
      }),
    );
    assert.deepStrictEqual(local, { type: "isLocal", result: true });

    const files = extractAnalyzeResult(
      response({
        case: "inputFiles",
        value: { $typeName: "spark.connect.AnalyzePlanResponse.InputFiles", files: ["/a", "/b"] },
      }),
    );
    assert.deepStrictEqual(files, { type: "inputFiles", result: ["/a", "/b"] });
  });
});

describe("buildCommandProto: checkpoint", () => {
  it("sends local, eager, and the storage level", () => {
    const cmd = buildCommandProto({
      type: "checkpoint",
      plan: PLAN,
      local: true,
      eager: false,
      storageLevel: MEMORY_ONLY,
    });
    if (cmd.commandType.case !== "checkpointCommand") {
      assert.fail("expected checkpointCommand");
    }
    const v = cmd.commandType.value;
    assert.equal(v.local, true);
    assert.equal(v.eager, false);
    assert.equal(v.storageLevel?.useMemory, true);
    assert.equal(v.relation?.relType.case, "sql");
  });

  it("leaves the storage level unset for a reliable checkpoint", () => {
    const cmd = buildCommandProto({ type: "checkpoint", plan: PLAN, local: false, eager: true });
    if (cmd.commandType.case !== "checkpointCommand") {
      assert.fail("expected checkpointCommand");
    }
    assert.equal(cmd.commandType.value.storageLevel, undefined);
  });
});

describe("decodeCommandResponse: checkpointCommandResult", () => {
  it("surfaces the relation id instead of dropping the result", () => {
    const decoded = decodeCommandResponse(
      create(ExecutePlanResponseSchema, {
        responseType: {
          case: "checkpointCommandResult",
          value: {
            $typeName: "spark.connect.CheckpointCommandResult",
            relation: { $typeName: "spark.connect.CachedRemoteRelation", relationId: "rel-9" },
          },
        },
      }),
    );
    assert.deepStrictEqual(decoded, { type: "checkpointCommandResult", relationId: "rel-9" });
  });
});
