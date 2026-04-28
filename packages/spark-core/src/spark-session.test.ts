import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { SparkSession } from "./spark-session.js";

function makeSession(analyzeResponse: Record<string, unknown>): {
  spark: SparkSession;
  calls: Record<string, unknown>[];
} {
  const calls: Record<string, unknown>[] = [];
  const transport = {
    executePlan: () => {
      throw new Error("not used in this test");
    },
    analyzePlan: async (_sessionId: string, request: Record<string, unknown>) => {
      calls.push(request);
      return analyzeResponse;
    },
  };
  const spark = SparkSession.builder().remote("sc://stub").transport(transport).getOrCreate();
  return { spark, calls };
}

describe("SparkSession.version", () => {
  it("issues an AnalyzePlan(sparkVersion) and returns the response version", async () => {
    const { spark, calls } = makeSession({ type: "sparkVersion", version: "4.0.0" });
    assert.equal(await spark.version(), "4.0.0");
    assert.deepStrictEqual(calls, [{ type: "sparkVersion" }]);
  });
});
