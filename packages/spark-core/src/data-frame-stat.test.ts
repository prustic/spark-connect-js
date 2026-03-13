import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { SparkSession } from "./spark-session.js";
import type { Transport } from "./spark-session.js";
import type { LogicalPlan } from "./plan/logical-plan.js";

function mockTransport(): Transport & { calls: LogicalPlan[] } {
  const calls: LogicalPlan[] = [];
  return {
    calls,
    async *executePlan(_sessionId: string, plan: LogicalPlan): AsyncIterable<Uint8Array> {
      calls.push(plan);
    },
  };
}

function createSession() {
  const transport = mockTransport();
  const spark = SparkSession.builder()
    .remote("sc://localhost:15002")
    .transport(transport)
    .getOrCreate();
  return { spark, transport };
}

describe("DataFrameStat", () => {
  it("corr() builds a statCorr plan", () => {
    const { spark } = createSession();
    const df = spark.sql("SELECT * FROM t").stat.corr("a", "b");
    assert.equal(df._plan.type, "statCorr");
    if (df._plan.type === "statCorr") {
      assert.equal(df._plan.col1, "a");
      assert.equal(df._plan.col2, "b");
      assert.equal(df._plan.method, "pearson");
    }
  });

  it("corr() accepts a custom method", () => {
    const { spark } = createSession();
    const df = spark.sql("SELECT * FROM t").stat.corr("a", "b", "spearman");
    if (df._plan.type === "statCorr") {
      assert.equal(df._plan.method, "spearman");
    }
  });

  it("cov() builds a statCov plan", () => {
    const { spark } = createSession();
    const df = spark.sql("SELECT * FROM t").stat.cov("a", "b");
    assert.equal(df._plan.type, "statCov");
    if (df._plan.type === "statCov") {
      assert.equal(df._plan.col1, "a");
      assert.equal(df._plan.col2, "b");
    }
  });

  it("crosstab() builds a statCrosstab plan", () => {
    const { spark } = createSession();
    const df = spark.sql("SELECT * FROM t").stat.crosstab("a", "b");
    assert.equal(df._plan.type, "statCrosstab");
    if (df._plan.type === "statCrosstab") {
      assert.equal(df._plan.col1, "a");
      assert.equal(df._plan.col2, "b");
    }
  });

  it("freqItems() builds a statFreqItems plan", () => {
    const { spark } = createSession();
    const df = spark.sql("SELECT * FROM t").stat.freqItems(["a", "b"], 0.5);
    assert.equal(df._plan.type, "statFreqItems");
    if (df._plan.type === "statFreqItems") {
      assert.deepStrictEqual(df._plan.cols, ["a", "b"]);
      assert.equal(df._plan.support, 0.5);
    }
  });

  it("freqItems() defaults support to undefined", () => {
    const { spark } = createSession();
    const df = spark.sql("SELECT * FROM t").stat.freqItems(["x"]);
    if (df._plan.type === "statFreqItems") {
      assert.equal(df._plan.support, undefined);
    }
  });

  it("approxQuantile() builds a statApproxQuantile plan", () => {
    const { spark } = createSession();
    const df = spark.sql("SELECT * FROM t").stat.approxQuantile(["a"], [0.25, 0.75], 0.01);
    assert.equal(df._plan.type, "statApproxQuantile");
    if (df._plan.type === "statApproxQuantile") {
      assert.deepStrictEqual(df._plan.cols, ["a"]);
      assert.deepStrictEqual(df._plan.probabilities, [0.25, 0.75]);
      assert.equal(df._plan.relativeError, 0.01);
    }
  });

  it("stat accessor returns the same class each call", () => {
    const { spark } = createSession();
    const df = spark.sql("SELECT * FROM t");
    const stat1 = df.stat;
    const stat2 = df.stat;
    assert.equal(stat1.constructor.name, "DataFrameStat");
    assert.equal(stat2.constructor.name, "DataFrameStat");
  });

  it("child plan is preserved through stat methods", () => {
    const { spark } = createSession();
    const df = spark.sql("SELECT * FROM t").stat.cov("x", "y");
    if (df._plan.type === "statCov") {
      assert.equal(df._plan.child.type, "sql");
    }
  });
});
