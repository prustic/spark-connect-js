import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { SparkSession } from "./spark-session.js";
import type { Transport } from "./spark-session.js";
import type { LogicalPlan } from "./plan/logical-plan.js";
import { col, lit } from "./column.js";

/** Minimal mock transport that records calls without doing I/O. */
function mockTransport(_rows?: Record<string, unknown>[]): Transport & { calls: LogicalPlan[] } {
  const calls: LogicalPlan[] = [];
  return {
    calls,
    async *executePlan(_sessionId: string, plan: LogicalPlan): AsyncIterable<Uint8Array> {
      calls.push(plan);
      // Yield nothing — no Arrow data
    },
  };
}

function createSession(transport?: ReturnType<typeof mockTransport>): {
  spark: SparkSession;
  transport: ReturnType<typeof mockTransport>;
} {
  const t = transport ?? mockTransport();
  const spark = SparkSession.builder().remote("sc://localhost:15002").transport(t).getOrCreate();
  return { spark, transport: t };
}

describe("SparkSession.builder()", () => {
  it("creates a session with a session ID", () => {
    const { spark } = createSession();
    assert.ok(spark.sessionId);
    assert.equal(typeof spark.sessionId, "string");
  });

  it("throws without remote", () => {
    const t = mockTransport();
    assert.throws(() => {
      SparkSession.builder().transport(t).getOrCreate();
    }, /remote URL/);
  });

  it("throws without transport", () => {
    assert.throws(() => {
      SparkSession.builder().remote("sc://localhost:15002").getOrCreate();
    }, /Transport/);
  });
});

describe("SparkSession.sql()", () => {
  it("returns a DataFrame with a SQL plan", () => {
    const { spark } = createSession();
    const df = spark.sql("SELECT * FROM users");
    assert.deepStrictEqual(df._plan, { type: "sql", query: "SELECT * FROM users" });
  });
});

describe("SparkSession.read", () => {
  it("creates a read plan with format and path", () => {
    const { spark } = createSession();
    const df = spark.read.format("parquet").load("/data/users.parquet");
    assert.equal(df._plan.type, "read");
    if (df._plan.type === "read") {
      assert.equal(df._plan.format, "parquet");
      assert.equal(df._plan.path, "/data/users.parquet");
    }
  });

  it("defaults to parquet format", () => {
    const { spark } = createSession();
    const df = spark.read.load("/data/file");
    if (df._plan.type === "read") {
      assert.equal(df._plan.format, "parquet");
    }
  });

  it("supports options", () => {
    const { spark } = createSession();
    const df = spark.read.format("csv").option("header", "true").load("/data/file.csv");
    if (df._plan.type === "read") {
      assert.deepStrictEqual(df._plan.options, { header: "true" });
    }
  });
});

describe("DataFrame transformations (lazy)", () => {
  it("filter() wraps plan in a filter node", () => {
    const { spark } = createSession();
    const df = spark.sql("SELECT * FROM t").filter(col("x").gt(lit(10)));
    assert.equal(df._plan.type, "filter");
    if (df._plan.type === "filter") {
      assert.equal(df._plan.child.type, "sql");
      assert.equal(df._plan.condition.type, "gt");
    }
  });

  it("where() is an alias for filter()", () => {
    const { spark } = createSession();
    const df = spark.sql("SELECT * FROM t").where(col("x").gt(lit(10)));
    assert.equal(df._plan.type, "filter");
  });

  it("select() wraps plan in a project node", () => {
    const { spark } = createSession();
    const df = spark.sql("SELECT * FROM t").select("a", "b");
    assert.equal(df._plan.type, "project");
    if (df._plan.type === "project") {
      assert.equal(df._plan.expressions.length, 2);
      assert.deepStrictEqual(df._plan.expressions[0], {
        type: "unresolvedAttribute",
        name: "a",
      });
    }
  });

  it("select() accepts Column objects", () => {
    const { spark } = createSession();
    const df = spark.sql("SELECT * FROM t").select(col("a"), col("b").alias("renamed"));
    if (df._plan.type === "project") {
      assert.equal(df._plan.expressions[1].type, "alias");
    }
  });

  it("limit() wraps plan in a limit node", () => {
    const { spark } = createSession();
    const df = spark.sql("SELECT * FROM t").limit(5);
    assert.equal(df._plan.type, "limit");
    if (df._plan.type === "limit") {
      assert.equal(df._plan.limit, 5);
    }
  });

  it("chaining builds nested plan tree", () => {
    const { spark } = createSession();
    const df = spark
      .sql("SELECT * FROM t")
      .filter(col("x").gt(lit(0)))
      .select("x")
      .limit(10);

    // Outermost: limit
    assert.equal(df._plan.type, "limit");
    if (df._plan.type !== "limit") return;

    // Next: project
    assert.equal(df._plan.child.type, "project");
    if (df._plan.child.type !== "project") return;

    // Next: filter
    assert.equal(df._plan.child.child.type, "filter");
    if (df._plan.child.child.type !== "filter") return;

    // Innermost: sql
    assert.equal(df._plan.child.child.child.type, "sql");
  });
});

describe("DataFrame.collect()", () => {
  it("throws without arrow decoder", async () => {
    const { spark } = createSession();
    const df = spark.sql("SELECT 1");
    await assert.rejects(df.collect(), /Arrow decoder/);
  });

  it("calls transport with the plan", async () => {
    const t = mockTransport();
    const spark = SparkSession.builder()
      .remote("sc://localhost:15002")
      .transport(t)
      .arrowDecoder(async () => [{ id: 1 }])
      .getOrCreate();

    const df = spark.sql("SELECT 1 as id");
    const rows = await df.collect();

    assert.equal(t.calls.length, 1);
    assert.deepStrictEqual(t.calls[0], { type: "sql", query: "SELECT 1 as id" });
    assert.deepStrictEqual(rows, [{ id: 1 }]);
  });
});

describe("GroupedData", () => {
  it("agg() builds aggregate plan", () => {
    const { spark } = createSession();
    const df = spark.sql("SELECT * FROM t").groupBy("dept").agg(col("salary").alias("total"));

    assert.equal(df._plan.type, "aggregate");
    if (df._plan.type === "aggregate") {
      assert.equal(df._plan.groupingExpressions.length, 1);
      assert.equal(df._plan.aggregateExpressions.length, 1);
    }
  });

  it("count() builds aggregate with count function", () => {
    const { spark } = createSession();
    const df = spark.sql("SELECT * FROM t").groupBy("dept").count();
    assert.equal(df._plan.type, "aggregate");
  });

  it("sum() builds aggregate with sum function", () => {
    const { spark } = createSession();
    const df = spark.sql("SELECT * FROM t").groupBy("dept").sum("salary");
    assert.equal(df._plan.type, "aggregate");
    if (df._plan.type === "aggregate") {
      assert.equal(df._plan.aggregateExpressions.length, 1);
    }
  });

  it("avg() builds aggregate with avg function", () => {
    const { spark } = createSession();
    const df = spark.sql("SELECT * FROM t").groupBy("dept").avg("salary", "bonus");
    assert.equal(df._plan.type, "aggregate");
    if (df._plan.type === "aggregate") {
      assert.equal(df._plan.aggregateExpressions.length, 2);
    }
  });
});
