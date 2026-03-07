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

describe("DataFrame set operations", () => {
  it("union() builds setOperation plan with union type", () => {
    const { spark } = createSession();
    const df1 = spark.sql("SELECT * FROM a");
    const df2 = spark.sql("SELECT * FROM b");
    const result = df1.union(df2);
    assert.equal(result._plan.type, "setOperation");
    if (result._plan.type === "setOperation") {
      assert.equal(result._plan.opType, "union");
      assert.equal(result._plan.isAll, true);
      assert.equal(result._plan.byName, false);
    }
  });

  it("unionByName() builds setOperation with byName=true", () => {
    const { spark } = createSession();
    const result = spark.sql("SELECT * FROM a").unionByName(spark.sql("SELECT * FROM b"), true);
    if (result._plan.type === "setOperation") {
      assert.equal(result._plan.byName, true);
      assert.equal(result._plan.allowMissingColumns, true);
    }
  });

  it("intersect() builds setOperation plan", () => {
    const { spark } = createSession();
    const result = spark.sql("SELECT * FROM a").intersect(spark.sql("SELECT * FROM b"));
    if (result._plan.type === "setOperation") {
      assert.equal(result._plan.opType, "intersect");
      assert.equal(result._plan.isAll, false);
    }
  });

  it("intersectAll() builds setOperation with isAll=true", () => {
    const { spark } = createSession();
    const result = spark.sql("SELECT * FROM a").intersectAll(spark.sql("SELECT * FROM b"));
    if (result._plan.type === "setOperation") {
      assert.equal(result._plan.opType, "intersect");
      assert.equal(result._plan.isAll, true);
    }
  });

  it("except() builds setOperation plan", () => {
    const { spark } = createSession();
    const result = spark.sql("SELECT * FROM a").except(spark.sql("SELECT * FROM b"));
    if (result._plan.type === "setOperation") {
      assert.equal(result._plan.opType, "except");
      assert.equal(result._plan.isAll, false);
    }
  });

  it("exceptAll() builds setOperation with isAll=true", () => {
    const { spark } = createSession();
    const result = spark.sql("SELECT * FROM a").exceptAll(spark.sql("SELECT * FROM b"));
    if (result._plan.type === "setOperation") {
      assert.equal(result._plan.opType, "except");
      assert.equal(result._plan.isAll, true);
    }
  });
});

describe("DataFrame.sample()", () => {
  it("builds sample plan", () => {
    const { spark } = createSession();
    const result = spark.sql("SELECT * FROM t").sample(0.5, false, 42);
    assert.equal(result._plan.type, "sample");
    if (result._plan.type === "sample") {
      assert.equal(result._plan.upperBound, 0.5);
      assert.equal(result._plan.withReplacement, false);
      assert.equal(result._plan.seed, 42);
    }
  });
});

describe("DataFrame.fillna()", () => {
  it("builds fillNa plan", () => {
    const { spark } = createSession();
    const result = spark.sql("SELECT * FROM t").fillna(0, ["age", "salary"]);
    assert.equal(result._plan.type, "fillNa");
    if (result._plan.type === "fillNa") {
      assert.deepStrictEqual(result._plan.cols, ["age", "salary"]);
      assert.deepStrictEqual(result._plan.values, [0]);
    }
  });
});

describe("DataFrame.dropna()", () => {
  it("builds dropNa plan with how=any", () => {
    const { spark } = createSession();
    const result = spark.sql("SELECT * FROM t").dropna("any");
    assert.equal(result._plan.type, "dropNa");
    if (result._plan.type === "dropNa") {
      assert.equal(result._plan.minNonNulls, undefined);
    }
  });

  it("builds dropNa plan with how=all", () => {
    const { spark } = createSession();
    const result = spark.sql("SELECT * FROM t").dropna("all");
    if (result._plan.type === "dropNa") {
      assert.equal(result._plan.minNonNulls, 1);
    }
  });
});

describe("DataFrame.toDF()", () => {
  it("builds toDF plan", () => {
    const { spark } = createSession();
    const result = spark.sql("SELECT * FROM t").toDF("a", "b", "c");
    assert.equal(result._plan.type, "toDF");
    if (result._plan.type === "toDF") {
      assert.deepStrictEqual(result._plan.columnNames, ["a", "b", "c"]);
    }
  });
});

describe("DataFrame.describe()", () => {
  it("builds describe plan", () => {
    const { spark } = createSession();
    const result = spark.sql("SELECT * FROM t").describe("age", "salary");
    assert.equal(result._plan.type, "describe");
    if (result._plan.type === "describe") {
      assert.deepStrictEqual(result._plan.cols, ["age", "salary"]);
    }
  });
});

describe("DataFrame.count()", () => {
  it("builds an aggregate count plan instead of collecting all data", async () => {
    const t = mockTransport();
    const spark = SparkSession.builder()
      .remote("sc://localhost:15002")
      .transport(t)
      .arrowDecoder(async () => [{ count: 42 }])
      .getOrCreate();

    const result = await spark.sql("SELECT * FROM t").count();

    assert.equal(result, 42);
    // Verify the plan sent to transport is an aggregate, not the original SQL
    assert.equal(t.calls.length, 1);
    assert.equal(t.calls[0].type, "aggregate");
    if (t.calls[0].type === "aggregate") {
      assert.equal(t.calls[0].groupingExpressions.length, 0);
      assert.equal(t.calls[0].aggregateExpressions.length, 1);
      const agg = t.calls[0].aggregateExpressions[0];
      assert.equal(agg.type, "alias");
      if (agg.type === "alias") {
        assert.equal(agg.name, "count");
        assert.equal(agg.inner.type, "unresolvedFunction");
      }
    }
  });

  it("returns 0 for empty result", async () => {
    const t = mockTransport();
    const spark = SparkSession.builder()
      .remote("sc://localhost:15002")
      .transport(t)
      .arrowDecoder(async () => [])
      .getOrCreate();

    const result = await spark.sql("SELECT * FROM t").count();
    assert.equal(result, 0);
  });
});

describe("DataFrame.toLocalIterator()", () => {
  it("yields rows one at a time from streaming chunks", async () => {
    let callCount = 0;
    const transport: Transport = {
      async *executePlan() {
        // Simulate two Arrow chunks arriving from the server
        callCount++;
        yield new Uint8Array([1]); // chunk 1
        yield new Uint8Array([2]); // chunk 2
      },
    };
    const spark = SparkSession.builder()
      .remote("sc://localhost:15002")
      .transport(transport)
      .arrowDecoder(async (chunks) => {
        // Decode each chunk into mock rows
        const id = chunks[0][0];
        return [{ id }, { id: id + 10 }];
      })
      .getOrCreate();

    const rows: Record<string, unknown>[] = [];
    for await (const row of spark.sql("SELECT * FROM t").toLocalIterator()) {
      rows.push(row);
    }

    assert.equal(callCount, 1);
    assert.deepStrictEqual(rows, [{ id: 1 }, { id: 11 }, { id: 2 }, { id: 12 }]);
  });

  it("throws without arrow decoder", async () => {
    const { spark } = createSession();
    const iter = spark.sql("SELECT 1").toLocalIterator();
    await assert.rejects(iter.next(), /Arrow decoder/);
  });
});

describe("DataFrame.forEach()", () => {
  it("calls callback for each row", async () => {
    const transport: Transport = {
      async *executePlan() {
        yield new Uint8Array([1]);
      },
    };
    const spark = SparkSession.builder()
      .remote("sc://localhost:15002")
      .transport(transport)
      .arrowDecoder(async () => [{ id: 1 }, { id: 2 }, { id: 3 }])
      .getOrCreate();

    const collected: number[] = [];
    await spark.sql("SELECT * FROM t").forEach((row) => {
      collected.push(row.id as number);
    });

    assert.deepStrictEqual(collected, [1, 2, 3]);
  });
});

describe("DataFrame.first() / head() / take()", () => {
  it("first() returns the first row", async () => {
    const t = mockTransport();
    const spark = SparkSession.builder()
      .remote("sc://localhost:15002")
      .transport(t)
      .arrowDecoder(async () => [{ id: 1 }])
      .getOrCreate();

    const row = await spark.sql("SELECT * FROM t").first();
    assert.deepStrictEqual(row, { id: 1 });
    // Verify it built a limit(1) plan
    assert.equal(t.calls[0].type, "limit");
    if (t.calls[0].type === "limit") {
      assert.equal(t.calls[0].limit, 1);
    }
  });

  it("first() returns null for empty result", async () => {
    const t = mockTransport();
    const spark = SparkSession.builder()
      .remote("sc://localhost:15002")
      .transport(t)
      .arrowDecoder(async () => [])
      .getOrCreate();

    const row = await spark.sql("SELECT * FROM t").first();
    assert.equal(row, null);
  });

  it("head(n) returns first n rows", async () => {
    const t = mockTransport();
    const spark = SparkSession.builder()
      .remote("sc://localhost:15002")
      .transport(t)
      .arrowDecoder(async () => [{ id: 1 }, { id: 2 }, { id: 3 }])
      .getOrCreate();

    const rows = await spark.sql("SELECT * FROM t").head(3);
    assert.equal(rows.length, 3);
    assert.equal(t.calls[0].type, "limit");
    if (t.calls[0].type === "limit") {
      assert.equal(t.calls[0].limit, 3);
    }
  });

  it("take(n) is an alias for head(n)", async () => {
    const t = mockTransport();
    const spark = SparkSession.builder()
      .remote("sc://localhost:15002")
      .transport(t)
      .arrowDecoder(async () => [{ id: 1 }, { id: 2 }])
      .getOrCreate();

    const rows = await spark.sql("SELECT * FROM t").take(2);
    assert.equal(rows.length, 2);
  });
});

describe("SparkSession.createDataFrame()", () => {
  it("builds a localRelation plan with Arrow data", () => {
    const { spark } = createSession();
    const arrowData = new Uint8Array([1, 2, 3]);
    const df = spark.createDataFrame(arrowData, "id INT, name STRING");
    assert.equal(df._plan.type, "localRelation");
    if (df._plan.type === "localRelation") {
      assert.deepStrictEqual(df._plan.data, arrowData);
      assert.equal(df._plan.schema, "id INT, name STRING");
    }
  });

  it("works without an explicit schema", () => {
    const { spark } = createSession();
    const df = spark.createDataFrame(new Uint8Array([1]));
    if (df._plan.type === "localRelation") {
      assert.equal(df._plan.schema, undefined);
    }
  });
});

describe("DataFrameReader shortcuts", () => {
  it("table() builds a readTable plan", () => {
    const { spark } = createSession();
    const df = spark.read.table("my_db.my_table");
    assert.equal(df._plan.type, "readTable");
    if (df._plan.type === "readTable") {
      assert.equal(df._plan.tableName, "my_db.my_table");
    }
  });

  it("json() builds a read plan with json format", () => {
    const { spark } = createSession();
    const df = spark.read.json("/data/file.json");
    assert.equal(df._plan.type, "read");
    if (df._plan.type === "read") {
      assert.equal(df._plan.format, "json");
      assert.equal(df._plan.path, "/data/file.json");
    }
  });

  it("csv() builds a read plan with csv format", () => {
    const { spark } = createSession();
    const df = spark.read.csv("/data/file.csv");
    if (df._plan.type === "read") {
      assert.equal(df._plan.format, "csv");
    }
  });

  it("parquet() builds a read plan with parquet format", () => {
    const { spark } = createSession();
    const df = spark.read.parquet("/data/file.parquet");
    if (df._plan.type === "read") {
      assert.equal(df._plan.format, "parquet");
    }
  });

  it("orc() builds a read plan with orc format", () => {
    const { spark } = createSession();
    const df = spark.read.orc("/data/file.orc");
    if (df._plan.type === "read") {
      assert.equal(df._plan.format, "orc");
    }
  });

  it("table() preserves options", () => {
    const { spark } = createSession();
    const df = spark.read.option("mergeSchema", "true").table("my_table");
    if (df._plan.type === "readTable") {
      assert.deepStrictEqual(df._plan.options, { mergeSchema: "true" });
    }
  });
});

// ── M1: New features ────────────────────────────────────────────────────────

describe("SparkSession.range()", () => {
  it("range(end) defaults start=0, step=1", () => {
    const { spark } = createSession();
    const df = spark.range(10);
    assert.equal(df._plan.type, "range");
    if (df._plan.type === "range") {
      assert.equal(df._plan.start, 0);
      assert.equal(df._plan.end, 10);
      assert.equal(df._plan.step, 1);
      assert.equal(df._plan.numPartitions, undefined);
    }
  });

  it("range(start, end) with explicit start", () => {
    const { spark } = createSession();
    const df = spark.range(5, 20);
    if (df._plan.type === "range") {
      assert.equal(df._plan.start, 5);
      assert.equal(df._plan.end, 20);
      assert.equal(df._plan.step, 1);
    }
  });

  it("range(start, end, step) with step", () => {
    const { spark } = createSession();
    const df = spark.range(0, 100, 10);
    if (df._plan.type === "range") {
      assert.equal(df._plan.start, 0);
      assert.equal(df._plan.end, 100);
      assert.equal(df._plan.step, 10);
    }
  });

  it("range() with numPartitions", () => {
    const { spark } = createSession();
    const df = spark.range(0, 100, 1, 4);
    if (df._plan.type === "range") {
      assert.equal(df._plan.numPartitions, 4);
    }
  });
});

describe("DataFrame.withColumnRenamed()", () => {
  it("wraps plan in a withColumnsRenamed node", () => {
    const { spark } = createSession();
    const df = spark.sql("SELECT * FROM t").withColumnRenamed("old", "new");
    assert.equal(df._plan.type, "withColumnsRenamed");
    if (df._plan.type === "withColumnsRenamed") {
      assert.deepStrictEqual(df._plan.renames, [{ colName: "old", newColName: "new" }]);
    }
  });
});

describe("DataFrame.withColumnsRenamed()", () => {
  it("renames multiple columns", () => {
    const { spark } = createSession();
    const df = spark.sql("SELECT * FROM t").withColumnsRenamed({ a: "x", b: "y" });
    assert.equal(df._plan.type, "withColumnsRenamed");
    if (df._plan.type === "withColumnsRenamed") {
      assert.equal(df._plan.renames.length, 2);
      assert.equal(df._plan.renames[0].colName, "a");
      assert.equal(df._plan.renames[0].newColName, "x");
    }
  });
});

describe("DataFrame.alias()", () => {
  it("wraps plan in a subqueryAlias node", () => {
    const { spark } = createSession();
    const df = spark.sql("SELECT * FROM t").alias("t1");
    assert.equal(df._plan.type, "subqueryAlias");
    if (df._plan.type === "subqueryAlias") {
      assert.equal(df._plan.alias, "t1");
      assert.equal(df._plan.child.type, "sql");
    }
  });
});

describe("DataFrame.hint()", () => {
  it("wraps plan in a hint node", () => {
    const { spark } = createSession();
    const df = spark.sql("SELECT * FROM t").hint("broadcast");
    assert.equal(df._plan.type, "hint");
    if (df._plan.type === "hint") {
      assert.equal(df._plan.name, "broadcast");
      assert.equal(df._plan.parameters.length, 0);
    }
  });

  it("accepts parameters", () => {
    const { spark } = createSession();
    const df = spark.sql("SELECT * FROM t").hint("repartition", 10);
    if (df._plan.type === "hint") {
      assert.equal(df._plan.parameters.length, 1);
      assert.deepStrictEqual(df._plan.parameters[0], { type: "literal", value: 10 });
    }
  });
});

describe("DataFrame.selectExpr()", () => {
  it("wraps plan in a project node with SQL expressions", () => {
    const { spark } = createSession();
    const df = spark.sql("SELECT * FROM t").selectExpr("age * 2 as doubled", "name");
    assert.equal(df._plan.type, "project");
    if (df._plan.type === "project") {
      assert.equal(df._plan.expressions.length, 2);
    }
  });
});

describe("DataFrame.transform()", () => {
  it("applies a function to the DataFrame", () => {
    const { spark } = createSession();
    const df = spark.sql("SELECT * FROM t");
    const result = df.transform((d) => d.limit(5));
    assert.equal(result._plan.type, "limit");
  });
});

describe("DataFrame.sortWithinPartitions()", () => {
  it("produces a sort node with isGlobal=false", () => {
    const { spark } = createSession();
    const df = spark.sql("SELECT * FROM t").sortWithinPartitions("x");
    assert.equal(df._plan.type, "sort");
    if (df._plan.type === "sort") {
      assert.equal(df._plan.isGlobal, false);
    }
  });
});

describe("DataFrame.tail()", () => {
  it("is an action that creates a tail plan and collects", async () => {
    const t = mockTransport();
    const { spark } = createSession(t);
    const df = spark.sql("SELECT * FROM t");
    // tail is an action — but without decoder it will throw
    await assert.rejects(() => df.tail(5), /Arrow decoder/);
  });
});

describe("DataFrame.isEmpty()", () => {
  it("returns true for empty DataFrame", async () => {
    const t = mockTransport();
    const spark = SparkSession.builder()
      .remote("sc://localhost:15002")
      .transport(t)
      .arrowDecoder(async () => [])
      .getOrCreate();
    const df = spark.sql("SELECT 1");
    const result = await df.isEmpty();
    assert.equal(result, true);
  });

  it("returns false for non-empty DataFrame", async () => {
    const t = mockTransport();
    const spark = SparkSession.builder()
      .remote("sc://localhost:15002")
      .transport(t)
      .arrowDecoder(async () => [{ id: 1 }])
      .getOrCreate();
    const df = spark.sql("SELECT 1");
    const result = await df.isEmpty();
    assert.equal(result, false);
  });
});

describe("DataFrame.columns() / dtypes()", () => {
  it("columns() uses analyzePlan to get column names", async () => {
    const transport: Transport & { calls: LogicalPlan[] } = {
      calls: [],
      async *executePlan(_sid: string, plan: LogicalPlan): AsyncIterable<Uint8Array> {
        this.calls.push(plan);
      },
      async analyzePlan(): Promise<Record<string, unknown>> {
        return {
          type: "schema",
          schema: {
            struct: {
              fields: [
                { name: "id", type: { kind: { case: "integer", value: {} } }, nullable: true },
                { name: "name", type: { kind: { case: "string", value: {} } }, nullable: true },
              ],
            },
          },
        };
      },
    };
    const spark = SparkSession.builder()
      .remote("sc://localhost:15002")
      .transport(transport)
      .getOrCreate();
    const df = spark.sql("SELECT * FROM t");
    const cols = await df.columns();
    assert.deepStrictEqual(cols, ["id", "name"]);
  });

  it("dtypes() returns [name, type] pairs", async () => {
    const transport: Transport & { calls: LogicalPlan[] } = {
      calls: [],
      async *executePlan(_sid: string, plan: LogicalPlan): AsyncIterable<Uint8Array> {
        this.calls.push(plan);
      },
      async analyzePlan(): Promise<Record<string, unknown>> {
        return {
          type: "schema",
          schema: {
            struct: {
              fields: [
                { name: "id", type: { kind: { case: "integer", value: {} } }, nullable: true },
                { name: "name", type: { kind: { case: "string", value: {} } }, nullable: true },
              ],
            },
          },
        };
      },
    };
    const spark = SparkSession.builder()
      .remote("sc://localhost:15002")
      .transport(transport)
      .getOrCreate();
    const df = spark.sql("SELECT * FROM t");
    const dtypes = await df.dtypes();
    assert.equal(dtypes.length, 2);
    assert.equal(dtypes[0][0], "id");
    assert.equal(dtypes[1][0], "name");
  });
});
