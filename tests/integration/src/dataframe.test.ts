import { describe, it, after } from "node:test";
import assert from "node:assert/strict";
import { col, lit, type DataFrame } from "@spark-connect-js/node";
import { spark, stopSession } from "./setup.js";

describe("DataFrame basics", () => {
  after(stopSession);

  it("range() with withColumn", async () => {
    const rows = await spark()
      .range(5)
      .withColumn("doubled", col("id").multiply(lit(2)))
      .collect();
    assert.equal(rows.length, 5);
    assert.equal(rows[0]["doubled"], 0);
    assert.equal(rows[2]["doubled"], 4);
  });

  it("filter + select", async () => {
    const rows = await spark()
      .range(10)
      .filter(col("id").gt(lit(7)))
      .select("id")
      .collect();
    assert.equal(rows.length, 2);
    assert.deepEqual(
      rows.map((r) => r["id"]),
      [8, 9],
    );
  });

  it("sort descending", async () => {
    const rows = await spark().range(5).sort(col("id").desc()).collect();
    assert.deepEqual(
      rows.map((r) => r["id"]),
      [4, 3, 2, 1, 0],
    );
  });

  it("limit + offset", async () => {
    const rows = await spark().range(10).limit(3).offset(2).collect();
    assert.equal(rows.length, 1);
  });

  it("withColumn / drop", async () => {
    const rows = await spark().range(3).withColumn("x", lit(1)).drop("x").collect();
    assert.ok(!("x" in rows[0]));
    assert.ok("id" in rows[0]);
  });

  it("distinct", async () => {
    const rows = await spark().sql("SELECT id % 3 AS bucket FROM range(10)").distinct().collect();
    assert.equal(rows.length, 3);
  });

  it("union / intersect / except", async () => {
    const a = spark().range(5);
    const b = spark().range(3, 8);

    const union = await a.union(b).distinct().sort(col("id").asc()).collect();
    assert.equal(union.length, 8); // 0..7

    const inter = await a.intersect(b).sort(col("id").asc()).collect();
    assert.deepEqual(
      inter.map((r) => r["id"]),
      [3, 4],
    );

    const except = await a.except(b).sort(col("id").asc()).collect();
    assert.deepEqual(
      except.map((r) => r["id"]),
      [0, 1, 2],
    );
  });

  it("count", async () => {
    const n = await spark().range(42).count();
    assert.equal(n, 42);
  });

  it("isEmpty", async () => {
    assert.equal(await spark().range(0).isEmpty(), true);
    assert.equal(await spark().range(1).isEmpty(), false);
  });

  it("columns / dtypes", async () => {
    const df = spark().range(1).withColumn("x", lit("hello"));
    const cols = await df.columns();
    assert.deepEqual(cols, ["id", "x"]);
    const dtypes = await df.dtypes();
    assert.equal(dtypes.length, 2);
  });

  it("first / head / take / tail", async () => {
    const df = spark().range(10);
    const first = await df.first();
    assert.ok(first !== null);

    const head = await df.head(3);
    assert.equal(head.length, 3);

    const take = await df.take(2);
    assert.equal(take.length, 2);

    const tail = await df.sort(col("id").asc()).tail(2);
    assert.equal(tail.length, 2);
  });

  it("toLocalIterator", async () => {
    const ids: unknown[] = [];
    for await (const row of spark().range(5).toLocalIterator()) {
      ids.push(row["id"]);
    }
    assert.equal(ids.length, 5);
  });

  it("forEach", async () => {
    const ids: unknown[] = [];
    await spark()
      .range(3)
      .forEach((row) => {
        ids.push(row["id"]);
      });
    assert.equal(ids.length, 3);
  });

  it("describe", async () => {
    const rows = await spark().range(10).describe("id").collect();
    assert.ok(rows.length > 0);
  });

  it("withColumnRenamed / withColumnsRenamed", async () => {
    const df = spark().range(1).withColumnRenamed("id", "num");
    const cols1 = await df.columns();
    assert.deepEqual(cols1, ["num"]);

    const df2 = spark().range(1).withColumnsRenamed({ id: "number" });
    const cols2 = await df2.columns();
    assert.deepEqual(cols2, ["number"]);
  });

  it("selectExpr", async () => {
    const rows = await spark().range(3).selectExpr("id", "id * 2 AS doubled").collect();
    assert.equal(rows[1]["doubled"], 2);
  });

  it("transform", async () => {
    const addX = (df: DataFrame) => df.withColumn("x", lit(42));
    const rows = await spark().range(1).transform(addX).collect();
    assert.equal(rows[0]["x"], 42);
  });

  it("alias + hint", async () => {
    const plan = await spark().range(1).alias("r").explain("simple");
    assert.ok(plan.length > 0);

    const hintPlan = await spark().range(1).hint("broadcast").explain("simple");
    assert.ok(hintPlan.length > 0);
  });

  it("explain", async () => {
    const plan = await spark()
      .range(10)
      .filter(col("id").gt(lit(5)))
      .explain("simple");
    assert.ok(plan.includes("Filter"));
  });

  it("printSchema", async () => {
    // Just verify it doesn't throw
    await spark().range(1).printSchema();
  });

  it("temp view + SQL", async () => {
    const df = spark().sql(`
      SELECT * FROM VALUES ('Alice', 90000), ('Bob', 85000) AS emp(name, salary)
    `);
    await df.createOrReplaceTempView("test_emp");
    const rows = await spark().sql("SELECT name FROM test_emp WHERE salary > 85000").collect();
    assert.equal(rows.length, 1);
    assert.equal(rows[0]["name"], "Alice");
  });

  it("catalog API", async () => {
    const db = await spark().catalog.currentDatabase();
    assert.equal(typeof db, "string");

    // Create a temp view so listTables has something
    await spark().range(1).createOrReplaceTempView("cat_test");
    const tables = await spark().catalog.listTables().collect();
    assert.ok(tables.some((t) => t["name"] === "cat_test"));
  });

  it("column methods: isNull, isin, like, between", async () => {
    const df = spark().sql(`
      SELECT * FROM VALUES ('Alice', 90000), ('Bob', NULL), ('Carol', 70000)
      AS data(name, salary)
    `);
    const notNull = await df.filter(col("name").isNotNull()).collect();
    assert.equal(notNull.length, 3);

    const nullSalary = await df.filter(col("salary").isNull()).collect();
    assert.equal(nullSalary.length, 1);

    const isin = await df.filter(col("name").isin("Alice", "Carol")).collect();
    assert.equal(isin.length, 2);

    const like = await df.filter(col("name").like("%o%")).collect();
    assert.equal(like.length, 2); // Bob, Carol

    const between = await df.filter(col("salary").between(lit(70000), lit(90000))).collect();
    assert.equal(between.length, 2);
  });

  it("DataFrameReader.table", async () => {
    await spark().range(5).createOrReplaceTempView("reader_test");
    const rows = await spark().read.table("reader_test").collect();
    assert.equal(rows.length, 5);
  });

  it("range() variants", async () => {
    const r1 = await spark().range(5).collect();
    assert.equal(r1.length, 5);

    const r2 = await spark().range(2, 10, 2).collect();
    assert.equal(r2.length, 4);
    assert.deepEqual(
      r2.map((r) => r["id"]),
      [2, 4, 6, 8],
    );
  });

  it("sortWithinPartitions", async () => {
    const rows = await spark().range(10).sortWithinPartitions(col("id").desc()).collect();
    assert.equal(rows.length, 10);
  });

  it("sort ordering: asc_nulls_first / desc_nulls_last", async () => {
    const df = spark().sql(`
      SELECT * FROM VALUES (90000), (NULL), (70000), (NULL), (95000)
      AS data(salary)
    `);
    const ascNF = await df.sort(col("salary").asc_nulls_first()).collect();
    assert.equal(ascNF[0]["salary"], null);

    const descNL = await df.sort(col("salary").desc_nulls_last()).collect();
    assert.equal(descNL[descNL.length - 1]["salary"], null);
  });

  it("ilike / substr", async () => {
    const df = spark().sql(`
      SELECT * FROM VALUES ('Alice'), ('Bob'), ('Carol') AS data(name)
    `);
    const ilike = await df.filter(col("name").ilike("%ALICE%")).collect();
    assert.equal(ilike.length, 1);

    const rows = await df
      .withColumn("short", col("name").substr(1, 3))
      .select("name", "short")
      .collect();
    assert.equal(rows[0]["short"], "Ali");
  });

  it("bitwise column operations", async () => {
    const rows = await spark()
      .range(8)
      .withColumn("and_3", col("id").bitwiseAND(lit(3)))
      .withColumn("or_4", col("id").bitwiseOR(lit(4)))
      .withColumn("xor_5", col("id").bitwiseXOR(lit(5)))
      .collect();
    assert.equal(rows.length, 8);
    assert.equal(rows[0]["and_3"], 0 & 3);
    assert.equal(rows[0]["or_4"], 0 | 4);
    assert.equal(rows[0]["xor_5"], 0 ^ 5);
  });
});
