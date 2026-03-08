import { describe, it, after } from "node:test";
import assert from "node:assert/strict";
import { col, lit, when, cast, upper, round, count, sum, avg } from "@spark-connect-js/node";
import { spark, stopSession } from "./setup.js";

const employees = () =>
  spark().sql(`
    SELECT * FROM VALUES
      ('Alice', 'Engineering', 90000),
      ('Bob',   'Engineering', 85000),
      ('Carol', 'Marketing',   70000),
      ('Dave',  'Marketing',   72000),
      ('Eve',   'Engineering', 95000)
    AS employees(name, department, salary)
  `);

describe("groupBy + aggregation", () => {
  after(stopSession);

  it("count", async () => {
    const rows = await employees().groupBy("department").count().collect();
    assert.equal(rows.length, 2);
  });

  it("avg", async () => {
    const rows = await employees().groupBy("department").avg("salary").collect();
    assert.equal(rows.length, 2);
  });

  it("min / max", async () => {
    const mins = await employees().groupBy("department").min("salary").collect();
    const maxs = await employees().groupBy("department").max("salary").collect();
    assert.equal(mins.length, 2);
    assert.equal(maxs.length, 2);
  });

  it("agg with multiple aggregates", async () => {
    const rows = await employees()
      .groupBy("department")
      .agg(
        count(col("name")).as("headcount"),
        round(avg(col("salary")), 0).as("avg_salary"),
        sum(col("salary")).as("total_salary"),
      )
      .collect();
    assert.equal(rows.length, 2);
    const eng = rows.find((r) => r["department"] === "Engineering");
    assert.equal(eng?.["headcount"], 3);
  });
});

describe("join", () => {
  after(stopSession);

  it("inner join", async () => {
    const departments = spark().sql(`
      SELECT * FROM VALUES
        ('Engineering', 'Building A'),
        ('Marketing',   'Building B'),
        ('Sales',       'Building C')
      AS departments(dept_name, location)
    `);
    const rows = await employees()
      .join(departments, col("department").eq(col("dept_name")), "inner")
      .select("name", "department", "location")
      .collect();
    assert.equal(rows.length, 5);
  });
});

describe("when / otherwise", () => {
  after(stopSession);

  it("conditional column", async () => {
    const rows = await employees()
      .withColumn(
        "tier",
        when(col("salary").gte(lit(90000)), lit("senior"))
          .when(col("salary").gte(lit(80000)), lit("mid"))
          .otherwise(lit("junior")),
      )
      .select("name", "tier")
      .collect();
    const alice = rows.find((r) => r["name"] === "Alice");
    assert.equal(alice?.["tier"], "senior");
    const carol = rows.find((r) => r["name"] === "Carol");
    assert.equal(carol?.["tier"], "junior");
  });
});

describe("cast + string functions", () => {
  after(stopSession);

  it("cast and upper", async () => {
    const rows = await employees()
      .withColumn("salary_str", cast(col("salary"), "string"))
      .withColumn("upper_name", upper(col("name")))
      .select("upper_name", "salary_str")
      .collect();
    assert.equal(rows[0]["upper_name"], "ALICE");
    assert.equal(typeof rows[0]["salary_str"], "string");
  });
});

describe("fillna / dropna", () => {
  after(stopSession);

  const withNulls = () =>
    spark().sql(`
      SELECT * FROM VALUES
        ('Alice', 90000),
        ('Bob',   NULL),
        ('Carol', 70000),
        (NULL,    80000)
      AS data(name, salary)
    `);

  it("fillna", async () => {
    const rows = await withNulls().fillna(0).collect();
    assert.equal(rows.length, 4);
    const bob = rows.find((r) => r["name"] === "Bob");
    assert.equal(bob?.["salary"], 0);
  });

  it("dropna", async () => {
    const rows = await withNulls().dropna().collect();
    assert.equal(rows.length, 2);
  });
});
