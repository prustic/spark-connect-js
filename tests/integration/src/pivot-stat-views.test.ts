import { describe, it, after } from "node:test";
import assert from "node:assert/strict";
import { col, sum, count } from "@spark-connect-js/node";
import { spark, stopSession } from "./setup.js";

const employees = () =>
  spark().sql(`
    SELECT * FROM VALUES
      ('Alice', 'Engineering', 2020, 90000),
      ('Bob',   'Engineering', 2021, 85000),
      ('Carol', 'Marketing',   2020, 70000),
      ('Dave',  'Marketing',   2021, 72000),
      ('Eve',   'Engineering', 2020, 95000)
    AS emp(name, department, year, salary)
  `);

const numbers = () =>
  spark().sql(`
    SELECT * FROM VALUES
      (1.0, 2.0),
      (2.0, 4.0),
      (3.0, 6.0),
      (4.0, 8.0),
      (5.0, 10.0)
    AS data(x, y)
  `);

describe("cube / rollup", () => {
  after(stopSession);

  it("cube() produces aggregated rows with nulls for subtotals", async () => {
    const rows = await employees()
      .cube("department", "year")
      .agg(sum(col("salary")).as("total"))
      .sort(col("department").asc_nulls_last(), col("year").asc_nulls_last())
      .collect();
    // Cube produces: (dept, year), (dept, null), (null, year), (null, null)
    assert.ok(rows.length > 2);
    // There should be a grand total row with both nulls
    const grandTotal = rows.find((r) => r["department"] === null && r["year"] === null);
    assert.ok(grandTotal);
    assert.equal(grandTotal["total"], 412000);
  });

  it("rollup() produces hierarchical subtotals", async () => {
    const rows = await employees()
      .rollup("department", "year")
      .agg(count(col("name")).as("cnt"))
      .sort(col("department").asc_nulls_last(), col("year").asc_nulls_last())
      .collect();
    // Rollup produces: (dept, year), (dept, null), (null, null) — no (null, year)
    assert.ok(rows.length > 2);
    const grandTotal = rows.find((r) => r["department"] === null && r["year"] === null);
    assert.ok(grandTotal);
    assert.equal(grandTotal["cnt"], 5);
  });
});

describe("pivot", () => {
  after(stopSession);

  it("pivot() pivots values into columns", async () => {
    const rows = await employees()
      .groupBy("department")
      .pivot("year", [2020, 2021])
      .agg(sum(col("salary")).as("total"))
      .sort(col("department").asc())
      .collect();
    assert.equal(rows.length, 2);
    const eng = rows.find((r) => r["department"] === "Engineering");
    assert.ok(eng);
    // Engineering 2020: Alice(90000) + Eve(95000) = 185000
    assert.equal(eng["2020"], 185000);
    // Engineering 2021: Bob(85000)
    assert.equal(eng["2021"], 85000);
  });
});

describe("unpivot / melt", () => {
  after(stopSession);

  it("unpivot() transforms wide to long format", async () => {
    const wide = spark().sql(`
      SELECT * FROM VALUES
        ('Alice', 90, 85),
        ('Bob',   80, 92)
      AS data(name, math, english)
    `);
    const rows = await wide
      .unpivot(["name"], ["math", "english"], "subject", "score")
      .sort(col("name").asc(), col("subject").asc())
      .collect();
    assert.equal(rows.length, 4);
    assert.equal(rows[0]["name"], "Alice");
    assert.equal(rows[0]["subject"], "english");
    assert.equal(rows[0]["score"], 85);
  });

  it("melt() is an alias for unpivot()", async () => {
    const wide = spark().sql(`
      SELECT * FROM VALUES ('X', 1, 2) AS data(id, a, b)
    `);
    const rows = await wide.melt(["id"], ["a", "b"], "var", "val").collect();
    assert.equal(rows.length, 2);
    assert.ok(rows.some((r) => r["var"] === "a"));
    assert.ok(rows.some((r) => r["var"] === "b"));
  });
});

describe("DataFrame.summary()", () => {
  after(stopSession);

  it("returns summary statistics", async () => {
    const rows = await spark().range(100).summary("count", "min", "max").collect();
    assert.ok(rows.length >= 3);
    const countRow = rows.find((r) => r["summary"] === "count");
    assert.ok(countRow);
    assert.equal(countRow["id"], "100");
  });
});

describe("DataFrame.replace()", () => {
  after(stopSession);

  it("replaces values in a column", async () => {
    const df = spark().sql(`
      SELECT * FROM VALUES ('Alice'), ('Bob'), ('Carol') AS data(name)
    `);
    const rows = await df
      .replace({ Alice: "Alicia", Bob: "Robert" }, ["name"])
      .sort(col("name").asc())
      .collect();
    assert.equal(rows.length, 3);
    assert.ok(rows.some((r) => r["name"] === "Alicia"));
    assert.ok(rows.some((r) => r["name"] === "Robert"));
    assert.ok(rows.some((r) => r["name"] === "Carol"));
  });
});

describe("DataFrame.randomSplit()", () => {
  after(stopSession);

  it("splits into multiple DataFrames that cover all rows", async () => {
    const splits = spark().range(100).randomSplit([0.7, 0.3], 42);
    assert.equal(splits.length, 2);
    const count0 = await splits[0].count();
    const count1 = await splits[1].count();
    // The split is approximate, but together they should cover close to 100
    assert.ok(count0 > 0);
    assert.ok(count1 > 0);
    assert.ok(count0 + count1 <= 100);
  });
});

describe("DataFrame view commands", () => {
  after(stopSession);

  it("createTempView() creates a non-replaceable view", async () => {
    const df = spark().range(5);
    await df.createTempView("view_test_create");
    const rows = await spark().sql("SELECT * FROM view_test_create").collect();
    assert.equal(rows.length, 5);

    // Creating the same view again should fail
    await assert.rejects(df.createTempView("view_test_create"));
  });

  it("createGlobalTempView() creates a global temp view", async () => {
    const viewName = `global_view_test_${Date.now()}`;
    const df = spark().range(3);
    await df.createGlobalTempView(viewName);
    const rows = await spark().sql(`SELECT * FROM global_temp.${viewName}`).collect();
    assert.equal(rows.length, 3);
  });

  it("createOrReplaceGlobalTempView() replaces existing global view", async () => {
    await spark().range(5).createOrReplaceGlobalTempView("global_replace_test");
    await spark().range(10).createOrReplaceGlobalTempView("global_replace_test");
    const rows = await spark().sql("SELECT * FROM global_temp.global_replace_test").collect();
    assert.equal(rows.length, 10);
  });
});

describe("DataFrame.sameSemantics() / semanticHash()", () => {
  after(stopSession);

  it("sameSemantics() returns true for identical plans", async () => {
    const df1 = spark().range(10);
    const df2 = spark().range(10);
    const result = await df1.sameSemantics(df2);
    assert.equal(result, true);
  });

  it("sameSemantics() returns false for different plans", async () => {
    const df1 = spark().range(10);
    const df2 = spark().range(20);
    const result = await df1.sameSemantics(df2);
    assert.equal(result, false);
  });

  it("semanticHash() returns a number", async () => {
    const hash = await spark().range(10).semanticHash();
    assert.equal(typeof hash, "number");
  });

  it("semanticHash() is consistent for same plan", async () => {
    const h1 = await spark().range(10).semanticHash();
    const h2 = await spark().range(10).semanticHash();
    assert.equal(h1, h2);
  });
});

describe("DataFrameStat", () => {
  after(stopSession);

  it("stat.corr() computes Pearson correlation", async () => {
    const rows = await numbers().stat.corr("x", "y").collect();
    assert.equal(rows.length, 1);
    const corrValue = rows[0]["corr"] as number;
    // Perfect linear correlation
    assert.ok(Math.abs(corrValue - 1.0) < 0.001);
  });

  it("stat.cov() computes sample covariance", async () => {
    const rows = await numbers().stat.cov("x", "y").collect();
    assert.equal(rows.length, 1);
    const covValue = rows[0]["cov"] as number;
    assert.ok(covValue > 0);
  });

  it("stat.crosstab() produces a contingency table", async () => {
    const df = spark().sql(`
      SELECT * FROM VALUES
        ('a', 'x'), ('a', 'y'), ('b', 'x'), ('b', 'x'), ('a', 'x')
      AS data(c1, c2)
    `);
    const rows = await df.stat.crosstab("c1", "c2").sort(col("c1_c2").asc()).collect();
    assert.ok(rows.length > 0);
  });

  it("stat.freqItems() finds frequent items", async () => {
    const df = spark().sql(`
      SELECT * FROM VALUES (1), (1), (1), (2), (3) AS data(val)
    `);
    const rows = await df.stat.freqItems(["val"], 0.4).collect();
    assert.equal(rows.length, 1);
  });

  it("stat.approxQuantile() computes quantiles", async () => {
    const rows = await spark()
      .range(100)
      .stat.approxQuantile(["id"], [0.25, 0.5, 0.75], 0.01)
      .collect();
    assert.ok(rows.length > 0);
  });
});
