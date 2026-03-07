/**
 * basic-query — Spark Connect example showcasing the DataFrame API
 *
 * Prerequisites:
 *   docker compose up -d        # start Spark Connect on port 15002
 *
 * Run:
 *   pnpm build && node dist/main.js
 */

import {
  connect,
  col,
  lit,
  when,
  cast,
  upper,
  round,
  count,
  sum,
  avg,
  row_number,
  rank,
  dense_rank,
  lag,
  Window,
  DataFrame,
} from "@spark-js/node";

const SPARK_REMOTE = process.env["SPARK_REMOTE"] ?? "sc://localhost:15002";

async function main(): Promise<void> {
  const spark = connect(SPARK_REMOTE);

  // ── 1. SparkSession.range() ────────────────────────────────────────────
  console.log("=== 1. SparkSession.range() ===");
  const range = spark.range(10).withColumn("doubled", col("id").multiply(lit(2)));
  await range.show();

  // ── 2. Filter + Select ──────────────────────────────────────────────────
  console.log("\n=== 2. Filter + Select ===");
  const filtered = range.filter(col("id").gt(lit(5))).select("id", "doubled");
  await filtered.show();

  // ── 3. Sort / OrderBy ──────────────────────────────────────────────────
  console.log("\n=== 3. Sort descending ===");
  const sorted = range.sort(col("id").desc());
  await sorted.show();

  // ── 4. Limit + Offset ──────────────────────────────────────────────────
  console.log("\n=== 4. Limit 3, Offset 2 ===");
  const paged = range.limit(3).offset(2);
  await paged.show();

  // ── 5. withColumn ──────────────────────────────────────────────────────
  console.log("\n=== 5. withColumn ===");
  const withTripled = range.withColumn("tripled", col("id").multiply(lit(3)));
  await withTripled.show(5);

  // ── 6. Drop columns ───────────────────────────────────────────────────
  console.log("\n=== 6. Drop 'doubled' column ===");
  const dropped = range.drop("doubled");
  await dropped.show(5);

  // ── 7. Distinct / dropDuplicates ───────────────────────────────────────
  console.log("\n=== 7. Distinct ===");
  const dupes = spark.sql("SELECT id % 3 AS bucket FROM range(10)");
  const distinct = dupes.distinct();
  await distinct.show();

  // ── 8. GroupBy + Aggregation ───────────────────────────────────────────
  console.log("\n=== 8. GroupBy + Count ===");
  const employees = spark.sql(`
    SELECT * FROM VALUES
      ('Alice', 'Engineering', 90000),
      ('Bob',   'Engineering', 85000),
      ('Carol', 'Marketing',   70000),
      ('Dave',  'Marketing',   72000),
      ('Eve',   'Engineering', 95000)
    AS employees(name, department, salary)
  `);
  await employees.show();

  const deptCounts = employees.groupBy("department").count();
  console.log("\nDepartment counts:");
  await deptCounts.show();

  const deptAvg = employees.groupBy("department").avg("salary");
  console.log("\nDepartment avg salary:");
  await deptAvg.show();

  // ── 9. Join ────────────────────────────────────────────────────────────
  console.log("\n=== 9. Join ===");
  const departments = spark.sql(`
    SELECT * FROM VALUES
      ('Engineering', 'Building A'),
      ('Marketing',   'Building B'),
      ('Sales',       'Building C')
    AS departments(dept_name, location)
  `);

  const joined = employees.join(departments, col("department").eq(col("dept_name")), "inner");
  await joined.select("name", "department", "location").show();

  // ── 10. Temp View + SQL query ──────────────────────────────────────────
  console.log("\n=== 10. Temp View + SQL ===");
  await employees.createOrReplaceTempView("emp");
  const topEarners = spark.sql(
    "SELECT name, salary FROM emp WHERE salary > 85000 ORDER BY salary DESC",
  );
  await topEarners.show();

  // ── 11. Explain plan ───────────────────────────────────────────────────
  console.log("\n=== 11. Explain Plan ===");
  const complex = employees
    .filter(col("salary").gt(lit(70000)))
    .select("name", "salary")
    .sort(col("salary").desc());
  const plan = await complex.explain("simple");
  console.log(plan);

  // ── 12. Collect as JSON ────────────────────────────────────────────────
  console.log("\n=== 12. Collect ===");
  const rows = await filtered.collect();
  console.log(`Collected ${String(rows.length)} rows:`);
  console.log(JSON.stringify(rows, null, 2));

  // ── 13. Expression Functions: when / otherwise ────────────────────────
  console.log("\n=== 13. When / Otherwise ===");
  const withTier = employees.withColumn(
    "tier",
    when(col("salary").gte(lit(90000)), lit("senior"))
      .when(col("salary").gte(lit(80000)), lit("mid"))
      .otherwise(lit("junior")),
  );
  await withTier.select("name", "salary", "tier").show();

  // ── 14. Cast + String Functions ───────────────────────────────────────
  console.log("\n=== 14. Cast + Upper ===");
  const casted = employees
    .withColumn("salary_str", cast(col("salary"), "string"))
    .withColumn("upper_name", upper(col("name")));
  await casted.select("upper_name", "salary_str").show();

  // ── 15. Math Functions ────────────────────────────────────────────────
  console.log("\n=== 15. Round / Aggregates ===");
  const deptStats = employees
    .groupBy("department")
    .agg(
      count(col("name")).as("headcount"),
      round(avg(col("salary")), 0).as("avg_salary"),
      sum(col("salary")).as("total_salary"),
    );
  await deptStats.show();

  // ── 16. Schema Inspection ─────────────────────────────────────────────
  console.log("\n=== 16. Schema ===");
  await employees.printSchema();

  // ── 17. Catalog API ───────────────────────────────────────────────────
  console.log("\n=== 17. Catalog ===");
  const currentDb = await spark.catalog.currentDatabase();
  console.log(`Current database: ${currentDb}`);

  const tables = spark.catalog.listTables();
  console.log("Tables:");
  await tables.show();

  const empExists = await spark.catalog.tableExists("emp");
  console.log(`Table 'emp' exists: ${String(empExists)}`);

  // ── 18. Union / Intersect / Except ─────────────────────────────────────
  console.log("\n=== 18. Union / Intersect / Except ===");
  const eng = employees.filter(col("department").eq(lit("Engineering")));
  const mkt = employees.filter(col("department").eq(lit("Marketing")));
  const unioned = eng.union(mkt);
  console.log("Union of Engineering + Marketing:");
  await unioned.show();

  const intersected = employees
    .filter(col("salary").gt(lit(80000)))
    .intersect(employees.filter(col("department").eq(lit("Engineering"))));
  console.log("Intersect (salary>80k ∩ Engineering):");
  await intersected.show();

  const excepted = employees.except(eng);
  console.log("Except (all - Engineering):");
  await excepted.show();

  // ── 19. Column Methods: isNull, isin, like, between ───────────────────
  console.log("\n=== 19. Column Methods ===");
  const withNulls = spark.sql(`
    SELECT * FROM VALUES
      ('Alice', 90000),
      ('Bob',   NULL),
      ('Carol', 70000),
      (NULL,    80000)
    AS data(name, salary)
  `);
  console.log("isNotNull filter:");
  await withNulls.filter(col("name").isNotNull()).show();

  console.log("isin filter:");
  await employees.filter(col("name").isin("Alice", "Eve")).show();

  console.log("like filter:");
  await employees.filter(col("name").like("%o%")).show();

  console.log("between filter:");
  await employees.filter(col("salary").between(lit(72000), lit(91000))).show();

  // ── 20. Window Functions ───────────────────────────────────────────────
  console.log("\n=== 20. Window Functions ===");
  const w = Window.partitionBy("department").orderBy(col("salary").desc());
  const ranked = employees
    .withColumn("row_num", row_number().over(w))
    .withColumn("rnk", rank().over(w))
    .withColumn("dense_rnk", dense_rank().over(w))
    .withColumn("prev_salary", lag("salary", 1).over(w));
  await ranked
    .select("name", "department", "salary", "row_num", "rnk", "dense_rnk", "prev_salary")
    .show();

  // ── 21. Describe ──────────────────────────────────────────────────────
  console.log("\n=== 21. Describe ===");
  const stats = employees.describe("salary");
  await stats.show();

  // ── 22. FillNA / DropNA ───────────────────────────────────────────────
  console.log("\n=== 22. FillNA / DropNA ===");
  console.log("Original with nulls:");
  await withNulls.show();
  console.log("After fillna(0):");
  await withNulls.fillna(0).show();
  console.log("After dropna:");
  await withNulls.dropna().show();

  // ── 23. Count (optimised) ─────────────────────────────────────────────
  console.log("\n=== 23. Count (optimised) ===");
  const empCount = await employees.count();
  console.log(`Employee count: ${empCount}`);

  // ── 24. toLocalIterator / forEach ─────────────────────────────────────
  console.log("\n=== 24. toLocalIterator ===");
  console.log("Streaming rows one-by-one:");
  for await (const row of employees.select("name", "salary").toLocalIterator()) {
    console.log(`  ${String(row.name)}: ${String(row.salary)}`);
  }

  console.log("\nforEach callback:");
  const names: string[] = [];
  await employees.select("name").forEach((row) => {
    names.push(row.name as string);
  });
  console.log(`  Collected names: ${names.join(", ")}`);

  // ── 25. first / head / take ───────────────────────────────────────────
  console.log("\n=== 25. first / head / take ===");
  const firstRow = await employees.first();
  console.log("first():", firstRow);

  const headRows = await employees.head(2);
  console.log("head(2):", headRows);

  const takeRows = await employees.take(3);
  console.log("take(3):", takeRows);

  // ── 26. DataFrameReader shortcuts ─────────────────────────────────────
  console.log("\n=== 26. DataFrameReader: table() ===");
  // "emp" temp view was registered in section 10
  const fromTable = spark.read.table("emp");
  await fromTable.show();

  // ── 27. SparkSession.range() variants ──────────────────────────────────
  console.log("\n=== 27. SparkSession.range() variants ===");
  console.log("range(5):");
  await spark.range(5).show();
  console.log("range(2, 10, 2):");
  await spark.range(2, 10, 2).show();

  // ── 28. withColumnRenamed / withColumnsRenamed ────────────────────────
  console.log("\n=== 28. Rename Columns ===");
  const renamed = employees
    .withColumnRenamed("name", "employee_name")
    .withColumnRenamed("salary", "pay");
  await renamed.show();

  const batchRenamed = employees.withColumnsRenamed({
    name: "emp_name",
    department: "dept",
    salary: "compensation",
  });
  await batchRenamed.show();

  // ── 29. selectExpr ────────────────────────────────────────────────────
  console.log("\n=== 29. selectExpr ===");
  const withExprs = employees.selectExpr(
    "name",
    "salary * 1.1 AS raised_salary",
    "upper(department) AS dept_upper",
  );
  await withExprs.show();

  // ── 30. transform (pipeline composition) ──────────────────────────────
  console.log("\n=== 30. transform ===");
  const addBonus = (df: DataFrame) => df.withColumn("bonus", col("salary").multiply(lit(0.1)));
  const addLevel = (df: DataFrame) =>
    df.withColumn(
      "level",
      when(col("salary").gte(lit(90000)), lit("senior")).otherwise(lit("junior")),
    );
  const pipeline = employees.transform(addBonus).transform(addLevel);
  await pipeline.select("name", "salary", "bonus", "level").show();

  // ── 31. alias + hint ──────────────────────────────────────────────────
  console.log("\n=== 31. alias + hint ===");
  const aliased = employees.alias("emp");
  const aliasedPlan = await aliased.select("emp.name", "emp.salary").explain("simple");
  console.log("Aliased plan:");
  console.log(aliasedPlan);

  const hinted = employees.hint("broadcast");
  const hintedPlan = await hinted.explain("simple");
  console.log("Broadcast hint plan:");
  console.log(hintedPlan);

  // ── 32. sortWithinPartitions ──────────────────────────────────────────
  console.log("\n=== 32. sortWithinPartitions ===");
  const partSorted = employees.sortWithinPartitions(col("salary").desc());
  await partSorted.show();

  // ── 33. tail ──────────────────────────────────────────────────────────
  console.log("\n=== 33. tail ===");
  const lastTwo = await employees.sort(col("salary").asc()).tail(2);
  console.log("Last 2 by salary:", lastTwo);

  // ── 34. columns / dtypes / isEmpty ────────────────────────────────────
  console.log("\n=== 34. columns / dtypes / isEmpty ===");
  const colNames = await employees.columns();
  console.log("Column names:", colNames);

  const colTypes = await employees.dtypes();
  console.log("Column types:", colTypes);

  const emptyCheck = await employees.filter(col("salary").lt(lit(0))).isEmpty();
  console.log("isEmpty (salary < 0):", emptyCheck);

  const notEmpty = await employees.isEmpty();
  console.log("isEmpty (all employees):", notEmpty);

  // ── 35. Column sort ordering variants ─────────────────────────────────
  console.log("\n=== 35. Sort ordering variants ===");
  const withNullSalaries = spark.sql(`
    SELECT * FROM VALUES
      ('Alice', 90000),
      ('Bob',   NULL),
      ('Carol', 70000),
      ('Dave',  NULL),
      ('Eve',   95000)
    AS data(name, salary)
  `);
  console.log("asc_nulls_first:");
  await withNullSalaries.sort(col("salary").asc_nulls_first()).show();
  console.log("desc_nulls_last:");
  await withNullSalaries.sort(col("salary").desc_nulls_last()).show();

  // ── 36. Column ilike / substr ─────────────────────────────────────────
  console.log("\n=== 36. ilike / substr ===");
  console.log("ilike (case-insensitive):");
  await employees.filter(col("name").ilike("%alice%")).show();

  console.log("substr:");
  await employees
    .withColumn("short_name", col("name").substr(1, 3))
    .select("name", "short_name")
    .show();

  // ── 37. Bitwise operations ────────────────────────────────────────────
  console.log("\n=== 37. Bitwise operations ===");
  const nums = spark.range(8);
  await nums
    .withColumn("and_3", col("id").bitwiseAND(lit(3)))
    .withColumn("or_4", col("id").bitwiseOR(lit(4)))
    .withColumn("xor_5", col("id").bitwiseXOR(lit(5)))
    .show();

  // ── 38. GroupedData min / max ─────────────────────────────────────────
  console.log("\n=== 38. GroupedData min / max ===");
  console.log("Min salary by department:");
  await employees.groupBy("department").min("salary").show();
  console.log("Max salary by department:");
  await employees.groupBy("department").max("salary").show();

  // ── Cleanup ────────────────────────────────────────────────────────────
  await spark.stop();
  console.log("\nSession stopped.");
}

main().catch((err: unknown) => {
  console.error(err);
  process.exit(1);
});
