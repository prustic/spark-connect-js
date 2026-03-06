/**
 * basic-query — Spark Connect example showcasing the DataFrame API
 *
 * Prerequisites:
 *   docker compose up -d        # start Spark Connect on port 15002
 *
 * Run:
 *   pnpm build && node dist/main.js
 */

import { connect, col, lit } from "@spark-js/node";

const SPARK_REMOTE = process.env["SPARK_REMOTE"] ?? "sc://localhost:15002";

async function main(): Promise<void> {
  const spark = connect(SPARK_REMOTE);

  // ── 1. Simple SQL query ─────────────────────────────────────────────────
  console.log("=== 1. SQL Query ===");
  const range = spark.sql("SELECT id, id * 2 AS doubled FROM range(10)");
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

  // ── Cleanup ────────────────────────────────────────────────────────────
  await spark.stop();
  console.log("\nSession stopped.");
}

main().catch((err: unknown) => {
  console.error(err);
  process.exit(1);
});
