import { connect, col, lit, avg, count } from "@spark-connect-js/node";

const SPARK_REMOTE = process.env["SPARK_REMOTE"] ?? "sc://localhost:15002";

const session = connect(SPARK_REMOTE);

const employees = session.sql(`
  SELECT * FROM VALUES
    ('Alice', 'Engineering', 90000),
    ('Bob',   'Engineering', 85000),
    ('Carol', 'Marketing',   70000),
    ('Dave',  'Marketing',   72000),
    ('Eve',   'Engineering', 95000)
  AS employees(name, department, salary)
`);

const topEarners = employees.filter(col("salary").gt(lit(75000))).sort(col("salary").desc());

console.log("Top earners:");
console.table(await topEarners.collect());

const byDept = employees
  .groupBy("department")
  .agg(count("*").alias("headcount"), avg("salary").alias("avg_salary"));

console.log("\nBy department:");
console.table(await byDept.collect());

await employees.createOrReplaceTempView("emp");
const sqlResult = session.sql(
  "SELECT department, MAX(salary) AS max_sal FROM emp GROUP BY department",
);

console.log("\nSQL result:");
console.table(await sqlResult.collect());

await session.stop();
console.log("\nSession stopped.");
