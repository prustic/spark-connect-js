import { connect, col, sum, MEMORY_ONLY } from "@spark-connect-js/node";

const SPARK_REMOTE = process.env["SPARK_REMOTE"] ?? "sc://localhost:15002";

const session = connect(SPARK_REMOTE);

const sales = session.sql(`
  SELECT * FROM VALUES
    ('Alice', 'Engineering', 'Q1', 12000),
    ('Alice', 'Engineering', 'Q2', 14000),
    ('Bob',   'Engineering', 'Q1', 11000),
    ('Bob',   'Engineering', 'Q2', 13000),
    ('Carol', 'Marketing',   'Q1',  8000),
    ('Carol', 'Marketing',   'Q2',  9500),
    ('Dave',  'Marketing',   'Q1',  7500),
    ('Dave',  'Marketing',   'Q2',  8500),
    ('Eve',   'Sales',       'Q1', 15000),
    ('Eve',   'Sales',       'Q2', 16000)
  AS sales(name, department, quarter, revenue)
`);

// MEMORY_ONLY since the data fits; cache() defaults to MEMORY_AND_DISK.
// All the queries below read from this cached result.
const cached = await sales.persist(MEMORY_ONLY);
console.log("Storage level:", await cached.getStorageLevel());

// Hash-partition by department, then coalesce down (no shuffle).
const partitioned = cached.repartition(4, "department");
const compact = partitioned.coalesce(2);

// The physical plan shows the hash exchange + coalesce, reading from cache.
console.log("\nQuery plan (repartition -> coalesce):");
console.log(await compact.explain());

const cubeResult = await cached
  .cube("department", "quarter")
  .agg(sum("revenue").alias("total"))
  .sort(col("department").asc_nulls_last(), col("quarter").asc_nulls_last())
  .collect();

console.log("\nCube (all subtotal combinations):");
console.table(cubeResult);

// rollup is like cube but only does hierarchical subtotals, not every combo.
const rollupResult = await cached
  .rollup("department", "quarter")
  .agg(sum("revenue").alias("total"))
  .sort(col("department").asc_nulls_last(), col("quarter").asc_nulls_last())
  .collect();

console.log("\nRollup (hierarchical subtotals):");
console.table(rollupResult);

const pivoted = await cached
  .groupBy("department")
  .pivot("quarter", ["Q1", "Q2"])
  .agg(sum("revenue"))
  .sort(col("department").asc())
  .collect();

console.log("\nPivot (quarters as columns):");
console.table(pivoted);

const unpivoted = await cached
  .groupBy("department")
  .pivot("quarter", ["Q1", "Q2"])
  .agg(sum("revenue"))
  .unpivot([col("department")], [col("Q1"), col("Q2")], "quarter", "total")
  .sort(col("department").asc(), col("quarter").asc())
  .collect();

console.log("\nUnpivot (back to long format):");
console.table(unpivoted);

const summary = await cached.select("revenue").summary("count", "min", "max", "mean").collect();
console.log("\nRevenue summary:");
console.table(summary);

const replaced = await cached
  .replace({ Marketing: "Growth" }, ["department"])
  .select("name", "department")
  .distinct()
  .sort(col("department").asc())
  .collect();

console.log("\nAfter replacing 'Marketing' with 'Growth':");
console.table(replaced);

const [train, test] = cached.randomSplit([0.7, 0.3], 42);
console.log("\nRandom split - train:", await train.count(), "test:", await test.count());

const numbers = session.sql(`
  SELECT * FROM VALUES (1, 2), (2, 4), (3, 6), (4, 8), (5, 10)
  AS data(x, y)
`);

const corr = await numbers.stat.corr("x", "y").collect();
console.log("\nPearson correlation (x, y):", corr[0]?.["corr"]);

const cov = await numbers.stat.cov("x", "y").collect();
console.log("Sample covariance (x, y):", cov[0]?.["cov"]);

const freqItems = await cached.stat.freqItems(["department"], 0.3).collect();
console.log("\nFrequent departments:", freqItems);

await cached.createTempView("sales_data");
const fromView = await session
  .sql("SELECT department, SUM(revenue) AS total FROM sales_data GROUP BY department")
  .collect();
console.log("\nQuery from temp view:");
console.table(fromView);

const df1 = session.range(10);
const df2 = session.range(10);
const df3 = session.range(20);

console.log("\nrange(10) same as range(10)?", await df1.sameSemantics(df2));
console.log("range(10) same as range(20)?", await df1.sameSemantics(df3));
console.log("range(10) semantic hash:", await df1.semanticHash());

await cached.unpersist();
await session.stop();
console.log("\nDone.");
