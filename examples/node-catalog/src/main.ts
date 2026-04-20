import { connect, StructType, StructField } from "@spark-connect-js/node";

const SPARK_REMOTE = process.env["SPARK_REMOTE"] ?? "sc://localhost:15002";
const session = connect(SPARK_REMOTE);
const catalog = session.catalog;

// ── Catalogs & databases ───────────────────────────────────────────

console.log("Current catalog:", await catalog.currentCatalog());

const catalogs = await catalog.listCatalogs().collect();
console.log("Available catalogs:");
console.table(catalogs);

console.log("Current database:", await catalog.currentDatabase());
console.log("'default' exists?", await catalog.databaseExists("default"));

const dbMeta = await catalog.getDatabase("default").collect();
console.log("\nDatabase metadata:");
console.table(dbMeta);

const databases = await catalog.listDatabases().collect();
console.log("All databases:");
console.table(databases);

// ── Functions ──────────────────────────────────────────────────────

console.log("\n'count' exists?", await catalog.functionExists("count"));

const countMeta = await catalog.getFunction("count").collect();
console.log("Function metadata for 'count':");
console.table(countMeta);

// listFunctions returns hundreds; just show the first 5.
const fns = await catalog.listFunctions().limit(5).collect();
console.log("\nFirst 5 functions:");
console.table(fns);

// ── Temp views: create, list, cache, drop ──────────────────────────

const employees = session.sql(`
  SELECT * FROM VALUES
    ('Alice', 'Engineering', 90000),
    ('Bob',   'Marketing',   75000),
    ('Carol', 'Engineering', 88000)
  AS employees(name, department, salary)
`);

await employees.createOrReplaceTempView("employees");
console.log("\n'employees' exists?", await catalog.tableExists("employees"));

const tableMeta = await catalog.getTable("employees").collect();
console.log("Table metadata:");
console.table(tableMeta);

const columns = await catalog.listColumns("employees").collect();
console.log("Columns:");
console.table(columns);

// Cache via the catalog API, verify, then uncache
await catalog.cacheTable("employees");
console.log("Is cached after cacheTable?", await catalog.isCached("employees"));

await catalog.uncacheTable("employees");
console.log("Is cached after uncacheTable?", await catalog.isCached("employees"));

// Cache again, then clear everything
await catalog.cacheTable("employees");
await catalog.clearCache();
console.log("Is cached after clearCache?", await catalog.isCached("employees"));

// Refresh metadata (smoke test — no error means success)
await employees.createOrReplaceTempView("employees");
await catalog.refreshTable("employees");
console.log("refreshTable('employees') succeeded");

// Drop the view
const dropped = await catalog.dropTempView("employees");
console.log("dropTempView returned:", dropped);
const droppedAgain = await catalog.dropTempView("employees");
console.log("dropTempView again:", droppedAgain);

// ── Global temp views ──────────────────────────────────────────────

await employees.createOrReplaceGlobalTempView("global_emp");
const gDropped = await catalog.dropGlobalTempView("global_emp");
console.log("\ndropGlobalTempView returned:", gDropped);

// ── createTable with schema ────────────────────────────────────────

const schema = new StructType([
  new StructField("id", "long"),
  new StructField("name", "string"),
  new StructField("value", "double"),
]);

// Drop leftover table from a previous run to keep the example idempotent
await session.sql("DROP TABLE IF EXISTS catalog_demo").collect();

console.log("\nCreating table with schema:", schema.toDDL());
const created = catalog.createTable("catalog_demo", {
  source: "parquet",
  schema,
});
const createdRows = await created.collect();
console.log("createTable result (empty table):");
console.table(createdRows);

// Verify it shows up in listTables
const tables = await catalog.listTables().collect();
console.log("Tables after createTable:");
console.table(tables);

// Clean up
await session.sql("DROP TABLE IF EXISTS catalog_demo").collect();

await session.stop();
console.log("\nDone.");
