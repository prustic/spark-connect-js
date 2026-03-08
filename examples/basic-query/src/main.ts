/**
 * basic-query -- Spark Connect example
 *
 * Start Spark:  docker compose up -d
 * Run:          pnpm build && node dist/main.js
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
  lead,
  ntile,
  cume_dist,
  percent_rank,
  Window,
  type DataFrame,
  // Math
  abs,
  ceil,
  floor,
  sqrt,
  pow,
  log,
  log2,
  log10,
  sin,
  cos,
  degrees,
  radians,
  factorial,
  greatest,
  least,
  hex,
  unhex,
  rand,
  e,
  pi,
  // String
  lower,
  trim,
  concat_ws,
  split,
  lpad,
  rpad,
  reverse,
  repeat,
  length,
  substring,
  instr,
  translate,
  regexp_replace,
  initcap,
  soundex,
  base64,
  unbase64,
  // Datetime
  current_date,
  current_timestamp,
  year,
  month,
  dayofweek,
  hour,
  date_add,
  date_sub,
  datediff,
  months_between,
  date_format,
  to_date,
  to_timestamp,
  unix_timestamp,
  from_unixtime,
  // Aggregate
  min,
  max,
  stddev,
  variance,
  collect_list,
  collect_set,
  first,
  last,
  countDistinct,
  approx_count_distinct,
  corr,
  // Collection / Array / Map
  array,
  array_contains,
  array_distinct,
  array_sort,
  array_union,
  array_intersect,
  array_except,
  size,
  explode,
  struct,
  create_map,
  map_keys,
  map_values,
  // Hash
  md5,
  sha1,
  sha2,
  hash,
  xxhash64,
  crc32,
  // JSON
  to_json,
  get_json_object,
  json_array_length,
  json_object_keys,
  // Bitwise
  bitwise_not,
  shiftleft,
  shiftright,
  // Sort
  asc,
  desc,
  asc_nulls_first,
  desc_nulls_last,
  // Conditional
  coalesce,
  isnull,
  isnan,
  nanvl,
  ifnull,
  typeof_,
  monotonically_increasing_id,
  expr,
} from "@spark-connect-js/node";

const SPARK_REMOTE = process.env["SPARK_REMOTE"] ?? "sc://localhost:15002";

async function main(): Promise<void> {
  const spark = connect(SPARK_REMOTE);

  // 1. SparkSession.range()
  console.log("SparkSession.range()");
  const range = spark.range(10).withColumn("doubled", col("id").multiply(lit(2)));
  await range.show();

  // 2. Filter + Select
  console.log("\nFilter + Select");
  const filtered = range.filter(col("id").gt(lit(5))).select("id", "doubled");
  await filtered.show();

  // 3. Sort / OrderBy
  console.log("\nSort descending");
  const sorted = range.sort(col("id").desc());
  await sorted.show();

  // 4. Limit + Offset
  console.log("\nLimit 3, Offset 2");
  const paged = range.limit(3).offset(2);
  await paged.show();

  // 5. withColumn
  console.log("\nwithColumn");
  const withTripled = range.withColumn("tripled", col("id").multiply(lit(3)));
  await withTripled.show(5);

  // 6. Drop columns
  console.log("\nDrop 'doubled' column");
  const dropped = range.drop("doubled");
  await dropped.show(5);

  // 7. Distinct / dropDuplicates
  console.log("\nDistinct");
  const dupes = spark.sql("SELECT id % 3 AS bucket FROM range(10)");
  const distinct = dupes.distinct();
  await distinct.show();

  // 8. GroupBy + Aggregation
  console.log("\nGroupBy + Count");
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

  // 9. Join
  console.log("\nJoin");
  const departments = spark.sql(`
    SELECT * FROM VALUES
      ('Engineering', 'Building A'),
      ('Marketing',   'Building B'),
      ('Sales',       'Building C')
    AS departments(dept_name, location)
  `);

  const joined = employees.join(departments, col("department").eq(col("dept_name")), "inner");
  await joined.select("name", "department", "location").show();

  // 10. Temp View + SQL query
  console.log("\nTemp View + SQL");
  await employees.createOrReplaceTempView("emp");
  const topEarners = spark.sql(
    "SELECT name, salary FROM emp WHERE salary > 85000 ORDER BY salary DESC",
  );
  await topEarners.show();

  // 11. Explain plan
  console.log("\nExplain Plan");
  const complex = employees
    .filter(col("salary").gt(lit(70000)))
    .select("name", "salary")
    .sort(col("salary").desc());
  const plan = await complex.explain("simple");
  console.log(plan);

  // 12. Collect as JSON
  console.log("\nCollect");
  const rows = await filtered.collect();
  console.log(`Collected ${String(rows.length)} rows:`);
  console.log(JSON.stringify(rows, null, 2));

  // 13. Expression Functions: when / otherwise
  console.log("\nWhen / Otherwise");
  const withTier = employees.withColumn(
    "tier",
    when(col("salary").gte(lit(90000)), lit("senior"))
      .when(col("salary").gte(lit(80000)), lit("mid"))
      .otherwise(lit("junior")),
  );
  await withTier.select("name", "salary", "tier").show();

  // 14. Cast + String Functions
  console.log("\nCast + Upper");
  const casted = employees
    .withColumn("salary_str", cast(col("salary"), "string"))
    .withColumn("upper_name", upper(col("name")));
  await casted.select("upper_name", "salary_str").show();

  // 15. Math Functions
  console.log("\nRound / Aggregates");
  const deptStats = employees
    .groupBy("department")
    .agg(
      count(col("name")).as("headcount"),
      round(avg(col("salary")), 0).as("avg_salary"),
      sum(col("salary")).as("total_salary"),
    );
  await deptStats.show();

  // 16. Schema Inspection
  console.log("\nSchema");
  await employees.printSchema();

  // 17. Catalog API
  console.log("\nCatalog");
  const currentDb = await spark.catalog.currentDatabase();
  console.log(`Current database: ${currentDb}`);

  const tables = spark.catalog.listTables();
  console.log("Tables:");
  await tables.show();

  const empExists = await spark.catalog.tableExists("emp");
  console.log(`Table 'emp' exists: ${String(empExists)}`);

  // 18. Union / Intersect / Except
  console.log("\nUnion / Intersect / Except");
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

  // 19. Column Methods: isNull, isin, like, between
  console.log("\nColumn Methods");
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

  // 20. Window Functions
  console.log("\nWindow Functions");
  const w = Window.partitionBy("department").orderBy(col("salary").desc());
  const ranked = employees
    .withColumn("row_num", row_number().over(w))
    .withColumn("rnk", rank().over(w))
    .withColumn("dense_rnk", dense_rank().over(w))
    .withColumn("prev_salary", lag("salary", 1).over(w));
  await ranked
    .select("name", "department", "salary", "row_num", "rnk", "dense_rnk", "prev_salary")
    .show();

  // 21. Describe
  console.log("\nDescribe");
  const stats = employees.describe("salary");
  await stats.show();

  // 22. FillNA / DropNA
  console.log("\nFillNA / DropNA");
  console.log("Original with nulls:");
  await withNulls.show();
  console.log("After fillna(0):");
  await withNulls.fillna(0).show();
  console.log("After dropna:");
  await withNulls.dropna().show();

  // 23. Count (optimised)
  console.log("\nCount (optimised)");
  const empCount = await employees.count();
  console.log(`Employee count: ${empCount}`);

  // 24. toLocalIterator / forEach
  console.log("\ntoLocalIterator");
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

  // 25. first / head / take
  console.log("\nfirst / head / take");
  const firstRow = await employees.first();
  console.log("first():", firstRow);

  const headRows = await employees.head(2);
  console.log("head(2):", headRows);

  const takeRows = await employees.take(3);
  console.log("take(3):", takeRows);

  // 26. DataFrameReader shortcuts
  console.log("\nDataFrameReader: table()");
  // "emp" temp view was registered in section 10
  const fromTable = spark.read.table("emp");
  await fromTable.show();

  // 27. SparkSession.range() variants
  console.log("\nSparkSession.range() variants");
  console.log("range(5):");
  await spark.range(5).show();
  console.log("range(2, 10, 2):");
  await spark.range(2, 10, 2).show();

  // 28. withColumnRenamed / withColumnsRenamed
  console.log("\nRename Columns");
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

  // 29. selectExpr
  console.log("\nselectExpr");
  const withExprs = employees.selectExpr(
    "name",
    "salary * 1.1 AS raised_salary",
    "upper(department) AS dept_upper",
  );
  await withExprs.show();

  // 30. transform (pipeline composition)
  console.log("\ntransform");
  const addBonus = (df: DataFrame) => df.withColumn("bonus", col("salary").multiply(lit(0.1)));
  const addLevel = (df: DataFrame) =>
    df.withColumn(
      "level",
      when(col("salary").gte(lit(90000)), lit("senior")).otherwise(lit("junior")),
    );
  const pipeline = employees.transform(addBonus).transform(addLevel);
  await pipeline.select("name", "salary", "bonus", "level").show();

  // 31. alias + hint
  console.log("\nalias + hint");
  const aliased = employees.alias("emp");
  const aliasedPlan = await aliased.select("emp.name", "emp.salary").explain("simple");
  console.log("Aliased plan:");
  console.log(aliasedPlan);

  const hinted = employees.hint("broadcast");
  const hintedPlan = await hinted.explain("simple");
  console.log("Broadcast hint plan:");
  console.log(hintedPlan);

  // 32. sortWithinPartitions
  console.log("\nsortWithinPartitions");
  const partSorted = employees.sortWithinPartitions(col("salary").desc());
  await partSorted.show();

  // 33. tail
  console.log("\ntail");
  const lastTwo = await employees.sort(col("salary").asc()).tail(2);
  console.log("Last 2 by salary:", lastTwo);

  // 34. columns / dtypes / isEmpty
  console.log("\ncolumns / dtypes / isEmpty");
  const colNames = await employees.columns();
  console.log("Column names:", colNames);

  const colTypes = await employees.dtypes();
  console.log("Column types:", colTypes);

  const emptyCheck = await employees.filter(col("salary").lt(lit(0))).isEmpty();
  console.log("isEmpty (salary < 0):", emptyCheck);

  const notEmpty = await employees.isEmpty();
  console.log("isEmpty (all employees):", notEmpty);

  // 35. Column sort ordering variants
  console.log("\nSort ordering variants");
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

  // 36. Column ilike / substr
  console.log("\nilike / substr");
  console.log("ilike (case-insensitive):");
  await employees.filter(col("name").ilike("%alice%")).show();

  console.log("substr:");
  await employees
    .withColumn("short_name", col("name").substr(1, 3))
    .select("name", "short_name")
    .show();

  // 37. Bitwise operations
  console.log("\nBitwise operations");
  const nums = spark.range(8);
  await nums
    .withColumn("and_3", col("id").bitwiseAND(lit(3)))
    .withColumn("or_4", col("id").bitwiseOR(lit(4)))
    .withColumn("xor_5", col("id").bitwiseXOR(lit(5)))
    .show();

  // 38. GroupedData min / max
  console.log("\nGroupedData min / max");
  console.log("Min salary by department:");
  await employees.groupBy("department").min("salary").show();
  console.log("Max salary by department:");
  await employees.groupBy("department").max("salary").show();

  // 39. Math Functions
  console.log("\nMath Functions");
  const mathDemo = spark.range(1, 7).withColumn("val", col("id").multiply(lit(10)));
  await mathDemo
    .withColumn("abs_neg", abs(col("id").multiply(lit(-1))))
    .withColumn("ceil_div", ceil(col("id").divide(lit(3))))
    .withColumn("floor_div", floor(col("id").divide(lit(3))))
    .withColumn("sqrt_val", sqrt(col("val")))
    .withColumn("pow_2", pow(col("id"), 2))
    .withColumn("log_val", log(col("val")))
    .withColumn("log2_val", log2(col("val")))
    .withColumn("log10_val", log10(col("val")))
    .select("id", "val", "abs_neg", "ceil_div", "floor_div", "sqrt_val", "pow_2", "log_val")
    .show();

  console.log("Trig + constants:");
  await spark
    .range(1, 4)
    .withColumn("sin_id", sin(col("id")))
    .withColumn("cos_id", cos(col("id")))
    .withColumn("deg", degrees(col("id")))
    .withColumn("rad", radians(lit(180)))
    .withColumn("fact", factorial(col("id")))
    .withColumn("hex_id", hex(col("id")))
    .withColumn("unhex_ff", unhex(lit("FF")))
    .select("id", "sin_id", "cos_id", "deg", "fact", "hex_id", "unhex_ff")
    .show();

  console.log("greatest / least / rand / e / pi:");
  await spark
    .range(1, 4)
    .withColumn("a", col("id").multiply(lit(3)))
    .withColumn("b", col("id").multiply(lit(7)))
    .withColumn("max_ab", greatest("a", "b"))
    .withColumn("min_ab", least("a", "b"))
    .withColumn("random", rand())
    .withColumn("euler", e())
    .withColumn("pi_val", pi())
    .select("id", "a", "b", "max_ab", "min_ab", "random", "euler", "pi_val")
    .show();

  // 40. String Functions
  console.log("\nString Functions");
  const strDemo = spark.sql(`
    SELECT * FROM VALUES
      ('Hello World', 'spark'),
      ('  trim me  ', 'java'),
      ('fooBarBaz',   'scala')
    AS data(text, lang)
  `);
  await strDemo
    .withColumn("lower_text", lower(col("text")))
    .withColumn("trimmed", trim(col("text")))
    .withColumn("len", length(col("text")))
    .withColumn("initcapped", initcap(col("text")))
    .withColumn("reversed", reverse(col("text")))
    .select("text", "lower_text", "trimmed", "len", "initcapped", "reversed")
    .show();

  console.log("concat_ws / split / pad / substring:");
  await strDemo
    .withColumn("merged", concat_ws("-", col("text"), col("lang")))
    .withColumn("words", split(col("text"), " "))
    .withColumn("lpadded", lpad(col("lang"), 10, "*"))
    .withColumn("rpadded", rpad(col("lang"), 10, "*"))
    .withColumn("sub", substring(col("text"), 1, 5))
    .select("text", "merged", "lpadded", "rpadded", "sub")
    .show();

  console.log("instr / translate / regexp_replace / soundex:");
  await strDemo
    .withColumn("pos_o", instr(col("text"), "o"))
    .withColumn("translated", translate(col("text"), "aeiou", "AEIOU"))
    .withColumn("replaced", regexp_replace(col("text"), "[A-Z]", "_"))
    .withColumn("snd", soundex(col("text")))
    .withColumn("b64", base64(col("lang")))
    .withColumn("ub64", unbase64(base64(col("lang"))))
    .withColumn("repeated", repeat(col("lang"), 3))
    .select("text", "pos_o", "translated", "replaced", "snd", "b64", "repeated")
    .show();

  // 41. Datetime Functions
  console.log("\nDatetime Functions");
  const dtDemo = spark.sql(`
    SELECT * FROM VALUES
      ('2024-01-15', '2024-06-30 14:30:00'),
      ('2024-03-20', '2024-12-25 09:15:00'),
      ('2024-07-04', '2024-07-04 00:00:00')
    AS data(date_str, ts_str)
  `);
  const withDates = dtDemo
    .withColumn("dt", to_date(col("date_str")))
    .withColumn("ts", to_timestamp(col("ts_str")));
  await withDates
    .withColumn("yr", year(col("dt")))
    .withColumn("mon", month(col("dt")))
    .withColumn("dow", dayofweek(col("dt")))
    .withColumn("hr", hour(col("ts")))
    .withColumn("formatted", date_format(col("dt"), "MMMM dd, yyyy"))
    .select("date_str", "yr", "mon", "dow", "hr", "formatted")
    .show();

  console.log("date_add / date_sub / datediff / months_between:");
  await withDates
    .withColumn("plus_7d", date_add(col("dt"), 7))
    .withColumn("minus_3d", date_sub(col("dt"), 3))
    .withColumn("diff_days", datediff(col("ts"), col("dt")))
    .withColumn("diff_months", months_between(col("ts"), col("dt")))
    .select("date_str", "ts_str", "plus_7d", "minus_3d", "diff_days", "diff_months")
    .show();

  console.log("unix_timestamp / from_unixtime / current_date / current_timestamp:");
  await withDates
    .withColumn("epoch", unix_timestamp(col("ts")))
    .withColumn("back_str", from_unixtime(unix_timestamp(col("ts")), "yyyy/MM/dd"))
    .withColumn("today", current_date())
    .withColumn("now", current_timestamp())
    .select("ts_str", "epoch", "back_str", "today", "now")
    .show();

  // 42. Expanded Aggregates
  console.log("\nExpanded Aggregates");
  console.log("stddev / variance / corr / min / max:");
  const empWithId = employees.withColumn("emp_id", monotonically_increasing_id());
  await empWithId
    .groupBy("department")
    .agg(
      count(col("name")).as("n"),
      min(col("salary")).as("min_sal"),
      max(col("salary")).as("max_sal"),
      round(stddev(col("salary")), 2).as("stddev_sal"),
      round(variance(col("salary")), 2).as("var_sal"),
      round(corr(col("salary"), col("emp_id")), 4).as("corr_sal_id"),
    )
    .show();

  console.log("collect_list / collect_set:");
  await employees
    .groupBy("department")
    .agg(collect_list(col("name")).as("name_list"), collect_set(col("name")).as("name_set"))
    .show();

  console.log("first / last / countDistinct / approx_count_distinct:");
  await employees
    .groupBy("department")
    .agg(
      first(col("name")).as("first_name"),
      last(col("name")).as("last_name"),
      countDistinct(col("salary")).as("distinct_salaries"),
      approx_count_distinct(col("salary")).as("approx_distinct"),
    )
    .show();

  // 43. Expanded Window Functions
  console.log("\nExpanded Window Functions");
  const w2 = Window.partitionBy("department").orderBy(col("salary").desc());
  await employees
    .withColumn("lead_sal", lead("salary", 1).over(w2))
    .withColumn("ntile_3", ntile(3).over(w2))
    .withColumn("cume", cume_dist().over(w2))
    .withColumn("pct_rank", percent_rank().over(w2))
    .select("name", "department", "salary", "lead_sal", "ntile_3", "cume", "pct_rank")
    .show();

  // 44. Collection / Array / Map
  console.log("\nCollection / Array / Map");
  const arrDemo = spark.sql(`
    SELECT * FROM VALUES
      (ARRAY(1, 2, 3, 2), ARRAY(2, 3, 4)),
      (ARRAY(5, 5, 6),    ARRAY(6, 7, 8)),
      (ARRAY(9, 10),      ARRAY(10, 11, 12))
    AS data(a, b)
  `);
  console.log("array ops (contains, distinct, sort, union, intersect, except, size):");
  await arrDemo
    .withColumn("has_2", array_contains(col("a"), lit(2)))
    .withColumn("a_dist", array_distinct(col("a")))
    .withColumn("a_sorted", array_sort(col("a")))
    .withColumn("a_union_b", array_union(col("a"), col("b")))
    .withColumn("a_inter_b", array_intersect(col("a"), col("b")))
    .withColumn("a_except_b", array_except(col("a"), col("b")))
    .withColumn("a_size", size(col("a")))
    .show(3, false);

  console.log("array() constructor + explode:");
  const arrDemo2 = spark.sql(`
    SELECT * FROM VALUES (1, 10), (2, 20), (3, 30) AS data(x, y)
  `);
  await arrDemo2
    .withColumn("arr", array(col("x"), col("y")))
    .select(col("x"), col("arr"), explode(col("arr")).as("element"))
    .show();

  console.log("struct / create_map / map_keys / map_values:");
  await employees
    .withColumn("info", struct("name", "department"))
    .withColumn("salary_map", create_map(lit("salary"), col("salary")))
    .withColumn("keys", map_keys(create_map(lit("salary"), col("salary"))))
    .withColumn("vals", map_values(create_map(lit("salary"), col("salary"))))
    .select("name", "info", "salary_map", "keys", "vals")
    .show(3, false);

  // 45. Hash Functions
  console.log("\nHash Functions");
  await employees
    .withColumn("md5_name", md5(col("name")))
    .withColumn("sha1_name", sha1(col("name")))
    .withColumn("sha2_name", sha2(col("name"), 256))
    .withColumn("hash_name", hash(col("name")))
    .withColumn("xxh64_name", xxhash64(col("name")))
    .withColumn("crc32_name", crc32(col("name")))
    .select("name", "md5_name", "sha1_name", "hash_name", "crc32_name")
    .show(3, false);

  // 46. JSON Functions
  console.log("\nJSON Functions");
  const jsonDemo = spark.sql(`
    SELECT * FROM VALUES
      ('{"name":"Alice","age":30}'),
      ('{"name":"Bob","age":25}'),
      ('[1,2,3]')
    AS data(json_str)
  `);
  await jsonDemo
    .withColumn("as_struct", to_json(struct(lit("hello").as("greeting"))))
    .withColumn("get_name", get_json_object(col("json_str"), "$.name"))
    .withColumn("arr_len", json_array_length(lit("[1,2,3]")))
    .withColumn("obj_keys", json_object_keys(col("json_str")))
    .select("json_str", "get_name", "arr_len", "obj_keys")
    .show(3, false);

  // 47. Bitwise Functions
  console.log("\nBitwise Functions");
  await spark
    .range(1, 9)
    .withColumn("not_id", bitwise_not(col("id")))
    .withColumn("shl_2", shiftleft(col("id"), 2))
    .withColumn("shr_1", shiftright(col("id"), 1))
    .show();

  // 48. Sort Functions
  console.log("\nSort Functions");
  const sortDemo = spark.sql(`
    SELECT * FROM VALUES
      ('Alice', 90000),
      ('Bob',   NULL),
      ('Carol', 70000),
      ('Dave',  NULL),
      ('Eve',   95000)
    AS data(name, salary)
  `);
  console.log("asc(name):");
  await sortDemo.sort(asc("name")).show();
  console.log("desc(salary):");
  await sortDemo.sort(desc("salary")).show();
  console.log("asc_nulls_first(salary):");
  await sortDemo.sort(asc_nulls_first("salary")).show();
  console.log("desc_nulls_last(salary):");
  await sortDemo.sort(desc_nulls_last("salary")).show();

  // 49. Conditional / Utility
  console.log("\nConditional / Utility");
  const condDemo = spark.sql(`
    SELECT * FROM VALUES
      (1,    NULL,  3),
      (NULL, 20,    NULL),
      (5,    CAST('NaN' AS DOUBLE), 7)
    AS data(a, b, c)
  `);
  console.log("coalesce / isnull / isnan / nanvl / ifnull:");
  await condDemo
    .withColumn("coal", coalesce(col("a"), col("b"), col("c")))
    .withColumn("a_null", isnull(col("a")))
    .withColumn("b_nan", isnan(col("b")))
    .withColumn("b_safe", nanvl(col("b"), lit(0)))
    .withColumn("a_or_c", ifnull(col("a"), col("c")))
    .select("a", "b", "c", "coal", "a_null", "b_nan", "b_safe", "a_or_c")
    .show();

  console.log("typeof / monotonically_increasing_id / expr:");
  await employees
    .withColumn("type_sal", typeof_(col("salary")))
    .withColumn("mono_id", monotonically_increasing_id())
    .withColumn("expr_calc", expr("salary * 2"))
    .select("name", "salary", "type_sal", "mono_id", "expr_calc")
    .show();

  // Cleanup
  await spark.stop();
  console.log("\nSession stopped.");
}

main().catch((err: unknown) => {
  console.error(err);
  process.exit(1);
});
