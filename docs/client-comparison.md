# Spark Connect Client Comparison

A comprehensive feature comparison of all known Spark Connect clients against the PySpark reference implementation. Every method and function from the PySpark SQL API reference is tracked here.

> **Data sources**: PySpark API Reference (4.0+), [spark-connect-go](https://github.com/apache/spark-connect-go), [spark-connect-rs README](https://github.com/apache/spark-connect-rust/blob/master/README.md), [spark-connect-swift](https://github.com/apache/spark-connect-swift), [spark-connect-dotnet](https://github.com/nicklhw/spark-connect-dotnet).

> **Why "spark-js"?** Unlike other Spark Connect clients named `spark-connect-{lang}`, this project is more than a client in a single language. spark-js is a platform that exposes Spark Connect through idiomatic adapters for JavaScript frameworks — currently Node.js, with planned support for NestJS, Express, Next.js, Nuxt.js, Fastify, Astro, Electron, and more. The name reflects this broader scope.

| Client                   | Language   | Maturity     | License    | Stars           | Version     |
| ------------------------ | ---------- | ------------ | ---------- | --------------- | ----------- |
| **PySpark**              | Python     | Production   | Apache 2.0 | (part of Spark) | 4.0+        |
| **spark-connect-go**     | Go         | Experimental | Apache 2.0 | 248             | v0.1.0      |
| **spark-connect-rs**     | Rust       | Active       | Apache 2.0 | —               | —           |
| **spark-connect-swift**  | Swift      | Experimental | Apache 2.0 | 30              | v0.5.0      |
| **spark-connect-dotnet** | C#         | Third-party  | MIT        | 26              | v4.0.0      |
| **spark-js**             | TypeScript | Active       | —          | —               | pre-release |

---

## 1. SparkSession

| Feature                                  | PySpark  | Go  | Rust | Swift | .NET | spark-js |
| ---------------------------------------- | -------- | --- | ---- | ----- | ---- | -------- |
| `builder` / `remote()`                   | ✅       | ✅  | ✅   | ✅    | ✅   | ✅       |
| `builder.appName(name)`                  | ✅       | ✅  | ✅   | —     | ✅   | ❌       |
| `builder.config(key, value)`             | ✅       | ✅  | ✅   | —     | ✅   | ❌       |
| `builder.master(master)`                 | ✅       | ✅  | —    | —     | —    | ❌       |
| `builder.enableHiveSupport()`            | ✅       | —   | —    | —     | —    | ❌       |
| `getOrCreate`                            | ✅       | ✅  | ✅   | ✅    | ✅   | ✅       |
| `builder.create()` (Connect-only)        | ✅       | —   | —    | —     | —    | ❌       |
| `active()` (static)                      | ✅       | —   | —    | —     | —    | ❌       |
| `getActiveSession()`                     | ✅       | —   | —    | —     | —    | ❌       |
| `sql(query, args?)`                      | ✅       | ✅  | ✅   | ✅    | ✅   | ✅       |
| `read` (DataFrameReader)                 | ✅       | ✅  | ✅   | ✅    | ✅   | ✅       |
| `table(name)`                            | ✅       | ✅  | ✅   | ✅    | ✅   | ✅       |
| `range(start, end, step, numPartitions)` | ✅       | ✅  | ✅   | ✅    | ✅   | ❌       |
| `createDataFrame`                        | ✅       | ✅  | ✅   | ✅    | ✅   | ✅       |
| `catalog`                                | ✅       | —   | ✅   | ✅    | —    | ✅       |
| `readStream`                             | ✅       | —   | ✅   | ✅    | —    | ❌       |
| `streams` (StreamingQueryManager)        | ✅       | —   | ✅   | —     | —    | ❌       |
| `stop()`                                 | ✅       | ✅  | —    | ✅    | ✅   | ✅       |
| `newSession()`                           | ✅       | —   | —    | —     | —    | ❌       |
| `conf` (RuntimeConfig)                   | ✅       | —   | ✅   | —     | ✅   | ❌       |
| `version`                                | ✅       | —   | ✅   | —     | —    | ❌       |
| `sparkContext`                           | ✅ (JVM) | —   | —    | —     | —    | N/A      |
| `addArtifact / addArtifacts`             | ✅       | —   | —    | —     | —    | ❌       |
| `addTag(tag)`                            | ✅       | —   | ✅   | —     | —    | ❌       |
| `removeTag(tag)`                         | ✅       | —   | ✅   | —     | —    | ❌       |
| `getTags()`                              | ✅       | —   | ✅   | —     | —    | ❌       |
| `clearTags()`                            | ✅       | —   | ✅   | —     | —    | ❌       |
| `interruptAll()`                         | ✅       | ✅  | ✅   | —     | —    | ❌       |
| `interruptOperation(op_id)`              | ✅       | ✅  | ✅   | —     | —    | ❌       |
| `interruptTag(tag)`                      | ✅       | ✅  | ✅   | —     | —    | ❌       |
| `udf` (UDFRegistration)                  | ✅       | —   | —    | —     | —    | ❌       |
| `udtf` (UDTFRegistration)                | ✅       | —   | —    | —     | —    | ❌       |
| `tvf` (TableValuedFunction)              | ✅       | —   | —    | —     | —    | ❌       |
| `dataSource` (DataSourceRegistration)    | ✅       | —   | —    | —     | —    | ❌       |
| `profile`                                | ✅       | —   | —    | —     | —    | ❌       |
| `client` (Connect-only)                  | ✅       | —   | ✅   | —     | —    | ❌       |
| `copyFromLocalToFs` (Connect-only)       | ✅       | —   | —    | —     | —    | ❌       |
| `registerProgressHandler` (Connect-only) | ✅       | —   | —    | —     | —    | ❌       |
| `removeProgressHandler` (Connect-only)   | ✅       | —   | —    | —     | —    | ❌       |
| `clearProgressHandlers` (Connect-only)   | ✅       | —   | —    | —     | —    | ❌       |
| `is_remote()` (module function)          | ✅       | —   | —    | —     | —    | ❌       |

### RuntimeConfig

| Method              | PySpark | Go  | Rust | Swift | .NET | spark-js |
| ------------------- | ------- | --- | ---- | ----- | ---- | -------- |
| `get(key)`          | ✅      | —   | ✅   | —     | ✅   | ❌       |
| `set(key, value)`   | ✅      | —   | ✅   | —     | ✅   | ❌       |
| `unset(key)`        | ✅      | —   | ✅   | —     | ✅   | ❌       |
| `getAll`            | ✅      | —   | —    | —     | —    | ❌       |
| `isModifiable(key)` | ✅      | —   | ✅   | —     | —    | ❌       |

---

## 2. DataFrame — Transformations

| Method                                                | PySpark     | Go  | Rust | Swift | .NET | spark-js             |
| ----------------------------------------------------- | ----------- | --- | ---- | ----- | ---- | -------------------- |
| `select(*cols)`                                       | ✅          | ✅  | ✅   | ✅    | ✅   | ✅                   |
| `selectExpr(*expr)`                                   | ✅          | ✅  | ✅   | ✅    | —    | ❌                   |
| `filter(condition)` / `where`                         | ✅          | ✅  | ✅   | ✅    | ✅   | ✅                   |
| `groupBy(*cols)`                                      | ✅          | ✅  | ✅   | ✅    | ✅   | ✅                   |
| `agg(*exprs)`                                         | ✅          | ✅  | ✅   | ✅    | ✅   | ✅ (via GroupedData) |
| `sort(*cols)` / `orderBy`                             | ✅          | ✅  | ✅   | ✅    | ✅   | ✅                   |
| `limit(num)`                                          | ✅          | ✅  | ✅   | ✅    | ✅   | ✅                   |
| `offset(num)`                                         | ✅          | —   | ✅   | —     | —    | ✅                   |
| `join(other, on, how)`                                | ✅          | ✅  | ✅   | ✅    | ✅   | ✅                   |
| `crossJoin(other)`                                    | ✅          | ✅  | ✅   | ✅    | ✅   | ✅                   |
| `lateralJoin(other, on, how)`                         | ✅          | —   | —    | ✅    | —    | ❌                   |
| `drop(*cols)`                                         | ✅          | ✅  | ✅   | ✅    | ✅   | ✅                   |
| `withColumn(name, col)`                               | ✅          | ✅  | ✅   | ✅    | ✅   | ✅                   |
| `withColumns(*colsMap)`                               | ✅          | ✅  | ✅   | ✅    | —    | ✅                   |
| `withColumnRenamed(existing, new)`                    | ✅          | ✅  | ✅   | ✅    | ✅   | ❌                   |
| `withColumnsRenamed(colsMap)`                         | ✅          | ✅  | ✅   | ✅    | —    | ❌                   |
| `toDF(*cols)` (rename all)                            | ✅          | —   | ✅   | ✅    | —    | ✅                   |
| `alias(name)`                                         | ✅          | ✅  | ✅   | ✅    | —    | ❌                   |
| `distinct()`                                          | ✅          | ✅  | ✅   | ✅    | ✅   | ✅                   |
| `dropDuplicates(subset?)`                             | ✅          | ✅  | ✅   | ✅    | ✅   | ✅                   |
| `drop_duplicates` (alias)                             | ✅          | —   | ✅   | —     | —    | ❌                   |
| `dropDuplicatesWithinWatermark(subset?)`              | ✅          | —   | ✅   | —     | —    | ❌                   |
| `union(other)`                                        | ✅          | ✅  | ✅   | ✅    | ✅   | ✅                   |
| `unionAll(other)`                                     | ✅          | ✅  | ✅   | ✅    | ✅   | ✅                   |
| `unionByName(other, allowMissing?)`                   | ✅          | ✅  | ✅   | ✅    | ✅   | ✅                   |
| `intersect(other)`                                    | ✅          | ✅  | ✅   | ✅    | ✅   | ✅                   |
| `intersectAll(other)`                                 | ✅          | ✅  | ✅   | ✅    | ✅   | ✅                   |
| `subtract(other)` / `except`                          | ✅          | ✅  | ✅   | ✅    | ✅   | ✅                   |
| `exceptAll(other)`                                    | ✅          | ✅  | ✅   | ✅    | ✅   | ✅                   |
| `sample(fraction, withReplacement?, seed?)`           | ✅          | ✅  | ✅   | ✅    | ✅   | ✅                   |
| `sampleBy(col, fractions, seed?)`                     | ✅          | —   | —    | —     | —    | ❌                   |
| `repartition(numPartitions, *cols)`                   | ✅          | ✅  | ✅   | ✅    | ✅   | ❌                   |
| `repartitionByRange(numPartitions, *cols)`            | ✅          | ✅  | —    | ✅    | —    | ❌                   |
| `coalesce(numPartitions)`                             | ✅          | ✅  | ✅   | ✅    | ✅   | ❌                   |
| `cache()`                                             | ✅          | ✅  | ✅   | ✅    | ✅   | ❌                   |
| `persist(storageLevel?)`                              | ✅          | ✅  | ✅   | ✅    | ✅   | ❌                   |
| `unpersist(blocking?)`                                | ✅          | ✅  | ✅   | ✅    | ✅   | ❌                   |
| `storageLevel` (property)                             | ✅          | ✅  | ✅   | ✅    | —    | ❌                   |
| `checkpoint(eager?)`                                  | ✅          | —   | —    | ✅    | —    | ❌                   |
| `localCheckpoint(eager?, storageLevel?)`              | ✅          | —   | —    | ✅    | —    | ❌                   |
| `describe(*cols)`                                     | ✅          | ✅  | ✅   | ✅    | ✅   | ✅                   |
| `summary(*statistics)`                                | ✅          | ✅  | ✅   | ✅    | —    | ❌                   |
| `fillna(value, subset?)` / `na.fill`                  | ✅          | ✅  | ✅   | ✅    | ✅   | ✅                   |
| `dropna(how?, thresh?, subset?)` / `na.drop`          | ✅          | ✅  | ✅   | ✅    | ✅   | ✅                   |
| `replace(to_replace, value?, subset?)` / `na.replace` | ✅          | ✅  | ✅   | ✅    | —    | ❌                   |
| `cube(*cols)`                                         | ✅          | ✅  | ✅   | ✅    | —    | ❌                   |
| `rollup(*cols)`                                       | ✅          | ✅  | ✅   | ✅    | —    | ❌                   |
| `groupingSets(groupingSets, *cols)`                   | ✅          | —   | —    | —     | —    | ❌                   |
| `melt(ids, values, ...)` / `unpivot`                  | ✅          | ✅  | ✅   | ✅    | —    | ❌                   |
| `transpose(indexColumn?)`                             | ✅          | —   | —    | ✅    | —    | ❌                   |
| `hint(name, *parameters)`                             | ✅          | —   | ✅   | ✅    | —    | ❌                   |
| `transform(func, *args, **kwargs)`                    | ✅          | —   | ✅   | —     | —    | ❌                   |
| `observe(observation, *exprs)`                        | ✅          | —   | —    | —     | —    | ❌                   |
| `to(schema)`                                          | ✅          | —   | ✅   | —     | —    | ❌                   |
| `withMetadata(columnName, metadata)`                  | ✅          | ✅  | ✅   | —     | —    | ❌                   |
| `withWatermark(eventTime, delayThreshold)`            | ✅          | ✅  | ✅   | ✅    | —    | ❌                   |
| `sortWithinPartitions(*cols)`                         | ✅          | ✅  | ✅   | ✅    | —    | ❌                   |
| `randomSplit(weights, seed?)`                         | ✅          | ✅  | ✅   | ✅    | —    | ❌                   |
| `colRegex(colName)`                                   | ✅          | —   | ✅   | —     | —    | ❌                   |
| `mapInPandas(func, schema)`                           | ✅ (Python) | —   | —    | —     | —    | N/A                  |
| `mapInArrow(func, schema)`                            | ✅ (Python) | —   | —    | —     | —    | N/A                  |
| `asTable()` (table argument for TVF)                  | ✅          | —   | —    | —     | —    | ❌                   |

---

## 3. DataFrame — Actions & Properties

| Method                                   | PySpark     | Go  | Rust | Swift | .NET | spark-js           |
| ---------------------------------------- | ----------- | --- | ---- | ----- | ---- | ------------------ |
| `collect()`                              | ✅          | ✅  | ✅   | ✅    | ✅   | ✅                 |
| `show(n?, truncate?, vertical?)`         | ✅          | ✅  | ✅   | ✅    | ✅   | ✅                 |
| `count()`                                | ✅          | ✅  | ✅   | ✅    | ✅   | ✅                 |
| `first()`                                | ✅          | ✅  | ✅   | ✅    | ✅   | ✅                 |
| `head(n?)`                               | ✅          | ✅  | ✅   | ✅    | —    | ✅                 |
| `take(num)`                              | ✅          | ✅  | ✅   | ✅    | ✅   | ✅                 |
| `tail(num)`                              | ✅          | ✅  | ✅   | ✅    | —    | ❌                 |
| `toLocalIterator(prefetchPartitions?)`   | ✅          | —   | —    | —     | —    | ✅                 |
| `foreach(f)`                             | ✅          | —   | —    | —     | —    | ✅                 |
| `foreachPartition(f)`                    | ✅          | —   | —    | —     | —    | ❌                 |
| `schema`                                 | ✅          | ✅  | ✅   | ✅    | ✅   | ✅                 |
| `printSchema(level?)`                    | ✅          | ✅  | ✅   | ✅    | ✅   | ✅                 |
| `explain(extended?, mode?)`              | ✅          | ✅  | ✅   | ✅    | ✅   | ✅                 |
| `columns` (property)                     | ✅          | ✅  | ✅   | ✅    | ✅   | ❌                 |
| `dtypes` (property)                      | ✅          | —   | ✅   | ✅    | —    | ❌                 |
| `isEmpty()`                              | ✅          | —   | ✅   | ✅    | —    | ❌                 |
| `isLocal()`                              | ✅          | —   | ✅   | —     | —    | ❌                 |
| `isStreaming` (property)                 | ✅          | —   | ✅   | —     | —    | ❌                 |
| `sparkSession` (property)                | ✅          | —   | ✅   | —     | —    | ❌                 |
| `executionInfo` (property, Connect-only) | ✅          | —   | —    | —     | —    | ❌                 |
| `na` (DataFrameNaFunctions accessor)     | ✅          | ✅  | ✅   | ✅    | —    | ❌ (methods on df) |
| `stat` (DataFrameStatFunctions accessor) | ✅          | ✅  | ✅   | —     | —    | ❌                 |
| `toArrow()`                              | ✅          | ✅  | ✅   | —     | —    | ❌                 |
| `toJSON(use_unicode?)`                   | ✅          | —   | ✅   | ✅    | —    | ❌                 |
| `toPandas()`                             | ✅ (Python) | —   | —    | —     | —    | N/A                |
| `pandas_api(index_col?)`                 | ✅ (Python) | —   | —    | —     | —    | N/A                |
| `rdd` (property)                         | ✅ (JVM)    | —   | —    | —     | —    | N/A                |
| `plot` (PySparkPlotAccessor)             | ✅ (Python) | —   | —    | —     | —    | N/A                |
| `createTempView(name)`                   | ✅          | ✅  | ✅   | ✅    | ✅   | ❌                 |
| `createOrReplaceTempView(name)`          | ✅          | ✅  | ✅   | ✅    | ✅   | ✅                 |
| `createGlobalTempView(name)`             | ✅          | ✅  | ✅   | ✅    | ✅   | ❌                 |
| `createOrReplaceGlobalTempView(name)`    | ✅          | ✅  | —    | ✅    | —    | ❌                 |
| `registerTempTable(name)` (deprecated)   | ✅          | —   | ✅   | —     | —    | N/A                |
| `exists()` (EXISTS subquery)             | ✅          | —   | —    | —     | —    | ❌                 |
| `scalar()` (SCALAR subquery)             | ✅          | —   | —    | —     | —    | ❌                 |
| `metadataColumn(colName)`                | ✅          | —   | —    | —     | —    | ❌                 |
| `inputFiles()`                           | ✅          | —   | ✅   | —     | —    | ❌                 |
| `write` (DataFrameWriter)                | ✅          | ✅  | ✅   | ✅    | ✅   | ✅                 |
| `writeTo(table)` (DataFrameWriterV2)     | ✅          | —   | ✅   | ✅    | —    | ❌                 |
| `writeStream` (DataStreamWriter)         | ✅          | —   | ✅   | ✅    | —    | ❌                 |
| `mergeInto(table, condition)`            | ✅          | —   | —    | ✅    | —    | ❌                 |

### DataFrameStatFunctions (via `.stat`)

| Method                                              | PySpark | Go  | Rust | Swift | .NET | spark-js |
| --------------------------------------------------- | ------- | --- | ---- | ----- | ---- | -------- |
| `approxQuantile(col, probabilities, relativeError)` | ✅      | ✅  | ✅   | —     | —    | ❌       |
| `corr(col1, col2, method?)`                         | ✅      | ✅  | ✅   | —     | —    | ❌       |
| `cov(col1, col2)`                                   | ✅      | ✅  | ✅   | —     | —    | ❌       |
| `crosstab(col1, col2)`                              | ✅      | ✅  | ✅   | —     | —    | ❌       |
| `freqItems(cols, support?)`                         | ✅      | ✅  | ✅   | —     | —    | ❌       |
| `sampleBy(col, fractions, seed?)`                   | ✅      | —   | —    | —     | —    | ❌       |

### DataFrame comparison

| Method                 | PySpark | Go  | Rust | Swift | .NET | spark-js |
| ---------------------- | ------- | --- | ---- | ----- | ---- | -------- |
| `sameSemantics(other)` | ✅      | ✅  | ✅   | —     | —    | ❌       |
| `semanticHash()`       | ✅      | ✅  | ✅   | —     | —    | ❌       |

---

## 4. Column

| Method                                    | PySpark | Go  | Rust | Swift | .NET | spark-js          |
| ----------------------------------------- | ------- | --- | ---- | ----- | ---- | ----------------- |
| `alias(*alias)` / `as` / `name`           | ✅      | ✅  | ✅   | ✅    | ✅   | ✅                |
| `cast(dataType)` / `astype`               | ✅      | ✅  | ✅   | ✅    | ✅   | ✅                |
| `try_cast(dataType)`                      | ✅      | —   | —    | —     | —    | ❌                |
| `asc()`                                   | ✅      | ✅  | ✅   | ✅    | ✅   | ✅                |
| `desc()`                                  | ✅      | ✅  | ✅   | ✅    | ✅   | ✅                |
| `asc_nulls_first()`                       | ✅      | —   | ✅   | —     | —    | ❌                |
| `asc_nulls_last()`                        | ✅      | —   | ✅   | —     | —    | ❌                |
| `desc_nulls_first()`                      | ✅      | —   | ✅   | —     | —    | ❌                |
| `desc_nulls_last()`                       | ✅      | —   | ✅   | —     | —    | ❌                |
| `== / != / > / < / >= / <=`               | ✅      | ✅  | ✅   | ✅    | ✅   | ✅                |
| `eqNullSafe(other)`                       | ✅      | —   | —    | —     | —    | ❌                |
| `& / \| (and / or)`                       | ✅      | ✅  | ✅   | ✅    | ✅   | ✅                |
| `~ (not / inversion)`                     | ✅      | —   | ✅   | —     | —    | ❌                |
| `+ / - / * / /`                           | ✅      | ✅  | ✅   | ✅    | ✅   | ✅                |
| `% (mod)`                                 | ✅      | —   | ✅   | —     | —    | ❌                |
| `** (power)`                              | ✅      | —   | —    | —     | —    | ❌                |
| `isNull()`                                | ✅      | ✅  | ✅   | ✅    | ✅   | ✅                |
| `isNotNull()`                             | ✅      | ✅  | ✅   | ✅    | ✅   | ✅                |
| `isNaN()`                                 | ✅      | —   | ✅   | —     | —    | ❌                |
| `isin(*cols)`                             | ✅      | ✅  | ✅   | ✅    | ✅   | ✅                |
| `between(lowerBound, upperBound)`         | ✅      | —   | —    | —     | —    | ✅                |
| `like(other)`                             | ✅      | ✅  | ✅   | ✅    | ✅   | ✅                |
| `rlike(other)`                            | ✅      | —   | ✅   | —     | —    | ✅                |
| `ilike(other)`                            | ✅      | —   | ✅   | —     | —    | ❌                |
| `startswith(other)`                       | ✅      | ✅  | ✅   | ✅    | ✅   | ✅                |
| `endswith(other)`                         | ✅      | ✅  | ✅   | ✅    | ✅   | ✅                |
| `contains(other)`                         | ✅      | —   | ✅   | —     | —    | ✅                |
| `substr(startPos, length)`                | ✅      | —   | ✅   | —     | —    | ❌                |
| `over(window)`                            | ✅      | —   | ✅   | ✅    | ✅   | ✅                |
| `when(condition, value)`                  | ✅      | —   | —    | —     | —    | ❌ (use function) |
| `otherwise(value)`                        | ✅      | —   | —    | —     | —    | ❌ (use function) |
| `bitwiseAND(other)`                       | ✅      | —   | ✅   | —     | —    | ❌                |
| `bitwiseOR(other)`                        | ✅      | —   | ✅   | —     | —    | ❌                |
| `bitwiseXOR(other)`                       | ✅      | —   | ✅   | —     | —    | ❌                |
| `getField(name)`                          | ✅      | —   | —    | —     | —    | ❌                |
| `getItem(key)`                            | ✅      | —   | —    | —     | —    | ❌                |
| `dropFields(*fieldNames)`                 | ✅      | —   | ✅   | —     | —    | ❌                |
| `withField(fieldName, col)`               | ✅      | —   | ✅   | —     | —    | ❌                |
| `outer()`                                 | ✅      | —   | —    | —     | —    | ❌                |
| `transform(f)`                            | ✅      | —   | —    | —     | —    | ❌                |
| `__getattr__(name)` / `__getitem__(item)` | ✅      | —   | —    | —     | —    | ❌                |

---

## 5. GroupedData

| Method                            | PySpark     | Go  | Rust | Swift | .NET | spark-js |
| --------------------------------- | ----------- | --- | ---- | ----- | ---- | -------- |
| `agg(*exprs)`                     | ✅          | ✅  | ✅   | ✅    | ✅   | ✅       |
| `count()`                         | ✅          | ✅  | ✅   | ✅    | ✅   | ✅       |
| `sum(*cols)`                      | ✅          | ✅  | ✅   | ✅    | ✅   | ✅       |
| `avg(*cols)` / `mean`             | ✅          | ✅  | ✅   | ✅    | ✅   | ✅       |
| `min(*cols)`                      | ✅          | ✅  | ✅   | ✅    | ✅   | ❌       |
| `max(*cols)`                      | ✅          | ✅  | ✅   | ✅    | ✅   | ❌       |
| `pivot(pivot_col, values?)`       | ✅          | —   | ✅   | ✅    | ✅   | ❌       |
| `apply(udf)`                      | ✅ (Python) | —   | —    | —     | —    | N/A      |
| `applyInPandas(func, schema)`     | ✅ (Python) | —   | —    | —     | —    | N/A      |
| `applyInArrow(func, schema)`      | ✅ (Python) | —   | —    | —     | —    | N/A      |
| `applyInPandasWithState(...)`     | ✅ (Python) | —   | —    | —     | —    | N/A      |
| `transformWithStateInPandas(...)` | ✅ (Python) | —   | —    | —     | —    | N/A      |
| `cogroup(other)`                  | ✅ (Python) | —   | —    | —     | —    | N/A      |

### PandasCogroupedOps

| Method                        | PySpark     | Go  | Rust | Swift | .NET | spark-js |
| ----------------------------- | ----------- | --- | ---- | ----- | ---- | -------- |
| `applyInPandas(func, schema)` | ✅ (Python) | —   | —    | —     | —    | N/A      |
| `applyInArrow(func, schema)`  | ✅ (Python) | —   | —    | —     | —    | N/A      |

---

## 6. Window

| Feature                               | PySpark | Go  | Rust | Swift | .NET | spark-js |
| ------------------------------------- | ------- | --- | ---- | ----- | ---- | -------- |
| `Window.partitionBy(*cols)`           | ✅      | —   | ✅   | ✅    | ✅   | ✅       |
| `Window.orderBy(*cols)`               | ✅      | —   | ✅   | ✅    | ✅   | ✅       |
| `Window.rowsBetween(start, end)`      | ✅      | —   | ✅   | ✅    | ✅   | ✅       |
| `Window.rangeBetween(start, end)`     | ✅      | —   | ✅   | ✅    | ✅   | ✅       |
| `Window.currentRow`                   | ✅      | —   | ✅   | ✅    | ✅   | ✅       |
| `Window.unboundedPreceding`           | ✅      | —   | ✅   | ✅    | ✅   | ✅       |
| `Window.unboundedFollowing`           | ✅      | —   | ✅   | ✅    | ✅   | ✅       |
| `WindowSpec.partitionBy(*cols)`       | ✅      | —   | ✅   | ✅    | ✅   | ✅       |
| `WindowSpec.orderBy(*cols)`           | ✅      | —   | ✅   | ✅    | ✅   | ✅       |
| `WindowSpec.rowsBetween(start, end)`  | ✅      | —   | ✅   | ✅    | ✅   | ✅       |
| `WindowSpec.rangeBetween(start, end)` | ✅      | —   | ✅   | ✅    | ✅   | ✅       |

---

## 7. DataFrameReader

| Method                          | PySpark | Go  | Rust | Swift | .NET | spark-js |
| ------------------------------- | ------- | --- | ---- | ----- | ---- | -------- |
| `format(source)`                | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `option(key, value)`            | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `options(**options)`            | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `schema(schema)`                | ✅      | ✅  | ✅   | ✅    | ✅   | ❌       |
| `load(path?, format?, schema?)` | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `table(tableName)`              | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `csv(path, ...)`                | ✅      | —   | ✅   | ✅    | ✅   | ❌       |
| `json(path, ...)`               | ✅      | —   | ✅   | ✅    | ✅   | ❌       |
| `parquet(*paths, ...)`          | ✅      | —   | ✅   | ✅    | ✅   | ❌       |
| `orc(path, ...)`                | ✅      | —   | ✅   | ✅    | ✅   | ❌       |
| `text(paths, ...)`              | ✅      | —   | ✅   | —     | ✅   | ❌       |
| `jdbc(url, table, ...)`         | ✅      | —   | —    | —     | —    | ❌       |

---

## 8. DataFrameWriter

| Method                                   | PySpark | Go  | Rust | Swift | .NET | spark-js |
| ---------------------------------------- | ------- | --- | ---- | ----- | ---- | -------- |
| `format(source)`                         | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `mode(saveMode)`                         | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `option(key, value)`                     | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `options(**options)`                     | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `partitionBy(*cols)`                     | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `sortBy(col, *cols)`                     | ✅      | ✅  | ✅   | ✅    | —    | ✅       |
| `bucketBy(numBuckets, col, *cols)`       | ✅      | ✅  | ✅   | ✅    | —    | ❌       |
| `save(path?, format?, mode?, ...)`       | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `saveAsTable(name, format?, mode?, ...)` | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `insertInto(tableName, overwrite?)`      | ✅      | ✅  | ✅   | ✅    | —    | ❌       |
| `csv(path, ...)`                         | ✅      | —   | ✅   | ✅    | ✅   | ❌       |
| `json(path, ...)`                        | ✅      | —   | ✅   | ✅    | ✅   | ❌       |
| `parquet(path, ...)`                     | ✅      | —   | ✅   | ✅    | ✅   | ❌       |
| `orc(path, ...)`                         | ✅      | —   | ✅   | ✅    | ✅   | ❌       |
| `text(path, ...)`                        | ✅      | —   | ✅   | —     | ✅   | ❌       |
| `jdbc(url, table, ...)`                  | ✅      | —   | —    | —     | —    | ❌       |

---

## 9. DataFrameWriterV2

| Method                           | PySpark | Go  | Rust | Swift | .NET | spark-js |
| -------------------------------- | ------- | --- | ---- | ----- | ---- | -------- |
| `using(provider)`                | ✅      | —   | ✅   | ✅    | —    | ❌       |
| `option(key, value)`             | ✅      | —   | ✅   | ✅    | —    | ❌       |
| `options(**options)`             | ✅      | —   | ✅   | ✅    | —    | ❌       |
| `tableProperty(property, value)` | ✅      | —   | ✅   | ✅    | —    | ❌       |
| `partitionedBy(col, *cols)`      | ✅      | —   | ✅   | ✅    | —    | ❌       |
| `create()`                       | ✅      | —   | ✅   | ✅    | —    | ❌       |
| `replace()`                      | ✅      | —   | ✅   | ✅    | —    | ❌       |
| `createOrReplace()`              | ✅      | —   | ✅   | ✅    | —    | ❌       |
| `append()`                       | ✅      | —   | ✅   | ✅    | —    | ❌       |
| `overwrite(condition)`           | ✅      | —   | ✅   | ✅    | —    | ❌       |
| `overwritePartitions()`          | ✅      | —   | ✅   | ✅    | —    | ❌       |

---

## 10. MergeIntoWriter

| Method                               | PySpark | Go  | Rust | Swift | .NET | spark-js |
| ------------------------------------ | ------- | --- | ---- | ----- | ---- | -------- |
| `whenMatched(condition?)`            | ✅      | —   | —    | ✅    | —    | ❌       |
| `whenNotMatched(condition?)`         | ✅      | —   | —    | ✅    | —    | ❌       |
| `whenNotMatchedBySource(condition?)` | ✅      | —   | —    | ✅    | —    | ❌       |
| `withSchemaEvolution()`              | ✅      | —   | —    | ✅    | —    | ❌       |
| `merge()`                            | ✅      | —   | —    | ✅    | —    | ❌       |

---

## 11. Catalog

| Method                                   | PySpark | Go  | Rust | Swift | .NET | spark-js |
| ---------------------------------------- | ------- | --- | ---- | ----- | ---- | -------- |
| `currentDatabase()`                      | ✅      | —   | ✅   | ✅    | —    | ✅       |
| `setCurrentDatabase(dbName)`             | ✅      | —   | ✅   | ✅    | —    | ✅       |
| `currentCatalog()`                       | ✅      | —   | ✅   | —     | —    | ❌       |
| `setCurrentCatalog(catalogName)`         | ✅      | —   | ✅   | —     | —    | ❌       |
| `listDatabases(pattern?)`                | ✅      | —   | ✅   | ✅    | —    | ✅       |
| `listTables(dbName?, pattern?)`          | ✅      | —   | ✅   | ✅    | —    | ✅       |
| `listColumns(tableName, dbName?)`        | ✅      | —   | ✅   | ✅    | —    | ✅       |
| `listFunctions(dbName?, pattern?)`       | ✅      | —   | ✅   | ✅    | —    | ❌       |
| `listCatalogs(pattern?)`                 | ✅      | —   | ✅   | —     | —    | ❌       |
| `databaseExists(dbName)`                 | ✅      | —   | ✅   | ✅    | —    | ✅       |
| `tableExists(tableName, dbName?)`        | ✅      | —   | ✅   | ✅    | —    | ✅       |
| `functionExists(functionName, dbName?)`  | ✅      | —   | ✅   | ✅    | —    | ❌       |
| `getDatabase(dbName)`                    | ✅      | —   | ✅   | ✅    | —    | ❌       |
| `getTable(tableName)`                    | ✅      | —   | ✅   | ✅    | —    | ❌       |
| `getFunction(functionName)`              | ✅      | —   | ✅   | ✅    | —    | ❌       |
| `dropTempView(viewName)`                 | ✅      | —   | ✅   | ✅    | —    | ❌       |
| `dropGlobalTempView(viewName)`           | ✅      | —   | ✅   | ✅    | —    | ❌       |
| `cacheTable(tableName, storageLevel?)`   | ✅      | —   | ✅   | —     | —    | ❌       |
| `uncacheTable(tableName)`                | ✅      | —   | ✅   | —     | —    | ❌       |
| `isCached(tableName)`                    | ✅      | —   | ✅   | —     | —    | ❌       |
| `clearCache()`                           | ✅      | —   | ✅   | —     | —    | ❌       |
| `refreshTable(tableName)`                | ✅      | —   | ✅   | —     | —    | ❌       |
| `refreshByPath(path)`                    | ✅      | —   | ✅   | —     | —    | ❌       |
| `recoverPartitions(tableName)`           | ✅      | —   | ✅   | —     | —    | ❌       |
| `createExternalTable(tableName, ...)`    | ✅      | —   | ✅   | —     | —    | ❌       |
| `createTable(tableName, ...)`            | ✅      | —   | ✅   | —     | —    | ❌       |
| `registerFunction(name, f, returnType?)` | ✅      | —   | —    | —     | —    | ❌       |

---

## 12. Structured Streaming

### DataStreamReader

| Method                          | PySpark | Go  | Rust | Swift | .NET | spark-js |
| ------------------------------- | ------- | --- | ---- | ----- | ---- | -------- |
| `format(source)`                | ✅      | —   | ✅   | ✅    | —    | ❌       |
| `option(key, value)`            | ✅      | —   | ✅   | ✅    | —    | ❌       |
| `options(**options)`            | ✅      | —   | ✅   | ✅    | —    | ❌       |
| `schema(schema)`                | ✅      | —   | ✅   | ✅    | —    | ❌       |
| `load(path?, format?, schema?)` | ✅      | —   | ✅   | ✅    | —    | ❌       |
| `table(tableName)`              | ✅      | —   | —    | —     | —    | ❌       |
| `csv(path, ...)`                | ✅      | —   | —    | —     | —    | ❌       |
| `json(path, ...)`               | ✅      | —   | —    | —     | —    | ❌       |
| `orc(path, ...)`                | ✅      | —   | —    | —     | —    | ❌       |
| `parquet(path, ...)`            | ✅      | —   | —    | —     | —    | ❌       |
| `text(paths, ...)`              | ✅      | —   | —    | —     | —    | ❌       |

### DataStreamWriter

| Method                                    | PySpark | Go  | Rust | Swift | .NET | spark-js |
| ----------------------------------------- | ------- | --- | ---- | ----- | ---- | -------- |
| `format(source)`                          | ✅      | —   | ✅   | ✅    | —    | ❌       |
| `option(key, value)`                      | ✅      | —   | ✅   | ✅    | —    | ❌       |
| `options(**options)`                      | ✅      | —   | ✅   | ✅    | —    | ❌       |
| `outputMode(outputMode)`                  | ✅      | —   | ✅   | ✅    | —    | ❌       |
| `trigger(...)`                            | ✅      | —   | ✅   | ✅    | —    | ❌       |
| `queryName(queryName)`                    | ✅      | —   | ✅   | ✅    | —    | ❌       |
| `partitionBy(*cols)`                      | ✅      | —   | ✅   | ✅    | —    | ❌       |
| `start(path?, format?, outputMode?, ...)` | ✅      | —   | ✅   | ✅    | —    | ❌       |
| `toTable(tableName, ...)`                 | ✅      | —   | ✅   | —     | —    | ❌       |
| `foreach(f)`                              | ✅      | —   | —    | —     | —    | ❌       |
| `foreachBatch(func)`                      | ✅      | —   | —    | —     | —    | ❌       |

### StreamingQuery

| Method                       | PySpark | Go  | Rust | Swift | .NET | spark-js |
| ---------------------------- | ------- | --- | ---- | ----- | ---- | -------- |
| `id` (property)              | ✅      | —   | ✅   | —     | —    | ❌       |
| `runId` (property)           | ✅      | —   | ✅   | —     | —    | ❌       |
| `name` (property)            | ✅      | —   | ✅   | —     | —    | ❌       |
| `isActive` (property)        | ✅      | —   | ✅   | —     | —    | ❌       |
| `stop()`                     | ✅      | —   | ✅   | —     | —    | ❌       |
| `awaitTermination(timeout?)` | ✅      | —   | ✅   | —     | —    | ❌       |
| `status` (property)          | ✅      | —   | ✅   | —     | —    | ❌       |
| `lastProgress` (property)    | ✅      | —   | ✅   | —     | —    | ❌       |
| `recentProgress` (property)  | ✅      | —   | ✅   | —     | —    | ❌       |
| `processAllAvailable()`      | ✅      | —   | ✅   | —     | —    | ❌       |
| `exception` (property)       | ✅      | —   | ✅   | —     | —    | ❌       |
| `explain(extended?)`         | ✅      | —   | ✅   | —     | —    | ❌       |

### StreamingQueryManager

| Method                          | PySpark | Go  | Rust | Swift | .NET | spark-js |
| ------------------------------- | ------- | --- | ---- | ----- | ---- | -------- |
| `active` (property)             | ✅      | —   | ✅   | —     | —    | ❌       |
| `get(id)`                       | ✅      | —   | ✅   | —     | —    | ❌       |
| `awaitAnyTermination(timeout?)` | ✅      | —   | ✅   | —     | —    | ❌       |
| `resetTerminated()`             | ✅      | —   | ✅   | —     | —    | ❌       |
| `addListener(listener)`         | ✅      | —   | —    | —     | —    | ❌       |
| `removeListener(listener)`      | ✅      | —   | —    | —     | —    | ❌       |

### StreamingQueryListener

| Method                     | PySpark | Go  | Rust | Swift | .NET | spark-js |
| -------------------------- | ------- | --- | ---- | ----- | ---- | -------- |
| `onQueryStarted(event)`    | ✅      | —   | —    | —     | —    | ❌       |
| `onQueryProgress(event)`   | ✅      | —   | —    | —     | —    | ❌       |
| `onQueryIdle(event)`       | ✅      | —   | —    | —     | —    | ❌       |
| `onQueryTerminated(event)` | ✅      | —   | —    | —     | —    | ❌       |

---

## 13. Functions

### 13a. Normal Functions

| Function                         | PySpark | Go  | Rust | Swift | .NET | spark-js |
| -------------------------------- | ------- | --- | ---- | ----- | ---- | -------- |
| `col(col)`                       | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `column(col)` (alias)            | ✅      | —   | —    | —     | —    | ❌       |
| `lit(col)`                       | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `expr(str)`                      | ✅      | ✅  | ✅   | —     | ✅   | ❌       |
| `broadcast(df)`                  | ✅      | —   | —    | —     | —    | ❌       |
| `call_function(funcName, *cols)` | ✅      | —   | —    | —     | —    | ❌       |

### 13b. Conditional Functions

| Function                 | PySpark | Go  | Rust | Swift | .NET | spark-js |
| ------------------------ | ------- | --- | ---- | ----- | ---- | -------- |
| `when(condition, value)` | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `coalesce(*cols)`        | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `ifnull(col1, col2)`     | ✅      | —   | ✅   | —     | —    | ❌       |
| `nanvl(col1, col2)`      | ✅      | —   | ✅   | —     | —    | ❌       |
| `nullif(col1, col2)`     | ✅      | —   | ✅   | —     | —    | ❌       |
| `nullifzero(col)`        | ✅      | —   | —    | —     | —    | ❌       |
| `nvl(col1, col2)`        | ✅      | —   | ✅   | —     | —    | ❌       |
| `nvl2(col1, col2, col3)` | ✅      | —   | ✅   | —     | —    | ❌       |
| `zeroifnull(col)`        | ✅      | —   | —    | —     | —    | ❌       |

### 13c. Predicate Functions

| Function                           | PySpark | Go  | Rust | Swift | .NET | spark-js |
| ---------------------------------- | ------- | --- | ---- | ----- | ---- | -------- |
| `isnull(col)`                      | ✅      | —   | ✅   | —     | —    | ✅       |
| `isnotnull(col)`                   | ✅      | —   | ✅   | —     | —    | ❌       |
| `isnan(col)`                       | ✅      | —   | ✅   | —     | —    | ✅       |
| `equal_null(col1, col2)`           | ✅      | —   | ✅   | —     | —    | ❌       |
| `like(str, pattern, escapeChar?)`  | ✅      | —   | ✅   | —     | —    | ❌       |
| `ilike(str, pattern, escapeChar?)` | ✅      | —   | ✅   | —     | —    | ❌       |
| `rlike(str, regexp)`               | ✅      | —   | ✅   | —     | —    | ❌       |
| `regexp(str, regexp)`              | ✅      | —   | ✅   | —     | —    | ❌       |
| `regexp_like(str, regexp)`         | ✅      | —   | ✅   | —     | —    | ❌       |

### 13d. Sort Functions

| Function                | PySpark | Go  | Rust | Swift | .NET | spark-js        |
| ----------------------- | ------- | --- | ---- | ----- | ---- | --------------- |
| `asc(col)`              | ✅      | ✅  | ✅   | ✅    | ✅   | ❌ (via Column) |
| `desc(col)`             | ✅      | ✅  | ✅   | ✅    | ✅   | ❌ (via Column) |
| `asc_nulls_first(col)`  | ✅      | —   | ✅   | —     | —    | ❌              |
| `asc_nulls_last(col)`   | ✅      | —   | ✅   | —     | —    | ❌              |
| `desc_nulls_first(col)` | ✅      | —   | ✅   | —     | —    | ❌              |
| `desc_nulls_last(col)`  | ✅      | —   | ✅   | —     | —    | ❌              |

### 13e. Mathematical Functions

| Function                               | PySpark | Go  | Rust | Swift | .NET | spark-js |
| -------------------------------------- | ------- | --- | ---- | ----- | ---- | -------- |
| `abs(col)`                             | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `acos(col)`                            | ✅      | ✅  | ✅   | ✅    | ✅   | ❌       |
| `acosh(col)`                           | ✅      | ✅  | ✅   | ✅    | ✅   | ❌       |
| `asin(col)`                            | ✅      | ✅  | ✅   | ✅    | ✅   | ❌       |
| `asinh(col)`                           | ✅      | ✅  | ✅   | ✅    | ✅   | ❌       |
| `atan(col)`                            | ✅      | ✅  | ✅   | ✅    | ✅   | ❌       |
| `atan2(col1, col2)`                    | ✅      | ✅  | ✅   | ✅    | ✅   | ❌       |
| `atanh(col)`                           | ✅      | ✅  | ✅   | ✅    | ✅   | ❌       |
| `bin(col)`                             | ✅      | ✅  | ✅   | —     | —    | ❌       |
| `bround(col, scale?)`                  | ✅      | —   | ✅   | —     | —    | ❌       |
| `cbrt(col)`                            | ✅      | ✅  | ✅   | ✅    | ✅   | ❌       |
| `ceil(col, scale?)` / `ceiling`        | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `conv(col, fromBase, toBase)`          | ✅      | —   | ✅   | —     | —    | ❌       |
| `cos(col)`                             | ✅      | ✅  | ✅   | ✅    | ✅   | ❌       |
| `cosh(col)`                            | ✅      | ✅  | ✅   | ✅    | ✅   | ❌       |
| `cot(col)`                             | ✅      | —   | ✅   | —     | —    | ❌       |
| `csc(col)`                             | ✅      | —   | ✅   | —     | —    | ❌       |
| `degrees(col)`                         | ✅      | ✅  | ✅   | —     | —    | ❌       |
| `e()`                                  | ✅      | —   | ✅   | —     | —    | ❌       |
| `exp(col)`                             | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `expm1(col)`                           | ✅      | —   | ✅   | —     | —    | ❌       |
| `factorial(col)`                       | ✅      | ✅  | ✅   | —     | —    | ❌       |
| `floor(col, scale?)`                   | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `greatest(*cols)`                      | ✅      | ✅  | ✅   | —     | ✅   | ❌       |
| `hex(col)`                             | ✅      | ✅  | ✅   | —     | —    | ❌       |
| `hypot(col1, col2)`                    | ✅      | —   | ✅   | —     | —    | ❌       |
| `least(*cols)`                         | ✅      | ✅  | ✅   | —     | ✅   | ❌       |
| `ln(col)`                              | ✅      | —   | ✅   | —     | —    | ❌       |
| `log(arg1, arg2?)`                     | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `log10(col)`                           | ✅      | ✅  | ✅   | ✅    | ✅   | ❌       |
| `log1p(col)`                           | ✅      | ✅  | ✅   | ✅    | ✅   | ❌       |
| `log2(col)`                            | ✅      | ✅  | ✅   | ✅    | ✅   | ❌       |
| `negate(col)` / `negative`             | ✅      | —   | ✅   | —     | —    | ❌       |
| `pi()`                                 | ✅      | —   | ✅   | —     | —    | ❌       |
| `pmod(dividend, divisor)`              | ✅      | —   | ✅   | —     | —    | ❌       |
| `positive(col)`                        | ✅      | —   | ✅   | —     | —    | ❌       |
| `pow(col1, col2)` / `power`            | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `radians(col)`                         | ✅      | ✅  | ✅   | —     | —    | ❌       |
| `rand(seed?)`                          | ✅      | ✅  | ✅   | ✅    | ✅   | ❌       |
| `randn(seed?)`                         | ✅      | ✅  | ✅   | ✅    | ✅   | ❌       |
| `random(seed?)` (alias)                | ✅      | —   | —    | —     | —    | ❌       |
| `rint(col)`                            | ✅      | —   | ✅   | —     | —    | ❌       |
| `round(col, scale?)`                   | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `sec(col)`                             | ✅      | —   | ✅   | —     | —    | ❌       |
| `sign(col)` / `signum`                 | ✅      | —   | ✅   | —     | —    | ❌       |
| `sin(col)`                             | ✅      | ✅  | ✅   | ✅    | ✅   | ❌       |
| `sinh(col)`                            | ✅      | ✅  | ✅   | ✅    | ✅   | ❌       |
| `sqrt(col)`                            | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `tan(col)`                             | ✅      | ✅  | ✅   | ✅    | ✅   | ❌       |
| `tanh(col)`                            | ✅      | ✅  | ✅   | ✅    | ✅   | ❌       |
| `try_add(left, right)`                 | ✅      | —   | ✅   | —     | —    | ❌       |
| `try_divide(left, right)`              | ✅      | —   | ✅   | —     | —    | ❌       |
| `try_mod(left, right)`                 | ✅      | —   | —    | —     | —    | ❌       |
| `try_multiply(left, right)`            | ✅      | —   | ✅   | —     | —    | ❌       |
| `try_subtract(left, right)`            | ✅      | —   | ✅   | —     | —    | ❌       |
| `unhex(col)`                           | ✅      | ✅  | ✅   | —     | —    | ❌       |
| `uniform(min, max, seed?)`             | ✅      | —   | —    | —     | —    | ❌       |
| `width_bucket(v, min, max, numBucket)` | ✅      | —   | ✅   | —     | —    | ❌       |

### 13f. String Functions

| Function                                             | PySpark | Go  | Rust | Swift | .NET | spark-js |
| ---------------------------------------------------- | ------- | --- | ---- | ----- | ---- | -------- |
| `ascii(col)`                                         | ✅      | ✅  | ✅   | —     | —    | ❌       |
| `base64(col)`                                        | ✅      | ✅  | ✅   | —     | —    | ❌       |
| `bit_length(col)`                                    | ✅      | —   | ✅   | —     | —    | ❌       |
| `btrim(str, trim?)`                                  | ✅      | —   | ✅   | —     | —    | ❌       |
| `char(col)` / `chr(n)`                               | ✅      | —   | ✅   | —     | —    | ❌       |
| `char_length(str)` / `character_length`              | ✅      | —   | ✅   | —     | —    | ❌       |
| `collate(col, collation)`                            | ✅      | —   | —    | —     | —    | ❌       |
| `collation(col)`                                     | ✅      | —   | —    | —     | —    | ❌       |
| `concat(*cols)`                                      | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `concat_ws(sep, *cols)`                              | ✅      | ✅  | ✅   | —     | ✅   | ❌       |
| `contains(left, right)`                              | ✅      | —   | ✅   | —     | —    | ✅       |
| `decode(col, charset)`                               | ✅      | ✅  | ✅   | —     | —    | ❌       |
| `elt(*inputs)`                                       | ✅      | —   | ✅   | —     | —    | ❌       |
| `encode(col, charset)`                               | ✅      | ✅  | ✅   | —     | —    | ❌       |
| `endswith(str, suffix)`                              | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `find_in_set(str, str_array)`                        | ✅      | —   | ✅   | —     | —    | ❌       |
| `format_number(col, d)`                              | ✅      | ✅  | ✅   | —     | ✅   | ❌       |
| `format_string(format, *cols)` / `printf`            | ✅      | ✅  | ✅   | —     | —    | ❌       |
| `initcap(col)`                                       | ✅      | ✅  | ✅   | —     | —    | ❌       |
| `instr(str, substr)`                                 | ✅      | ✅  | ✅   | —     | —    | ❌       |
| `is_valid_utf8(str)`                                 | ✅      | —   | —    | —     | —    | ❌       |
| `lcase(str)` (alias for lower)                       | ✅      | —   | ✅   | —     | —    | ❌       |
| `left(str, len)`                                     | ✅      | —   | ✅   | —     | —    | ❌       |
| `length(col)`                                        | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `levenshtein(left, right, threshold?)`               | ✅      | —   | ✅   | —     | —    | ❌       |
| `locate(substr, str, pos?)`                          | ✅      | ✅  | ✅   | —     | —    | ❌       |
| `lower(col)`                                         | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `lpad(col, len, pad)`                                | ✅      | ✅  | ✅   | ✅    | ✅   | ❌       |
| `ltrim(col, trim?)`                                  | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `make_valid_utf8(str)`                               | ✅      | —   | —    | —     | —    | ❌       |
| `mask(col, upperChar?, lowerChar?, digitChar?, ...)` | ✅      | —   | —    | —     | —    | ❌       |
| `octet_length(col)`                                  | ✅      | —   | ✅   | —     | —    | ❌       |
| `overlay(src, replace, pos, len?)`                   | ✅      | —   | ✅   | —     | —    | ❌       |
| `position(substr, str, start?)`                      | ✅      | —   | ✅   | —     | —    | ❌       |
| `quote(col)`                                         | ✅      | —   | —    | —     | —    | ❌       |
| `randstr(length, seed?)`                             | ✅      | —   | —    | —     | —    | ❌       |
| `regexp_count(str, regexp)`                          | ✅      | —   | ✅   | —     | —    | ❌       |
| `regexp_extract(str, pattern, idx)`                  | ✅      | ✅  | ✅   | —     | ✅   | ❌       |
| `regexp_extract_all(str, regexp, idx?)`              | ✅      | —   | ✅   | —     | —    | ❌       |
| `regexp_instr(str, regexp, idx?)`                    | ✅      | —   | ✅   | —     | —    | ❌       |
| `regexp_replace(string, pattern, replacement)`       | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `regexp_substr(str, regexp)`                         | ✅      | —   | ✅   | —     | —    | ❌       |
| `repeat(col, n)`                                     | ✅      | ✅  | ✅   | —     | ✅   | ❌       |
| `replace(src, search, replace?)`                     | ✅      | —   | ✅   | —     | —    | ❌       |
| `reverse(col)`                                       | ✅      | ✅  | ✅   | —     | ✅   | ❌       |
| `right(str, len)`                                    | ✅      | —   | ✅   | —     | —    | ❌       |
| `rpad(col, len, pad)`                                | ✅      | ✅  | ✅   | ✅    | ✅   | ❌       |
| `rtrim(col, trim?)`                                  | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `sentences(string, language?, country?)`             | ✅      | —   | ✅   | —     | —    | ❌       |
| `soundex(col)`                                       | ✅      | ✅  | ✅   | —     | —    | ❌       |
| `split(str, pattern, limit?)`                        | ✅      | ✅  | ✅   | —     | ✅   | ❌       |
| `split_part(src, delimiter, partNum)`                | ✅      | —   | ✅   | —     | —    | ❌       |
| `startswith(str, prefix)`                            | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `substr(str, pos, len?)`                             | ✅      | —   | ✅   | —     | —    | ❌       |
| `substring(str, pos, len)`                           | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `substring_index(str, delim, count)`                 | ✅      | —   | ✅   | —     | —    | ❌       |
| `to_binary(col, format?)`                            | ✅      | —   | ✅   | —     | —    | ❌       |
| `to_char(col, format)`                               | ✅      | —   | ✅   | —     | —    | ❌       |
| `to_number(col, format)`                             | ✅      | —   | ✅   | —     | —    | ❌       |
| `to_varchar(col, format)`                            | ✅      | —   | ✅   | —     | —    | ❌       |
| `translate(srcCol, matching, replace)`               | ✅      | ✅  | ✅   | —     | —    | ❌       |
| `trim(col, trim?)`                                   | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `try_to_binary(col, format?)`                        | ✅      | —   | ✅   | —     | —    | ❌       |
| `try_to_number(col, format)`                         | ✅      | —   | ✅   | —     | —    | ❌       |
| `try_validate_utf8(str)`                             | ✅      | —   | —    | —     | —    | ❌       |
| `ucase(str)` (alias for upper)                       | ✅      | —   | ✅   | —     | —    | ❌       |
| `unbase64(col)`                                      | ✅      | ✅  | ✅   | —     | —    | ❌       |
| `upper(col)`                                         | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `validate_utf8(str)`                                 | ✅      | —   | —    | —     | —    | ❌       |

### 13g. Bitwise Functions

| Function                           | PySpark | Go  | Rust | Swift | .NET | spark-js |
| ---------------------------------- | ------- | --- | ---- | ----- | ---- | -------- |
| `bit_count(col)`                   | ✅      | —   | ✅   | —     | —    | ❌       |
| `bit_get(col, pos)` / `getbit`     | ✅      | —   | ✅   | —     | —    | ❌       |
| `bitwise_not(col)`                 | ✅      | —   | ✅   | —     | —    | ❌       |
| `shiftleft(col, numBits)`          | ✅      | —   | ✅   | —     | —    | ❌       |
| `shiftright(col, numBits)`         | ✅      | —   | ✅   | —     | —    | ❌       |
| `shiftrightunsigned(col, numBits)` | ✅      | —   | ✅   | —     | —    | ❌       |

### 13h. Date and Timestamp Functions

| Function                                              | PySpark | Go  | Rust | Swift | .NET | spark-js |
| ----------------------------------------------------- | ------- | --- | ---- | ----- | ---- | -------- |
| `add_months(start, months)`                           | ✅      | ✅  | ✅   | —     | ✅   | ❌       |
| `convert_timezone(sourceTz, targetTz, sourceTs)`      | ✅      | —   | ✅   | —     | —    | ❌       |
| `curdate()`                                           | ✅      | —   | ✅   | —     | —    | ❌       |
| `current_date()`                                      | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `current_time(precision?)`                            | ✅      | —   | —    | —     | —    | ❌       |
| `current_timestamp()`                                 | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `current_timezone()`                                  | ✅      | —   | ✅   | —     | —    | ❌       |
| `date_add(start, days)` / `dateadd`                   | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `date_diff(end, start)` / `datediff`                  | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `date_format(date, format)`                           | ✅      | ✅  | ✅   | ✅    | ✅   | ❌       |
| `date_from_unix_date(days)`                           | ✅      | —   | ✅   | —     | —    | ❌       |
| `date_part(field, source)` / `datepart`               | ✅      | —   | ✅   | —     | —    | ❌       |
| `date_sub(start, days)`                               | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `date_trunc(format, timestamp)`                       | ✅      | ✅  | ✅   | —     | ✅   | ❌       |
| `day(col)` (alias for dayofmonth)                     | ✅      | ✅  | ✅   | ✅    | ✅   | ❌       |
| `dayname(col)`                                        | ✅      | —   | —    | —     | —    | ❌       |
| `dayofmonth(col)`                                     | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `dayofweek(col)`                                      | ✅      | ✅  | ✅   | —     | —    | ❌       |
| `dayofyear(col)`                                      | ✅      | ✅  | ✅   | —     | —    | ❌       |
| `extract(field, source)`                              | ✅      | —   | ✅   | —     | —    | ❌       |
| `from_unixtime(timestamp, format?)`                   | ✅      | ✅  | ✅   | —     | ✅   | ❌       |
| `from_utc_timestamp(timestamp, tz)`                   | ✅      | —   | ✅   | —     | —    | ❌       |
| `hour(col)`                                           | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `last_day(date)`                                      | ✅      | ✅  | ✅   | —     | ✅   | ❌       |
| `localtimestamp()`                                    | ✅      | —   | ✅   | —     | —    | ❌       |
| `make_date(year, month, day)`                         | ✅      | —   | ✅   | —     | —    | ❌       |
| `make_dt_interval(days?, hours?, mins?, secs?)`       | ✅      | —   | ✅   | —     | —    | ❌       |
| `make_interval(years?, months?, weeks?, days?, ...)`  | ✅      | —   | ✅   | —     | —    | ❌       |
| `make_time(hour, minute, second)`                     | ✅      | —   | —    | —     | —    | ❌       |
| `make_timestamp(years?, months?, days?, hours?, ...)` | ✅      | —   | ✅   | —     | —    | ❌       |
| `make_timestamp_ltz(years, months, days, ...)`        | ✅      | —   | ✅   | —     | —    | ❌       |
| `make_timestamp_ntz(years?, months?, days?, ...)`     | ✅      | —   | ✅   | —     | —    | ❌       |
| `make_ym_interval(years?, months?)`                   | ✅      | —   | ✅   | —     | —    | ❌       |
| `minute(col)`                                         | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `month(col)`                                          | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `monthname(col)`                                      | ✅      | —   | —    | —     | —    | ❌       |
| `months_between(date1, date2, roundOff?)`             | ✅      | ✅  | ✅   | —     | ✅   | ❌       |
| `next_day(date, dayOfWeek)`                           | ✅      | ✅  | ✅   | —     | ✅   | ❌       |
| `now()`                                               | ✅      | —   | ✅   | —     | —    | ❌       |
| `quarter(col)`                                        | ✅      | ✅  | ✅   | —     | —    | ❌       |
| `second(col)`                                         | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `session_window(timeColumn, gapDuration)`             | ✅      | —   | ✅   | —     | —    | ❌       |
| `time_diff(unit, start, end)`                         | ✅      | —   | —    | —     | —    | ❌       |
| `time_trunc(unit, time)`                              | ✅      | —   | —    | —     | —    | ❌       |
| `timestamp_add(unit, quantity, ts)`                   | ✅      | —   | —    | —     | —    | ❌       |
| `timestamp_diff(unit, start, end)`                    | ✅      | —   | —    | —     | —    | ❌       |
| `timestamp_micros(col)`                               | ✅      | —   | ✅   | —     | —    | ❌       |
| `timestamp_millis(col)`                               | ✅      | —   | ✅   | —     | —    | ❌       |
| `timestamp_seconds(col)`                              | ✅      | —   | ✅   | —     | —    | ❌       |
| `to_date(col, format?)`                               | ✅      | ✅  | ✅   | ✅    | ✅   | ❌       |
| `to_time(str, format?)`                               | ✅      | —   | —    | —     | —    | ❌       |
| `to_timestamp(col, format?)`                          | ✅      | ✅  | ✅   | ✅    | ✅   | ❌       |
| `to_timestamp_ltz(timestamp, format?)`                | ✅      | —   | —    | —     | —    | ❌       |
| `to_timestamp_ntz(timestamp, format?)`                | ✅      | —   | —    | —     | —    | ❌       |
| `to_unix_timestamp(timestamp, format?)`               | ✅      | —   | ✅   | —     | —    | ❌       |
| `to_utc_timestamp(timestamp, tz)`                     | ✅      | —   | ✅   | —     | —    | ❌       |
| `trunc(date, format)`                                 | ✅      | ✅  | ✅   | —     | ✅   | ❌       |
| `try_make_interval(years?, months?, ...)`             | ✅      | —   | —    | —     | —    | ❌       |
| `try_make_timestamp(years?, months?, ...)`            | ✅      | —   | —    | —     | —    | ❌       |
| `try_make_timestamp_ltz(years, months, ...)`          | ✅      | —   | —    | —     | —    | ❌       |
| `try_make_timestamp_ntz(years?, months?, ...)`        | ✅      | —   | —    | —     | —    | ❌       |
| `try_to_date(col, format?)`                           | ✅      | —   | —    | —     | —    | ❌       |
| `try_to_time(str, format?)`                           | ✅      | —   | —    | —     | —    | ❌       |
| `try_to_timestamp(col, format?)`                      | ✅      | —   | —    | —     | —    | ❌       |
| `unix_date(col)`                                      | ✅      | —   | ✅   | —     | —    | ❌       |
| `unix_micros(col)`                                    | ✅      | —   | —    | —     | —    | ❌       |
| `unix_millis(col)`                                    | ✅      | —   | ✅   | —     | —    | ❌       |
| `unix_seconds(col)`                                   | ✅      | —   | ✅   | —     | —    | ❌       |
| `unix_timestamp(timestamp?, format?)`                 | ✅      | ✅  | ✅   | —     | ✅   | ❌       |
| `weekday(col)`                                        | ✅      | —   | ✅   | —     | —    | ❌       |
| `weekofyear(col)`                                     | ✅      | ✅  | ✅   | —     | —    | ❌       |
| `window(timeColumn, windowDuration, ...)`             | ✅      | —   | ✅   | —     | —    | ❌       |
| `window_time(windowColumn)`                           | ✅      | —   | ✅   | —     | —    | ❌       |
| `year(col)`                                           | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |

### 13i. Hash Functions

| Function             | PySpark | Go  | Rust | Swift | .NET | spark-js |
| -------------------- | ------- | --- | ---- | ----- | ---- | -------- |
| `crc32(col)`         | ✅      | —   | ✅   | —     | —    | ❌       |
| `hash(*cols)`        | ✅      | —   | ✅   | —     | —    | ❌       |
| `md5(col)`           | ✅      | —   | ✅   | —     | —    | ❌       |
| `sha(col)` / `sha1`  | ✅      | —   | ✅   | —     | —    | ❌       |
| `sha2(col, numBits)` | ✅      | —   | ✅   | —     | —    | ❌       |
| `xxhash64(*cols)`    | ✅      | —   | ✅   | —     | —    | ❌       |

### 13j. Collection Functions

| Function                                                  | PySpark | Go  | Rust | Swift | .NET | spark-js |
| --------------------------------------------------------- | ------- | --- | ---- | ----- | ---- | -------- |
| `aggregate(col, initialValue, merge, finish?)` / `reduce` | ✅      | —   | —    | —     | —    | ❌       |
| `array_sort(col, comparator?)`                            | ✅      | ✅  | —    | —     | ✅   | ❌       |
| `cardinality(col)`                                        | ✅      | —   | —    | —     | —    | ❌       |
| `concat(*cols)`                                           | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `element_at(col, extraction)`                             | ✅      | —   | ✅   | —     | —    | ❌       |
| `exists(col, f)`                                          | ✅      | —   | —    | —     | —    | ❌       |
| `filter(col, f)`                                          | ✅      | —   | —    | —     | —    | ❌       |
| `forall(col, f)`                                          | ✅      | —   | —    | —     | —    | ❌       |
| `map_filter(col, f)`                                      | ✅      | —   | —    | —     | —    | ❌       |
| `map_zip_with(col1, col2, f)`                             | ✅      | —   | —    | —     | —    | ❌       |
| `reverse(col)`                                            | ✅      | ✅  | ✅   | —     | ✅   | ❌       |
| `size(col)`                                               | ✅      | ✅  | ✅   | —     | —    | ✅       |
| `transform(col, f)`                                       | ✅      | —   | —    | —     | —    | ❌       |
| `transform_keys(col, f)`                                  | ✅      | —   | —    | —     | —    | ❌       |
| `transform_values(col, f)`                                | ✅      | —   | —    | —     | —    | ❌       |
| `try_element_at(col, extraction)`                         | ✅      | —   | ✅   | —     | —    | ❌       |
| `zip_with(left, right, f)`                                | ✅      | —   | —    | —     | —    | ❌       |

### 13k. Array Functions

| Function                                        | PySpark | Go  | Rust | Swift | .NET | spark-js |
| ----------------------------------------------- | ------- | --- | ---- | ----- | ---- | -------- |
| `array(*cols)`                                  | ✅      | ✅  | ✅   | ✅    | —    | ✅       |
| `array_append(col, value)`                      | ✅      | —   | ✅   | —     | —    | ❌       |
| `array_compact(col)`                            | ✅      | —   | ✅   | —     | —    | ❌       |
| `array_contains(col, value)`                    | ✅      | ✅  | ✅   | —     | ✅   | ❌       |
| `array_distinct(col)`                           | ✅      | —   | ✅   | —     | —    | ❌       |
| `array_except(col1, col2)`                      | ✅      | —   | ✅   | —     | —    | ❌       |
| `array_insert(arr, pos, value)`                 | ✅      | —   | ✅   | —     | —    | ❌       |
| `array_intersect(col1, col2)`                   | ✅      | —   | ✅   | —     | —    | ❌       |
| `array_join(col, delimiter, null_replacement?)` | ✅      | ✅  | ✅   | —     | ✅   | ❌       |
| `array_max(col)`                                | ✅      | —   | ✅   | —     | —    | ❌       |
| `array_min(col)`                                | ✅      | —   | ✅   | —     | —    | ❌       |
| `array_position(col, value)`                    | ✅      | —   | ✅   | —     | —    | ❌       |
| `array_prepend(col, value)`                     | ✅      | —   | ✅   | —     | —    | ❌       |
| `array_remove(col, element)`                    | ✅      | —   | ✅   | —     | —    | ❌       |
| `array_repeat(col, count)`                      | ✅      | —   | ✅   | —     | —    | ❌       |
| `array_size(col)`                               | ✅      | —   | ✅   | —     | —    | ❌       |
| `array_union(col1, col2)`                       | ✅      | —   | ✅   | —     | —    | ❌       |
| `arrays_overlap(a1, a2)`                        | ✅      | —   | ✅   | —     | —    | ❌       |
| `arrays_zip(*cols)`                             | ✅      | —   | ✅   | —     | —    | ❌       |
| `flatten(col)`                                  | ✅      | —   | ✅   | —     | —    | ❌       |
| `get(col, index)`                               | ✅      | —   | ✅   | —     | —    | ❌       |
| `sequence(start, stop, step?)`                  | ✅      | —   | ✅   | —     | —    | ❌       |
| `shuffle(col, seed?)`                           | ✅      | —   | ✅   | —     | —    | ❌       |
| `slice(x, start, length)`                       | ✅      | ✅  | ✅   | —     | —    | ❌       |
| `sort_array(col, asc?)`                         | ✅      | ✅  | ✅   | —     | —    | ❌       |

### 13l. Struct Functions

| Function              | PySpark | Go  | Rust | Swift | .NET | spark-js |
| --------------------- | ------- | --- | ---- | ----- | ---- | -------- |
| `named_struct(*cols)` | ✅      | —   | ✅   | —     | —    | ❌       |
| `struct(*cols)`       | ✅      | ✅  | —    | —     | —    | ✅       |

### 13m. Map Functions

| Function                                       | PySpark | Go  | Rust | Swift | .NET | spark-js |
| ---------------------------------------------- | ------- | --- | ---- | ----- | ---- | -------- |
| `create_map(*cols)`                            | ✅      | ✅  | ✅   | —     | —    | ❌       |
| `map_concat(*cols)`                            | ✅      | —   | ✅   | —     | —    | ❌       |
| `map_contains_key(col, value)`                 | ✅      | —   | ✅   | —     | —    | ❌       |
| `map_entries(col)`                             | ✅      | —   | ✅   | —     | —    | ❌       |
| `map_from_arrays(col1, col2)`                  | ✅      | —   | ✅   | —     | —    | ❌       |
| `map_from_entries(col)`                        | ✅      | —   | ✅   | —     | —    | ❌       |
| `map_keys(col)`                                | ✅      | —   | ✅   | —     | —    | ❌       |
| `map_values(col)`                              | ✅      | —   | ✅   | —     | —    | ❌       |
| `str_to_map(text, pairDelim?, keyValueDelim?)` | ✅      | —   | ✅   | —     | —    | ❌       |

### 13n. Aggregate Functions

| Function                                                    | PySpark | Go  | Rust | Swift | .NET | spark-js |
| ----------------------------------------------------------- | ------- | --- | ---- | ----- | ---- | -------- |
| `any_value(col, ignoreNulls?)`                              | ✅      | —   | ✅   | —     | —    | ❌       |
| `approx_count_distinct(col, rsd?)`                          | ✅      | ✅  | ✅   | —     | ✅   | ❌       |
| `approx_percentile(col, percentage, accuracy?)`             | ✅      | —   | ✅   | —     | —    | ❌       |
| `array_agg(col)`                                            | ✅      | —   | ✅   | —     | —    | ❌       |
| `avg(col)`                                                  | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `bit_and(col)`                                              | ✅      | —   | ✅   | —     | —    | ❌       |
| `bit_or(col)`                                               | ✅      | —   | ✅   | —     | —    | ❌       |
| `bit_xor(col)`                                              | ✅      | —   | ✅   | —     | —    | ❌       |
| `bitmap_construct_agg(col)`                                 | ✅      | —   | ✅   | —     | —    | ❌       |
| `bitmap_or_agg(col)`                                        | ✅      | —   | ✅   | —     | —    | ❌       |
| `bool_and(col)` / `every`                                   | ✅      | —   | ✅   | —     | —    | ❌       |
| `bool_or(col)` / `some`                                     | ✅      | —   | ✅   | —     | —    | ❌       |
| `collect_list(col)`                                         | ✅      | ✅  | ✅   | ✅    | ✅   | ❌       |
| `collect_set(col)`                                          | ✅      | ✅  | ✅   | ✅    | ✅   | ❌       |
| `corr(col1, col2)`                                          | ✅      | ✅  | ✅   | —     | —    | ❌       |
| `count(col)`                                                | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `count_distinct(col, *cols)`                                | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `count_if(col)`                                             | ✅      | —   | ✅   | —     | —    | ❌       |
| `count_min_sketch(col, eps, confidence, seed?)`             | ✅      | —   | ✅   | —     | —    | ❌       |
| `covar_pop(col1, col2)`                                     | ✅      | ✅  | ✅   | —     | —    | ❌       |
| `covar_samp(col1, col2)`                                    | ✅      | ✅  | ✅   | —     | —    | ❌       |
| `first(col, ignorenulls?)` / `first_value`                  | ✅      | ✅  | ✅   | ✅    | ✅   | ❌       |
| `grouping(col)`                                             | ✅      | —   | ✅   | —     | —    | ❌       |
| `grouping_id(*cols)`                                        | ✅      | —   | ✅   | —     | —    | ❌       |
| `histogram_numeric(col, nBins)`                             | ✅      | —   | ✅   | —     | —    | ❌       |
| `hll_sketch_agg(col, lgConfigK?)`                           | ✅      | —   | ✅   | —     | —    | ❌       |
| `hll_union_agg(col, allowDifferentLgConfigK?)`              | ✅      | —   | ✅   | —     | —    | ❌       |
| `kll_sketch_agg_bigint(col, k?)`                            | ✅      | —   | —    | —     | —    | ❌       |
| `kll_sketch_agg_double(col, k?)`                            | ✅      | —   | —    | —     | —    | ❌       |
| `kll_sketch_agg_float(col, k?)`                             | ✅      | —   | —    | —     | —    | ❌       |
| `kurtosis(col)`                                             | ✅      | ✅  | ✅   | —     | —    | ❌       |
| `last(col, ignorenulls?)` / `last_value`                    | ✅      | ✅  | ✅   | ✅    | ✅   | ❌       |
| `listagg(col, delimiter?)` / `string_agg`                   | ✅      | —   | —    | —     | —    | ❌       |
| `listagg_distinct(col, delimiter?)` / `string_agg_distinct` | ✅      | —   | —    | —     | —    | ❌       |
| `max(col)`                                                  | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `max_by(col, ord)`                                          | ✅      | —   | ✅   | —     | —    | ❌       |
| `mean(col)` (alias for avg)                                 | ✅      | ✅  | ✅   | ✅    | ✅   | ❌       |
| `median(col)`                                               | ✅      | —   | ✅   | —     | —    | ❌       |
| `min(col)`                                                  | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `min_by(col, ord)`                                          | ✅      | —   | ✅   | —     | —    | ❌       |
| `mode(col, deterministic?)`                                 | ✅      | —   | ✅   | —     | —    | ❌       |
| `percentile(col, percentage, frequency?)`                   | ✅      | —   | ✅   | —     | —    | ❌       |
| `percentile_approx(col, percentage, accuracy?)`             | ✅      | —   | ✅   | —     | —    | ❌       |
| `product(col)`                                              | ✅      | ✅  | ✅   | —     | —    | ❌       |
| `regr_avgx(y, x)`                                           | ✅      | —   | ✅   | —     | —    | ❌       |
| `regr_avgy(y, x)`                                           | ✅      | —   | ✅   | —     | —    | ❌       |
| `regr_count(y, x)`                                          | ✅      | —   | ✅   | —     | —    | ❌       |
| `regr_intercept(y, x)`                                      | ✅      | —   | ✅   | —     | —    | ❌       |
| `regr_r2(y, x)`                                             | ✅      | —   | ✅   | —     | —    | ❌       |
| `regr_slope(y, x)`                                          | ✅      | —   | ✅   | —     | —    | ❌       |
| `regr_sxx(y, x)`                                            | ✅      | —   | ✅   | —     | —    | ❌       |
| `regr_sxy(y, x)`                                            | ✅      | —   | ✅   | —     | —    | ❌       |
| `regr_syy(y, x)`                                            | ✅      | —   | ✅   | —     | —    | ❌       |
| `skewness(col)`                                             | ✅      | ✅  | ✅   | —     | —    | ❌       |
| `std(col)` / `stddev` (alias for stddev_samp)               | ✅      | ✅  | ✅   | —     | ✅   | ❌       |
| `stddev_pop(col)`                                           | ✅      | ✅  | ✅   | —     | ✅   | ❌       |
| `stddev_samp(col)`                                          | ✅      | ✅  | ✅   | —     | ✅   | ❌       |
| `sum(col)`                                                  | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `sum_distinct(col)`                                         | ✅      | —   | ✅   | —     | —    | ❌       |
| `theta_intersection_agg(col)`                               | ✅      | —   | —    | —     | —    | ❌       |
| `theta_sketch_agg(col, lgNomEntries?)`                      | ✅      | —   | —    | —     | —    | ❌       |
| `theta_union_agg(col, lgNomEntries?)`                       | ✅      | —   | —    | —     | —    | ❌       |
| `try_avg(col)`                                              | ✅      | —   | ✅   | —     | —    | ❌       |
| `try_sum(col)`                                              | ✅      | —   | ✅   | —     | —    | ❌       |
| `var_pop(col)`                                              | ✅      | ✅  | ✅   | —     | ✅   | ❌       |
| `var_samp(col)` / `variance`                                | ✅      | ✅  | ✅   | —     | ✅   | ❌       |

### 13o. Window Functions

| Function                               | PySpark | Go  | Rust | Swift | .NET | spark-js |
| -------------------------------------- | ------- | --- | ---- | ----- | ---- | -------- |
| `cume_dist()`                          | ✅      | ✅  | ✅   | —     | ✅   | ❌       |
| `dense_rank()`                         | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `lag(col, offset?, default?)`          | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `lead(col, offset?, default?)`         | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `nth_value(col, offset, ignoreNulls?)` | ✅      | —   | ✅   | —     | —    | ❌       |
| `ntile(n)`                             | ✅      | ✅  | ✅   | —     | ✅   | ✅       |
| `percent_rank()`                       | ✅      | ✅  | ✅   | —     | ✅   | ❌       |
| `rank()`                               | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `row_number()`                         | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |

### 13p. Generator Functions

| Function                | PySpark | Go  | Rust | Swift | .NET | spark-js |
| ----------------------- | ------- | --- | ---- | ----- | ---- | -------- |
| `explode(col)`          | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `explode_outer(col)`    | ✅      | —   | ✅   | ✅    | ✅   | ❌       |
| `inline(col)`           | ✅      | —   | ✅   | —     | —    | ❌       |
| `inline_outer(col)`     | ✅      | —   | ✅   | —     | —    | ❌       |
| `posexplode(col)`       | ✅      | —   | ✅   | —     | —    | ❌       |
| `posexplode_outer(col)` | ✅      | —   | ✅   | —     | —    | ❌       |
| `stack(*cols)`          | ✅      | —   | ✅   | —     | —    | ❌       |

### 13q. Partition Transformation Functions

| Function                               | PySpark | Go  | Rust | Swift | .NET | spark-js |
| -------------------------------------- | ------- | --- | ---- | ----- | ---- | -------- |
| `partitioning.years(col)`              | ✅      | —   | ✅   | —     | —    | ❌       |
| `partitioning.months(col)`             | ✅      | —   | ✅   | —     | —    | ❌       |
| `partitioning.days(col)`               | ✅      | —   | ✅   | —     | —    | ❌       |
| `partitioning.hours(col)`              | ✅      | —   | ✅   | —     | —    | ❌       |
| `partitioning.bucket(numBuckets, col)` | ✅      | —   | ✅   | —     | —    | ❌       |

### 13r. CSV Functions

| Function                          | PySpark | Go  | Rust | Swift | .NET | spark-js |
| --------------------------------- | ------- | --- | ---- | ----- | ---- | -------- |
| `from_csv(col, schema, options?)` | ✅      | —   | ✅   | —     | —    | ❌       |
| `schema_of_csv(csv, options?)`    | ✅      | —   | ✅   | —     | —    | ❌       |
| `to_csv(col, options?)`           | ✅      | —   | ✅   | —     | —    | ❌       |

### 13s. JSON Functions

| Function                           | PySpark | Go  | Rust | Swift | .NET | spark-js |
| ---------------------------------- | ------- | --- | ---- | ----- | ---- | -------- |
| `from_json(col, schema, options?)` | ✅      | —   | ✅   | —     | ✅   | ❌       |
| `get_json_object(col, path)`       | ✅      | —   | ✅   | —     | —    | ❌       |
| `json_array_length(col)`           | ✅      | —   | ✅   | —     | —    | ❌       |
| `json_object_keys(col)`            | ✅      | —   | ✅   | —     | —    | ❌       |
| `json_tuple(col, *fields)`         | ✅      | —   | ✅   | —     | —    | ❌       |
| `schema_of_json(json, options?)`   | ✅      | —   | ✅   | —     | —    | ❌       |
| `to_json(col, options?)`           | ✅      | —   | ✅   | —     | ✅   | ❌       |

### 13t. Variant Functions

| Function                               | PySpark | Go  | Rust | Swift | .NET | spark-js |
| -------------------------------------- | ------- | --- | ---- | ----- | ---- | -------- |
| `is_variant_null(v)`                   | ✅      | —   | —    | —     | —    | ❌       |
| `parse_json(col)`                      | ✅      | —   | —    | —     | —    | ❌       |
| `try_parse_json(col)`                  | ✅      | —   | —    | —     | —    | ❌       |
| `schema_of_variant(v)`                 | ✅      | —   | —    | —     | —    | ❌       |
| `schema_of_variant_agg(v)`             | ✅      | —   | —    | —     | —    | ❌       |
| `try_variant_get(v, path, targetType)` | ✅      | —   | —    | —     | —    | ❌       |
| `variant_get(v, path, targetType)`     | ✅      | —   | —    | —     | —    | ❌       |
| `to_variant_object(col)`               | ✅      | —   | —    | —     | —    | ❌       |

### 13u. XML Functions

| Function                                   | PySpark | Go  | Rust | Swift | .NET | spark-js |
| ------------------------------------------ | ------- | --- | ---- | ----- | ---- | -------- |
| `from_xml(col, schema, options?)`          | ✅      | —   | —    | —     | —    | ❌       |
| `schema_of_xml(xml, options?)`             | ✅      | —   | —    | —     | —    | ❌       |
| `to_xml(col, options?)`                    | ✅      | —   | —    | —     | —    | ❌       |
| `xpath(xml, path)`                         | ✅      | —   | ✅   | —     | —    | ❌       |
| `xpath_boolean(xml, path)`                 | ✅      | —   | ✅   | —     | —    | ❌       |
| `xpath_double(xml, path)` / `xpath_number` | ✅      | —   | ✅   | —     | —    | ❌       |
| `xpath_float(xml, path)`                   | ✅      | —   | ✅   | —     | —    | ❌       |
| `xpath_int(xml, path)`                     | ✅      | —   | ✅   | —     | —    | ❌       |
| `xpath_long(xml, path)`                    | ✅      | —   | ✅   | —     | —    | ❌       |
| `xpath_short(xml, path)`                   | ✅      | —   | ✅   | —     | —    | ❌       |
| `xpath_string(xml, path)`                  | ✅      | —   | ✅   | —     | —    | ❌       |

### 13v. URL Functions

| Function                                  | PySpark | Go  | Rust | Swift | .NET | spark-js |
| ----------------------------------------- | ------- | --- | ---- | ----- | ---- | -------- |
| `parse_url(url, partToExtract, key?)`     | ✅      | —   | ✅   | —     | —    | ❌       |
| `try_parse_url(url, partToExtract, key?)` | ✅      | —   | —    | —     | —    | ❌       |
| `url_decode(str)`                         | ✅      | —   | ✅   | —     | —    | ❌       |
| `url_encode(str)`                         | ✅      | —   | ✅   | —     | —    | ❌       |
| `try_url_decode(str)`                     | ✅      | —   | —    | —     | —    | ❌       |

### 13w. Geospatial ST Functions

| Function                | PySpark | Go  | Rust | Swift | .NET | spark-js |
| ----------------------- | ------- | --- | ---- | ----- | ---- | -------- |
| `st_asbinary(geo)`      | ✅      | —   | —    | —     | —    | ❌       |
| `st_geogfromwkb(wkb)`   | ✅      | —   | —    | —     | —    | ❌       |
| `st_geomfromwkb(wkb)`   | ✅      | —   | —    | —     | —    | ❌       |
| `st_setsrid(geo, srid)` | ✅      | —   | —    | —     | —    | ❌       |
| `st_srid(geo)`          | ✅      | —   | —    | —     | —    | ❌       |

### 13x. Misc Functions

| Function                                              | PySpark  | Go  | Rust | Swift | .NET | spark-js |
| ----------------------------------------------------- | -------- | --- | ---- | ----- | ---- | -------- |
| `aes_decrypt(input, key, mode?, padding?, aad?)`      | ✅       | —   | ✅   | —     | —    | ❌       |
| `aes_encrypt(input, key, mode?, padding?, iv?, aad?)` | ✅       | —   | ✅   | —     | —    | ❌       |
| `try_aes_decrypt(input, key, mode?, padding?, aad?)`  | ✅       | —   | ✅   | —     | —    | ❌       |
| `assert_true(col, errMsg?)`                           | ✅       | —   | ✅   | —     | —    | ❌       |
| `bitmap_bit_position(col)`                            | ✅       | —   | ✅   | —     | —    | ❌       |
| `bitmap_bucket_number(col)`                           | ✅       | —   | ✅   | —     | —    | ❌       |
| `bitmap_count(col)`                                   | ✅       | —   | ✅   | —     | —    | ❌       |
| `cast` (as function)                                  | ✅       | ✅  | ✅   | ✅    | ✅   | ✅       |
| `current_catalog()`                                   | ✅       | —   | ✅   | —     | —    | ❌       |
| `current_database()` / `current_schema`               | ✅       | —   | ✅   | —     | —    | ❌       |
| `current_user()`                                      | ✅       | —   | ✅   | —     | —    | ❌       |
| `hll_sketch_estimate(col)`                            | ✅       | —   | ✅   | —     | —    | ❌       |
| `hll_union(col1, col2, allowDifferentLgConfigK?)`     | ✅       | —   | ✅   | —     | —    | ❌       |
| `input_file_block_length()`                           | ✅       | —   | ✅   | —     | —    | ❌       |
| `input_file_block_start()`                            | ✅       | —   | ✅   | —     | —    | ❌       |
| `input_file_name()`                                   | ✅       | —   | ✅   | —     | —    | ❌       |
| `java_method(*cols)` / `reflect`                      | ✅ (JVM) | —   | ✅   | —     | —    | N/A      |
| `try_reflect(*cols)`                                  | ✅ (JVM) | —   | —    | —     | —    | N/A      |
| `kll_sketch_get_n_bigint(col)`                        | ✅       | —   | —    | —     | —    | ❌       |
| `kll_sketch_get_n_double(col)`                        | ✅       | —   | —    | —     | —    | ❌       |
| `kll_sketch_get_n_float(col)`                         | ✅       | —   | —    | —     | —    | ❌       |
| `kll_sketch_get_quantile_bigint(sketch, rank)`        | ✅       | —   | —    | —     | —    | ❌       |
| `kll_sketch_get_quantile_double(sketch, rank)`        | ✅       | —   | —    | —     | —    | ❌       |
| `kll_sketch_get_quantile_float(sketch, rank)`         | ✅       | —   | —    | —     | —    | ❌       |
| `kll_sketch_get_rank_bigint(sketch, quantile)`        | ✅       | —   | —    | —     | —    | ❌       |
| `kll_sketch_get_rank_double(sketch, quantile)`        | ✅       | —   | —    | —     | —    | ❌       |
| `kll_sketch_get_rank_float(sketch, quantile)`         | ✅       | —   | —    | —     | —    | ❌       |
| `kll_sketch_merge_bigint(left, right)`                | ✅       | —   | —    | —     | —    | ❌       |
| `kll_sketch_merge_double(left, right)`                | ✅       | —   | —    | —     | —    | ❌       |
| `kll_sketch_merge_float(left, right)`                 | ✅       | —   | —    | —     | —    | ❌       |
| `kll_sketch_to_string_bigint(col)`                    | ✅       | —   | —    | —     | —    | ❌       |
| `kll_sketch_to_string_double(col)`                    | ✅       | —   | —    | —     | —    | ❌       |
| `kll_sketch_to_string_float(col)`                     | ✅       | —   | —    | —     | —    | ❌       |
| `monotonically_increasing_id()`                       | ✅       | —   | ✅   | —     | —    | ❌       |
| `raise_error(errMsg)`                                 | ✅       | —   | ✅   | —     | —    | ❌       |
| `session_user()`                                      | ✅       | —   | —    | —     | —    | ❌       |
| `spark_partition_id()`                                | ✅       | —   | ✅   | —     | —    | ❌       |
| `theta_difference(col1, col2)`                        | ✅       | —   | —    | —     | —    | ❌       |
| `theta_intersection(col1, col2)`                      | ✅       | —   | —    | —     | —    | ❌       |
| `theta_sketch_estimate(col)`                          | ✅       | —   | —    | —     | —    | ❌       |
| `theta_union(col1, col2, lgNomEntries?)`              | ✅       | —   | —    | —     | —    | ❌       |
| `typeof(col)`                                         | ✅       | —   | —    | —     | —    | ❌       |
| `user()`                                              | ✅       | —   | ✅   | —     | —    | ❌       |
| `uuid(seed?)`                                         | ✅       | —   | —    | —     | —    | ❌       |
| `version()`                                           | ✅       | —   | ✅   | —     | —    | ❌       |

### 13y. UDF, UDTF and UDT

| Function                                     | PySpark     | Go  | Rust | Swift | .NET | spark-js |
| -------------------------------------------- | ----------- | --- | ---- | ----- | ---- | -------- |
| `udf(f?, returnType?, useArrow?)`            | ✅          | —   | —    | —     | —    | ❌       |
| `arrow_udf(f?, returnType?, functionType?)`  | ✅          | —   | —    | —     | —    | ❌       |
| `pandas_udf(f?, returnType?, functionType?)` | ✅ (Python) | —   | —    | —     | —    | N/A      |
| `udtf(cls?, returnType?, useArrow?)`         | ✅          | —   | —    | —     | —    | ❌       |
| `arrow_udtf(cls?, returnType?)`              | ✅          | —   | —    | —     | —    | ❌       |
| `call_udf(udfName, *cols)`                   | ✅          | —   | —    | —     | —    | ❌       |
| `unwrap_udt(col)`                            | ✅          | —   | —    | —     | —    | ❌       |

### 13z. Table-Valued Functions

| Function                                             | PySpark     | Go  | Rust | Swift | .NET | spark-js |
| ---------------------------------------------------- | ----------- | --- | ---- | ----- | ---- | -------- |
| `TableValuedFunction.collations()`                   | ✅          | —   | —    | —     | —    | ❌       |
| `TableValuedFunction.explode(collection)`            | ✅          | —   | —    | —     | —    | ❌       |
| `TableValuedFunction.explode_outer(collection)`      | ✅          | —   | —    | —     | —    | ❌       |
| `TableValuedFunction.inline(input)`                  | ✅          | —   | —    | —     | —    | ❌       |
| `TableValuedFunction.inline_outer(input)`            | ✅          | —   | —    | —     | —    | ❌       |
| `TableValuedFunction.json_tuple(input, *fields)`     | ✅          | —   | —    | —     | —    | ❌       |
| `TableValuedFunction.posexplode(collection)`         | ✅          | —   | —    | —     | —    | ❌       |
| `TableValuedFunction.posexplode_outer(collection)`   | ✅          | —   | —    | —     | —    | ❌       |
| `TableValuedFunction.python_worker_logs()`           | ✅ (Python) | —   | —    | —     | —    | N/A      |
| `TableValuedFunction.range(start, end?, step?, ...)` | ✅          | —   | —    | —     | —    | ❌       |
| `TableValuedFunction.sql_keywords()`                 | ✅          | —   | —    | —     | —    | ❌       |
| `TableValuedFunction.stack(n, *fields)`              | ✅          | —   | —    | —     | —    | ❌       |
| `TableValuedFunction.variant_explode(input)`         | ✅          | —   | —    | —     | —    | ❌       |
| `TableValuedFunction.variant_explode_outer(input)`   | ✅          | —   | —    | —     | —    | ❌       |

---

## 14. Data Types

| Type              | PySpark | Go  | Rust | Swift | .NET | spark-js |
| ----------------- | ------- | --- | ---- | ----- | ---- | -------- |
| Boolean           | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| Byte (TinyInt)    | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| Short (SmallInt)  | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| Integer (Int)     | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| Long (BigInt)     | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| Float             | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| Double            | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| Decimal           | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| String            | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| Binary            | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| Date              | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| Timestamp         | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| TimestampNTZ      | ✅      | ✅  | ✅   | ✅    | ✅   | ❌       |
| Time              | ✅      | —   | —    | —     | —    | ❌       |
| Array             | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| Map               | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| Struct            | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| Null              | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| DayTimeInterval   | ✅      | ✅  | ✅   | —     | ✅   | ❌       |
| YearMonthInterval | ✅      | ✅  | ✅   | —     | ✅   | ❌       |
| CalendarInterval  | ✅      | —   | —    | —     | —    | ❌       |
| Variant           | ✅      | —   | —    | —     | —    | ❌       |

---

## 15. StructType / StructField

| Feature                                | PySpark | Go  | Rust | Swift | .NET | spark-js |
| -------------------------------------- | ------- | --- | ---- | ----- | ---- | -------- |
| StructType / StructField classes       | ✅      | ✅  | ✅   | ✅    | ✅   | ✅       |
| `add(field/name, dataType, nullable?)` | ✅      | —   | —    | —     | —    | ✅       |
| `fieldNames` (property)                | ✅      | ✅  | ✅   | ✅    | —    | ✅       |
| `treeString()`                         | ✅      | —   | —    | —     | —    | ✅       |
| `fromProto / toProto`                  | —       | —   | ✅   | —     | —    | ✅       |

---

## 16. Row

| Feature               | PySpark | Go  | Rust | Swift | .NET | spark-js      |
| --------------------- | ------- | --- | ---- | ----- | ---- | ------------- |
| Row class / type      | ✅      | ✅  | ✅   | ✅    | ✅   | ✅ (Record)   |
| `asDict()`            | ✅      | —   | —    | —     | —    | N/A (is dict) |
| Field access by name  | ✅      | ✅  | ✅   | ✅    | ✅   | ✅            |
| Field access by index | ✅      | ✅  | ✅   | ✅    | ✅   | ❌            |

---

## 17. TableArg (Table Argument for TVFs)

| Feature                          | PySpark | Go  | Rust | Swift | .NET | spark-js |
| -------------------------------- | ------- | --- | ---- | ----- | ---- | -------- |
| `TableArg.partitionBy(*cols)`    | ✅      | —   | —    | —     | —    | ❌       |
| `TableArg.orderBy(*cols)`        | ✅      | —   | —    | —     | —    | ❌       |
| `TableArg.withSinglePartition()` | ✅      | —   | —    | —     | —    | ❌       |

---

## Summary Score

Approximate feature coverage vs PySpark (DataFrame + Column + Functions + I/O + Catalog + Streaming):

| Client                      | Estimated Coverage |
| --------------------------- | ------------------ |
| PySpark                     | 100% (reference)   |
| spark-connect-rs (Rust)     | ~85%               |
| spark-connect-go (Go)       | ~45%               |
| spark-connect-swift (Swift) | ~40%               |
| spark-connect-dotnet (.NET) | ~25%               |
| **spark-js**                | **~15-20%**        |

### spark-js Strengths

- Clean TypeScript API with full type safety
- Modern async/await + AsyncIterableIterator patterns
- Zero Java dependency — pure gRPC + Arrow
- Full Window support (spec, functions, frame bounds)
- Good error handling with SparkConnectError
- Strong DataFrame core (select, filter, join, group, sort, set ops, sample, null handling)
- DataFrameWriter with save/saveAsTable

### spark-js Key Gaps (vs other clients)

1. **Functions**: ~55 implemented vs ~400+ in PySpark (biggest gap)
2. **Caching**: No cache/persist/unpersist
3. **Repartitioning**: No repartition/coalesce
4. **Advanced Grouping**: No cube/rollup/pivot
5. **Streaming**: No DataStreamReader/Writer/StreamingQuery
6. **DataFrame breadth**: Missing withColumnRenamed, selectExpr, tail, isEmpty, columns, summary, replace, sortWithinPartitions, hint, alias
7. **DataFrameReader**: No schema(), no format shortcuts (csv/json/parquet/orc)
8. **DataFrameWriter**: No format shortcuts, no bucketBy, no insertInto
9. **DataFrameWriterV2**: Not implemented at all
10. **Catalog**: 7 methods vs Rust's 25+ / PySpark's 25+
11. **Column methods**: No substr, bitwiseOps, getField, getItem, dropFields, withField, null-ordering sort
12. **GroupedData**: No min/max/pivot
13. **SparkSession**: No range(), conf, version, interrupt support
