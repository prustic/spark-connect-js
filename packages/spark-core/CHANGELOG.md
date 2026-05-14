# @spark-connect-js/core

## 0.4.0

### Minor Changes

- [#61](https://github.com/prustic/spark-connect-js/pull/61) [`aa22cc6`](https://github.com/prustic/spark-connect-js/commit/aa22cc675cc3531056d5fc2f9715198b4b89dd2f) Thanks [@prustic](https://github.com/prustic)! - - Catalog parity with PySpark: the full `spark.catalog` surface (`currentCatalog`/`setCurrentCatalog`, `listCatalogs`/`listDatabases`/`listTables`/`listColumns`/`listFunctions`, `databaseExists`/`tableExists`/`functionExists`, `getDatabase`/`getTable`/`getFunction`, `dropTempView`/`dropGlobalTempView`, `cacheTable`/`uncacheTable`/`clearCache`/`isCached`, `refreshTable`/`refreshByPath`, `recoverPartitions`, `createTable`/`createExternalTable`)
  - `spark.udf.registerJavaFunction(name, className, returnType?)` and `spark.udf.registerJavaUDAF(name, className)` for binding Java UDFs and UDAFs already on the server's classpath to a SQL function name
  - `SparkSession.version()` returns the server's Spark version
  - `SparkSession.builder().sessionId(uuid)` to reuse a server-side session by ID
  - `RuntimeConfig` on `spark.conf` with `get`, `set`, `unset`, `getAll`, `isModifiable`
  - Session tags and interrupts: `addTag`, `removeTag`, `getTags`, `clearTags`, `interruptAll`, `interruptTag`, `interruptOperation`
  - `Transport` interface gains optional `config` and `interrupt` methods; `ExecuteOptions` plumbs per-call tags
  - `SparkConnectError` exposes `errorClass`, `sqlState`, `messageParameters`, `errorTypeHierarchy`, and `serverStackTrace`
  - Fix `count("*")` to send `count(1)` on the wire instead of `count(<unresolved-*>)`, matching PySpark and Scala behavior

## 0.3.0

### Minor Changes

- [#40](https://github.com/prustic/spark-connect-js/pull/40) [`d468479`](https://github.com/prustic/spark-connect-js/commit/d46847934011df16aefb39db7c3bb5fdcf220f73) Thanks [@prustic](https://github.com/prustic)!
  - DataFrameReader shortcuts: `csv()`, `json()`, `parquet()`, `orc()`, `text()`, `schema()`
  - DataFrameWriter shortcuts: `csv()`, `json()`, `parquet()`, `orc()`, `text()`, `bucketBy()`, `insertInto()`
  - DataFrameWriterV2 with full `writeTo()` API: `create`, `replace`, `createOrReplace`, `append`, `overwrite`, `overwritePartitions`
  - Typed client error hierarchy: `SparkClientError`, `InvalidConfigError`, `InvalidInputError`, `UnsupportedOperationError`
  - `isDistinct` propagation on aggregate functions
  - Cross join validation rejects join conditions

## 0.2.0

### Minor Changes

- [#18](https://github.com/prustic/spark-connect-js/pull/18) [`924ea50`](https://github.com/prustic/spark-connect-js/commit/924ea50d700711733cef96857a48c900dc8d7f4b) Thanks [@prustic](https://github.com/prustic)!
  - `DataFrame.cube()`, `.rollup()` for multi-dimensional aggregation
  - `DataFrame.unpivot()` / `.melt()` for wide-to-long reshaping
  - `DataFrame.summary()` for descriptive statistics
  - `DataFrame.replace()` for value substitution via `NAReplace`
  - `DataFrame.randomSplit()` for splitting into multiple DataFrames
  - `DataFrame.createTempView()`, `.createGlobalTempView()`, `.createOrReplaceGlobalTempView()`
  - `DataFrame.sameSemantics()` and `.semanticHash()` for plan comparison
  - `DataFrameStat` class (`.stat` accessor) with `corr()`, `cov()`, `crosstab()`, `freqItems()`, `approxQuantile()`
  - `GroupedData.pivot()` support with cube/rollup/pivot group types

## 0.1.0

### Minor Changes

- [#10](https://github.com/prustic/spark-connect-js/pull/10) [`895f389`](https://github.com/prustic/spark-connect-js/commit/895f389d703182ed149c4a634f48b894aa7d5131) Thanks [@prustic](https://github.com/prustic)! - Initial release. Platform-agnostic DataFrame API and logical plan builder with zero runtime dependencies.
  - **SparkSession**: connect via `sc://` URL, execute SQL, read tables, create DataFrames from local data
  - **DataFrame**: 30+ transformations (select, filter, join, groupBy, sort, union, intersect, sample, fillna, dropna, and more), actions (collect, show, count, head, tail, toLocalIterator), properties (schema, columns, dtypes, isEmpty, printSchema, explain)
  - **Column**: comparisons, arithmetic, logical ops, cast, alias, null checks, pattern matching, bitwise ops, window support
  - **GroupedData**: agg, count, sum, avg, mean, min, max
  - **Window**: partitionBy, orderBy, rowsBetween, rangeBetween
  - **DataFrameReader**: format, option, options, load, table
  - **DataFrameWriter**: format, mode, option, options, partitionBy, sortBy, save, saveAsTable
  - **Catalog**: currentDatabase, setCurrentDatabase, listDatabases, listTables, listColumns, databaseExists, tableExists
  - **248 built-in functions** across 12 categories: aggregate, math, string, date/timestamp, window, collection, conditional, hash, JSON, CSV, bitwise, sort
  - **PlanBuilder**: constructs Spark Connect logical plan protobuf messages from the DataFrame API
  - Zero runtime dependencies
