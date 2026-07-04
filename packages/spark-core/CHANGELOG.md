# @spark-connect-js/core

## 0.5.1

### Patch Changes

- [#101](https://github.com/prustic/spark-connect-js/pull/101) [`0a8e2de`](https://github.com/prustic/spark-connect-js/commit/0a8e2dee8f9df0911cbd2962314ac4cf743c6aa5) Thanks [@prustic](https://github.com/prustic)! - Drop the hardcoded version from the README development-status note. 0.5.0 published with the note still reading v0.4.0, since npm snapshots the README at publish time, and a version-free note cannot go stale.

## 0.5.0

### Minor Changes

- [#98](https://github.com/prustic/spark-connect-js/pull/98) [`a6237a1`](https://github.com/prustic/spark-connect-js/commit/a6237a1e5abd87ab8786d58dc1cd7b390fedd7d8) Thanks [@prustic](https://github.com/prustic)!
  - Structured Streaming: `spark.readStream` (`DataStreamReader`) and `df.writeStream` (`DataStreamWriter`) with `Trigger` factories (`processingTime`, `availableNow`, `once`, `continuous`); `start()` returns a `StreamingQuery` (`id`, `runId`, `name`, `isActive`, `stop`, `awaitTermination`, `status`, `lastProgress`, `recentProgress`, `processAllAvailable`, `exception`, `explain`)
  - `spark.streams` (`StreamingQueryManager`): `active`, `get`, `awaitAnyTermination`, `resetTerminated`, `addListener`/`removeListener` with `StreamingQueryListener` callbacks (`onQueryStarted`, `onQueryProgress`, `onQueryIdle`, `onQueryTerminated`) and typed `StreamingQueryProgress`
  - Event-time aggregation: `DataFrame.withWatermark(eventTimeColumn, delayThreshold)`, `window(timeColumn, windowDuration, slideDuration?, startTime?)`, `session_window(timeColumn, gapDuration)`
  - `createDataFrame(rows)` accepts plain row objects, encoded via the new `arrowEncoder` builder hook; `Uint8Array` input is validated as Arrow IPC stream format (file-format and empty input throw `InvalidInputError`)
  - `spark.table(name)` reads a catalog table or temp view, shorthand for `spark.read.table(name)`
  - Typed row access: `df.as<Schema>()` narrows collected rows at compile time; the `row` accessor namespace (`getInt`, `getLong`, `getDouble`, `getString`, `getBoolean`, `getBinary`, `getDate`) validates at runtime
  - `df.agg(...exprs)` aggregates without grouping; `df.col(name)` binds a column reference to its DataFrame for self-joins
  - Comparison, arithmetic, and bitwise `Column` methods accept raw primitives and wrap them as literals
  - `filter` and `where` accept SQL string predicates
  - `count()` returns `bigint`, matching the `LongType` result; wrap in `Number(...)` when the count is known to fit a JS safe integer
  - `show()` renders dates, maps, structs, arrays, and binary in Spark's display style
  - `lit(null)` emits a typed NULL literal and `lit(undefined)` throws `InvalidInputError`; `pivot(col, values)` accepts `null` values
  - Optional `Transport.executeCommandStream` method for custom transports that stream command result frames
  - `pow` accepts a `Column | number` exponent; `regexp_replace` accepts `Column | string` pattern and replacement; `element_at` accepts a numeric index
  - `isSessionInvalidated(err)` matches `INVALID_HANDLE.*` errors so callers can rebuild the session after a server restart

## 0.4.0

### Minor Changes

- [#61](https://github.com/prustic/spark-connect-js/pull/61) [`aa22cc6`](https://github.com/prustic/spark-connect-js/commit/aa22cc675cc3531056d5fc2f9715198b4b89dd2f) Thanks [@prustic](https://github.com/prustic)!
  - Catalog parity with PySpark: the full `spark.catalog` surface (`currentCatalog`/`setCurrentCatalog`, `listCatalogs`/`listDatabases`/`listTables`/`listColumns`/`listFunctions`, `databaseExists`/`tableExists`/`functionExists`, `getDatabase`/`getTable`/`getFunction`, `dropTempView`/`dropGlobalTempView`, `cacheTable`/`uncacheTable`/`clearCache`/`isCached`, `refreshTable`/`refreshByPath`, `recoverPartitions`, `createTable`/`createExternalTable`)
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
