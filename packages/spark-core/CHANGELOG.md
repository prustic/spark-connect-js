# @spark-connect-js/core

## 0.6.0

### Minor Changes

- [#130](https://github.com/prustic/spark-connect-js/pull/130) [`df4c3c6`](https://github.com/prustic/spark-connect-js/commit/df4c3c6a7d227b6f343ba464d5c9b993adc9700b) Thanks [@prustic](https://github.com/prustic)! - `Column` gains `when`/`otherwise` as methods, `try_cast`, `mod`, `pow`, `not`, `negate`, `transform`, and the `name`/`astype` aliases of `alias`/`cast`. A CASE WHEN chain is now a `Column`, so `when(...)` results can be used directly without `toColumn()`, and calling `when`/`otherwise` on a column that is not an open chain throws `InvalidInputError` instead of building an invalid expression.

- [#113](https://github.com/prustic/spark-connect-js/pull/113) [`13645bc`](https://github.com/prustic/spark-connect-js/commit/13645bcbd76a59af2aa052b11b6077ab085b0157) Thanks [@prustic](https://github.com/prustic)! - `createDataFrame` accepts a `StructType` schema and honors its per-field nullability, so `NOT NULL` schemas round-trip through `schema()` and back. The schema's field names are validated against the row keys, and a null in a `NOT NULL` column throws `InvalidInputError` naming the column and row before anything is sent. The `clientType` version string reported to the server is now kept in lockstep with the package version, enforced by a test that fails when they drift.

- [#129](https://github.com/prustic/spark-connect-js/pull/129) [`c700e57`](https://github.com/prustic/spark-connect-js/commit/c700e57886605892057d9492c048ae6ddb68c0a3) Thanks [@prustic](https://github.com/prustic)! - `DataFrame.mergeInto(table, condition)` returns a `MergeIntoWriter` for MERGE INTO with chainable `whenMatched`, `whenNotMatched`, and `whenNotMatchedBySource` clauses, each supporting update/insert/delete actions with optional conditions, plus `withSchemaEvolution()`. Merge and clause conditions accept a `Column` or a SQL string, and assignment keys are parsed as SQL expression strings, so nested fields like `"address.city"` work. `merge()` validates client-side that at least one clause action is defined and that `update`/`insert` assignment maps are non-empty.

- [#112](https://github.com/prustic/spark-connect-js/pull/112) [`cb28a25`](https://github.com/prustic/spark-connect-js/commit/cb28a25705be98d9e220a66f784f645d59c3fdcc) Thanks [@prustic](https://github.com/prustic)! - `DataFrame.observe(observation, ...metrics)` attaches named aggregate metrics, computed alongside the query without a second pass. Pass an `Observation` and read `observation.get` after an action, or pass a string name for metrics consumed via `StreamingQueryProgress.observedMetrics`. Metric values decode with the Arrow policy (longs as `bigint`, decimals as strings, temporals as `Date`). `ExecuteOptions` gains an `onObservedMetrics` callback for custom transports, and the `CollectMetrics` relation is wired through both plan builders.

- [#111](https://github.com/prustic/spark-connect-js/pull/111) [`6f6cca0`](https://github.com/prustic/spark-connect-js/commit/6f6cca0ea889f331b961586528f8a37f4c11b731) Thanks [@prustic](https://github.com/prustic)! - The regex extraction and matching family joins `regexp_replace`: `regexp_extract(col, pattern, groupIdx)`, `regexp_extract_all(col, pattern, groupIdx?)`, `regexp_like(col, pattern)`, `regexp_count(col, pattern)`, `regexp_substr(col, pattern)`, and `regexp_instr(col, pattern, groupIdx?)`. Patterns accept a `Column` for per-row values or a string literal, matching `regexp_replace`.

- [#104](https://github.com/prustic/spark-connect-js/pull/104) [`fbcdbf4`](https://github.com/prustic/spark-connect-js/commit/fbcdbf4110a612958efd7523f27d20d673dfd719) Thanks [@prustic](https://github.com/prustic)! - `DataFrame.schema()` returns a `StructType` instead of the raw protobuf schema object. Nested types render as DDL simple strings per field (`decimal(10,2)`, `array<string>`, `map<string,int>`, `struct<a:int,b:string>`), `columns()`, `dtypes()`, and `printSchema()` share the same typed path, and the unused `Schema` and `FieldDescriptor` type exports are removed. Code that read the raw proto shape should use `schema.fields` (or `schema.toDDL()` where a DDL string is needed).

- [#110](https://github.com/prustic/spark-connect-js/pull/110) [`75c5c56`](https://github.com/prustic/spark-connect-js/commit/75c5c5659ef0f04dfc0a677d43ea840d70696d9c) Thanks [@prustic](https://github.com/prustic)! - `StreamingQueryProgress` is fully typed against real Spark 4.0 Connect payloads from both delivery paths (`lastProgress()` and listener-bus events): new exported `SourceProgress`, `SinkProgress`, and `StateOperatorProgress` interfaces replace the `unknown` nested shapes, `eventTime` and `observedMetrics` are added, and the dead `bigint` branch on `numInputRows` is dropped (progress is JSON-parsed, never Arrow-decoded). Top-level row counts are absent from `lastProgress()` where the per-source values are authoritative, so the new `totalInputRows(progress)` helper sums `sources[].numInputRows` on either path.

### Patch Changes

- [#132](https://github.com/prustic/spark-connect-js/pull/132) [`2759a64`](https://github.com/prustic/spark-connect-js/commit/2759a64bac2365ecf6d5f474014c620531babd54) Thanks [@prustic](https://github.com/prustic)! - `DataFrame.filter`, `where`, `join`, and `DataFrameWriterV2.overwrite` reject a condition that is not a usable expression, instead of letting it reach the plan builder as an undefined one. `join` is the notable case: an invalid condition previously read as no condition at all, silently widening the query to a cartesian product. Predicate arguments now share one coercion helper, so `filter` and `where` continue to accept a `Column` or a SQL string, while `join` and `overwrite` take a `Column` only, since a bare string there would mean a column name rather than a predicate. Each method reports its own name in the error.

- [#131](https://github.com/prustic/spark-connect-js/pull/131) [`31b5bdf`](https://github.com/prustic/spark-connect-js/commit/31b5bdf59b1ffdae3026a93ac706726f8f1d083c) Thanks [@prustic](https://github.com/prustic)! - `StreamingQueryProgress.observedMetrics` now matches its declared `Record<string, Row>` type. The server sends each metric as a `{ values, schema }` wrapper, so field access previously returned `undefined`; values are zipped against the schema field names on both the polling and listener paths. A payload is only reshaped when it carries both the values array and the schema fields, so a metric row with a column of its own named `values` is left intact.

  `StructType.toDDL()` quotes field names that are not plain identifiers and doubles inner back-ticks, matching Spark's own rule, so schemas with spaces or dots in column names produce valid DDL instead of a broken statement. A schema with non-nullable fields renders `NOT NULL`, which `createDataFrame(rows, ddl)` now rejects. Pass the `StructType` itself instead of its DDL string.

  `createDataFrame(rows, ddl)` rejects a DDL string containing an unquoted `NOT NULL` up front, pointing at the `StructType` overload that can carry nullability, instead of letting the server fail with a positional column name.

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
