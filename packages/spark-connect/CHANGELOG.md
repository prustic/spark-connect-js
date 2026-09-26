# @spark-connect-js/connect

## 0.6.0

### Minor Changes

- [#144](https://github.com/prustic/spark-connect-js/pull/144) [`f57b670`](https://github.com/prustic/spark-connect-js/commit/f57b670730b5dd320deda25225c959c47ad112d8) Thanks [@prustic](https://github.com/prustic)! - `Column.getField`, `withField`, and `dropFields` now work. They previously sent functions Spark does not define (`get_field`, `with_field`, `drop_fields`) and failed with `UNRESOLVED_ROUTINE` on every call. `getItem` on a map now works too. They send the same extract-value and update-fields expressions as PySpark, and reject an empty field name, an unusable key or value, or a `dropFields()` call with no names.

  `getItem` on an array index past the end now throws `INVALID_ARRAY_INDEX` under Spark's default ANSI mode, as PySpark does, where it previously returned null. Use the `get` function for a null-returning lookup.

  `df.col("*")` now expands to the DataFrame's columns instead of failing with `UNRESOLVED_COLUMN`. `df.col("t.*")` throws `InvalidInputError` pointing at `col("t.*")`, since a qualified star cannot be bound to a DataFrame and the server's error does not say so.

- [#142](https://github.com/prustic/spark-connect-js/pull/142) [`51ea296`](https://github.com/prustic/spark-connect-js/commit/51ea2960bc5fbf556ed5d6a06595f8b61916bef5) Thanks [@prustic](https://github.com/prustic)! - New `DataFrame` methods: `colRegex`, `metadataColumn`, `withMetadata`, `toJSON`, `toArrow`, and the `sparkSession` getter, plus `GroupedData.mean`. `toJSON` returns a DataFrame with a single `value` column, following PySpark's Connect client on `master` (its 4.0 release raises `NOT_IMPLEMENTED` there). `toArrow` returns the raw Arrow IPC streams rather than a decoded table, since the core package has no Arrow dependency. Larger results arrive as several streams, which must be decoded together.

  `col("*")` and `col("t.*")` now expand to every column. They previously reached the server as a column literally named `*` and failed with `UNRESOLVED_COLUMN`, so `select("*")` did not work.

- [#141](https://github.com/prustic/spark-connect-js/pull/141) [`0145e72`](https://github.com/prustic/spark-connect-js/commit/0145e72006899b1320f5cf3f3f320d71c5c19224) Thanks [@prustic](https://github.com/prustic)! - New `DataFrame` methods backed by Spark Connect relations: `transpose`, `lateralJoin`, `to(schema)`, `sampleBy` (also on `df.stat`), `groupingSets`, and `dropDuplicatesWithinWatermark`.

  `to` accepts a `StructType` or a DDL string. `sampleBy` takes fractions as a record, or a `Map` for strata that are not strings, and always sends a seed so repeated samples differ when none is given. `groupingSets` computes one aggregate per listed column set, alongside the existing `rollup` and `cube`.

- [#145](https://github.com/prustic/spark-connect-js/pull/145) [`906c66c`](https://github.com/prustic/spark-connect-js/commit/906c66c45e1bc3e1d08e5dd6d6582d206a4a8a8b) Thanks [@prustic](https://github.com/prustic)! - New `DataFrame` methods answered by the server: `isLocal`, `isStreaming`, `inputFiles`, `checkpoint`, and `localCheckpoint`. All five are async, where PySpark's are synchronous and `isStreaming` is a property, since each needs a round trip. A response missing its result throws rather than defaulting to `false` or an empty list.

  `checkpoint` requires the server to be started with `spark.checkpoint.dir`, a static setting a client cannot change at runtime; `localCheckpoint` needs no directory. The server releases a checkpointed relation once the DataFrame and everything derived from it are garbage-collected, or when the session stops. This is best effort, as in the Scala and PySpark clients. Reliable checkpoint files outlive both: the server deletes them only if it enables `spark.cleaner.referenceTracking.cleanCheckpoints`, and only after the relation is released.

- [#129](https://github.com/prustic/spark-connect-js/pull/129) [`c700e57`](https://github.com/prustic/spark-connect-js/commit/c700e57886605892057d9492c048ae6ddb68c0a3) Thanks [@prustic](https://github.com/prustic)! - `DataFrame.mergeInto(table, condition)` returns a `MergeIntoWriter` for MERGE INTO with chainable `whenMatched`, `whenNotMatched`, and `whenNotMatchedBySource` clauses, each supporting update/insert/delete actions with optional conditions, plus `withSchemaEvolution()`. Merge and clause conditions accept a `Column` or a SQL string, and assignment keys are parsed as SQL expression strings, so nested fields like `"address.city"` work. `merge()` validates client-side that at least one clause action is defined and that `update`/`insert` assignment maps are non-empty.

## 0.5.1

### Patch Changes

- [#101](https://github.com/prustic/spark-connect-js/pull/101) [`0a8e2de`](https://github.com/prustic/spark-connect-js/commit/0a8e2dee8f9df0911cbd2962314ac4cf743c6aa5) Thanks [@prustic](https://github.com/prustic)! - Drop the hardcoded version from the README development-status note. 0.5.0 published with the note still reading v0.4.0, since npm snapshots the README at publish time, and a version-free note cannot go stale.

## 0.5.0

### Minor Changes

- [#98](https://github.com/prustic/spark-connect-js/pull/98) [`a6237a1`](https://github.com/prustic/spark-connect-js/commit/a6237a1e5abd87ab8786d58dc1cd7b390fedd7d8) Thanks [@prustic](https://github.com/prustic)!
  - Re-exported proto schemas: `WriteStreamOperationStart`/`WriteStreamOperationStartResult`, `StreamingQueryCommand`/`StreamingQueryCommandResult`, `StreamingQueryManagerCommand`/`StreamingQueryManagerCommandResult`, `StreamingQueryListenerBusCommand`, `StreamingQueryListenerEvent`/`StreamingQueryListenerEventsResult`, `StreamingQueryEventType`, `StreamingQueryInstanceId`, `WithWatermark`, `DataType_NULL`, and `RelationCommon`, with their result and sub-command messages, consumed by `@spark-connect-js/node` for streaming commands, watermarks, and typed NULL literals

## 0.4.0

### Minor Changes

- [#61](https://github.com/prustic/spark-connect-js/pull/61) [`aa22cc6`](https://github.com/prustic/spark-connect-js/commit/aa22cc675cc3531056d5fc2f9715198b4b89dd2f) Thanks [@prustic](https://github.com/prustic)!
  - Vendored `google.rpc.Status` and `google.rpc.ErrorInfo` proto definitions, plus regenerated bindings for `FetchErrorDetailsRequest`/`Response`, consumed by `@spark-connect-js/node` for error-trailer decoding

## 0.3.0

### Minor Changes

- [#40](https://github.com/prustic/spark-connect-js/pull/40) [`d468479`](https://github.com/prustic/spark-connect-js/commit/d46847934011df16aefb39db7c3bb5fdcf220f73) Thanks [@prustic](https://github.com/prustic)!
  - Re-exported proto schemas: `WriteOperationV2Schema`, `WriteOperationV2_ModeSchema`

## 0.2.0

### Minor Changes

- [#18](https://github.com/prustic/spark-connect-js/pull/18) [`924ea50`](https://github.com/prustic/spark-connect-js/commit/924ea50d700711733cef96857a48c900dc8d7f4b) Thanks [@prustic](https://github.com/prustic)!
  - Re-exported proto schemas: `StatSummarySchema`, `NAReplaceSchema`, `NAReplace_ReplacementSchema`, `StatCorrSchema`, `StatCovSchema`, `StatCrosstabSchema`, `StatFreqItemsSchema`, `StatApproxQuantileSchema`, `UnpivotSchema`, `Unpivot_ValuesSchema`, `Aggregate_PivotSchema`
  - Re-exported analyze-plan schemas for `SameSemantics` and `SemanticHash`

## 0.1.0

### Minor Changes

- [#10](https://github.com/prustic/spark-connect-js/pull/10) [`895f389`](https://github.com/prustic/spark-connect-js/commit/895f389d703182ed149c4a634f48b894aa7d5131) Thanks [@prustic](https://github.com/prustic)! - Initial release. Generated TypeScript types and service stubs from the Spark Connect protobuf definitions.
  - **Protobuf types**: Plan, Relation, Expression, DataType, and all nested message types
  - **Service stubs**: ExecutePlanRequest/Response, AnalyzePlanRequest/Response, ConfigRequest/Response, AddArtifactsRequest/Response, ArtifactStatusesRequest/Response
  - **Schema objects**: StructType, StructField, MapType, ArrayType, and all Spark data type descriptors
  - Single runtime dependency: `@bufbuild/protobuf`
