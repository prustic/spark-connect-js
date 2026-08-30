# @spark-connect-js/node

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

- Updated dependencies [[`df4c3c6`](https://github.com/prustic/spark-connect-js/commit/df4c3c6a7d227b6f343ba464d5c9b993adc9700b), [`13645bc`](https://github.com/prustic/spark-connect-js/commit/13645bcbd76a59af2aa052b11b6077ab085b0157), [`2759a64`](https://github.com/prustic/spark-connect-js/commit/2759a64bac2365ecf6d5f474014c620531babd54), [`c700e57`](https://github.com/prustic/spark-connect-js/commit/c700e57886605892057d9492c048ae6ddb68c0a3), [`cb28a25`](https://github.com/prustic/spark-connect-js/commit/cb28a25705be98d9e220a66f784f645d59c3fdcc), [`6f6cca0`](https://github.com/prustic/spark-connect-js/commit/6f6cca0ea889f331b961586528f8a37f4c11b731), [`31b5bdf`](https://github.com/prustic/spark-connect-js/commit/31b5bdf59b1ffdae3026a93ac706726f8f1d083c), [`fbcdbf4`](https://github.com/prustic/spark-connect-js/commit/fbcdbf4110a612958efd7523f27d20d673dfd719), [`75c5c56`](https://github.com/prustic/spark-connect-js/commit/75c5c5659ef0f04dfc0a677d43ea840d70696d9c)]:
  - @spark-connect-js/core@0.6.0
  - @spark-connect-js/connect@0.6.0

## 0.5.1

### Patch Changes

- [#101](https://github.com/prustic/spark-connect-js/pull/101) [`0a8e2de`](https://github.com/prustic/spark-connect-js/commit/0a8e2dee8f9df0911cbd2962314ac4cf743c6aa5) Thanks [@prustic](https://github.com/prustic)! - Drop the hardcoded version from the README development-status note. 0.5.0 published with the note still reading v0.4.0, since npm snapshots the README at publish time, and a version-free note cannot go stale.
- Updated dependencies [[`0a8e2de`](https://github.com/prustic/spark-connect-js/commit/0a8e2dee8f9df0911cbd2962314ac4cf743c6aa5)]:
  - @spark-connect-js/core@0.5.1
  - @spark-connect-js/connect@0.5.1

## 0.5.0

### Minor Changes

- [#98](https://github.com/prustic/spark-connect-js/pull/98) [`a6237a1`](https://github.com/prustic/spark-connect-js/commit/a6237a1e5abd87ab8786d58dc1cd7b390fedd7d8) Thanks [@prustic](https://github.com/prustic)!
  - Type-driven Arrow decode keyed on the column's Arrow type: `DECIMAL(p, s)` as a fixed-point string honoring scale, `DATE`/`TIMESTAMP` as `Date`, `MAP<K, V>` as `Map<K, V>` with typed keys, `LONG` always as `bigint` (wrap in `Number(...)` for values known to fit a JS safe integer), applied recursively through structs and arrays
  - `ArrowEncoder` backs `createDataFrame(rows)` with type inference over `string`, `number`, `boolean`, `bigint`, `Date`, and nulls; strings encode as materialized `Utf8` (never dictionary-encoded, which Spark `LocalRelation` misreads)
  - `GrpcTransportOptions.handshakeTimeoutMs` (default `10_000`, `0` disables): the channel handshake fails with `errorClass: "CONNECTION_TIMEOUT"` instead of hanging on an unreachable or misconfigured endpoint
  - `RetryPolicy.maxConsecutiveNoProgressReattaches` (default 3, `0` disables): a stream that keeps reattaching without delivering data throws `errorClass: "REATTACH_NO_PROGRESS"` instead of retrying forever
  - `GrpcTransport` implements the streaming command RPCs (`WriteStreamOperationStart`, `StreamingQueryCommand`, `StreamingQueryManagerCommand`) and the listener event stream
  - `parseConnectionString` rejects non-`sc://` schemes and userinfo in the host with messages naming the offending segment

### Patch Changes

- Updated dependencies [[`a6237a1`](https://github.com/prustic/spark-connect-js/commit/a6237a1e5abd87ab8786d58dc1cd7b390fedd7d8), [`a6237a1`](https://github.com/prustic/spark-connect-js/commit/a6237a1e5abd87ab8786d58dc1cd7b390fedd7d8)]:
  - @spark-connect-js/core@0.5.0
  - @spark-connect-js/connect@0.5.0

## 0.4.0

### Minor Changes

- [#61](https://github.com/prustic/spark-connect-js/pull/61) [`aa22cc6`](https://github.com/prustic/spark-connect-js/commit/aa22cc675cc3531056d5fc2f9715198b4b89dd2f) Thanks [@prustic](https://github.com/prustic)!
  - Full `sc://` connection-string grammar parsed: TLS via `use_ssl=true`, bearer `token`, `user_id`, `user_agent`, `session_id` (UUID), `grpc_max_message_size`, plus arbitrary `key=value` pairs that pass through as gRPC metadata on every RPC
  - Bearer token attached as `authorization: Bearer <token>` via `combineChannelCredentials(createSsl(), createFromMetadataGenerator(...))`
  - Canonical `user_agent` suffix: `<your prefix> spark-connect-js/<ver> (node <ver>; <platform>)`.
  - Per-request operation IDs (UUIDv4) on every `ExecutePlan` request
  - `ReattachExecute` iterator resumes server-streaming responses after transient gRPC drops (`UNAVAILABLE`, `INTERNAL` with `INVALID_CURSOR.DISCONNECTED`) without re-executing the plan
  - Configurable retry policy via `GrpcTransportOptions.retryPolicy`; default mirrors PySpark (`maxRetries=15`, `initialBackoffMs=50`, `maxBackoffMs=60_000`, `backoffMultiplier=4`, `jitterMs=500`)
  - Error trailers: decode `grpc-status-details-bin` (`google.rpc.Status` + `ErrorInfo`) to populate `errorClass`, `sqlState`, `messageParameters` on `SparkConnectError`, with fallback to a `FetchErrorDetails` RPC for `errorTypeHierarchy` and `serverStackTrace` when the inline trailer is incomplete
  - `client_observed_server_side_session_id` captured from every response and echoed back on subsequent RPCs for stale-session detection; cleared on `ReleaseSession`
  - `Config` and `Interrupt` RPCs wired (consumed by `spark.conf` and `interrupt*` on core)

### Patch Changes

- Updated dependencies [[`aa22cc6`](https://github.com/prustic/spark-connect-js/commit/aa22cc675cc3531056d5fc2f9715198b4b89dd2f), [`aa22cc6`](https://github.com/prustic/spark-connect-js/commit/aa22cc675cc3531056d5fc2f9715198b4b89dd2f)]:
  - @spark-connect-js/core@0.4.0
  - @spark-connect-js/connect@0.4.0

## 0.3.0

### Minor Changes

- [#40](https://github.com/prustic/spark-connect-js/pull/40) [`d468479`](https://github.com/prustic/spark-connect-js/commit/d46847934011df16aefb39db7c3bb5fdcf220f73) Thanks [@prustic](https://github.com/prustic)!
  - Proto serialization for `WriteOperationV2` command
  - `SparkProcessManager` throw sites reclassified to `SparkClientError`
  - Re-exported typed client errors from core

### Patch Changes

- Updated dependencies [[`d468479`](https://github.com/prustic/spark-connect-js/commit/d46847934011df16aefb39db7c3bb5fdcf220f73), [`d468479`](https://github.com/prustic/spark-connect-js/commit/d46847934011df16aefb39db7c3bb5fdcf220f73)]:
  - @spark-connect-js/connect@0.3.0
  - @spark-connect-js/core@0.3.0

## 0.2.0

### Minor Changes

- [#18](https://github.com/prustic/spark-connect-js/pull/18) [`924ea50`](https://github.com/prustic/spark-connect-js/commit/924ea50d700711733cef96857a48c900dc8d7f4b) Thanks [@prustic](https://github.com/prustic)!
  - Proto serialization for `StatSummary`, `NAReplace`, `Unpivot`, `StatCorr`, `StatCov`, `StatCrosstab`, `StatFreqItems`, `StatApproxQuantile`, and `Aggregate_Pivot`
  - Added analyze-plan request/response handling for `sameSemantics` and `semanticHash`
  - Re-exported `DataFrameStat` from package index

### Patch Changes

- Updated dependencies [[`924ea50`](https://github.com/prustic/spark-connect-js/commit/924ea50d700711733cef96857a48c900dc8d7f4b)]:
  - @spark-connect-js/core@0.2.0
  - @spark-connect-js/connect@0.2.0

## 0.1.0

### Minor Changes

- [#10](https://github.com/prustic/spark-connect-js/pull/10) [`895f389`](https://github.com/prustic/spark-connect-js/commit/895f389d703182ed149c4a634f48b894aa7d5131) Thanks [@prustic](https://github.com/prustic)! - Initial release. Node.js runtime adapter for Spark Connect with gRPC transport, Arrow decoding, and convenience re-exports of the full core API.
  - **GrpcTransport**: connects to Spark Connect over gRPC, streams ExecutePlan responses, handles metadata and session management
  - **ArrowDecoder**: deserializes Arrow IPC batches into JavaScript row objects
  - **SparkProcessManager**: launches and manages local `spark-connect` server processes for development
  - **buildRelation / buildExpression**: serializes logical plan nodes and expressions to protobuf wire format
  - Re-exports the entire `@spark-connect-js/core` public API (SparkSession, DataFrame, Column, functions, etc.) for single-package convenience

### Patch Changes

- Updated dependencies [[`895f389`](https://github.com/prustic/spark-connect-js/commit/895f389d703182ed149c4a634f48b894aa7d5131)]:
  - @spark-connect-js/connect@0.1.0
  - @spark-connect-js/core@0.1.0
