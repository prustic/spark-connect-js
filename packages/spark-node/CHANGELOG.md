# @spark-connect-js/node

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
