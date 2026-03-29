# @spark-connect-js/node

## 0.3.0

### Minor Changes

- [#40](https://github.com/prustic/spark-connect-js/pull/40) [`d468479`](https://github.com/prustic/spark-connect-js/commit/d46847934011df16aefb39db7c3bb5fdcf220f73) Thanks [@prustic](https://github.com/prustic)! - - DataFrameReader shortcuts: `csv()`, `json()`, `parquet()`, `orc()`, `text()`, `schema()`
  - DataFrameWriter shortcuts: `csv()`, `json()`, `parquet()`, `orc()`, `text()`, `bucketBy()`, `insertInto()`
  - DataFrameWriterV2 with full `writeTo()` API: `create`, `replace`, `createOrReplace`, `append`, `overwrite`, `overwritePartitions`
  - All throw sites classified with typed client errors from core

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
