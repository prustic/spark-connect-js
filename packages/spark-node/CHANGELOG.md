# @spark-connect-js/node

## 0.2.0

### Minor Changes

- [#18](https://github.com/prustic/spark-connect-js/pull/18) [`924ea50`](https://github.com/prustic/spark-connect-js/commit/924ea50d700711733cef96857a48c900dc8d7f4b) Thanks [@prustic](https://github.com/prustic)! - ### @spark-connect-js/core
  - `DataFrame.cube()`, `.rollup()` for multi-dimensional aggregation
  - `DataFrame.unpivot()` / `.melt()` for wide-to-long reshaping
  - `DataFrame.summary()` for descriptive statistics
  - `DataFrame.replace()` for value substitution via `NAReplace`
  - `DataFrame.randomSplit()` for splitting into multiple DataFrames
  - `DataFrame.createTempView()`, `.createGlobalTempView()`, `.createOrReplaceGlobalTempView()`
  - `DataFrame.sameSemantics()` and `.semanticHash()` for plan comparison
  - `DataFrameStat` class (`.stat` accessor) with `corr()`, `cov()`, `crosstab()`, `freqItems()`, `approxQuantile()`
  - `GroupedData.pivot()` support with cube/rollup/pivot group types

  ### @spark-connect-js/node
  - Proto serialization for `StatSummary`, `NAReplace`, `Unpivot`, `StatCorr`, `StatCov`, `StatCrosstab`, `StatFreqItems`, `StatApproxQuantile`, and `Aggregate_Pivot`
  - Added analyze-plan request/response handling for `sameSemantics` and `semanticHash`
  - Re-exported `DataFrameStat` from package index

  ### @spark-connect-js/connect
  - Re-exported proto schemas: `StatSummarySchema`, `NAReplaceSchema`, `NAReplace_ReplacementSchema`, `StatCorrSchema`, `StatCovSchema`, `StatCrosstabSchema`, `StatFreqItemsSchema`, `StatApproxQuantileSchema`, `UnpivotSchema`, `Unpivot_ValuesSchema`, `Aggregate_PivotSchema`
  - Re-exported analyze-plan schemas for `SameSemantics` and `SemanticHash`

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
