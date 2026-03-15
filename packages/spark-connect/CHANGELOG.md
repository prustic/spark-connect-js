# @spark-connect-js/connect

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

## 0.1.0

### Minor Changes

- [#10](https://github.com/prustic/spark-connect-js/pull/10) [`895f389`](https://github.com/prustic/spark-connect-js/commit/895f389d703182ed149c4a634f48b894aa7d5131) Thanks [@prustic](https://github.com/prustic)! - Initial release. Generated TypeScript types and service stubs from the Spark Connect protobuf definitions.
  - **Protobuf types**: Plan, Relation, Expression, DataType, and all nested message types
  - **Service stubs**: ExecutePlanRequest/Response, AnalyzePlanRequest/Response, ConfigRequest/Response, AddArtifactsRequest/Response, ArtifactStatusesRequest/Response
  - **Schema objects**: StructType, StructField, MapType, ArrayType, and all Spark data type descriptors
  - Single runtime dependency: `@bufbuild/protobuf`
