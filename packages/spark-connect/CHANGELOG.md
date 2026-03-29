# @spark-connect-js/connect

## 0.3.0

### Minor Changes

- [#40](https://github.com/prustic/spark-connect-js/pull/40) [`d468479`](https://github.com/prustic/spark-connect-js/commit/d46847934011df16aefb39db7c3bb5fdcf220f73) Thanks [@prustic](https://github.com/prustic)! - - Re-exported proto schemas: `WriteOperationV2Schema`, `WriteOperationV2_ModeSchema`

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
