# @spark-connect-js/connect

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
