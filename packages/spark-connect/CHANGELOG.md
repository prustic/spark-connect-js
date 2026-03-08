# @spark-connect-js/connect

## 0.1.0

### Minor Changes

- [#10](https://github.com/prustic/spark-connect-js/pull/10) [`895f389`](https://github.com/prustic/spark-connect-js/commit/895f389d703182ed149c4a634f48b894aa7d5131) Thanks [@prustic](https://github.com/prustic)! - Initial release. Generated TypeScript types and service stubs from the Spark Connect protobuf definitions.
  - **Protobuf types**: Plan, Relation, Expression, DataType, and all nested message types
  - **Service stubs**: ExecutePlanRequest/Response, AnalyzePlanRequest/Response, ConfigRequest/Response, AddArtifactsRequest/Response, ArtifactStatusesRequest/Response
  - **Schema objects**: StructType, StructField, MapType, ArrayType, and all Spark data type descriptors
  - Single runtime dependency: `@bufbuild/protobuf`
