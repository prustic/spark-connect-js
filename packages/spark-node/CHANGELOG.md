# @spark-connect-js/node

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
