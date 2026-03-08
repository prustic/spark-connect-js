# @spark-connect-js/core

Pure TypeScript DataFrame API and Spark Connect plan builder — zero runtime dependencies.

Part of [spark-connect-js](https://github.com/prustic/spark-connect-js). Most users should install `@spark-connect-js/node` instead, which includes this package and adds the gRPC + Arrow runtime.

## What's in this package

- `DataFrame`, `Column`, `GroupedData`, `DataFrameWriter`
- `SparkSession`, `Catalog`, `WindowSpec`
- 248 built-in functions (math, string, datetime, aggregate, window, collection, etc.)
- `PlanBuilder` — serializes the logical plan to Spark Connect protobuf
- `DataType`, `StructType`, `StructField`

## Install

```bash
npm install @spark-connect-js/core
```

## License

[Apache-2.0](https://github.com/prustic/spark-connect-js/blob/main/LICENSE)
