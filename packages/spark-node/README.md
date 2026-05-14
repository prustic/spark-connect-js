# @spark-connect-js/node

[![npm version](https://img.shields.io/npm/v/@spark-connect-js/node?style=flat&colorA=000000&colorB=000000)](https://www.npmjs.com/package/@spark-connect-js/node)
[![CI](https://github.com/prustic/spark-connect-js/actions/workflows/ci.yml/badge.svg)](https://github.com/prustic/spark-connect-js/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/prustic/spark-connect-js/graph/badge.svg)](https://codecov.io/gh/prustic/spark-connect-js)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://github.com/prustic/spark-connect-js/blob/main/LICENSE)

TypeScript client for [Apache Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html). Talks to Spark over gRPC and decodes results with Apache Arrow. No JVM in your Node process.

> **Note:** This project is in early development (v0.3.0) and is not recommended for production usage, but feedback is very welcome on [GitHub](https://github.com/prustic/spark-connect-js/issues).

## Install

```bash
npm install @spark-connect-js/node
```

## Quick example

```typescript
import { connect, col, sum, desc } from "@spark-connect-js/node";

const spark = connect("sc://localhost:15002");

const result = await spark
  .table("employees")
  .filter(col("age").gt(30))
  .groupBy("dept")
  .agg(sum("salary").alias("total"))
  .sort(desc("total"))
  .collect();

await spark.stop();
```

DataFrame methods build a logical plan locally. The plan is sent to the server only when an action runs (`collect`, `count`, `show`, `first`, `head`, `take`, `toLocalIterator`, or a `DataFrameWriter` save method).

## Compatibility

| Component            | Required                                            |
| -------------------- | --------------------------------------------------- |
| Node.js              | 22 LTS or 24.x                                      |
| Spark Connect server | Spark 3.4+ (recommended: 4.0+)                      |
| Connection scheme    | `sc://host:port[/;use_ssl=true][;token=...][;...]`  |

## Package layout

This package re-exports the full public API from [`@spark-connect-js/core`](https://www.npmjs.com/package/@spark-connect-js/core) and adds the gRPC transport (via [`@grpc/grpc-js`](https://www.npmjs.com/package/@grpc/grpc-js)) and the Arrow IPC decoder (via [`apache-arrow`](https://www.npmjs.com/package/apache-arrow)). For application code, this is the only package you install. [`@spark-connect-js/connect`](https://www.npmjs.com/package/@spark-connect-js/connect) is pulled in transitively as an implementation detail.

## Documentation

Full docs at [prustic.github.io/spark-connect-js](https://prustic.github.io/spark-connect-js/).

- [Quickstart](https://prustic.github.io/spark-connect-js/quickstart/): start a local Spark Connect server, run a query
- [SQL and DataFrame guide](https://prustic.github.io/spark-connect-js/sql-and-dataframe-guide/): transformations, actions, the Column DSL
- [Comparison to PySpark](https://prustic.github.io/spark-connect-js/pyspark-comparison/): differences between PySpark and this client
- [I/O](https://prustic.github.io/spark-connect-js/io/): reading and writing Parquet, ORC, CSV, JSON
- [Error handling](https://prustic.github.io/spark-connect-js/error-handling/): typed error hierarchy and gRPC status codes
- [Roadmap](https://prustic.github.io/spark-connect-js/roadmap/): what's shipped, what's planned

Runnable example scripts in [`examples/`](https://github.com/prustic/spark-connect-js/tree/main/examples).

## License

[Apache-2.0](https://github.com/prustic/spark-connect-js/blob/main/LICENSE)
