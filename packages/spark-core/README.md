# @spark-connect-js/core

[![npm version](https://img.shields.io/npm/v/@spark-connect-js/core?style=flat&colorA=000000&colorB=000000)](https://www.npmjs.com/package/@spark-connect-js/core)
[![CI](https://github.com/prustic/spark-connect-js/actions/workflows/ci.yml/badge.svg)](https://github.com/prustic/spark-connect-js/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/prustic/spark-connect-js/graph/badge.svg)](https://codecov.io/gh/prustic/spark-connect-js)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://github.com/prustic/spark-connect-js/blob/main/LICENSE)

DataFrame API and logical plan builder for [Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html), in pure TypeScript with zero runtime dependencies.

> **Note:** This project is in early development and is not recommended for production usage, but feedback is very welcome on [GitHub](https://github.com/prustic/spark-connect-js/issues).

## Install

```bash
npm install @spark-connect-js/core
```

Most applications install [`@spark-connect-js/node`](https://www.npmjs.com/package/@spark-connect-js/node) instead, which re-exports this package and adds a transport. Install `core` directly only if you're writing your own runtime adapter (Bun, Deno, browser, custom RPC).

## Quick example

```typescript
import { SparkSession, col, sum, desc, type Transport } from "@spark-connect-js/core";

const spark = SparkSession.builder()
  .remote("sc://localhost:15002")
  .transport(transport)
  .getOrCreate();

const df = spark
  .table("events")
  .filter(col("ts").gt("2025-01-01"))
  .groupBy("category")
  .agg(sum("amount").alias("total"))
  .sort(desc("total"));
```

Provides `SparkSession`, `DataFrame`, `Column`, `Catalog`, `WindowSpec`, `DataFrameWriter`, `DataFrameWriterV2`, `GroupedData`, `DataFrameStat`, the typed error hierarchy, and the built-in function set. Plans are serialized to Spark Connect protobuf, but no I/O happens here; you supply the `Transport`.

## The `Transport` interface

A runtime adapter implements `Transport` (one method per Spark Connect RPC) and hands it to `SparkSession`. The full interface and the contract for each method are in the [architecture guide](https://prustic.github.io/spark-connect-js/architecture/). [`@spark-connect-js/node`](https://www.npmjs.com/package/@spark-connect-js/node) is the reference implementation built on `@grpc/grpc-js` and `apache-arrow`.

## Documentation

Full docs at [prustic.github.io/spark-connect-js](https://prustic.github.io/spark-connect-js/).

- [Architecture](https://prustic.github.io/spark-connect-js/architecture/): the plan pipeline, types over the wire, sessions
- [SQL and DataFrame guide](https://prustic.github.io/spark-connect-js/sql-and-dataframe-guide/): transformations, actions, the Column DSL
- [Error handling](https://prustic.github.io/spark-connect-js/error-handling/): the typed error hierarchy
- [Roadmap](https://prustic.github.io/spark-connect-js/roadmap/): what's shipped, what's planned

## License

[Apache-2.0](https://github.com/prustic/spark-connect-js/blob/main/LICENSE)
