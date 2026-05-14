# @spark-connect-js/connect

[![npm version](https://img.shields.io/npm/v/@spark-connect-js/connect?style=flat&colorA=000000&colorB=000000)](https://www.npmjs.com/package/@spark-connect-js/connect)
[![CI](https://github.com/prustic/spark-connect-js/actions/workflows/ci.yml/badge.svg)](https://github.com/prustic/spark-connect-js/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://github.com/prustic/spark-connect-js/blob/main/LICENSE)

Generated TypeScript types and service definitions from the [Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html) protobuf spec. Internal to the [`@spark-connect-js`](https://github.com/prustic/spark-connect-js) packages; **most users don't install this directly.**

> **Note:** This project is in early development (v0.4.0) and is not recommended for production usage, but feedback is very welcome on [GitHub](https://github.com/prustic/spark-connect-js/issues).

If you're building an application against Spark Connect, use [`@spark-connect-js/node`](https://www.npmjs.com/package/@spark-connect-js/node). This package is what `node` and [`@spark-connect-js/core`](https://www.npmjs.com/package/@spark-connect-js/core) use internally to encode and decode the wire protocol.

## Install

```bash
npm install @spark-connect-js/connect
```

## Usage

```typescript
import { Plan, Relation, Expression, DataType, SparkConnectService } from "@spark-connect-js/connect";
```

Types are regenerated from the protobuf definitions in the [Apache Spark source tree](https://github.com/apache/spark/tree/master/sql/connect/common/src/main/protobuf/spark/connect) using [`buf`](https://buf.build/).

Single runtime dependency: [`@bufbuild/protobuf`](https://www.npmjs.com/package/@bufbuild/protobuf).

## Documentation

Full docs at [prustic.github.io/spark-connect-js](https://prustic.github.io/spark-connect-js/).

## License

[Apache-2.0](https://github.com/prustic/spark-connect-js/blob/main/LICENSE)
