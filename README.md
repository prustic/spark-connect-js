<p align="center">
  <h2 align="center">spark-connect-js</h2>
  <p align="center">
    TypeScript client for Apache Spark Connect
    <br />
    <br />
    <a href="https://github.com/prustic/spark-connect-js/issues">Issues</a>
    ·
    <a href="https://github.com/prustic/spark-connect-js/blob/main/CONTRIBUTING.md">Contributing</a>
    ·
    <a href="https://github.com/prustic/spark-connect-js/blob/main/docs/api.md">API</a>
  </p>

  <p align="center">
    <a href="https://github.com/prustic/spark-connect-js/actions/workflows/ci.yml"><img src="https://github.com/prustic/spark-connect-js/actions/workflows/ci.yml/badge.svg" alt="CI" /></a>
    <a href="https://codecov.io/gh/prustic/spark-connect-js"><img src="https://codecov.io/gh/prustic/spark-connect-js/graph/badge.svg" alt="codecov" /></a>
    <a href="https://www.npmjs.com/package/@spark-connect-js/node"><img src="https://img.shields.io/npm/v/@spark-connect-js/node?style=flat&colorA=000000&colorB=000000" alt="npm version" /></a>
    <a href="LICENSE"><img src="https://img.shields.io/badge/License-Apache_2.0-blue.svg" alt="License" /></a>
  </p>
</p>

> [!NOTE]
> This project is in early development. APIs may change between releases.

## About

spark-connect-js is a TypeScript client for [Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html), the thin client protocol introduced in Spark 3.4 and expanded in Spark 4.0. It provides a Spark-like DataFrame API with full TypeScript types.

Plans are built in TypeScript and executed on the server over gRPC. The core package has no runtime dependencies, so it can work with different runtimes. Right now there's a Node.js adapter using gRPC and Apache Arrow.

## Quick Start (Node.js)

```bash
npm install @spark-connect-js/node
```

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

Requires a running Spark Connect server (Spark 4.0+). The Node.js adapter requires Node >= 22.

## Packages

| Package                     | Description                                        |
| --------------------------- | -------------------------------------------------- |
| `@spark-connect-js/node`    | Node.js runtime: gRPC transport + Arrow decoding   |
| `@spark-connect-js/core`    | DataFrame API and plan builder (platform-agnostic) |
| `@spark-connect-js/connect` | Generated protobuf types                           |

## Development

```bash
git clone https://github.com/prustic/spark-connect-js.git && cd spark-connect-js
pnpm install && pnpm blt
```

## Contribution

spark-connect-js is free and open source, licensed under [Apache-2.0](LICENSE).

- [Contribute to the source code](CONTRIBUTING.md)
- [Report bugs and suggest features](https://github.com/prustic/spark-connect-js/issues)

## Security

If you discover a security vulnerability, please see [SECURITY.md](SECURITY.md) for how to report it responsibly.
