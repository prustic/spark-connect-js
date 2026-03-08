# @spark-connect-js/node

Node.js runtime for [spark-connect-js](https://github.com/prustic/spark-connect-js) — gRPC transport, Arrow decoding, and the `connect()` helper.

This is the main package most users need. It re-exports everything from `@spark-connect-js/core`.

## Install

```bash
npm install @spark-connect-js/node
```

## Usage

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

## Requirements

- Node.js >= 22
- Spark Connect server (Spark 4.0+)

## License

[Apache-2.0](https://github.com/prustic/spark-connect-js/blob/main/LICENSE)
