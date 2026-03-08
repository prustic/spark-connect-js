# @spark-connect-js/core

DataFrame API and logical plan builder for [Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html), in pure TypeScript with zero runtime dependencies.

```bash
npm install @spark-connect-js/core
```

```typescript
import { SparkSession, col, sum, desc, type Transport } from "@spark-connect-js/core";

const spark = new SparkSession(transport);

const df = spark
  .table("events")
  .filter(col("ts").gt(lit("2025-01-01")))
  .groupBy("category")
  .agg(sum("amount").alias("total"))
  .sort(desc("total"));
```

Provides `DataFrame`, `Column`, `SparkSession`, `Catalog`, `WindowSpec`, `DataFrameWriter`, `GroupedData`, and 248 built-in functions. Plans are serialized to Spark Connect protobuf but no I/O happens here. Bring your own `Transport` implementation, or use [`@spark-connect-js/node`](https://www.npmjs.com/package/@spark-connect-js/node) which wires everything together.

## Documentation

See the [spark-connect-js](https://github.com/prustic/spark-connect-js) repository.

## License

[Apache-2.0](https://github.com/prustic/spark-connect-js/blob/main/LICENSE)
