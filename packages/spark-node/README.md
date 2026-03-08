# @spark-connect-js/node

TypeScript client for [Apache Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html). Talks to Spark over gRPC and decodes results with Apache Arrow.

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

Requires Node.js >= 22 and a running [Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html) server (Spark 4.0+).

## Documentation

See the [spark-connect-js](https://github.com/prustic/spark-connect-js) repository for full documentation, examples, and contributing guidelines.

## License

[Apache-2.0](https://github.com/prustic/spark-connect-js/blob/main/LICENSE)
