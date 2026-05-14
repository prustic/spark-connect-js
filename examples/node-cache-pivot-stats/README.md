# node-cache-pivot-stats

A broader DataFrame workload: caching with explicit `persist` / `unpersist`, repartitioning, multi-dimensional aggregation via `cube` and `rollup`, reshaping with `pivot` / `unpivot`, the `DataFrameStat` functions (`corr`, `cov`, `freqItems`), `summary` and `replace`, `randomSplit`, temp views, and `sameSemantics` plan comparison.

## Run

```sh
pnpm spark:up
pnpm build && node dist/main.js
pnpm spark:down
```

Connects to `sc://localhost:15002` by default; override with `SPARK_REMOTE`. Requires Node.js 22+ and Docker.

See the [SQL and DataFrame guide](https://prustic.github.io/spark-connect-js/sql-and-dataframe-guide/) for the underlying transformations and actions.
