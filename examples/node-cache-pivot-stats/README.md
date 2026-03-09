# Caching, Pivot & Stats

Demonstrates caching, repartitioning, cube/rollup, pivot/unpivot, stat functions, summary, replace, random splits, views, and semantic comparison.

## Prerequisites

- Node.js >= 22
- Docker

## Run

```sh
# Start a local Spark Connect server
pnpm spark:up

# Build and run
pnpm build && node dist/main.js

# Stop Spark when done
pnpm spark:down
```

## What it covers

1. **Caching** — `persist(MEMORY_ONLY)`, `getStorageLevel()`, `unpersist()`
2. **Repartitioning** — `repartition()` by column, `coalesce()`
3. **Cube / Rollup** — multi-dimensional subtotals
4. **Pivot / Unpivot** — reshape between wide and long formats
5. **Summary / Replace** — descriptive stats and value substitution
6. **Random Split** — partition a DataFrame into train/test sets
7. **Stat Functions** — `stat.corr()`, `stat.cov()`, `stat.freqItems()`
8. **Views** — `createTempView()` and SQL queries over views
9. **Semantic Comparison** — `sameSemantics()` and `semanticHash()`

Override the Spark address with `SPARK_REMOTE`:

```sh
SPARK_REMOTE=sc://my-spark-host:15002 node dist/main.js
```
