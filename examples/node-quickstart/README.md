# Node.js Quick Start

Minimal example that connects to a Spark Connect server, runs a few queries, and collects the results.

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

## What it does

1. Connects to Spark Connect at `sc://localhost:15002`
2. Creates an in-memory employees table via SQL
3. Filters and sorts rows using the DataFrame API
4. Aggregates with `groupBy` + `agg`
5. Registers a temp view and queries it with SQL
6. Stops the session

Override the Spark address with `SPARK_REMOTE`:

```sh
SPARK_REMOTE=sc://my-spark-host:15002 node dist/main.js
```
