# Node.js Read / Write

Demonstrates reading and writing data in multiple formats, applying schemas on read, bucketing tables, and appending with `insertInto`.

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

1. Creates an in-memory sensor readings dataset
2. Roundtrips data through every format: CSV, JSON, Parquet, ORC, and text
3. Reads CSV back with an explicit DDL schema
4. Writes a bucketed table partitioned by `sensor_type`
5. Appends new rows with `insertInto`

Override the Spark address with `SPARK_REMOTE`:

```sh
SPARK_REMOTE=sc://my-spark-host:15002 node dist/main.js
```
