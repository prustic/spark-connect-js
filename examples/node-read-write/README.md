# node-read-write

Round-trips an in-memory sensor dataset through every file format spark-connect-js has shortcut methods for (CSV, JSON, Parquet, ORC, text), reads CSV back with a DDL schema, writes a bucketed `saveAsTable`, and appends to it with `insertInto`.

## Run

```sh
pnpm spark:up
pnpm build && node dist/main.js
pnpm spark:down
```

Connects to `sc://localhost:15002` by default; override with `SPARK_REMOTE`. Requires Node.js 22+ and Docker.

See the [I/O](https://prustic.github.io/spark-connect-js/io/) guide for the reader and writer APIs.
