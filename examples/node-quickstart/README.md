# node-quickstart

A small DataFrame pipeline against a local Spark Connect server: an inline `VALUES` table of employees, a filter + groupBy aggregation, and a temp view round-tripped through SQL.

## Run

```sh
pnpm spark:up
pnpm build && node dist/main.js
pnpm spark:down
```

Connects to `sc://localhost:15002` by default; override with `SPARK_REMOTE`. Requires Node.js 22+ and Docker.

See the [Quickstart](https://prustic.github.io/spark-connect-js/quickstart/) page for the same code with commentary.
