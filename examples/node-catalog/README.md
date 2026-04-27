# node-catalog

Walks the `session.catalog` API: catalogs, databases, tables, functions, temp views, cache control, metadata refresh, and `createTable` against a `StructType` schema.

## Run

```sh
pnpm spark:up
pnpm build && node dist/main.js
pnpm spark:down
```

Connects to `sc://localhost:15002` by default; override with `SPARK_REMOTE`. Requires Node.js 22+ and Docker.

See the [Catalog](https://prustic.github.io/spark-connect-js/catalog/) guide for the API in detail.
