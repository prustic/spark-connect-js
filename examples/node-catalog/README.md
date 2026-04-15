# Catalog API

Demonstrates the full Spark Catalog API: inspecting databases, tables, functions, caching, and creating tables with schemas.

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

1. **Catalogs** — `currentCatalog()`, `listCatalogs()`
2. **Databases** — `currentDatabase()`, `databaseExists()`, `getDatabase()`, `listDatabases()`
3. **Functions** — `functionExists()`, `getFunction()`, `listFunctions()`
4. **Tables** — `tableExists()`, `getTable()`, `listTables()`, `listColumns()`
5. **Caching** — `cacheTable()`, `isCached()`, `uncacheTable()`, `clearCache()`
6. **Metadata refresh** — `refreshTable()`
7. **View management** — `dropTempView()`, `dropGlobalTempView()`
8. **Table creation** — `createTable()` with `StructType` schema

Override the Spark address with `SPARK_REMOTE`:

```sh
SPARK_REMOTE=sc://my-spark-host:15002 node dist/main.js
```
