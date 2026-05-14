---
"@spark-connect-js/core": minor
---

- Catalog parity with PySpark: the full `spark.catalog` surface (`currentCatalog`/`setCurrentCatalog`, `listCatalogs`/`listDatabases`/`listTables`/`listColumns`/`listFunctions`, `databaseExists`/`tableExists`/`functionExists`, `getDatabase`/`getTable`/`getFunction`, `dropTempView`/`dropGlobalTempView`, `cacheTable`/`uncacheTable`/`clearCache`/`isCached`, `refreshTable`/`refreshByPath`, `recoverPartitions`, `createTable`/`createExternalTable`)
- `spark.udf.registerJavaFunction(name, className, returnType?)` and `spark.udf.registerJavaUDAF(name, className)` for binding Java UDFs and UDAFs already on the server's classpath to a SQL function name
- `SparkSession.version()` returns the server's Spark version
- `SparkSession.builder().sessionId(uuid)` to reuse a server-side session by ID
- `RuntimeConfig` on `spark.conf` with `get`, `set`, `unset`, `getAll`, `isModifiable`
- Session tags and interrupts: `addTag`, `removeTag`, `getTags`, `clearTags`, `interruptAll`, `interruptTag`, `interruptOperation`
- `Transport` interface gains optional `config` and `interrupt` methods; `ExecuteOptions` plumbs per-call tags
- `SparkConnectError` exposes `errorClass`, `sqlState`, `messageParameters`, `errorTypeHierarchy`, and `serverStackTrace`
- Fix `count("*")` to send `count(1)` on the wire instead of `count(<unresolved-*>)`, matching PySpark and Scala behavior
