---
"@spark-connect-js/core": patch
---

`StreamingQueryProgress.observedMetrics` now matches its declared `Record<string, Row>` type. The server sends each metric as a `{ values, schema }` wrapper, so field access previously returned `undefined`; values are zipped against the schema field names on both the polling and listener paths.

`StructType.toDDL()` quotes field names that are not plain identifiers and doubles inner back-ticks, matching Spark's own rule, so schemas with spaces or dots in column names round-trip instead of producing invalid DDL.

`createDataFrame(rows, ddl)` rejects a DDL string containing `NOT NULL` up front, pointing at the `StructType` overload that can carry nullability, instead of letting the server fail with a positional column name.
