---
"@spark-connect-js/core": minor
"@spark-connect-js/node": minor
---

`createDataFrame` accepts a `StructType` schema and honors its per-field nullability, so `NOT NULL` schemas round-trip through `schema()` and back. The schema's field names are validated against the row keys, and a null in a `NOT NULL` column throws `InvalidInputError` naming the column and row before anything is sent. The `clientType` version string reported to the server is now kept in lockstep with the package version, enforced by a test that fails when they drift.
