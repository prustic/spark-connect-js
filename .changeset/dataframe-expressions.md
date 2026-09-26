---
"@spark-connect-js/core": minor
"@spark-connect-js/node": minor
"@spark-connect-js/connect": minor
---

New `DataFrame` methods: `colRegex`, `metadataColumn`, `withMetadata`, `toJSON`, `toArrow`, and the `sparkSession` getter, plus `GroupedData.mean`. `toJSON` returns a DataFrame with a single `value` column, as PySpark's Connect client does. `toArrow` returns the raw Arrow IPC streams rather than a decoded table, since the core package has no Arrow dependency.

`col("*")` and `col("t.*")` now expand to every column. They previously reached the server as a column literally named `*` and failed with `UNRESOLVED_COLUMN`, so `select("*")` did not work.
