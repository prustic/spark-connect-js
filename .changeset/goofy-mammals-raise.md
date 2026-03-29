---
"@spark-connect-js/node": minor
---

- DataFrameReader shortcuts: `csv()`, `json()`, `parquet()`, `orc()`, `text()`, `schema()`
- DataFrameWriter shortcuts: `csv()`, `json()`, `parquet()`, `orc()`, `text()`, `bucketBy()`, `insertInto()`
- DataFrameWriterV2 with full `writeTo()` API: `create`, `replace`, `createOrReplace`, `append`, `overwrite`, `overwritePartitions`
- All throw sites classified with typed client errors from core