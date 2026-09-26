---
"@spark-connect-js/core": minor
"@spark-connect-js/node": minor
"@spark-connect-js/connect": minor
---

New `DataFrame` methods answered by the server: `isLocal`, `isStreaming`, `inputFiles`, `checkpoint`, and `localCheckpoint`. All five are async, where PySpark's are synchronous and `isStreaming` is a property, since each needs a round trip. A response missing its result throws rather than defaulting to `false` or an empty list.

`checkpoint` requires the server to be started with `spark.checkpoint.dir`, a static setting a client cannot change at runtime; `localCheckpoint` needs no directory. A checkpointed DataFrame's server-side data lives until the session stops, since it is not released when the DataFrame is garbage-collected as it is in PySpark.
