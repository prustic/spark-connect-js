---
"@spark-connect-js/core": minor
"@spark-connect-js/node": minor
"@spark-connect-js/connect": minor
---

New `DataFrame` methods answered by the server: `isLocal`, `isStreaming`, `inputFiles`, `checkpoint`, and `localCheckpoint`. All five are async, where PySpark's are synchronous and `isStreaming` is a property, since each needs a round trip. A response missing its result throws rather than defaulting to `false` or an empty list.

`checkpoint` requires the server to be started with `spark.checkpoint.dir`, a static setting a client cannot change at runtime; `localCheckpoint` needs no directory. The server releases a checkpointed relation once the DataFrame and everything derived from it are garbage-collected, or when the session stops. This is best effort, as in the Scala and PySpark clients. Reliable checkpoint files outlive both: the server deletes them only if it enables `spark.cleaner.referenceTracking.cleanCheckpoints`, and only after the relation is released.
