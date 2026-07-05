---
"@spark-connect-js/core": minor
"@spark-connect-js/node": minor
---

`StreamingQueryProgress` is fully typed against real Spark 4.0 Connect payloads from both delivery paths (`lastProgress()` and listener-bus events): new exported `SourceProgress`, `SinkProgress`, and `StateOperatorProgress` interfaces replace the `unknown` nested shapes, `eventTime` and `observedMetrics` are added, and the dead `bigint` branch on `numInputRows` is dropped (progress is JSON-parsed, never Arrow-decoded). Top-level row counts are absent from `lastProgress()` where the per-source values are authoritative, so the new `totalInputRows(progress)` helper sums `sources[].numInputRows` on either path.
