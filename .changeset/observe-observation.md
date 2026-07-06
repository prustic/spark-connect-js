---
"@spark-connect-js/core": minor
"@spark-connect-js/node": minor
---

`DataFrame.observe(observation, ...metrics)` attaches named aggregate metrics, computed alongside the query without a second pass. Pass an `Observation` and read `observation.get` after an action, or pass a string name for metrics consumed via `StreamingQueryProgress.observedMetrics`. Metric values decode with the Arrow policy (longs as `bigint`, decimals as strings, temporals as `Date`). `ExecuteOptions` gains an `onObservedMetrics` callback for custom transports, and the `CollectMetrics` relation is wired through both plan builders.
