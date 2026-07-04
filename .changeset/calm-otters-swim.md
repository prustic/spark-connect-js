---
"@spark-connect-js/node": minor
---

- Type-driven Arrow decode keyed on the column's Arrow type: `DECIMAL(p, s)` as a fixed-point string honoring scale, `DATE`/`TIMESTAMP` as `Date`, `MAP<K, V>` as `Map<K, V>` with typed keys, `LONG` always as `bigint`, applied recursively through structs and arrays
- `ArrowEncoder` backs `createDataFrame(rows)` with type inference over `string`, `number`, `boolean`, `bigint`, `Date`, and nulls; strings encode as materialized `Utf8` (never dictionary-encoded, which Spark `LocalRelation` misreads)
- `GrpcTransportOptions.handshakeTimeoutMs` (default `10_000`, `0` disables): the channel handshake fails with `errorClass: "CONNECTION_TIMEOUT"` instead of hanging on an unreachable or misconfigured endpoint
- `RetryPolicy.maxConsecutiveNoProgressReattaches` (default 3, `0` disables): a stream that keeps reattaching without delivering data throws `errorClass: "REATTACH_NO_PROGRESS"` instead of retrying forever
- `GrpcTransport` implements the streaming command RPCs (`WriteStreamOperationStart`, `StreamingQueryCommand`, `StreamingQueryManagerCommand`) and the listener event stream
- `parseConnectionString` rejects non-`sc://` schemes and userinfo in the host with messages naming the offending segment
