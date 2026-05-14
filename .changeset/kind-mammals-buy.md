---
"@spark-connect-js/node": minor
---

- Full `sc://` connection-string grammar parsed: TLS via `use_ssl=true`, bearer `token`, `user_id`, `user_agent`, `session_id` (UUID), `grpc_max_message_size`, plus arbitrary `key=value` pairs that pass through as gRPC metadata on every RPC
- Bearer token attached as `authorization: Bearer <token>` via `combineChannelCredentials(createSsl(), createFromMetadataGenerator(...))`
- Canonical `user_agent` suffix: `<your prefix> spark-connect-js/<ver> (node <ver>; <platform>)`.
- Per-request operation IDs (UUIDv4) on every `ExecutePlan` request
- `ReattachExecute` iterator resumes server-streaming responses after transient gRPC drops (`UNAVAILABLE`, `INTERNAL` with `INVALID_CURSOR.DISCONNECTED`) without re-executing the plan
- Configurable retry policy via `GrpcTransportOptions.retryPolicy`; default mirrors PySpark (`maxRetries=15`, `initialBackoffMs=50`, `maxBackoffMs=60_000`, `backoffMultiplier=4`, `jitterMs=500`)
- Error trailers: decode `grpc-status-details-bin` (`google.rpc.Status` + `ErrorInfo`) to populate `errorClass`, `sqlState`, `messageParameters` on `SparkConnectError`, with fall back to a `FetchErrorDetails` RPC for `errorTypeHierarchy` and `serverStackTrace` when the inline trailer is incomplete
- `client_observed_server_side_session_id` captured from every response and echoed back on subsequent RPCs for stale-session detection; cleared on `ReleaseSession`
- `Config` and `Interrupt` RPCs wired (consumed by `spark.conf` and `interrupt*` on core)