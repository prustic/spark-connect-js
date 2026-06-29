# node-streaming

A small Structured Streaming pipeline against a local Spark Connect server: a `rate` source into a `memory` sink, with a `StreamingQueryListener` printing lifecycle events and a `spark.streams.active()` round-trip.

## Run

```sh
pnpm spark:up
pnpm build && node dist/main.js
pnpm spark:down
```

Connects to `sc://localhost:15002` by default; override with `SPARK_REMOTE`. Requires Node.js 22+ and Docker.

Output looks like:

```text
started    abc12...  run=def34...
active queries: [abc12...]
progress   batch=0 inputRowsPerSecond=0.0
progress   batch=1 inputRowsPerSecond=5.1
progress   batch=2 inputRowsPerSecond=5.0
progress   batch=3 inputRowsPerSecond=5.0
last batch: 4
terminated abc12... (clean)
after stop: 0 active queries
session stopped
```

See the [Structured Streaming](https://prustic.github.io/spark-connect-js/guides/structured-streaming/) guide for commentary.
