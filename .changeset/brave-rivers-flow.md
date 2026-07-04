---
"@spark-connect-js/core": minor
---

- Structured Streaming: `spark.readStream` (`DataStreamReader`) and `df.writeStream` (`DataStreamWriter`) with `Trigger` factories (`processingTime`, `availableNow`, `once`, `continuous`); `start()` returns a `StreamingQuery` (`id`, `runId`, `name`, `isActive`, `stop`, `awaitTermination`, `status`, `lastProgress`, `recentProgress`, `processAllAvailable`, `exception`, `explain`)
- `spark.streams` (`StreamingQueryManager`): `active`, `get`, `awaitAnyTermination`, `resetTerminated`, `addListener`/`removeListener` with `StreamingQueryListener` callbacks (`onQueryStarted`, `onQueryProgress`, `onQueryIdle`, `onQueryTerminated`) and typed `StreamingQueryProgress`
- Event-time aggregation: `DataFrame.withWatermark(eventTimeColumn, delayThreshold)`, `window(timeColumn, windowDuration, slideDuration?, startTime?)`, `session_window(timeColumn, gapDuration)`
- `createDataFrame(rows)` accepts plain row objects, encoded via the new `arrowEncoder` builder hook; `Uint8Array` input is validated as Arrow IPC stream format (file-format and empty input throw `InvalidInputError`)
- `spark.table(name)` reads a catalog table or temp view, shorthand for `spark.read.table(name)`
- Typed row access: `df.as<Schema>()` narrows collected rows at compile time; the `row` accessor namespace (`getInt`, `getLong`, `getDouble`, `getString`, `getBoolean`, `getBinary`, `getDate`) validates at runtime
- `df.agg(...exprs)` aggregates without grouping; `df.col(name)` binds a column reference to its DataFrame for self-joins
- Comparison, arithmetic, and bitwise `Column` methods accept raw primitives and wrap them as literals
- `filter` and `where` accept SQL string predicates
- `count()` returns `bigint`, matching the `LongType` result; wrap in `Number(...)` when the count is known to fit a JS safe integer
- `show()` renders dates, maps, structs, arrays, and binary in Spark's display style
- `lit(null)` emits a typed NULL literal and `lit(undefined)` throws `InvalidInputError`; `pivot(col, values)` accepts `null` values
- Optional `Transport.executeCommandStream` method for custom transports that stream command result frames
- `pow` accepts a `Column | number` exponent; `regexp_replace` accepts `Column | string` pattern and replacement; `element_at` accepts a numeric index
- `isSessionInvalidated(err)` matches `INVALID_HANDLE.*` errors so callers can rebuild the session after a server restart
