# spark-js Roadmap

A prioritized implementation plan organized into milestones, based on the [client comparison](./client-comparison.md) gap analysis.

> **Why "spark-js"?** Unlike `spark-connect-{lang}` naming, this project isn't just a Spark Connect client in a single language — it's a platform providing idiomatic Spark Connect adapters for JavaScript frameworks. Currently Node.js, with planned adapters for NestJS, Express, Next.js, Nuxt.js, Fastify, Astro, Electron, and more.

**Current state**: 248 functions (across 12 category files), 51 DataFrame methods, 42 Column methods, 6 GroupedData methods, 9 DataFrameReader + 8 DataFrameWriter methods, 7 Catalog methods, full Window support. 343 tests passing.

**PySpark reference surface**: ~500+ functions/methods across all categories (see [client-comparison.md](./client-comparison.md) for exhaustive tracking).

---

## Milestone 1 — Core DataFrame Gaps (Foundation) ✅

**Status**: Complete. All P0/P1/P2 items shipped except `summary()` and `replace()` (deferred to M3).

**Goal**: Close the most visible gaps that every Spark tutorial and StackOverflow answer assumes exist. These are the methods users reach for first.

### 1.1 DataFrame missing essentials

| Feature                            | Priority | Complexity | Status                                       |
| ---------------------------------- | -------- | ---------- | -------------------------------------------- |
| `withColumnRenamed(existing, new)` | P0       | Low        | ✅ Done                                      |
| `withColumnsRenamed(colsMap)`      | P0       | Low        | ✅ Done                                      |
| `columns` (property)               | P0       | Low        | ✅ Done                                      |
| `dtypes` (property)                | P0       | Low        | ✅ Done                                      |
| `isEmpty()`                        | P0       | Low        | ✅ Done                                      |
| `tail(n)`                          | P1       | Low        | ✅ Done                                      |
| `alias(name)`                      | P1       | Low        | ✅ Done                                      |
| `selectExpr(...exprs)`             | P1       | Low        | ✅ Done — uses `Expression.ExpressionString` |
| `summary(...statistics)`           | P1       | Low        | ⏭ Deferred to M3 (`StatSummary` relation)   |
| `replace(to, value, subset)`       | P1       | Medium     | ⏭ Deferred to M3 (`NAReplace` relation)     |
| `hint(name, ...params)`            | P2       | Low        | ✅ Done                                      |
| `transform(fn)`                    | P2       | Low        | ✅ Done                                      |
| `sortWithinPartitions(...)`        | P2       | Low        | ✅ Done                                      |

### 1.2 GroupedData missing methods

| Feature        | Priority | Complexity | Status  |
| -------------- | -------- | ---------- | ------- |
| `min(...cols)` | P0       | Low        | ✅ Done |
| `max(...cols)` | P0       | Low        | ✅ Done |

### 1.3 Column missing methods

| Feature                 | Priority | Complexity | Status                  |
| ----------------------- | -------- | ---------- | ----------------------- |
| `isNaN()`               | P0       | Low        | ✅ Done                 |
| `substr(pos, len)`      | P1       | Low        | ✅ Done                 |
| `asc_nulls_first/last`  | P1       | Low        | ✅ Done                 |
| `desc_nulls_first/last` | P1       | Low        | ✅ Done                 |
| `bitwiseAND/OR/XOR`     | P2       | Low        | ✅ Done                 |
| `getField(name)`        | P2       | Low        | Expression extraction   |
| `getItem(key)`          | P2       | Low        | Expression extraction   |
| `dropFields(*names)`    | P2       | Medium     | UpdateFields expression |
| `withField(name, col)`  | P2       | Medium     | UpdateFields expression |
| `eqNullSafe(other)`     | P2       | Low        | Binary operator `<=>`   |
| `ilike(pattern)`        | P2       | Low        | UnresolvedFunction      |

### 1.4 SparkSession gaps

| Feature                   | Priority | Complexity | Notes                                |
| ------------------------- | -------- | ---------- | ------------------------------------ |
| `range(start, end, step)` | P0       | Low        | Uses `Range` relation (proto exists) |

**Estimated items**: ~25-28 features
**Estimated tests**: ~40-50 new tests

---

## Milestone 2 — Functions Expansion (Breadth) ✅

**Status**: Complete. 248 exported functions across 12 category files (math, string, datetime, aggregate, window, conditional, collection, hash, json, bitwise, csv, sort). 93 new tests added (343 total).

**Goal**: Bring functions from ~55 to ~150+. Focus on the most commonly used categories. Each function is a thin wrapper over `UnresolvedFunction` — fast to implement in batch.

### 2.1 Math functions (high-frequency)

`log2`, `log10`, `log1p`, `sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `atan2`, `sinh`, `cosh`, `tanh`, `cbrt`, `signum`/`sign`, `degrees`, `radians`, `factorial`, `greatest`, `least`, `hex`, `unhex`, `bin`, `bround`, `rand`, `randn`, `pmod`, `rint`, `hypot`, `expm1`, `negate`/`negative`/`positive`, `e`, `pi`

**Count**: ~35 functions

### 2.2 String functions (high-frequency)

`concat_ws`, `split`, `initcap`, `lpad`, `rpad`, `repeat`, `reverse`, `instr`, `locate`, `translate`, `ascii`, `format_number`, `format_string`, `base64`, `unbase64`, `soundex`, `levenshtein`, `overlay`, `btrim`, `left`, `right`, `char_length`/`character_length`, `octet_length`, `bit_length`, `decode`, `encode`

**Count**: ~27 functions

### 2.3 Date/Timestamp functions (high-frequency)

`dayofweek`, `dayofyear`, `weekofyear`, `quarter`, `months_between`, `next_day`, `last_day`, `add_months`, `date_format`, `to_date`, `to_timestamp`, `from_unixtime`, `unix_timestamp`, `date_trunc`, `trunc`, `extract`/`date_part`

**Count**: ~17 functions

### 2.4 Aggregate functions (high-frequency)

`first`/`first_value`, `last`/`last_value`, `collect_list`, `collect_set`, `stddev`, `stddev_pop`, `stddev_samp`, `var_pop`, `var_samp`/`variance`, `corr`, `covar_pop`, `covar_samp`, `approx_count_distinct`, `skewness`, `kurtosis`, `product`, `sum_distinct`, `any_value`, `count_if`, `max_by`, `min_by`

**Count**: ~22 functions

### 2.5 Window functions (remaining)

`cume_dist`, `percent_rank`, `nth_value`

**Count**: 3 functions

### 2.6 Sort functions

`asc`, `desc`, `asc_nulls_first`, `asc_nulls_last`, `desc_nulls_first`, `desc_nulls_last`

**Count**: 6 functions

### 2.7 Conditional / Predicate functions

`expr`, `nanvl`, `ifnull`/`nvl`, `nvl2`, `nullif`, `nullifzero`, `zeroifnull`, `equal_null`, `isnotnull` (function), `like`/`ilike`/`rlike`/`regexp`/`regexp_like` (as functions), `call_function`, `column` (alias), `broadcast`

**Count**: ~16 functions

### 2.8 Collection / Array / Map functions (common subset)

`create_map`, `map_keys`, `map_values`, `map_entries`, `map_from_entries`, `map_from_arrays`, `map_concat`, `array_contains`, `array_distinct`, `array_union`, `array_intersect`, `array_except`, `array_join`, `array_sort`, `sort_array`, `array_max`, `array_min`, `array_position`, `array_remove`, `array_repeat`, `array_compact`, `arrays_overlap`, `arrays_zip`, `flatten`, `sequence`, `element_at`, `get`, `slice`, `explode_outer`, `posexplode`, `posexplode_outer`

**Count**: ~31 functions

### 2.9 Hash functions

`md5`, `sha1`, `sha2`, `hash`, `xxhash64`, `crc32`

**Count**: 6 functions

### 2.10 JSON functions

`from_json`, `to_json`, `get_json_object`, `json_tuple`, `schema_of_json`, `json_array_length`, `json_object_keys`

**Count**: 7 functions

### 2.11 Bitwise functions

`bit_count`, `bit_get`/`getbit`, `bitwise_not`, `shiftleft`, `shiftright`, `shiftrightunsigned`

**Count**: 6 functions

### 2.12 Generator functions (common)

`explode_outer`, `posexplode`, `posexplode_outer`, `inline`, `inline_outer`, `stack`

**Count**: 6 functions

### 2.13 CSV functions

`from_csv`, `to_csv`, `schema_of_csv`

**Count**: 3 functions

**Milestone 2 total**: ~185 new functions
**Estimated tests**: ~100-120+ new tests

---

## Milestone 2.5 — Publish 0.0.1 (First Release)

**Goal**: Ship the first npm package. Set up CI/CD, documentation, and publish infrastructure. This is not a marketing event — it's making the package available.

### 2.5.1 CI/CD

| Task                                | Complexity | Notes                              |
| ----------------------------------- | ---------- | ---------------------------------- |
| GitHub Actions: lint + build + test | Low        | Matrix: Node 22, 24                |
| GitHub Actions: publish on tag      | Low        | `pnpm publish` to npm on `v*` tags |
| Changesets or manual versioning     | Low        | Keep it simple for 0.x             |
| Provenance (npm `--provenance`)     | Low        | Signed builds from GH Actions      |

### 2.5.2 Documentation

| Task                                | Complexity | Notes                            |
| ----------------------------------- | ---------- | -------------------------------- |
| README with quickstart + install    | Low        | Connection, basic query, collect |
| API reference (auto-gen from TSDoc) | Medium     | typedoc or similar               |
| `CONTRIBUTING.md`                   | Low        | Dev setup, testing, PR process   |
| `LICENSE`                           | Low        | Confirm license choice           |

### 2.5.3 Package hygiene

| Task                                 | Complexity | Notes                                          |
| ------------------------------------ | ---------- | ---------------------------------------------- |
| `package.json` metadata (all 3 pkgs) | Low        | description, keywords, repo, homepage, license |
| `.npmignore` / `files` field         | Low        | Ship dist + types only                         |
| `engines` / `peerDependencies` audit | Low        | Ensure correct Node/protobuf constraints       |
| Smoke test: install from tarball     | Low        | `pnpm pack` → `npm install ./spark-*.tgz`      |

**Estimated effort**: ~15-20 tasks
**Deliverable**: `@spark-connect-js/core@0.0.1`, `@spark-connect-js/node@0.0.1`, `@spark-connect-js/connect@0.0.1` on npm

---

## Milestone 3 — Caching, Repartitioning & Advanced Grouping

**Goal**: Implement the "production DataFrame" features that power real Spark workloads.

### 3.1 Caching & Persistence

| Feature                   | Priority | Complexity | Notes                                         |
| ------------------------- | -------- | ---------- | --------------------------------------------- |
| `cache()`                 | P0       | Medium     | Needs `CacheTable`-style command or plan hint |
| `persist(storageLevel)`   | P0       | Medium     | Same with StorageLevel param                  |
| `unpersist(blocking?)`    | P0       | Medium     | Release cache                                 |
| `storageLevel` (property) | P1       | Low        | AnalyzePlan query                             |

### 3.2 Repartitioning

| Feature                               | Priority | Complexity | Notes                            |
| ------------------------------------- | -------- | ---------- | -------------------------------- |
| `repartition(numPartitions, ...cols)` | P0       | Medium     | `Repartition` relation           |
| `repartitionByRange(n, ...cols)`      | P1       | Medium     | `RepartitionByExpression`        |
| `coalesce(numPartitions)`             | P0       | Medium     | `Repartition` with shuffle=false |

### 3.3 Advanced Grouping

| Feature               | Priority | Complexity | Notes                                |
| --------------------- | -------- | ---------- | ------------------------------------ |
| `cube(...cols)`       | P0       | Medium     | `Aggregate` with `GROUP_TYPE_CUBE`   |
| `rollup(...cols)`     | P0       | Medium     | `Aggregate` with `GROUP_TYPE_ROLLUP` |
| `pivot(col, values?)` | P1       | Medium     | `Aggregate` with `GROUP_TYPE_PIVOT`  |
| `melt / unpivot`      | P1       | Medium     | `Unpivot` relation                   |

### 3.4 Stat functions (via `.stat` accessor)

| Feature                             | Priority | Complexity | Notes                   |
| ----------------------------------- | -------- | ---------- | ----------------------- |
| `corr(col1, col2)`                  | P1       | Medium     | `StatCorr` or aggregate |
| `cov(col1, col2)`                   | P1       | Medium     | `StatCov`               |
| `crosstab(col1, col2)`              | P2       | Medium     | `StatCrosstab`          |
| `freqItems(cols, support?)`         | P2       | Medium     | `StatFreqItems`         |
| `approxQuantile(col, probs, error)` | P2       | Medium     | `StatApproxQuantile`    |

### 3.5 DataFrame misc (includes M1 deferred items)

| Feature                         | Priority | Complexity | Notes                                     |
| ------------------------------- | -------- | ---------- | ----------------------------------------- |
| `summary(...statistics)`        | P1       | Low        | Deferred from M1 — `StatSummary` relation |
| `replace(to, value, subset)`    | P1       | Medium     | Deferred from M1 — `NAReplace` relation   |
| `randomSplit(weights, seed?)`   | P1       | Medium     | Multiple Sample operations                |
| `sameSemantics(other)`          | P2       | Low        | AnalyzePlan comparison                    |
| `semanticHash()`                | P2       | Low        | AnalyzePlan                               |
| `createGlobalTempView(name)`    | P1       | Low        | Already have temp view command            |
| `createOrReplaceGlobalTempView` | P1       | Low        | Same                                      |
| `createTempView(name)`          | P0       | Low        | `replace=false` variant                   |

**Estimated tests**: ~40-50 new tests

---

## Milestone 4 — I/O Expansion

**Goal**: Bring DataFrameReader/Writer to feature parity with Rust/Swift clients.

### 4.1 DataFrameReader shortcuts

| Feature          | Priority | Complexity | Notes                          |
| ---------------- | -------- | ---------- | ------------------------------ |
| `schema(schema)` | P0       | Medium     | Set schema on reader           |
| `csv(path)`      | P0       | Low        | `format("csv").load(path)`     |
| `json(path)`     | P0       | Low        | `format("json").load(path)`    |
| `parquet(path)`  | P0       | Low        | `format("parquet").load(path)` |
| `orc(path)`      | P0       | Low        | `format("orc").load(path)`     |
| `text(path)`     | P1       | Low        | `format("text").load(path)`    |

### 4.2 DataFrameWriter shortcuts

| Feature                     | Priority | Complexity | Notes                          |
| --------------------------- | -------- | ---------- | ------------------------------ |
| `csv(path)`                 | P0       | Low        | `format("csv").save(path)`     |
| `json(path)`                | P0       | Low        | `format("json").save(path)`    |
| `parquet(path)`             | P0       | Low        | `format("parquet").save(path)` |
| `orc(path)`                 | P0       | Low        | `format("orc").save(path)`     |
| `text(path)`                | P1       | Low        | `format("text").save(path)`    |
| `bucketBy(n, col, ...cols)` | P1       | Medium     | WriteOperation field           |
| `insertInto(tableName)`     | P1       | Medium     | WriteOperation variant         |

### 4.3 DataFrameWriterV2

| Feature                                          | Priority | Complexity | Notes                          |
| ------------------------------------------------ | -------- | ---------- | ------------------------------ |
| Full `writeTo(table)` API                        | P1       | Medium     | New `WriteOperationV2` command |
| `using / option / partitionedBy / tableProperty` | P1       | Low        | Builder pattern                |
| `create / replace / createOrReplace`             | P1       | Medium     | Different save modes           |
| `append / overwrite / overwritePartitions`       | P1       | Medium     | Same                           |

**Estimated tests**: ~25-30 new tests

---

## Milestone 5 — Catalog Expansion

**Goal**: Complete Catalog API coverage.

| Feature                           | Priority | Complexity | Notes                        |
| --------------------------------- | -------- | ---------- | ---------------------------- |
| `listFunctions(db?, pattern?)`    | P1       | Low        | `ListFunctions` proto exists |
| `listCatalogs(pattern?)`          | P1       | Low        | If proto available           |
| `currentCatalog()`                | P1       | Low        | Proto exists                 |
| `setCurrentCatalog(name)`         | P1       | Low        | Proto exists                 |
| `functionExists(name, db?)`       | P1       | Low        | `FunctionExists` proto       |
| `getDatabase(name)`               | P1       | Low        | `GetDatabase` proto          |
| `getTable(name)`                  | P1       | Low        | `GetTable` proto             |
| `getFunction(name)`               | P1       | Low        | `GetFunction` proto          |
| `dropTempView(name)`              | P0       | Low        | Command                      |
| `dropGlobalTempView(name)`        | P0       | Low        | Command                      |
| `cacheTable(name, storageLevel?)` | P2       | Medium     | Command                      |
| `uncacheTable(name)`              | P2       | Low        | Command                      |
| `isCached(name)`                  | P2       | Low        | AnalyzePlan                  |
| `clearCache()`                    | P2       | Low        | Command                      |
| `refreshTable(name)`              | P2       | Low        | Command                      |
| `refreshByPath(path)`             | P2       | Low        | Command                      |
| `recoverPartitions(name)`         | P2       | Low        | Command                      |

**Estimated tests**: ~20-25 new tests

---

## Milestone 6 — Structured Streaming (MVP)

**Goal**: Basic streaming support — read stream, write stream, manage queries.

### 6.1 DataStreamReader

| Feature                              | Complexity | Notes               |
| ------------------------------------ | ---------- | ------------------- |
| `format / option / options / schema` | Low        | Builder pattern     |
| `load`                               | Medium     | Uses streaming plan |
| `table`                              | Low        | Named table source  |

### 6.2 DataStreamWriter

| Feature                     | Complexity | Notes                                |
| --------------------------- | ---------- | ------------------------------------ |
| `format / option / options` | Low        | Builder pattern                      |
| `outputMode`                | Low        | append/complete/update               |
| `trigger`                   | Low        | ProcessingTime / AvailableNow / Once |
| `queryName`                 | Low        | String                               |
| `partitionBy`               | Low        |                                      |
| `start`                     | Medium     | Initiates streaming query            |
| `toTable`                   | Medium     |                                      |

### 6.3 StreamingQuery / Manager

| Feature                                       | Complexity | Notes      |
| --------------------------------------------- | ---------- | ---------- |
| `id / runId / name`                           | Low        | Properties |
| `isActive`                                    | Low        |            |
| `stop`                                        | Medium     |            |
| `awaitTermination`                            | Medium     |            |
| `status / lastProgress / recentProgress`      | Medium     |            |
| `processAllAvailable`                         | Medium     |            |
| `exception`                                   | Low        |            |
| `explain(extended?)`                          | Low        |            |
| Manager: `active / get / awaitAnyTermination` | Medium     |            |
| Manager: `resetTerminated`                    | Low        |            |
| Manager: `addListener / removeListener`       | Medium     |            |

### 6.4 StreamingQueryListener

| Feature                    | Complexity | Notes |
| -------------------------- | ---------- | ----- |
| `onQueryStarted(event)`    | Medium     |       |
| `onQueryProgress(event)`   | Medium     |       |
| `onQueryIdle(event)`       | Medium     |       |
| `onQueryTerminated(event)` | Medium     |       |

**Estimated tests**: ~20-25 new tests

---

## Milestone 7 — Advanced Features

**Goal**: Match the feature depth of the Rust and Swift clients.

### 7.1 MergeIntoWriter

- `mergeInto(table, condition)` → MergeIntoWriter
- `whenMatched / whenNotMatched / whenNotMatchedBySource`
- `withSchemaEvolution / merge`

### 7.2 SparkSession enhancements

- `conf` (RuntimeConfig — get/set/unset/getAll/isModifiable)
- `version`
- `newSession()`
- `active()` / `getActiveSession()` (static)
- `builder.appName / builder.config / builder.master / builder.enableHiveSupport`
- `interruptAll / interruptOperation / interruptTag`
- `addTag / removeTag / clearTags / getTags`
- `addArtifact / addArtifacts`
- `profile`
- `dataSource` (DataSourceRegistration)
- `tvf` (TableValuedFunction accessor)
- Spark Connect-only: `copyFromLocalToFs`, `registerProgressHandler`, `removeProgressHandler`, `clearProgressHandlers`, `client`

### 7.3 Additional DataFrame features

- `checkpoint / localCheckpoint`
- `observe(name, ...exprs)`
- `withWatermark(eventTime, delayThreshold)`
- `withMetadata(column, metadata)`
- `lateralJoin`
- `toArrow()` (return raw Arrow table)
- `toJSON()`
- `inputFiles()`
- `isLocal()`
- `isStreaming`
- `sparkSession` (property)
- `executionInfo` (property, Connect-only)
- `transpose(indexCol?)`
- `sampleBy(col, fractions, seed?)`
- `groupingSets`
- `colRegex(colName)`
- `to(schema)`
- `exists()` / `scalar()` (subquery methods)
- `metadataColumn(colName)`
- `dropDuplicatesWithinWatermark(subset?)`
- `foreachPartition(f)`
- `asTable()` (table argument for TVF)
- `mergeInto(table, condition)` → via M7.1
- `writeTo(table)` → via M4.3
- `writeStream` → via M6

### 7.3b Additional Column methods

- `outer()`
- `transform(f)`
- `try_cast(dataType)`
- `ilike(other)`
- `__getattr__` / `__getitem__` (subscript access)
- `~ (not / inversion)`, `% (mod)`, `** (power)`

### 7.4 More functions (~120 additional)

- **String**: mask, sentences, regex variants (regexp*count, regexp_extract_all, regexp_instr, regexp_substr), replace, collate/collation, randstr, quote, split_part, substring_index, to_binary/to_char/to_number/to_varchar, elt, find_in_set, is_valid_utf8/make_valid_utf8/validate_utf8/try_validate_utf8, left/right, position, char/chr, char_length + all `try*\*` variants
- **Date/Timestamp**: make*timestamp/\_ltz/\_ntz, make_interval/make_dt_interval/make_ym_interval, make_time, make_date, timestamp_add/timestamp_diff, time_diff/time_trunc, to_time/try_to_time, to_timestamp_ltz/\_ntz, to_unix_timestamp, unix_date/unix_micros/unix_millis/unix_seconds, curdate, current_time, current_timezone, now, dayname/monthname, weekday, window_time, localtimestamp, date_from_unix_date, convert_timezone, session_window + all `try*\*` variants
- **Aggregate**: percentile, median, mode, grouping, grouping*id, histogram_numeric, count_min_sketch, listagg/string_agg + distinct variants, try_avg, try_sum, mean, percentile_approx, array_agg, bool_and/bool_or/every/some, bit_and/bit_or/bit_xor, regr*_ (9 functions), bitmap*construct_agg/bitmap_or_agg, hll_sketch_agg/hll_union_agg, kll_sketch_agg*_ (3), theta\_\*\_agg (3)
- **URL functions**: parse_url, try_parse_url, url_decode, url_encode, try_url_decode
- **Variant functions**: parse_json, try_parse_json, schema_of_variant, schema_of_variant_agg, variant_get, try_variant_get, is_variant_null, to_variant_object
- **XML functions**: from_xml, to_xml, schema_of_xml, xpath, xpath_boolean, xpath_double/xpath_number, xpath_float, xpath_int, xpath_long, xpath_short, xpath_string
- **Partition transformation functions**: partitioning.years/months/days/hours/bucket
- **Geospatial functions**: st_asbinary, st_geogfromwkb, st_geomfromwkb, st_setsrid, st_srid
- **Misc**: bitmap*bit_position/bitmap_bucket_number/bitmap_count, hll_sketch_estimate/hll_union, kll_sketch*\* (18 non-agg), theta_difference/theta_intersection/theta_sketch_estimate/theta_union, try_aes_decrypt, monotonically_increasing_id, spark_partition_id, typeof, uuid, version

**Estimated tests**: ~60-80 new tests

---

## Milestone 8 — UDFs, TVFs & Advanced Interop

**Goal**: User-defined function support and Table-Valued Functions (where feasible in a non-JVM client).

### 8.1 UDFs

- `udf(f, returnType, useArrow?)` — register UDF
- `arrow_udf` / `arrow_udtf`
- `call_udf(udfName, *cols)` — invoke registered UDF by name
- `unwrap_udt(col)`
- `mapInArrow` / `mapInPandas`-equivalent (Arrow-based batch transforms)

### 8.2 Table-Valued Functions

- `TableValuedFunction.explode / explode_outer`
- `TableValuedFunction.inline / inline_outer`
- `TableValuedFunction.posexplode / posexplode_outer`
- `TableValuedFunction.json_tuple`
- `TableValuedFunction.range / stack`
- `TableValuedFunction.variant_explode / variant_explode_outer`
- `TableValuedFunction.collations / sql_keywords`
- `TableArg.partitionBy / orderBy / withSinglePartition`

> **Note**: UDFs in Spark Connect require sending serialized function code to the server. For non-Python/non-JVM clients, this typically means either Arrow-UDFs or "call a pre-registered server-side UDF by name." Full user-defined function support may be limited by Spark Connect's protocol.

---

## Priority Summary

| Milestone                     | Est. Features | Est. New Tests | Cumulative Tests | Status          |
| ----------------------------- | ------------- | -------------- | ---------------- | --------------- |
| **M1**: Core DataFrame Gaps   | ~28           | ~45            | 250              | ✅ **Complete** |
| **M2**: Functions Expansion   | ~193          | ~93            | 343              | ✅ **Complete** |
| **M2.5**: Publish 0.0.1       | ~18           | —              | 343              | Not started     |
| **M3**: Cache/Repart/Grouping | ~20           | ~45            | ~415             | Not started     |
| **M4**: I/O Expansion         | ~20           | ~30            | ~445             | Not started     |
| **M5**: Catalog Expansion     | ~17           | ~25            | ~470             | Not started     |
| **M6**: Streaming MVP         | ~30           | ~25            | ~495             | Not started     |
| **M7**: Advanced Features     | ~120+         | ~80            | ~575             | Not started     |
| **M8**: UDFs & TVFs           | ~25           | ~20            | ~595             | Not started     |

---

## What We Deliberately Skip (Scope Boundaries)

These PySpark features are **not applicable** to a TypeScript Spark Connect client:

| Feature                            | Reason                                                      |
| ---------------------------------- | ----------------------------------------------------------- |
| `rdd` / `toRDD`                    | RDD is a JVM-only concept; not in Spark Connect protocol    |
| `toPandas()`                       | Python-specific; our equivalent is `collect()` → JS objects |
| `pandas_api()`                     | Python-specific                                             |
| `mapInPandas` / `applyInPandas`    | Requires Python UDF serialization                           |
| `sparkContext`                     | JVM-only; Spark Connect has no SparkContext                 |
| `registerTempTable` (deprecated)   | Use `createOrReplaceTempView`                               |
| `foreachPartition`                 | Requires sending code to executors                          |
| Plotting (`DataFrame.plot`)        | Python matplotlib-specific                                  |
| `repartition` by Column expression | Protocol-dependent; start with numPartitions only           |

---

## Implementation Notes

### Architecture for new features

Most new features follow one of these patterns:

1. **New DataFrame method → uses existing proto** (easiest)
   - Add method to `DataFrame` class
   - Build appropriate `LogicalPlan` node
   - PlanBuilder already handles proto conversion
   - Example: `withColumnRenamed` uses `WithColumnsRenamed` relation

2. **New function → `UnresolvedFunction` wrapper** (bulk work)
   - Add function to `functions.ts`
   - Creates `call` expression with function name
   - No proto changes needed
   - Example: All math/string/date/aggregate functions

3. **New command → uses existing `Command` proto** (medium)
   - Execute via `executeCommand` transport method
   - Example: `cache()`, `persist()`, catalog operations

4. **New relation type → may need proto update** (heaviest)
   - Check if proto already exists in `@spark-connect-js/connect`
   - Add relation variant to `LogicalPlan` union type
   - Update `PlanBuilder.toRelation()`
   - Example: `Repartition`, `Unpivot`, `Hint`

### Testing strategy

- Unit tests in `@spark-connect-js/core` — mock transport, verify proto serialization
- Integration tests in `@spark-connect-js/node` — run against Docker Spark Connect
- Each new function gets at least one roundtrip test
- Group related functions into test files (e.g., `math-functions.test.ts`)

### Function implementation pattern

```typescript
// In functions.ts — each function is ~3-5 lines
export function log2(col: Column | string): Column {
  return callFunction("log2", [toCol(col)]);
}
```

This makes Milestone 2 (160 functions) achievable as batch work — the pattern is highly repetitive.
