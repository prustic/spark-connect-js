# Feature Coverage

How spark-connect-js maps to the PySpark DataFrame API. Targets Spark 4.0+ via the [Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html) protocol.

---

## SparkSession

| Feature                                | Status |
| -------------------------------------- | ------ |
| `connect(url)` / `builder.remote(url)` | ✅     |
| `sql(query)`                           | ✅     |
| `table(name)`                          | ✅     |
| `read` (DataFrameReader)               | ✅     |
| `createDataFrame(data, schema)`        | ✅     |
| `catalog`                              | ✅     |
| `stop()`                               | ✅     |
| `range(start, end, step)`              | -      |
| `conf` (RuntimeConfig)                 | -      |
| `readStream` (Structured Streaming)    | -      |
| `newSession()`                         | -      |

## DataFrame

### Transformations

| Feature                                    | Status |
| ------------------------------------------ | ------ |
| `select` / `selectExpr`                    | ✅     |
| `filter` / `where`                         | ✅     |
| `groupBy` / `agg`                          | ✅     |
| `sort` / `orderBy`                         | ✅     |
| `limit` / `offset`                         | ✅     |
| `join` / `crossJoin`                       | ✅     |
| `drop`                                     | ✅     |
| `withColumn` / `withColumns`               | ✅     |
| `withColumnRenamed` / `withColumnsRenamed` | ✅     |
| `toDF`                                     | ✅     |
| `alias`                                    | ✅     |
| `distinct` / `dropDuplicates`              | ✅     |
| `union` / `unionAll` / `unionByName`       | ✅     |
| `intersect` / `intersectAll`               | ✅     |
| `subtract` / `exceptAll`                   | ✅     |
| `sample`                                   | ✅     |
| `describe`                                 | ✅     |
| `fillna` / `dropna`                        | ✅     |
| `hint`                                     | ✅     |
| `transform`                                | ✅     |
| `sortWithinPartitions`                     | ✅     |
| `repartition` / `coalesce`                 | ✅     |
| `repartitionByRange`                       | ✅     |
| `cache` / `persist` / `unpersist`          | ✅     |
| `getStorageLevel`                          | ✅     |
| `cube` / `rollup` / `pivot`                | -      |
| `melt` / `unpivot`                         | -      |
| `summary` / `replace`                      | -      |

### Actions

| Feature                              | Status |
| ------------------------------------ | ------ |
| `collect`                            | ✅     |
| `show`                               | ✅     |
| `count`                              | ✅     |
| `first` / `head` / `take` / `tail`   | ✅     |
| `toLocalIterator`                    | ✅     |
| `forEach`                            | ✅     |
| `schema` / `printSchema` / `explain` | ✅     |
| `columns` / `dtypes` / `isEmpty`     | ✅     |
| `createOrReplaceTempView`            | ✅     |
| `write` (DataFrameWriter)            | ✅     |

## Column

| Feature                                                   | Status |
| --------------------------------------------------------- | ------ |
| Comparisons (`eq`, `gt`, `lt`, etc.)                      | ✅     |
| Arithmetic (`plus`, `minus`, `multiply`, `divide`, `mod`) | ✅     |
| Logical (`and`, `or`, `not`)                              | ✅     |
| `alias` / `cast`                                          | ✅     |
| `asc` / `desc` (with null ordering)                       | ✅     |
| `isNull` / `isNotNull` / `isNaN`                          | ✅     |
| `isin` / `between`                                        | ✅     |
| `like` / `rlike` / `startsWith` / `endsWith` / `contains` | ✅     |
| `substr`                                                  | ✅     |
| `over` (window)                                           | ✅     |
| `bitwiseAND` / `bitwiseOR` / `bitwiseXOR`                 | ✅     |
| `getField` / `getItem` / `dropFields` / `withField`       | -      |

## GroupedData

| Feature                          | Status |
| -------------------------------- | ------ |
| `agg`                            | ✅     |
| `count` / `sum` / `avg` / `mean` | ✅     |
| `min` / `max`                    | ✅     |
| `pivot`                          | -      |

## Window

| Feature                                                    | Status |
| ---------------------------------------------------------- | ------ |
| `Window.partitionBy` / `Window.orderBy`                    | ✅     |
| `rowsBetween` / `rangeBetween`                             | ✅     |
| `unboundedPreceding` / `unboundedFollowing` / `currentRow` | ✅     |

## DataFrameReader

| Feature                                                    | Status |
| ---------------------------------------------------------- | ------ |
| `format` / `option` / `options` / `load` / `table`         | ✅     |
| `schema(schema)`                                           | -      |
| Format shortcuts (`csv`, `json`, `parquet`, `orc`, `text`) | -      |

## DataFrameWriter

| Feature                                                    | Status |
| ---------------------------------------------------------- | ------ |
| `format` / `mode` / `option` / `options`                   | ✅     |
| `partitionBy` / `sortBy`                                   | ✅     |
| `save` / `saveAsTable`                                     | ✅     |
| `bucketBy` / `insertInto`                                  | -      |
| Format shortcuts (`csv`, `json`, `parquet`, `orc`, `text`) | -      |

## Catalog

| Feature                                        | Status |
| ---------------------------------------------- | ------ |
| `currentDatabase` / `setCurrentDatabase`       | ✅     |
| `listDatabases` / `listTables` / `listColumns` | ✅     |
| `databaseExists` / `tableExists`               | ✅     |
| `listFunctions` / `functionExists`             | -      |
| `getDatabase` / `getTable`                     | -      |
| `dropTempView` / `dropGlobalTempView`          | -      |
| `cacheTable` / `uncacheTable` / `clearCache`   | -      |

## Functions

248 built-in functions across 12 categories.

| Category       | Count | Examples                                                                                  |
| -------------- | ----- | ----------------------------------------------------------------------------------------- |
| Aggregate      | 45    | `count`, `sum`, `avg`, `max`, `min`, `collect_list`, `stddev`, `approx_count_distinct`    |
| Math           | 44    | `abs`, `round`, `sqrt`, `pow`, `log`, `sin`, `cos`, `rand`, `greatest`, `least`           |
| String         | 38    | `upper`, `lower`, `trim`, `concat`, `substring`, `split`, `regexp_replace`, `levenshtein` |
| Date/Timestamp | 28    | `year`, `month`, `hour`, `datediff`, `date_add`, `to_timestamp`, `unix_timestamp`         |
| Collection     | 37    | `array`, `struct`, `explode`, `flatten`, `create_map`, `map_keys`, `element_at`           |
| Conditional    | 17    | `when`, `coalesce`, `isnull`, `isnan`, `nvl`, `expr`, `cast`                              |
| Window         | 9     | `row_number`, `rank`, `dense_rank`, `lag`, `lead`, `ntile`                                |
| Hash           | 6     | `md5`, `sha1`, `sha2`, `xxhash64`, `crc32`                                                |
| JSON           | 7     | `from_json`, `to_json`, `get_json_object`, `schema_of_json`                               |
| CSV            | 3     | `from_csv`, `to_csv`, `schema_of_csv`                                                     |
| Bitwise        | 7     | `bitwise_not`, `bit_count`, `shiftleft`, `shiftright`                                     |
| Sort           | 6     | `asc`, `desc`, `asc_nulls_first`, `desc_nulls_last`                                       |

## Not Yet Implemented

These are planned but not available in the current release:

- **Advanced grouping**: `cube()`, `rollup()`, `pivot()`, `melt()`/`unpivot()`
- **Stat functions**: `.stat.corr()`, `.stat.cov()`, `.stat.crosstab()`
- **I/O shortcuts**: `read.csv()`, `read.parquet()`, `write.json()`, etc.
- **DataFrameWriterV2**: `writeTo()`, `create()`, `replace()`, `append()`
- **Catalog expansion**: `dropTempView()`, `cacheTable()`, `listFunctions()`
- **RuntimeConfig**: `conf.get()`, `conf.set()`
- **Structured Streaming**: `readStream`, `writeStream`, `StreamingQuery`
