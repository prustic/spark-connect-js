# API Reference

Full API surface of spark-connect-js. All exports are available from `@spark-connect-js/node`.

```typescript
import { connect, col, sum, desc, Window, ... } from "@spark-connect-js/node";
```

---

## connect()

Creates a `SparkSession` connected to a Spark Connect server.

```typescript
const spark = connect("sc://localhost:15002");
```

---

## SparkSession

| Method                          | Description                                    |
| ------------------------------- | ---------------------------------------------- |
| `sql(query)`                    | Execute a SQL query, returns a DataFrame       |
| `table(name)`                   | Read a table by name                           |
| `read`                          | Returns a DataFrameReader                      |
| `createDataFrame(data, schema)` | Create a DataFrame from local data             |
| `catalog`                       | Access the Catalog API                         |
| `stop()`                        | Close the session and release server resources |

---

## DataFrame

### Transformations

| Method                                       | Description                             |
| -------------------------------------------- | --------------------------------------- |
| `select(...cols)`                            | Project a set of columns or expressions |
| `selectExpr(...exprs)`                       | Project using SQL expression strings    |
| `filter(condition)`                          | Filter rows by condition                |
| `where(condition)`                           | Alias for `filter`                      |
| `groupBy(...cols)`                           | Group by columns, returns GroupedData   |
| `sort(...cols)`                              | Sort by columns                         |
| `orderBy(...cols)`                           | Alias for `sort`                        |
| `limit(n)`                                   | Take the first n rows                   |
| `offset(n)`                                  | Skip the first n rows                   |
| `join(other, on, how)`                       | Join with another DataFrame             |
| `crossJoin(other)`                           | Cross join                              |
| `drop(...cols)`                              | Drop columns                            |
| `withColumn(name, col)`                      | Add or replace a column                 |
| `withColumns(colsMap)`                       | Add or replace multiple columns         |
| `withColumnRenamed(existing, new)`           | Rename a column                         |
| `withColumnsRenamed(colsMap)`                | Rename multiple columns                 |
| `toDF(...cols)`                              | Rename all columns                      |
| `alias(name)`                                | Assign a table alias                    |
| `distinct()`                                 | Remove duplicate rows                   |
| `dropDuplicates(subset?)`                    | Remove duplicates, optionally by subset |
| `union(other)`                               | Union two DataFrames                    |
| `unionAll(other)`                            | Alias for `union`                       |
| `unionByName(other, allowMissing?)`          | Union by column name                    |
| `intersect(other)`                           | Set intersection                        |
| `intersectAll(other)`                        | Set intersection preserving duplicates  |
| `subtract(other)`                            | Set difference                          |
| `exceptAll(other)`                           | Set difference preserving duplicates    |
| `sample(fraction, withReplacement?, seed?)`  | Random sample                           |
| `describe(...cols)`                          | Summary statistics                      |
| `fillna(value, subset?)`                     | Replace nulls                           |
| `dropna(how?, thresh?, subset?)`             | Drop rows with nulls                    |
| `hint(name, ...params)`                      | Optimizer hint                          |
| `transform(fn)`                              | Apply a DataFrame-to-DataFrame function |
| `sortWithinPartitions(...cols)`              | Sort within each partition              |
| `repartition(numPartitions, ...cols?)`       | Repartition with optional columns       |
| `repartitionByRange(numPartitions, ...cols)` | Range-partition by columns              |
| `coalesce(numPartitions)`                    | Reduce partitions without shuffle       |

### Actions

| Method                           | Description                  |
| -------------------------------- | ---------------------------- |
| `collect()`                      | Return all rows as an array  |
| `show(n?, truncate?, vertical?)` | Print rows to console        |
| `count()`                        | Number of rows               |
| `first()`                        | First row                    |
| `head(n?)`                       | First n rows                 |
| `take(n)`                        | Alias for `head`             |
| `tail(n)`                        | Last n rows                  |
| `toLocalIterator()`              | Async iterator over rows     |
| `forEach(fn)`                    | Apply a function to each row |

### Caching & Persistence

| Method                   | Description                                        |
| ------------------------ | -------------------------------------------------- |
| `cache()`                | Cache with default storage level (MEMORY_AND_DISK) |
| `persist(storageLevel?)` | Cache with a specific StorageLevel                 |
| `unpersist(blocking?)`   | Remove from cache                                  |
| `getStorageLevel()`      | Get the current StorageLevel                       |

### Properties

| Property                        | Description                       |
| ------------------------------- | --------------------------------- |
| `schema`                        | StructType schema                 |
| `columns`                       | Column names                      |
| `dtypes`                        | Column name/type pairs            |
| `isEmpty()`                     | Whether the DataFrame has no rows |
| `printSchema(level?)`           | Print the schema tree             |
| `explain(extended?, mode?)`     | Show the execution plan           |
| `write`                         | Returns a DataFrameWriter         |
| `createOrReplaceTempView(name)` | Register as a temp view           |

---

## Column

```typescript
import { col, lit } from "@spark-connect-js/node";

col("age").gt(30);
col("name").startsWith("A");
col("salary").plus(col("bonus")).alias("total");
```

### Operators

`eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `plus`, `minus`, `multiply`, `divide`, `mod`, `and`, `or`, `not`

### Methods

| Method                                                         | Description          |
| -------------------------------------------------------------- | -------------------- |
| `alias(name)`                                                  | Rename the column    |
| `cast(dataType)`                                               | Cast to another type |
| `asc()` / `desc()`                                             | Sort order           |
| `asc_nulls_first()` / `asc_nulls_last()`                       | Null ordering        |
| `desc_nulls_first()` / `desc_nulls_last()`                     | Null ordering        |
| `isNull()` / `isNotNull()` / `isNaN()`                         | Null checks          |
| `isin(...values)`                                              | Membership test      |
| `between(lower, upper)`                                        | Range check          |
| `like(pattern)` / `rlike(pattern)`                             | Pattern matching     |
| `startsWith(prefix)` / `endsWith(suffix)`                      | String prefix/suffix |
| `contains(other)`                                              | String containment   |
| `substr(pos, len)`                                             | Substring            |
| `over(windowSpec)`                                             | Apply over a window  |
| `bitwiseAND(other)` / `bitwiseOR(other)` / `bitwiseXOR(other)` | Bitwise ops          |

---

## GroupedData

Returned by `df.groupBy(...)`.

| Method                           | Description                |
| -------------------------------- | -------------------------- |
| `agg(...exprs)`                  | Aggregate with expressions |
| `count()`                        | Count per group            |
| `sum(...cols)`                   | Sum per group              |
| `avg(...cols)` / `mean(...cols)` | Average per group          |
| `min(...cols)`                   | Min per group              |
| `max(...cols)`                   | Max per group              |

---

## Window

```typescript
import { Window, row_number, col } from "@spark-connect-js/node";

const w = Window.partitionBy("dept").orderBy(col("salary").desc());
df.withColumn("rank", row_number().over(w));
```

| Method                                | Description             |
| ------------------------------------- | ----------------------- |
| `Window.partitionBy(...cols)`         | Partition the window    |
| `Window.orderBy(...cols)`             | Order within partitions |
| `WindowSpec.rowsBetween(start, end)`  | Row-based frame         |
| `WindowSpec.rangeBetween(start, end)` | Range-based frame       |

Constants: `Window.unboundedPreceding`, `Window.unboundedFollowing`, `Window.currentRow`

---

## DataFrameReader

```typescript
spark.read.format("parquet").option("mergeSchema", "true").load("/data/events");
```

| Method               | Description            |
| -------------------- | ---------------------- |
| `format(source)`     | Set data source format |
| `option(key, value)` | Set a read option      |
| `options(opts)`      | Set multiple options   |
| `load(path?)`        | Read from path         |
| `table(name)`        | Read a named table     |

---

## DataFrameWriter

```typescript
df.write.format("parquet").mode("overwrite").partitionBy("year").save("/output");
```

| Method                 | Description                                      |
| ---------------------- | ------------------------------------------------ |
| `format(source)`       | Set data source format                           |
| `mode(saveMode)`       | `"append"`, `"overwrite"`, `"ignore"`, `"error"` |
| `option(key, value)`   | Set a write option                               |
| `options(opts)`        | Set multiple options                             |
| `partitionBy(...cols)` | Partition output by columns                      |
| `sortBy(...cols)`      | Sort within partitions                           |
| `save(path?)`          | Write to path                                    |
| `saveAsTable(name)`    | Write to a table                                 |

---

## Catalog

```typescript
const catalog = spark.catalog;
const tables = await catalog.listTables();
```

| Method                            | Description                |
| --------------------------------- | -------------------------- |
| `currentDatabase()`               | Get the current database   |
| `setCurrentDatabase(name)`        | Set the current database   |
| `listDatabases(pattern?)`         | List databases             |
| `listTables(dbName?, pattern?)`   | List tables                |
| `listColumns(tableName, dbName?)` | List columns of a table    |
| `databaseExists(name)`            | Check if a database exists |
| `tableExists(name, dbName?)`      | Check if a table exists    |

---

## Functions

248 built-in functions organized by category. All are importable from `@spark-connect-js/node`.

```typescript
import { col, sum, avg, year, upper, when, Window } from "@spark-connect-js/node";
```

### Aggregate

`count`, `sum`, `avg`, `mean`, `min`, `max`, `countDistinct`, `sum_distinct`, `first`, `first_value`, `last`, `last_value`, `collect_list`, `collect_set`, `array_agg`, `stddev`, `stddev_pop`, `stddev_samp`, `var_pop`, `var_samp`, `variance`, `corr`, `covar_pop`, `covar_samp`, `approx_count_distinct`, `skewness`, `kurtosis`, `product`, `any_value`, `count_if`, `max_by`, `min_by`, `percentile_approx`, `median`, `mode`, `grouping`, `grouping_id`, `bool_and`, `every`, `bool_or`, `some`, `bit_and`, `bit_or`, `bit_xor`

### Math

`abs`, `round`, `bround`, `ceil`, `floor`, `sqrt`, `cbrt`, `pow`, `log`, `log2`, `log10`, `log1p`, `exp`, `expm1`, `sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `atan2`, `sinh`, `cosh`, `tanh`, `signum`, `sign`, `degrees`, `radians`, `factorial`, `greatest`, `least`, `hex`, `unhex`, `bin`, `rand`, `randn`, `pmod`, `rint`, `hypot`, `negate`, `negative`, `positive`, `e`, `pi`

### String

`upper`, `lower`, `trim`, `ltrim`, `rtrim`, `btrim`, `length`, `char_length`, `character_length`, `octet_length`, `bit_length`, `concat`, `concat_ws`, `substring`, `regexp_replace`, `contains`, `startswith`, `endswith`, `split`, `initcap`, `lpad`, `rpad`, `repeat`, `reverse`, `instr`, `locate`, `translate`, `ascii`, `format_number`, `format_string`, `base64`, `unbase64`, `soundex`, `levenshtein`, `overlay`, `left`, `right`, `decode`, `encode`

### Date & Timestamp

`year`, `month`, `dayofmonth`, `dayofweek`, `dayofyear`, `weekofyear`, `quarter`, `hour`, `minute`, `second`, `current_date`, `current_timestamp`, `datediff`, `date_add`, `date_sub`, `months_between`, `next_day`, `last_day`, `add_months`, `date_format`, `to_date`, `to_timestamp`, `from_unixtime`, `unix_timestamp`, `date_trunc`, `trunc`, `extract`, `date_part`

### Window

`row_number`, `rank`, `dense_rank`, `cume_dist`, `percent_rank`, `nth_value`, `lag`, `lead`, `ntile`

### Collection

`struct`, `array`, `array_contains`, `array_distinct`, `array_union`, `array_intersect`, `array_except`, `array_join`, `array_sort`, `sort_array`, `array_max`, `array_min`, `array_position`, `array_remove`, `array_repeat`, `array_compact`, `arrays_overlap`, `arrays_zip`, `flatten`, `sequence`, `element_at`, `get`, `slice`, `explode`, `explode_outer`, `posexplode`, `posexplode_outer`, `inline`, `inline_outer`, `stack`, `size`, `create_map`, `map_keys`, `map_values`, `map_entries`, `map_from_entries`, `map_from_arrays`, `map_concat`

### Conditional

`when`, `cast`, `coalesce`, `isnull`, `isnan`, `isnotnull`, `nanvl`, `ifnull`, `nvl`, `nvl2`, `nullif`, `expr`, `monotonically_increasing_id`, `spark_partition_id`, `typeof_`, `uuid`, `broadcast`

### Hash

`md5`, `sha1`, `sha2`, `hash`, `xxhash64`, `crc32`

### JSON

`from_json`, `to_json`, `get_json_object`, `json_tuple`, `schema_of_json`, `json_array_length`, `json_object_keys`

### CSV

`from_csv`, `to_csv`, `schema_of_csv`

### Bitwise

`bitwise_not`, `bit_count`, `bit_get`, `getbit`, `shiftleft`, `shiftright`, `shiftrightunsigned`

### Sort

`asc`, `desc`, `asc_nulls_first`, `asc_nulls_last`, `desc_nulls_first`, `desc_nulls_last`

---

## Types

```typescript
import { DataType, StructType, StructField, StorageLevel } from "@spark-connect-js/node";
```

`DataType`, `StructType`, `StructField` for schema definitions and `cast()` operations.

`StorageLevel` for cache persistence levels. Pre-defined constants:

`MEMORY_ONLY`, `MEMORY_ONLY_2`, `MEMORY_ONLY_SER`, `MEMORY_ONLY_SER_2`, `MEMORY_AND_DISK`, `MEMORY_AND_DISK_2`, `MEMORY_AND_DISK_SER`, `MEMORY_AND_DISK_SER_2`, `DISK_ONLY`, `DISK_ONLY_2`, `OFF_HEAP`, `NONE`

---

## Error Handling

```typescript
import { SparkConnectError, GrpcStatusCode } from "@spark-connect-js/node";
```

`SparkConnectError` wraps gRPC errors from the Spark Connect server with typed status codes.
