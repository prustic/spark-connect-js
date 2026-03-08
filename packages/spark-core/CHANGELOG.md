# @spark-connect-js/core

## 0.1.0

### Minor Changes

- [#10](https://github.com/prustic/spark-connect-js/pull/10) [`895f389`](https://github.com/prustic/spark-connect-js/commit/895f389d703182ed149c4a634f48b894aa7d5131) Thanks [@prustic](https://github.com/prustic)! - Initial release. TypeScript client for Apache Spark Connect.
  - **SparkSession**: connect via `sc://` URL, execute SQL, read tables, create DataFrames from local data
  - **DataFrame**: 30+ transformations (select, filter, join, groupBy, sort, union, intersect, sample, fillna, dropna, and more), actions (collect, show, count, head, tail, toLocalIterator), properties (schema, columns, dtypes, isEmpty, printSchema, explain)
  - **Column**: comparisons, arithmetic, logical ops, cast, alias, null checks, pattern matching, bitwise ops, window support
  - **GroupedData**: agg, count, sum, avg, mean, min, max
  - **Window**: partitionBy, orderBy, rowsBetween, rangeBetween
  - **DataFrameReader**: format, option, options, load, table
  - **DataFrameWriter**: format, mode, option, options, partitionBy, sortBy, save, saveAsTable
  - **Catalog**: currentDatabase, setCurrentDatabase, listDatabases, listTables, listColumns, databaseExists, tableExists
  - **248 built-in functions** across 12 categories: aggregate, math, string, date/timestamp, window, collection, conditional, hash, JSON, CSV, bitwise, sort
  - Zero runtime dependencies in `@spark-connect-js/core`
