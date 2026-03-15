# @spark-connect-js/core

## 0.2.0

### Minor Changes

- [#18](https://github.com/prustic/spark-connect-js/pull/18) [`924ea50`](https://github.com/prustic/spark-connect-js/commit/924ea50d700711733cef96857a48c900dc8d7f4b) Thanks [@prustic](https://github.com/prustic)! - ### @spark-connect-js/core
  - `DataFrame.cube()`, `.rollup()` for multi-dimensional aggregation
  - `DataFrame.unpivot()` / `.melt()` for wide-to-long reshaping
  - `DataFrame.summary()` for descriptive statistics
  - `DataFrame.replace()` for value substitution via `NAReplace`
  - `DataFrame.randomSplit()` for splitting into multiple DataFrames
  - `DataFrame.createTempView()`, `.createGlobalTempView()`, `.createOrReplaceGlobalTempView()`
  - `DataFrame.sameSemantics()` and `.semanticHash()` for plan comparison
  - `DataFrameStat` class (`.stat` accessor) with `corr()`, `cov()`, `crosstab()`, `freqItems()`, `approxQuantile()`
  - `GroupedData.pivot()` support with cube/rollup/pivot group types

  ### @spark-connect-js/node
  - Proto serialization for `StatSummary`, `NAReplace`, `Unpivot`, `StatCorr`, `StatCov`, `StatCrosstab`, `StatFreqItems`, `StatApproxQuantile`, and `Aggregate_Pivot`
  - Added analyze-plan request/response handling for `sameSemantics` and `semanticHash`
  - Re-exported `DataFrameStat` from package index

  ### @spark-connect-js/connect
  - Re-exported proto schemas: `StatSummarySchema`, `NAReplaceSchema`, `NAReplace_ReplacementSchema`, `StatCorrSchema`, `StatCovSchema`, `StatCrosstabSchema`, `StatFreqItemsSchema`, `StatApproxQuantileSchema`, `UnpivotSchema`, `Unpivot_ValuesSchema`, `Aggregate_PivotSchema`
  - Re-exported analyze-plan schemas for `SameSemantics` and `SemanticHash`

## 0.1.0

### Minor Changes

- [#10](https://github.com/prustic/spark-connect-js/pull/10) [`895f389`](https://github.com/prustic/spark-connect-js/commit/895f389d703182ed149c4a634f48b894aa7d5131) Thanks [@prustic](https://github.com/prustic)! - Initial release. Platform-agnostic DataFrame API and logical plan builder with zero runtime dependencies.
  - **SparkSession**: connect via `sc://` URL, execute SQL, read tables, create DataFrames from local data
  - **DataFrame**: 30+ transformations (select, filter, join, groupBy, sort, union, intersect, sample, fillna, dropna, and more), actions (collect, show, count, head, tail, toLocalIterator), properties (schema, columns, dtypes, isEmpty, printSchema, explain)
  - **Column**: comparisons, arithmetic, logical ops, cast, alias, null checks, pattern matching, bitwise ops, window support
  - **GroupedData**: agg, count, sum, avg, mean, min, max
  - **Window**: partitionBy, orderBy, rowsBetween, rangeBetween
  - **DataFrameReader**: format, option, options, load, table
  - **DataFrameWriter**: format, mode, option, options, partitionBy, sortBy, save, saveAsTable
  - **Catalog**: currentDatabase, setCurrentDatabase, listDatabases, listTables, listColumns, databaseExists, tableExists
  - **248 built-in functions** across 12 categories: aggregate, math, string, date/timestamp, window, collection, conditional, hash, JSON, CSV, bitwise, sort
  - **PlanBuilder**: constructs Spark Connect logical plan protobuf messages from the DataFrame API
  - Zero runtime dependencies
