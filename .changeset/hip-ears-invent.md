---
"@spark-connect-js/core": minor
"@spark-connect-js/node": minor
"@spark-connect-js/connect": minor
---

### @spark-connect-js/core

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
- `sameSemantics` and `semanticHash` analyze plan request/response handling
- Re-exported `DataFrameStat` from package index

### @spark-connect-js/connect

- Re-exported proto schemas: `StatSummarySchema`, `NAReplaceSchema`, `NAReplace_ReplacementSchema`, `StatCorrSchema`, `StatCovSchema`, `StatCrosstabSchema`, `StatFreqItemsSchema`, `StatApproxQuantileSchema`, `UnpivotSchema`, `Unpivot_ValuesSchema`, `Aggregate_PivotSchema`
- Re-exported `SameSemantics` and `SemanticHash` analyze plan schemas