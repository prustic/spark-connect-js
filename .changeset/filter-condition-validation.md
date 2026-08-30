---
"@spark-connect-js/core": patch
---

`DataFrame.filter`, `where`, `join`, and `DataFrameWriterV2.overwrite` reject a condition that is not a usable expression, instead of letting it reach the plan builder as an undefined one. `join` is the notable case: an invalid condition previously read as no condition at all, silently widening the query to a cartesian product. Predicate arguments now share one coercion helper, so `filter` and `where` continue to accept a `Column` or a SQL string, while `join` and `overwrite` take a `Column` only, since a bare string there would mean a column name rather than a predicate. Each method reports its own name in the error.
