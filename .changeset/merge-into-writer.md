---
"@spark-connect-js/core": minor
"@spark-connect-js/node": minor
"@spark-connect-js/connect": minor
---

`DataFrame.mergeInto(table, condition)` returns a `MergeIntoWriter` for MERGE INTO with chainable `whenMatched`, `whenNotMatched`, and `whenNotMatchedBySource` clauses, each supporting update/insert/delete actions with optional conditions, plus `withSchemaEvolution()`. Assignment keys are parsed as SQL expression strings, so nested fields like `"address.city"` work. `merge()` validates client-side that at least one clause action is defined and that `update`/`insert` assignment maps are non-empty.
