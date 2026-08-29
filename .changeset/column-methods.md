---
"@spark-connect-js/core": minor
"@spark-connect-js/node": minor
---

`Column` gains `when`/`otherwise` as methods, `try_cast`, `mod`, `pow`, `not`, `negate`, `transform`, and the `name`/`astype` aliases of `alias`/`cast`. A CASE WHEN chain is now a `Column`, so `when(...)` results can be used directly without `toColumn()`, and calling `when`/`otherwise` on a column that is not an open chain throws `InvalidInputError` instead of building an invalid expression.
