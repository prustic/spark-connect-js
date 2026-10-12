---
"@spark-connect-js/core": minor
"@spark-connect-js/node": minor
---

`contains`, `startswith`, and `endswith` read a string second argument as a column name, as PySpark does, instead of as a literal. This is a breaking change: `contains(col("s"), "abc")` now looks up a column named `abc`. Pass `lit("abc")`, or use the Column methods `col("s").contains("abc")`, `startsWith`, and `endsWith`, which take a literal as in PySpark.
