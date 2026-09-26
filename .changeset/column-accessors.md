---
"@spark-connect-js/core": patch
"@spark-connect-js/node": patch
"@spark-connect-js/connect": minor
---

`Column.getField`, `withField`, and `dropFields` now work. They previously sent functions Spark does not define (`get_field`, `with_field`, `drop_fields`) and failed with `UNRESOLVED_ROUTINE` on every call. `getItem` on a map now works too. They send the same extract-value and update-fields expressions as PySpark, and reject an empty field name, an unusable key or value, or a `dropFields()` call with no names.

`getItem` on an array index past the end now throws `INVALID_ARRAY_INDEX` under Spark's default ANSI mode, as PySpark does, where it previously returned null. Use the `get` function for a null-returning lookup.

`df.col("*")` now expands to the DataFrame's columns instead of failing with `UNRESOLVED_COLUMN`.
