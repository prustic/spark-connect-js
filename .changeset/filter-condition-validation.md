---
"@spark-connect-js/core": patch
---

`DataFrame.filter` and `where` reject a condition that is neither a `Column` nor a SQL string, instead of letting an unusable value reach the plan builder as an undefined expression. They now share the coercion helper already used by `mergeInto` and `when`, so every predicate entry point behaves the same way.
