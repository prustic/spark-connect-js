---
"@spark-connect-js/core": minor
"@spark-connect-js/node": minor
---

The regex extraction and matching family joins `regexp_replace`: `regexp_extract(col, pattern, groupIdx)`, `regexp_extract_all(col, pattern, groupIdx?)`, `regexp_like(col, pattern)`, `regexp_count(col, pattern)`, `regexp_substr(col, pattern)`, and `regexp_instr(col, pattern, groupIdx?)`. As in PySpark, `regexp_extract` takes its pattern as a string literal, like `regexp_replace`, while the other five take the pattern as a column. A string there names a column, so a literal pattern goes through `lit()`, as in `regexp_like(col("s"), lit("^a"))`.
