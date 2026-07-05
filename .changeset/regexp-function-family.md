---
"@spark-connect-js/core": minor
"@spark-connect-js/node": minor
---

The regex extraction and matching family joins `regexp_replace`: `regexp_extract(col, pattern, groupIdx)`, `regexp_extract_all(col, pattern, groupIdx?)`, `regexp_like(col, pattern)`, `regexp_count(col, pattern)`, `regexp_substr(col, pattern)`, and `regexp_instr(col, pattern, groupIdx?)`. Patterns accept a `Column` for per-row values or a string literal, matching `regexp_replace`.
