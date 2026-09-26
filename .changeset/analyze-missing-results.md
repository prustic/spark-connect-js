---
"@spark-connect-js/core": patch
---

`sameSemantics`, `semanticHash`, and `explain` throw `SparkClientError` when the server's response is missing its result, instead of returning `false`, `0`, or an empty string. Those defaults are plausible wrong answers; a hash of zero in particular looks like a real one.
