---
"@spark-connect-js/core": minor
"@spark-connect-js/node": minor
"@spark-connect-js/connect": minor
---

New `DataFrame` methods backed by Spark Connect relations: `transpose`, `lateralJoin`, `to(schema)`, `sampleBy` (also on `df.stat`), `groupingSets`, and `dropDuplicatesWithinWatermark`.

`to` accepts a `StructType` or a DDL string. `sampleBy` takes fractions as a record, or a `Map` for strata that are not strings, and always sends a seed so repeated samples differ when none is given. `groupingSets` computes one aggregate per listed column set, alongside the existing `rollup` and `cube`.
