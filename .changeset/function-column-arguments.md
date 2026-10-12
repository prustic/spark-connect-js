---
"@spark-connect-js/core": minor
"@spark-connect-js/node": minor
---

Function arguments now mean what they mean in PySpark and Scala. Two groups change behavior, which is breaking for callers relying on the old reading.

`contains`, `startswith`, and `endswith` read a string second argument as a column name instead of a literal. A call such as `contains(col("s"), "abc")` now looks up a column named `abc`. Pass `lit("abc")`, or use the Column methods `col("s").contains("abc")`, `startsWith`, and `endsWith`, which take a literal as in PySpark.

`element_at`, `array_contains`, `array_position`, `array_remove`, `lag`, and `lead` read a string value as a literal instead of a column name. Previously `element_at(col("m"), "k")` looked up the map by the value of a column named `k`, silently returning nulls when such a column existed. It now looks up the key `"k"`. Their values also accept numbers and booleans, so `lag(col("v"), 1, 0)` works. Pass a Column to compare or look up by another column.

Counts, positions, and lengths that took only a number now also accept a Column or column name, as in PySpark: `date_add`, `date_sub`, `add_months`, `substring`, `slice`, `get`, `array_repeat`, `repeat`, `overlay`, `left`, `right`, and `pow`. `extract` and `date_part` also accept a Column for `field`, so `extract(lit("YEAR"), col("ts"))` from PySpark works unchanged.
