---
"@spark-connect-js/core": minor
"@spark-connect-js/node": minor
---

New math functions: `acosh`, `asinh`, `atanh`, `cot`, `csc`, `sec`, `ceiling`, `conv`, `ln`, `power`, `random`, `uniform`, `width_bucket`, `try_add`, `try_subtract`, `try_multiply`, `try_divide`, and `try_mod`. New null-handling functions: `equal_null`, `zeroifnull`, and `nullifzero`. New string functions: `char`, `chr`, `collate`, `collation`, `elt`, `find_in_set`, `lcase`, `ucase`, `mask`, `position`, `printf`, `randstr`, `regexp`, `like`, `ilike`, `rlike`, `replace`, `sentences`, `split_part`, `substr`, `substring_index`, `str_to_map`, `to_binary`, `try_to_binary`, `to_char`, `to_varchar`, `to_number`, `try_to_number`, `is_valid_utf8`, `make_valid_utf8`, `validate_utf8`, `try_validate_utf8`, and `sha`.

Arguments follow PySpark's types. Where PySpark takes a column, such as the pattern of `like`, the format of `to_char`, or the replacement characters of `mask`, a string names a column, so a literal goes through `lit()`. `collate`, `conv`, and `substring_index` take their settings as plain values.

`ceil` and `floor` take an optional `scale`. `rand` and `randn` called without a seed now pick one on the client, as PySpark and Scala do, so every action on the same DataFrame sees the same values. They previously drew new values on each action.
