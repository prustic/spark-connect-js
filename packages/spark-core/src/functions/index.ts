/**
 * ─── Functions ──────────────────────────────────────────────────────────────
 *
 * Built-in functions that mirror PySpark's `pyspark.sql.functions` module.
 *
 * @see PySpark functions: python/pyspark/sql/functions/builtin.py
 * @see Spark built-in functions registry: sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/FunctionRegistry.scala
 *
 * All functions return Column objects wrapping expression AST nodes.
 * They are NEVER evaluated in JavaScript — the server resolves them.
 *
 * Split into category files for maintainability; this barrel re-exports
 * everything so consumers still import from a single path.
 */

// Re-export col and lit so users can import everything from functions
export { col, lit } from "../column.js";

export { when, WhenBuilder, cast, coalesce, isnull, isnan } from "./conditional.js";
export {
  upper,
  lower,
  trim,
  ltrim,
  rtrim,
  length,
  concat,
  substring,
  regexp_replace,
  contains,
  startswith,
  endswith,
} from "./string.js";
export {
  year,
  month,
  dayofmonth,
  hour,
  minute,
  second,
  current_date,
  current_timestamp,
  datediff,
  date_add,
  date_sub,
} from "./datetime.js";
export { abs, round, ceil, floor, sqrt, pow, log, exp } from "./math.js";
export { count, sum, avg, min, max, countDistinct } from "./aggregate.js";
export { struct, array, explode, size } from "./collection.js";
export { row_number, rank, dense_rank, lag, lead, ntile } from "./window.js";
