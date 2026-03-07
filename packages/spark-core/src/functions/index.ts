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

// ─── Conditional / Predicate ─────────────────────────────────────────────────
export {
  when,
  WhenBuilder,
  cast,
  coalesce,
  isnull,
  isnan,
  isnotnull,
  nanvl,
  ifnull,
  nvl,
  nvl2,
  nullif,
  expr,
  monotonically_increasing_id,
  spark_partition_id,
  typeof_,
  uuid,
  broadcast,
} from "./conditional.js";

// ─── String ──────────────────────────────────────────────────────────────────
export {
  upper,
  lower,
  trim,
  ltrim,
  rtrim,
  btrim,
  length,
  char_length,
  character_length,
  octet_length,
  bit_length,
  concat,
  concat_ws,
  substring,
  regexp_replace,
  contains,
  startswith,
  endswith,
  split,
  initcap,
  lpad,
  rpad,
  repeat,
  reverse,
  instr,
  locate,
  translate,
  ascii,
  format_number,
  format_string,
  base64,
  unbase64,
  soundex,
  levenshtein,
  overlay,
  left,
  right,
  decode,
  encode,
} from "./string.js";

// ─── Date / Timestamp ────────────────────────────────────────────────────────
export {
  year,
  month,
  dayofmonth,
  dayofweek,
  dayofyear,
  weekofyear,
  quarter,
  hour,
  minute,
  second,
  current_date,
  current_timestamp,
  datediff,
  date_add,
  date_sub,
  months_between,
  next_day,
  last_day,
  add_months,
  date_format,
  to_date,
  to_timestamp,
  from_unixtime,
  unix_timestamp,
  date_trunc,
  trunc,
  extract,
  date_part,
} from "./datetime.js";

// ─── Math ────────────────────────────────────────────────────────────────────
export {
  abs,
  round,
  bround,
  ceil,
  floor,
  sqrt,
  cbrt,
  pow,
  log,
  log2,
  log10,
  log1p,
  exp,
  expm1,
  sin,
  cos,
  tan,
  asin,
  acos,
  atan,
  atan2,
  sinh,
  cosh,
  tanh,
  signum,
  sign,
  degrees,
  radians,
  factorial,
  greatest,
  least,
  hex,
  unhex,
  bin,
  rand,
  randn,
  pmod,
  rint,
  hypot,
  negate,
  negative,
  positive,
  e,
  pi,
} from "./math.js";

// ─── Aggregate ───────────────────────────────────────────────────────────────
export {
  count,
  sum,
  avg,
  mean,
  min,
  max,
  countDistinct,
  sum_distinct,
  first,
  first_value,
  last,
  last_value,
  collect_list,
  collect_set,
  array_agg,
  stddev,
  stddev_pop,
  stddev_samp,
  var_pop,
  var_samp,
  variance,
  corr,
  covar_pop,
  covar_samp,
  approx_count_distinct,
  skewness,
  kurtosis,
  product,
  any_value,
  count_if,
  max_by,
  min_by,
  percentile_approx,
  median,
  mode,
  grouping,
  grouping_id,
  bool_and,
  every,
  bool_or,
  some,
  bit_and,
  bit_or,
  bit_xor,
} from "./aggregate.js";

// ─── Collection / Array / Map ────────────────────────────────────────────────
export {
  struct,
  array,
  array_contains,
  array_distinct,
  array_union,
  array_intersect,
  array_except,
  array_join,
  array_sort,
  sort_array,
  array_max,
  array_min,
  array_position,
  array_remove,
  array_repeat,
  array_compact,
  arrays_overlap,
  arrays_zip,
  flatten,
  sequence,
  element_at,
  get,
  slice,
  explode,
  explode_outer,
  posexplode,
  posexplode_outer,
  inline,
  inline_outer,
  stack,
  size,
  create_map,
  map_keys,
  map_values,
  map_entries,
  map_from_entries,
  map_from_arrays,
  map_concat,
} from "./collection.js";

// ─── Window ──────────────────────────────────────────────────────────────────
export {
  row_number,
  rank,
  dense_rank,
  cume_dist,
  percent_rank,
  nth_value,
  lag,
  lead,
  ntile,
} from "./window.js";

// ─── Hash ────────────────────────────────────────────────────────────────────
export { md5, sha1, sha2, hash, xxhash64, crc32 } from "./hash.js";

// ─── JSON ────────────────────────────────────────────────────────────────────
export {
  from_json,
  to_json,
  get_json_object,
  json_tuple,
  schema_of_json,
  json_array_length,
  json_object_keys,
} from "./json.js";

// ─── Bitwise ─────────────────────────────────────────────────────────────────
export {
  bitwise_not,
  bit_count,
  bit_get,
  getbit,
  shiftleft,
  shiftright,
  shiftrightunsigned,
} from "./bitwise.js";

// ─── CSV ─────────────────────────────────────────────────────────────────────
export { from_csv, to_csv, schema_of_csv } from "./csv.js";

// ─── Sort ────────────────────────────────────────────────────────────────────
export {
  asc,
  desc,
  asc_nulls_first,
  asc_nulls_last,
  desc_nulls_first,
  desc_nulls_last,
} from "./sort.js";
