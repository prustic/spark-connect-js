/**
 * String functions.
 */

import {
  Column,
  fnExpr,
  toExpr,
  toNumberOrExpr,
  _lit,
  _liftCol,
  randomSeed,
  type ColOrName,
  fn,
} from "./_helpers.js";
import type { Expression } from "../plan/logical-plan.js";

// An optional argument PySpark fills with a literal default when omitted.
function toExprOr(value: ColOrName | undefined, fallback: string | null): Expression {
  return value !== undefined ? toExpr(value) : _lit(fallback)._expr;
}

/** Converts a string column to upper case. */
export function upper(column: ColOrName): Column {
  return fn("upper", column);
}

/** Converts a string column to lower case. */
export function lower(column: ColOrName): Column {
  return fn("lower", column);
}

/** Converts a string column to upper case, like {@link upper}. */
export function ucase(column: ColOrName): Column {
  return fn("ucase", column);
}

/** Converts a string column to lower case, like {@link lower}. */
export function lcase(column: ColOrName): Column {
  return fn("lcase", column);
}

/** Removes leading and trailing whitespace from the string. */
export function trim(column: ColOrName): Column {
  return fn("trim", column);
}

/** Removes leading whitespace from the string. */
export function ltrim(column: ColOrName): Column {
  return fn("ltrim", column);
}

/** Removes trailing whitespace from the string. */
export function rtrim(column: ColOrName): Column {
  return fn("rtrim", column);
}

/** Removes leading and trailing whitespace from the string. */
export function btrim(column: ColOrName): Column {
  return fn("btrim", column);
}

/** Returns the length of the string or binary value. */
export function length(column: ColOrName): Column {
  return fn("length", column);
}

/** Returns the character length of string data or number of bytes of binary data. */
export function char_length(column: ColOrName): Column {
  return fn("char_length", column);
}

/** Alias for {@link char_length}. */
export const character_length = char_length;

/** Returns the byte length of string data or number of bytes of binary data. */
export function octet_length(column: ColOrName): Column {
  return fn("octet_length", column);
}

/** Returns the bit length of string data or number of bits of binary data. */
export function bit_length(column: ColOrName): Column {
  return fn("bit_length", column);
}

/** Concatenates multiple input columns together into a single column. */
export function concat(...columns: ColOrName[]): Column {
  return fn("concat", ...columns);
}

/** Concatenates multiple input string columns together with the given separator. */
export function concat_ws(sep: string, ...columns: ColOrName[]): Column {
  return new Column(fnExpr("concat_ws", _lit(sep)._expr, ...columns.map(toExpr)));
}

/** Returns the substring from a string column starting at the given position with the given length. */
export function substring(
  column: ColOrName,
  pos: ColOrName | number,
  len: ColOrName | number,
): Column {
  return new Column(fnExpr("substring", toExpr(column), toNumberOrExpr(pos), toNumberOrExpr(len)));
}

/**
 * Replaces all substrings that match the regex pattern with the replacement.
 * `pattern` and `replacement` accept a `Column` (per-row values) or a string
 * literal.
 */
export function regexp_replace(
  column: ColOrName,
  pattern: Column | string,
  replacement: Column | string,
): Column {
  return new Column(
    fnExpr("regexp_replace", toExpr(column), _liftCol(pattern)._expr, _liftCol(replacement)._expr),
  );
}

/**
 * Extracts the group matched by the regex pattern, or the empty string when
 * the pattern does not match. `groupIdx` 0 is the whole match.
 */
export function regexp_extract(column: ColOrName, pattern: string, groupIdx: number): Column {
  return new Column(
    fnExpr("regexp_extract", toExpr(column), _lit(pattern)._expr, _lit(groupIdx)._expr),
  );
}

/**
 * Extracts all occurrences of the group matched by the regex pattern as an
 * array. `groupIdx` defaults to 1 on the server.
 *
 * As in PySpark, a string `pattern` names a column, so wrap a literal pattern
 * in `lit()`.
 *
 * @example regexp_extract_all(col("s"), lit("(\\d+)"))
 */
export function regexp_extract_all(
  column: ColOrName,
  pattern: ColOrName,
  groupIdx?: Column | number,
): Column {
  const args = [toExpr(column), toExpr(pattern)];
  if (groupIdx !== undefined) args.push(_liftCol(groupIdx)._expr);
  return new Column(fnExpr("regexp_extract_all", ...args));
}

/**
 * Returns true when the string matches the regex pattern.
 *
 * As in PySpark, a string `pattern` names a column, so wrap a literal pattern
 * in `lit()`.
 *
 * @example regexp_like(col("s"), lit("^user-\\d+$"))
 */
export function regexp_like(column: ColOrName, pattern: ColOrName): Column {
  return new Column(fnExpr("regexp_like", toExpr(column), toExpr(pattern)));
}

/**
 * Counts how many times the regex pattern matches in the string.
 *
 * As in PySpark, a string `pattern` names a column, so wrap a literal pattern
 * in `lit()`.
 */
export function regexp_count(column: ColOrName, pattern: ColOrName): Column {
  return new Column(fnExpr("regexp_count", toExpr(column), toExpr(pattern)));
}

/**
 * Returns the first substring matching the regex pattern, or NULL when the
 * pattern does not match.
 *
 * As in PySpark, a string `pattern` names a column, so wrap a literal pattern
 * in `lit()`.
 */
export function regexp_substr(column: ColOrName, pattern: ColOrName): Column {
  return new Column(fnExpr("regexp_substr", toExpr(column), toExpr(pattern)));
}

/**
 * Returns the 1-based position of the first match of the regex pattern, or 0
 * when the pattern does not match. `groupIdx` selects a match group.
 *
 * As in PySpark, a string `pattern` names a column, so wrap a literal pattern
 * in `lit()`.
 */
export function regexp_instr(
  column: ColOrName,
  pattern: ColOrName,
  groupIdx?: Column | number,
): Column {
  const args = [toExpr(column), toExpr(pattern)];
  if (groupIdx !== undefined) args.push(_liftCol(groupIdx)._expr);
  return new Column(fnExpr("regexp_instr", ...args));
}

/**
 * Returns true when the string matches the regex `regexp`, like
 * {@link regexp_like}.
 *
 * As in PySpark, a string `regexp` names a column, so wrap a literal pattern
 * in `lit()`, or use the `col("s").rlike("^a")` method.
 */
export function rlike(str: ColOrName, regexp: ColOrName): Column {
  return fn("rlike", str, regexp);
}

/**
 * Returns true when the string matches the regex `regexp`, like
 * {@link regexp_like}.
 *
 * As in PySpark, a string `regexp` names a column, so wrap a literal pattern
 * in `lit()`.
 */
export function regexp(str: ColOrName, regexp: ColOrName): Column {
  return fn("regexp", str, regexp);
}

/**
 * Returns true when the string matches the SQL LIKE `pattern`, where `_`
 * matches one character and `%` any run of characters. `escapeChar` replaces
 * the default `\` escape.
 *
 * As in PySpark, a string `pattern` names a column, so wrap a literal pattern
 * in `lit()`, or use the `col("s").like("a%")` method.
 */
export function like(str: ColOrName, pattern: ColOrName, escapeChar?: ColOrName): Column {
  return escapeChar === undefined ? fn("like", str, pattern) : fn("like", str, pattern, escapeChar);
}

/**
 * Case-insensitive {@link like}.
 *
 * As in PySpark, a string `pattern` names a column, so wrap a literal pattern
 * in `lit()`, or use the `col("s").ilike("a%")` method.
 */
export function ilike(str: ColOrName, pattern: ColOrName, escapeChar?: ColOrName): Column {
  return escapeChar === undefined
    ? fn("ilike", str, pattern)
    : fn("ilike", str, pattern, escapeChar);
}

/**
 * Returns true if `column` contains `value`.
 *
 * As in PySpark, a string `value` names a column. For a literal, use
 * `lit("abc")` here or the `col("s").contains("abc")` method.
 */
export function contains(column: ColOrName, value: ColOrName): Column {
  return new Column(fnExpr("contains", toExpr(column), toExpr(value)));
}

/**
 * Returns true if `column` starts with `prefix`.
 *
 * As in PySpark, a string `prefix` names a column. For a literal, use
 * `lit("abc")` here or the `col("s").startsWith("abc")` method.
 */
export function startswith(column: ColOrName, prefix: ColOrName): Column {
  return new Column(fnExpr("startswith", toExpr(column), toExpr(prefix)));
}

/**
 * Returns true if `column` ends with `suffix`.
 *
 * As in PySpark, a string `suffix` names a column. For a literal, use
 * `lit("abc")` here or the `col("s").endsWith("abc")` method.
 */
export function endswith(column: ColOrName, suffix: ColOrName): Column {
  return new Column(fnExpr("endswith", toExpr(column), toExpr(suffix)));
}

/** Splits the string around matches of the given pattern. */
export function split(column: ColOrName, pattern: string, limit = -1): Column {
  return new Column(fnExpr("split", toExpr(column), _lit(pattern)._expr, _lit(limit)._expr));
}

/** Returns the string with the first letter of each word capitalized. */
export function initcap(column: ColOrName): Column {
  return fn("initcap", column);
}

/** Left-pads the string column to the given length with the specified pad string. */
export function lpad(column: ColOrName, len: number, pad: string): Column {
  return new Column(fnExpr("lpad", toExpr(column), _lit(len)._expr, _lit(pad)._expr));
}

/** Right-pads the string column to the given length with the specified pad string. */
export function rpad(column: ColOrName, len: number, pad: string): Column {
  return new Column(fnExpr("rpad", toExpr(column), _lit(len)._expr, _lit(pad)._expr));
}

/** Repeats a string column n times. */
export function repeat(column: ColOrName, n: ColOrName | number): Column {
  return new Column(fnExpr("repeat", toExpr(column), toNumberOrExpr(n)));
}

/** Reverses the string column. */
export function reverse(column: ColOrName): Column {
  return fn("reverse", column);
}

/** Returns the position of the first occurrence of substr in the string column (1-based). */
export function instr(column: ColOrName, substr: string): Column {
  return new Column(fnExpr("instr", toExpr(column), _lit(substr)._expr));
}

/** Returns the position of the first occurrence of substr in the string column, starting from the given position (1-based). */
export function locate(substr: string, column: ColOrName, pos = 1): Column {
  return new Column(fnExpr("locate", _lit(substr)._expr, toExpr(column), _lit(pos)._expr));
}

/**
 * Replaces every occurrence of `search` in `src` with `replace`, or removes it
 * when `replace` is omitted.
 *
 * As in PySpark, string arguments name columns, so wrap literals in `lit()`.
 */
export function replace(src: ColOrName, search: ColOrName, replace?: ColOrName): Column {
  return replace === undefined ? fn("replace", src, search) : fn("replace", src, search, replace);
}

/**
 * Returns the 1-based position of the first occurrence of `substr` in `str`
 * at or after `start`, or 0 when there is none.
 *
 * As in PySpark, string arguments name columns, so wrap literals in `lit()`.
 */
export function position(substr: ColOrName, str: ColOrName, start?: ColOrName): Column {
  return start === undefined ? fn("position", substr, str) : fn("position", substr, str, start);
}

/**
 * Returns the 1-based position of `str` in the comma-separated list
 * `strArray`, or 0 when it is absent.
 */
export function find_in_set(str: ColOrName, strArray: ColOrName): Column {
  return fn("find_in_set", str, strArray);
}

/** Translates any character in the string by a character in the matching string to the corresponding character in the replace string. */
export function translate(column: ColOrName, matching: string, replace: string): Column {
  return new Column(fnExpr("translate", toExpr(column), _lit(matching)._expr, _lit(replace)._expr));
}

/** Returns the numeric value of the first character of the string column. */
export function ascii(column: ColOrName): Column {
  return fn("ascii", column);
}

/** Formats a numeric column to a string with the given number of decimal places. */
export function format_number(column: ColOrName, d: number): Column {
  return new Column(fnExpr("format_number", toExpr(column), _lit(d)._expr));
}

/** Formats the arguments in printf-style and returns the result as a string column. */
export function format_string(format: string, ...columns: ColOrName[]): Column {
  return new Column(fnExpr("format_string", _lit(format)._expr, ...columns.map(toExpr)));
}

/**
 * Formats the arguments printf-style.
 *
 * As in PySpark, a string `format` names a column, so wrap a literal format in
 * `lit()`, or use {@link format_string}, which takes it as a literal.
 */
export function printf(format: ColOrName, ...columns: ColOrName[]): Column {
  return fn("printf", format, ...columns);
}

/** Computes the BASE64 encoding of a binary column and returns it as a string column. */
export function base64(column: ColOrName): Column {
  return fn("base64", column);
}

/** Decodes a BASE64 encoded string column and returns it as a binary column. */
export function unbase64(column: ColOrName): Column {
  return fn("unbase64", column);
}

/** Returns the SoundEx encoding for a string. */
export function soundex(column: ColOrName): Column {
  return fn("soundex", column);
}

/** Computes the Levenshtein distance of the two given string columns. */
export function levenshtein(left: ColOrName, right: ColOrName): Column {
  return fn("levenshtein", left, right);
}

/** Overlays the specified portion of a string with another string. */
export function overlay(
  src: ColOrName,
  replace: ColOrName,
  pos: ColOrName | number,
  len: ColOrName | number = -1,
): Column {
  return new Column(
    fnExpr("overlay", toExpr(src), toExpr(replace), toNumberOrExpr(pos), toNumberOrExpr(len)),
  );
}

/** Returns the leftmost len characters from the string column. */
export function left(column: ColOrName, len: ColOrName | number): Column {
  return new Column(fnExpr("left", toExpr(column), toNumberOrExpr(len)));
}

/** Returns the rightmost len characters from the string column. */
export function right(column: ColOrName, len: ColOrName | number): Column {
  return new Column(fnExpr("right", toExpr(column), toNumberOrExpr(len)));
}

/** Computes the first argument into a binary from a string using the provided character set. */
export function decode(column: ColOrName, charset: string): Column {
  return new Column(fnExpr("decode", toExpr(column), _lit(charset)._expr));
}

/** Computes the first argument into a string from a binary using the provided character set. */
export function encode(column: ColOrName, charset: string): Column {
  return new Column(fnExpr("encode", toExpr(column), _lit(charset)._expr));
}

/**
 * Returns the substring of `str` starting at the 1-based `pos`, `len`
 * characters long when given.
 *
 * As in PySpark, `pos` and `len` are columns, so pass `lit(2)` for a constant,
 * or use {@link substring}, which also takes numbers.
 */
export function substr(str: ColOrName, pos: ColOrName, len?: ColOrName): Column {
  return len === undefined ? fn("substr", str, pos) : fn("substr", str, pos, len);
}

/**
 * Returns the substring before the `count`-th occurrence of `delim`, or after
 * it, counting from the right, when `count` is negative.
 */
export function substring_index(str: ColOrName, delim: string, count: number): Column {
  return new Column(fnExpr("substring_index", toExpr(str), _lit(delim)._expr, _lit(count)._expr));
}

/**
 * Splits `src` on `delimiter` and returns the part at the 1-based `partNum`,
 * counting from the end when negative.
 *
 * As in PySpark, string arguments name columns, so wrap literals in `lit()`.
 */
export function split_part(src: ColOrName, delimiter: ColOrName, partNum: ColOrName): Column {
  return fn("split_part", src, delimiter, partNum);
}

/**
 * Returns the input at the 1-based index given by the first input, or null
 * when it is out of range.
 */
export function elt(...inputs: ColOrName[]): Column {
  return fn("elt", ...inputs);
}

/** Returns the ASCII character for the given code, taken modulo 256. */
export function char(column: ColOrName): Column {
  return fn("char", column);
}

/** Returns the ASCII character for the given code, like {@link char}. */
export function chr(column: ColOrName): Column {
  return fn("chr", column);
}

/** Gives the string the named collation, such as `UTF8_LCASE`. */
export function collate(column: ColOrName, collation: string): Column {
  return new Column(fnExpr("collate", toExpr(column), _lit(collation)._expr));
}

/** Returns the collation name of the string. */
export function collation(column: ColOrName): Column {
  return fn("collation", column);
}

/**
 * Masks the string, replacing upper-case letters with `upperChar` (`X` by
 * default), lower-case letters with `lowerChar` (`x`), digits with `digitChar`
 * (`n`), and other characters with `otherChar` (kept by default).
 *
 * As in PySpark, a string replacement names a column, so wrap a literal
 * character in `lit()`.
 */
export function mask(
  column: ColOrName,
  upperChar?: ColOrName,
  lowerChar?: ColOrName,
  digitChar?: ColOrName,
  otherChar?: ColOrName,
): Column {
  return new Column(
    fnExpr(
      "mask",
      toExpr(column),
      toExprOr(upperChar, "X"),
      toExprOr(lowerChar, "x"),
      toExprOr(digitChar, "n"),
      toExprOr(otherChar, null),
    ),
  );
}

/**
 * Returns a random string of `length` characters drawn from 0-9, a-z, and
 * A-Z. Without a `seed`, one is picked on the client, as in PySpark and Scala.
 */
export function randstr(length: Column | number, seed?: Column | number): Column {
  return new Column(
    fnExpr(
      "randstr",
      toNumberOrExpr(length),
      seed !== undefined ? toNumberOrExpr(seed) : randomSeed(),
    ),
  );
}

/**
 * Splits the string into sentences, each an array of words, using the locale
 * given by `language` and `country`.
 *
 * As in PySpark, string arguments name columns, so wrap literals in `lit()`.
 */
export function sentences(string: ColOrName, language?: ColOrName, country?: ColOrName): Column {
  return new Column(
    fnExpr("sentences", toExpr(string), toExprOr(language, ""), toExprOr(country, "")),
  );
}

/**
 * Splits the string into a map, with `pairDelim` (`,` by default) between
 * entries and `keyValueDelim` (`:` by default) between each key and value.
 *
 * As in PySpark, a string delimiter names a column, so wrap a literal
 * delimiter in `lit()`.
 */
export function str_to_map(
  text: ColOrName,
  pairDelim?: ColOrName,
  keyValueDelim?: ColOrName,
): Column {
  return new Column(
    fnExpr("str_to_map", toExpr(text), toExprOr(pairDelim, ","), toExprOr(keyValueDelim, ":")),
  );
}

/**
 * Converts the string to binary using `format`, one of `hex` (the default),
 * `utf-8`, `utf8`, or `base64`.
 *
 * As in PySpark, a string `format` names a column, so wrap a literal format in
 * `lit()`.
 */
export function to_binary(column: ColOrName, format?: ColOrName): Column {
  return format === undefined ? fn("to_binary", column) : fn("to_binary", column, format);
}

/** Like {@link to_binary}, but returns null where the conversion fails. */
export function try_to_binary(column: ColOrName, format?: ColOrName): Column {
  return format === undefined ? fn("try_to_binary", column) : fn("try_to_binary", column, format);
}

/**
 * Converts the value to a string using `format`, such as `"$99.99"` for a
 * number or `"yyyy-MM-dd"` for a date.
 *
 * As in PySpark, a string `format` names a column, so wrap a literal format in
 * `lit()`.
 */
export function to_char(column: ColOrName, format: ColOrName): Column {
  return fn("to_char", column, format);
}

/** Converts the value to a string using `format`, like {@link to_char}. */
export function to_varchar(column: ColOrName, format: ColOrName): Column {
  return fn("to_varchar", column, format);
}

/**
 * Converts the string to a decimal using `format`, such as `"$99.99"`.
 *
 * As in PySpark, a string `format` names a column, so wrap a literal format in
 * `lit()`.
 */
export function to_number(column: ColOrName, format: ColOrName): Column {
  return fn("to_number", column, format);
}

/** Like {@link to_number}, but returns null where the conversion fails. */
export function try_to_number(column: ColOrName, format: ColOrName): Column {
  return fn("try_to_number", column, format);
}

/** Returns whether the string is valid UTF-8. */
export function is_valid_utf8(str: ColOrName): Column {
  return fn("is_valid_utf8", str);
}

/** Replaces invalid UTF-8 byte sequences in the string with U+FFFD. */
export function make_valid_utf8(str: ColOrName): Column {
  return fn("make_valid_utf8", str);
}

/** Returns the string when it is valid UTF-8, and fails otherwise. */
export function validate_utf8(str: ColOrName): Column {
  return fn("validate_utf8", str);
}

/** Returns the string when it is valid UTF-8, or null otherwise. */
export function try_validate_utf8(str: ColOrName): Column {
  return fn("try_validate_utf8", str);
}
