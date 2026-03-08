/**
 * String functions.
 */

import { Column, fnExpr, toExpr, _lit, type ColOrName, fn } from "./_helpers.js";

/** Converts a string column to upper case. */
export function upper(column: ColOrName): Column {
  return fn("upper", column);
}

/** Converts a string column to lower case. */
export function lower(column: ColOrName): Column {
  return fn("lower", column);
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
export function substring(column: ColOrName, pos: number, len: number): Column {
  return new Column(fnExpr("substring", toExpr(column), _lit(pos)._expr, _lit(len)._expr));
}

/** Replaces all substrings of the specified string value that match the pattern with the replacement. */
export function regexp_replace(column: ColOrName, pattern: string, replacement: string): Column {
  return new Column(
    fnExpr("regexp_replace", toExpr(column), _lit(pattern)._expr, _lit(replacement)._expr),
  );
}

/** Returns true if the string column contains the specified string. */
export function contains(column: ColOrName, value: string): Column {
  return new Column(fnExpr("contains", toExpr(column), _lit(value)._expr));
}

/** Returns true if the string column starts with the specified prefix. */
export function startswith(column: ColOrName, prefix: string): Column {
  return new Column(fnExpr("startswith", toExpr(column), _lit(prefix)._expr));
}

/** Returns true if the string column ends with the specified suffix. */
export function endswith(column: ColOrName, suffix: string): Column {
  return new Column(fnExpr("endswith", toExpr(column), _lit(suffix)._expr));
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
export function repeat(column: ColOrName, n: number): Column {
  return new Column(fnExpr("repeat", toExpr(column), _lit(n)._expr));
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
export function overlay(src: ColOrName, replace: ColOrName, pos: number, len = -1): Column {
  return new Column(
    fnExpr("overlay", toExpr(src), toExpr(replace), _lit(pos)._expr, _lit(len)._expr),
  );
}

/** Returns the leftmost len characters from the string column. */
export function left(column: ColOrName, len: number): Column {
  return new Column(fnExpr("left", toExpr(column), _lit(len)._expr));
}

/** Returns the rightmost len characters from the string column. */
export function right(column: ColOrName, len: number): Column {
  return new Column(fnExpr("right", toExpr(column), _lit(len)._expr));
}

/** Computes the first argument into a binary from a string using the provided character set. */
export function decode(column: ColOrName, charset: string): Column {
  return new Column(fnExpr("decode", toExpr(column), _lit(charset)._expr));
}

/** Computes the first argument into a string from a binary using the provided character set. */
export function encode(column: ColOrName, charset: string): Column {
  return new Column(fnExpr("encode", toExpr(column), _lit(charset)._expr));
}
