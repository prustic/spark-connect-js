/**
 * String functions.
 */

import { Column, fnExpr, toExpr, _lit, type ColOrName, fn } from "./_helpers.js";

export function upper(column: ColOrName): Column {
  return fn("upper", column);
}

export function lower(column: ColOrName): Column {
  return fn("lower", column);
}

export function trim(column: ColOrName): Column {
  return fn("trim", column);
}

export function ltrim(column: ColOrName): Column {
  return fn("ltrim", column);
}

export function rtrim(column: ColOrName): Column {
  return fn("rtrim", column);
}

export function btrim(column: ColOrName): Column {
  return fn("btrim", column);
}

export function length(column: ColOrName): Column {
  return fn("length", column);
}

export function char_length(column: ColOrName): Column {
  return fn("char_length", column);
}

export const character_length = char_length;

export function octet_length(column: ColOrName): Column {
  return fn("octet_length", column);
}

export function bit_length(column: ColOrName): Column {
  return fn("bit_length", column);
}

export function concat(...columns: ColOrName[]): Column {
  return fn("concat", ...columns);
}

export function concat_ws(sep: string, ...columns: ColOrName[]): Column {
  return new Column(fnExpr("concat_ws", _lit(sep)._expr, ...columns.map(toExpr)));
}

export function substring(column: ColOrName, pos: number, len: number): Column {
  return new Column(fnExpr("substring", toExpr(column), _lit(pos)._expr, _lit(len)._expr));
}

export function regexp_replace(column: ColOrName, pattern: string, replacement: string): Column {
  return new Column(
    fnExpr("regexp_replace", toExpr(column), _lit(pattern)._expr, _lit(replacement)._expr),
  );
}

export function contains(column: ColOrName, value: string): Column {
  return new Column(fnExpr("contains", toExpr(column), _lit(value)._expr));
}

export function startswith(column: ColOrName, prefix: string): Column {
  return new Column(fnExpr("startswith", toExpr(column), _lit(prefix)._expr));
}

export function endswith(column: ColOrName, suffix: string): Column {
  return new Column(fnExpr("endswith", toExpr(column), _lit(suffix)._expr));
}

export function split(column: ColOrName, pattern: string, limit = -1): Column {
  return new Column(fnExpr("split", toExpr(column), _lit(pattern)._expr, _lit(limit)._expr));
}

export function initcap(column: ColOrName): Column {
  return fn("initcap", column);
}

export function lpad(column: ColOrName, len: number, pad: string): Column {
  return new Column(fnExpr("lpad", toExpr(column), _lit(len)._expr, _lit(pad)._expr));
}

export function rpad(column: ColOrName, len: number, pad: string): Column {
  return new Column(fnExpr("rpad", toExpr(column), _lit(len)._expr, _lit(pad)._expr));
}

export function repeat(column: ColOrName, n: number): Column {
  return new Column(fnExpr("repeat", toExpr(column), _lit(n)._expr));
}

export function reverse(column: ColOrName): Column {
  return fn("reverse", column);
}

export function instr(column: ColOrName, substr: string): Column {
  return new Column(fnExpr("instr", toExpr(column), _lit(substr)._expr));
}

export function locate(substr: string, column: ColOrName, pos = 1): Column {
  return new Column(fnExpr("locate", _lit(substr)._expr, toExpr(column), _lit(pos)._expr));
}

export function translate(column: ColOrName, matching: string, replace: string): Column {
  return new Column(fnExpr("translate", toExpr(column), _lit(matching)._expr, _lit(replace)._expr));
}

export function ascii(column: ColOrName): Column {
  return fn("ascii", column);
}

export function format_number(column: ColOrName, d: number): Column {
  return new Column(fnExpr("format_number", toExpr(column), _lit(d)._expr));
}

export function format_string(format: string, ...columns: ColOrName[]): Column {
  return new Column(fnExpr("format_string", _lit(format)._expr, ...columns.map(toExpr)));
}

export function base64(column: ColOrName): Column {
  return fn("base64", column);
}

export function unbase64(column: ColOrName): Column {
  return fn("unbase64", column);
}

export function soundex(column: ColOrName): Column {
  return fn("soundex", column);
}

export function levenshtein(left: ColOrName, right: ColOrName): Column {
  return fn("levenshtein", left, right);
}

export function overlay(src: ColOrName, replace: ColOrName, pos: number, len = -1): Column {
  return new Column(
    fnExpr("overlay", toExpr(src), toExpr(replace), _lit(pos)._expr, _lit(len)._expr),
  );
}

export function left(column: ColOrName, len: number): Column {
  return new Column(fnExpr("left", toExpr(column), _lit(len)._expr));
}

export function right(column: ColOrName, len: number): Column {
  return new Column(fnExpr("right", toExpr(column), _lit(len)._expr));
}

export function decode(column: ColOrName, charset: string): Column {
  return new Column(fnExpr("decode", toExpr(column), _lit(charset)._expr));
}

export function encode(column: ColOrName, charset: string): Column {
  return new Column(fnExpr("encode", toExpr(column), _lit(charset)._expr));
}
