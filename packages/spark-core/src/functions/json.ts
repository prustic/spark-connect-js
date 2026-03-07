/**
 * JSON functions.
 */

import { Column, fnExpr, toExpr, _lit, type ColOrName, fn } from "./_helpers.js";

export function from_json(column: ColOrName, schema: string): Column {
  return new Column(fnExpr("from_json", toExpr(column), _lit(schema)._expr));
}

export function to_json(column: ColOrName): Column {
  return fn("to_json", column);
}

export function get_json_object(column: ColOrName, path: string): Column {
  return new Column(fnExpr("get_json_object", toExpr(column), _lit(path)._expr));
}

export function json_tuple(column: ColOrName, ...fields: string[]): Column {
  return new Column(fnExpr("json_tuple", toExpr(column), ...fields.map((f) => _lit(f)._expr)));
}

export function schema_of_json(json: string): Column {
  return new Column(fnExpr("schema_of_json", _lit(json)._expr));
}

export function json_array_length(column: ColOrName): Column {
  return fn("json_array_length", column);
}

export function json_object_keys(column: ColOrName): Column {
  return fn("json_object_keys", column);
}
