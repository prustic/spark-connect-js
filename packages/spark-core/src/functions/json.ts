/**
 * JSON functions.
 */

import { Column, fnExpr, toExpr, _lit, type ColOrName, fn } from "./_helpers.js";

/** Parses a column containing a JSON string into a StructType or ArrayType with the specified schema. */
export function from_json(column: ColOrName, schema: string): Column {
  return new Column(fnExpr("from_json", toExpr(column), _lit(schema)._expr));
}

/** Converts a column containing a StructType, ArrayType, or MapType into a JSON string. */
export function to_json(column: ColOrName): Column {
  return fn("to_json", column);
}

/** Extracts a JSON object from a JSON string based on the specified JSON path. */
export function get_json_object(column: ColOrName, path: string): Column {
  return new Column(fnExpr("get_json_object", toExpr(column), _lit(path)._expr));
}

/** Creates a new row for a JSON column according to the given field names. */
export function json_tuple(column: ColOrName, ...fields: string[]): Column {
  return new Column(fnExpr("json_tuple", toExpr(column), ...fields.map((f) => _lit(f)._expr)));
}

/** Parses a JSON string and infers its schema in DDL format. */
export function schema_of_json(json: string): Column {
  return new Column(fnExpr("schema_of_json", _lit(json)._expr));
}

/** Returns the number of elements in the outermost JSON array. */
export function json_array_length(column: ColOrName): Column {
  return fn("json_array_length", column);
}

/** Returns all the keys of the outermost JSON object as an array. */
export function json_object_keys(column: ColOrName): Column {
  return fn("json_object_keys", column);
}
