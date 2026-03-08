/**
 * CSV functions.
 */

import { Column, fnExpr, toExpr, _lit, type ColOrName } from "./_helpers.js";

/** Parses a column containing a CSV string into a StructType with the specified schema. */
export function from_csv(column: ColOrName, schema: string): Column {
  return new Column(fnExpr("from_csv", toExpr(column), _lit(schema)._expr));
}

/** Converts a column containing a StructType into a CSV string. */
export function to_csv(column: ColOrName): Column {
  return new Column(fnExpr("to_csv", toExpr(column)));
}

/** Parses a CSV string and infers its schema in DDL format. */
export function schema_of_csv(csv: string): Column {
  return new Column(fnExpr("schema_of_csv", _lit(csv)._expr));
}
