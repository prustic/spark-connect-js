/**
 * CSV functions.
 */

import { Column, fnExpr, toExpr, _lit, type ColOrName } from "./_helpers.js";

export function from_csv(column: ColOrName, schema: string): Column {
  return new Column(fnExpr("from_csv", toExpr(column), _lit(schema)._expr));
}

export function to_csv(column: ColOrName): Column {
  return new Column(fnExpr("to_csv", toExpr(column)));
}

export function schema_of_csv(csv: string): Column {
  return new Column(fnExpr("schema_of_csv", _lit(csv)._expr));
}
