/**
 * Sort functions — standalone sort order wrappers.
 * These complement the Column methods (col.asc(), col.desc(), etc.)
 */

import { Column, toExpr, type ColOrName } from "./_helpers.js";

export function asc(column: ColOrName): Column {
  return new Column({
    type: "sortOrder",
    inner: toExpr(column),
    direction: "ascending",
    nullOrdering: "nulls_last",
  });
}

export function desc(column: ColOrName): Column {
  return new Column({
    type: "sortOrder",
    inner: toExpr(column),
    direction: "descending",
    nullOrdering: "nulls_last",
  });
}

export function asc_nulls_first(column: ColOrName): Column {
  return new Column({
    type: "sortOrder",
    inner: toExpr(column),
    direction: "ascending",
    nullOrdering: "nulls_first",
  });
}

export function asc_nulls_last(column: ColOrName): Column {
  return new Column({
    type: "sortOrder",
    inner: toExpr(column),
    direction: "ascending",
    nullOrdering: "nulls_last",
  });
}

export function desc_nulls_first(column: ColOrName): Column {
  return new Column({
    type: "sortOrder",
    inner: toExpr(column),
    direction: "descending",
    nullOrdering: "nulls_first",
  });
}

export function desc_nulls_last(column: ColOrName): Column {
  return new Column({
    type: "sortOrder",
    inner: toExpr(column),
    direction: "descending",
    nullOrdering: "nulls_last",
  });
}
