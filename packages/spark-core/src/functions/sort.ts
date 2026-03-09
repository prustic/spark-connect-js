/**
 * Sort functions — standalone sort order wrappers.
 * These complement the Column methods (col.asc(), col.desc(), etc.)
 */

import { Column, toExpr, type ColOrName } from "./_helpers.js";

/** Returns a sort expression based on the ascending order of the given column. */
export function asc(column: ColOrName): Column {
  return new Column({
    type: "sortOrder",
    inner: toExpr(column),
    direction: "ascending",
    nullOrdering: "nulls_last",
  });
}

/** Returns a sort expression based on the descending order of the given column. */
export function desc(column: ColOrName): Column {
  return new Column({
    type: "sortOrder",
    inner: toExpr(column),
    direction: "descending",
    nullOrdering: "nulls_last",
  });
}

/** Returns a sort expression based on ascending order, with null values returned before non-null values. */
export function asc_nulls_first(column: ColOrName): Column {
  return new Column({
    type: "sortOrder",
    inner: toExpr(column),
    direction: "ascending",
    nullOrdering: "nulls_first",
  });
}

/** Returns a sort expression based on ascending order, with null values returned after non-null values. */
export function asc_nulls_last(column: ColOrName): Column {
  return new Column({
    type: "sortOrder",
    inner: toExpr(column),
    direction: "ascending",
    nullOrdering: "nulls_last",
  });
}

/** Returns a sort expression based on descending order, with null values returned before non-null values. */
export function desc_nulls_first(column: ColOrName): Column {
  return new Column({
    type: "sortOrder",
    inner: toExpr(column),
    direction: "descending",
    nullOrdering: "nulls_first",
  });
}

/** Returns a sort expression based on descending order, with null values returned after non-null values. */
export function desc_nulls_last(column: ColOrName): Column {
  return new Column({
    type: "sortOrder",
    inner: toExpr(column),
    direction: "descending",
    nullOrdering: "nulls_last",
  });
}
