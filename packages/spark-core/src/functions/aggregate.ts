/**
 * Aggregate functions.
 */

import { Column, toExpr, type ColOrName, fn } from "./_helpers.js";

export function count(column: ColOrName): Column {
  return fn("count", column);
}

export function sum(column: ColOrName): Column {
  return fn("sum", column);
}

export function avg(column: ColOrName): Column {
  return fn("avg", column);
}

export function min(column: ColOrName): Column {
  return fn("min", column);
}

export function max(column: ColOrName): Column {
  return fn("max", column);
}

export function countDistinct(column: ColOrName, ...more: ColOrName[]): Column {
  return new Column({
    type: "unresolvedFunction",
    name: "count",
    arguments: [column, ...more].map(toExpr),
    isDistinct: true,
  });
}
