import type { Expression, SortOrder, WindowFrame, FrameBoundary } from "./plan/logical-plan.js";
import { col } from "./column.js";
import type { Column } from "./column.js";

type ColOrName = Column | string;

function toExpr(c: ColOrName): Expression {
  return typeof c === "string" ? col(c)._expr : c._expr;
}

function toSortOrder(c: ColOrName): SortOrder {
  const expr = toExpr(c);
  if (expr.type === "sortOrder") {
    return {
      expression: expr.inner,
      direction: expr.direction,
      nullOrdering: expr.nullOrdering,
    };
  }
  return {
    expression: expr,
    direction: "ascending",
    nullOrdering: "nulls_last",
  };
}

/**
 * Describes how to partition, order, and frame a window for window functions
 * such as `row_number`, `rank`, and `lag`.
 *
 * Obtain a `WindowSpec` from the {@link Window} factory. Instances are
 * immutable; every builder method returns a new `WindowSpec`.
 *
 * @example Ranking within a partition
 * ```ts
 * import { Window, col } from "@spark-connect-js/core";
 * import { row_number } from "@spark-connect-js/core/functions";
 *
 * const spec = Window.partitionBy("country").orderBy(col("score").desc());
 * const ranked = df.withColumn("rank", row_number().over(spec));
 * ```
 *
 * @see [Spark source: Window.scala](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/expressions/Window.scala)
 */
export class WindowSpec {
  /** @internal */
  readonly _partitionSpec: Expression[];
  /** @internal */
  readonly _orderSpec: SortOrder[];
  /** @internal */
  readonly _frameSpec?: WindowFrame;

  constructor(
    partitionSpec: Expression[] = [],
    orderSpec: SortOrder[] = [],
    frameSpec?: WindowFrame,
  ) {
    this._partitionSpec = [...partitionSpec];
    this._orderSpec = [...orderSpec];
    this._frameSpec = frameSpec;
  }

  /** Add partition-by columns to this window spec. */
  partitionBy(...cols: ColOrName[]): WindowSpec {
    return new WindowSpec(cols.map(toExpr), this._orderSpec, this._frameSpec);
  }

  /** Add order-by columns to this window spec. */
  orderBy(...cols: ColOrName[]): WindowSpec {
    return new WindowSpec(this._partitionSpec, cols.map(toSortOrder), this._frameSpec);
  }

  /** Set a row-based frame between the given boundaries. */
  rowsBetween(start: number, end: number): WindowSpec {
    return new WindowSpec(this._partitionSpec, this._orderSpec, {
      frameType: "row",
      lower: toBoundary(start),
      upper: toBoundary(end),
    });
  }

  /** Set a range-based frame between the given boundaries. */
  rangeBetween(start: number, end: number): WindowSpec {
    return new WindowSpec(this._partitionSpec, this._orderSpec, {
      frameType: "range",
      lower: toBoundary(start),
      upper: toBoundary(end),
    });
  }
}

function toBoundary(value: number): FrameBoundary {
  if (value === 0) return { type: "currentRow" };
  if (value === Window.unboundedPreceding || value === Window.unboundedFollowing) {
    return { type: "unbounded" };
  }
  return { type: "value", value: { type: "literal", value } };
}

/**
 * Entry point for building a {@link WindowSpec}.
 *
 * Mirrors PySpark's `Window` object. Expose `partitionBy`, `orderBy`,
 * `rowsBetween`, and `rangeBetween` as factories, plus the boundary
 * constants `unboundedPreceding`, `unboundedFollowing`, and `currentRow`.
 *
 * @example
 * ```ts
 * import { Window, col } from "@spark-connect-js/core";
 *
 * const spec = Window
 *   .partitionBy("country")
 *   .orderBy(col("ts"))
 *   .rowsBetween(Window.unboundedPreceding, Window.currentRow);
 * ```
 */
export const Window = {
  unboundedPreceding: -2147483648, // Integer.MIN_VALUE (Spark convention)
  unboundedFollowing: 2147483647, // Integer.MAX_VALUE (Spark convention)
  currentRow: 0,

  /** Create a window spec partitioned by the given columns. */
  partitionBy(...cols: ColOrName[]): WindowSpec {
    return new WindowSpec().partitionBy(...cols);
  },

  /** Create a window spec ordered by the given columns. */
  orderBy(...cols: ColOrName[]): WindowSpec {
    return new WindowSpec().orderBy(...cols);
  },

  /** Create an unpartitioned, unordered window with row-based frame. */
  rowsBetween(start: number, end: number): WindowSpec {
    return new WindowSpec().rowsBetween(start, end);
  },

  /** Create an unpartitioned, unordered window with range-based frame. */
  rangeBetween(start: number, end: number): WindowSpec {
    return new WindowSpec().rangeBetween(start, end);
  },
};
