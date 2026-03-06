/**
 * ─── Column ─────────────────────────────────────────────────────────────────
 *
 * Represents a column expression in Spark's expression tree.
 *
 * @see Spark source: sql/core/src/main/scala/org/apache/spark/sql/Column.scala
 * @see Catalyst expressions: sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/
 *
 * In the JVM, Column wraps a Catalyst Expression.  Every operation on a Column
 * (>, <, ===, +, alias) produces a NEW expression tree node, never mutating
 * the original.  The full tree is serialised as a Spark Connect `Expression`
 * protobuf when the plan is sent to the server.
 *
 * Critical design constraint:
 *   These expressions are NEVER evaluated in JavaScript.  They are
 *   descriptions of computation that the JVM will execute.  We are building
 *   an AST, not running a query engine.
 */

import type { Expression } from "./plan/logical-plan.js";

export class Column {
  /** @internal — the raw expression tree node */
  readonly _expr: Expression;

  constructor(expr: Expression) {
    this._expr = expr;
  }

  // ── Comparison operators ──────────────────────────────────────────────────
  // Each returns a new Column wrapping a comparison expression node.

  gt(other: Column): Column {
    return new Column({ type: "gt", left: this._expr, right: other._expr });
  }

  lt(other: Column): Column {
    return new Column({ type: "lt", left: this._expr, right: other._expr });
  }

  eq(other: Column): Column {
    return new Column({ type: "eq", left: this._expr, right: other._expr });
  }

  neq(other: Column): Column {
    return new Column({ type: "neq", left: this._expr, right: other._expr });
  }

  gte(other: Column): Column {
    return new Column({ type: "gte", left: this._expr, right: other._expr });
  }

  lte(other: Column): Column {
    return new Column({ type: "lte", left: this._expr, right: other._expr });
  }

  // ── Logical operators ─────────────────────────────────────────────────────

  and(other: Column): Column {
    return new Column({ type: "and", left: this._expr, right: other._expr });
  }

  or(other: Column): Column {
    return new Column({ type: "or", left: this._expr, right: other._expr });
  }

  // ── Arithmetic ────────────────────────────────────────────────────────────

  plus(other: Column): Column {
    return new Column({ type: "add", left: this._expr, right: other._expr });
  }

  minus(other: Column): Column {
    return new Column({ type: "subtract", left: this._expr, right: other._expr });
  }

  multiply(other: Column): Column {
    return new Column({ type: "multiply", left: this._expr, right: other._expr });
  }

  divide(other: Column): Column {
    return new Column({ type: "divide", left: this._expr, right: other._expr });
  }

  // ── Aliasing ──────────────────────────────────────────────────────────────

  /**
   * Rename this column expression.  Maps to Catalyst's `Alias(expr, name)`.
   */
  alias(name: string): Column {
    return new Column({ type: "alias", inner: this._expr, name });
  }

  as(name: string): Column {
    return this.alias(name);
  }
}

// ─── Convenience factories ──────────────────────────────────────────────────
// These mirror PySpark's `from pyspark.sql.functions import col, lit`

/**
 * Reference an unresolved column by name.
 *
 * "Unresolved" means the server will resolve it against the schema at
 * analysis time.  If the column doesn't exist, the JVM throws
 * AnalysisException — we can't catch that at compile time.
 */
export function col(name: string): Column {
  return new Column({ type: "unresolvedAttribute", name });
}

/**
 * Create a literal value expression.
 *
 * ⚠️  Type mapping caveat:
 *   JS `number` is always IEEE-754 float64.  If you pass an integer literal
 *   that Spark expects as LongType, precision can be silently lost for values
 *   > Number.MAX_SAFE_INTEGER (2^53 - 1).  For large integers, use BigInt
 *   literals and ensure the plan builder maps them to Spark's LongType.
 */
export function lit(value: string | number | boolean | bigint | null): Column {
  return new Column({ type: "literal", value });
}
