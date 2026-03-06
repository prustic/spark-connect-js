/**
 * ─── PlanBuilder ────────────────────────────────────────────────────────────
 *
 * Serialises the TypeScript LogicalPlan tree into the wire format expected by
 * Spark Connect's protobuf schema.
 *
 * The Spark Connect protobuf schema lives in the Spark repo at:
 *   connector/connect/common/src/main/protobuf/spark/connect/relations.proto
 *   connector/connect/common/src/main/protobuf/spark/connect/expressions.proto
 *
 * @see Spark Connect proto (Relation): connector/connect/common/src/main/protobuf/spark/connect/relations.proto
 * @see Spark Connect proto (Expression): connector/connect/common/src/main/protobuf/spark/connect/expressions.proto
 *
 * This builder walks our plan tree and produces plain objects matching the
 * proto message shapes.  Actual protobuf serialisation (to binary) is handled
 * by the runtime adapter, which uses a proto library (e.g. protobuf-ts,
 * @grpc/proto-loader, or buf-generated code).
 *
 * Why not generate types directly from .proto files?
 *   We want spark-core to be dependency-free.  Generated proto types pull in
 *   protobufjs or similar, adding ~200KB+ to the bundle.  Instead, we define
 *   our own lean TypeScript types and map them in PlanBuilder.
 */

import type { LogicalPlan, Expression } from "./logical-plan.js";

/**
 * Converts a LogicalPlan tree into a plain object that mirrors the Spark
 * Connect proto Relation message.  This is an intermediate representation —
 * the runtime adapter further encodes it to binary protobuf for the wire.
 */
export class PlanBuilder {
  /**
   * Recursively convert a LogicalPlan into a Spark Connect `Relation`
   * compatible plain object.
   */
  static toRelation(plan: LogicalPlan): Record<string, unknown> {
    switch (plan.type) {
      case "read":
        return {
          read: {
            dataSource: {
              format: plan.format,
              paths: [plan.path],
              options: plan.options,
            },
          },
        };

      case "sql":
        return {
          sql: {
            query: plan.query,
          },
        };

      case "filter":
        return {
          filter: {
            input: PlanBuilder.toRelation(plan.child),
            condition: PlanBuilder.toExpression(plan.condition),
          },
        };

      case "project":
        return {
          project: {
            input: PlanBuilder.toRelation(plan.child),
            expressions: plan.expressions.map((e) => PlanBuilder.toExpression(e)),
          },
        };

      case "aggregate":
        return {
          aggregate: {
            input: PlanBuilder.toRelation(plan.child),
            groupType: "GROUP_TYPE_GROUPBY",
            groupingExpressions: plan.groupingExpressions.map((e) => PlanBuilder.toExpression(e)),
            aggregateExpressions: plan.aggregateExpressions.map((e) => PlanBuilder.toExpression(e)),
          },
        };

      case "limit":
        return {
          limit: {
            input: PlanBuilder.toRelation(plan.child),
            limit: plan.limit,
          },
        };

      case "sort":
        return {
          sort: {
            input: PlanBuilder.toRelation(plan.child),
            order: plan.order.map((o) => ({
              child: PlanBuilder.toExpression(o.expression),
              direction: o.direction,
              nullOrdering: o.nullOrdering,
            })),
            isGlobal: plan.isGlobal,
          },
        };

      case "join":
        return {
          join: {
            left: PlanBuilder.toRelation(plan.left),
            right: PlanBuilder.toRelation(plan.right),
            joinCondition: plan.condition ? PlanBuilder.toExpression(plan.condition) : undefined,
            joinType: plan.joinType,
          },
        };

      case "drop":
        return {
          drop: {
            input: PlanBuilder.toRelation(plan.child),
            columnNames: plan.columnNames,
          },
        };

      case "withColumns":
        return {
          withColumns: {
            input: PlanBuilder.toRelation(plan.child),
            aliases: plan.aliases.map((a) => ({
              expr: PlanBuilder.toExpression(a.expression),
              name: [a.name],
            })),
          },
        };

      case "deduplicate":
        return {
          deduplicate: {
            input: PlanBuilder.toRelation(plan.child),
            columnNames: plan.columnNames ?? [],
            allColumnsAsKeys: plan.allColumnsAsKeys,
          },
        };

      case "offset":
        return {
          offset: {
            input: PlanBuilder.toRelation(plan.child),
            offset: plan.offset,
          },
        };
    }
  }

  /** Convert an Expression tree node into a Spark Connect Expression object. */
  static toExpression(expr: Expression): Record<string, unknown> {
    switch (expr.type) {
      case "unresolvedAttribute":
        return {
          unresolvedAttribute: {
            unparsedIdentifier: expr.name,
          },
        };

      case "literal": {
        if (expr.value === null) return { literal: { null: {} } };
        if (typeof expr.value === "string") return { literal: { string: expr.value } };
        if (typeof expr.value === "boolean") return { literal: { boolean: expr.value } };
        if (typeof expr.value === "bigint") return { literal: { long: expr.value.toString() } };
        // number → double (safest default for JS numbers)
        return { literal: { double: expr.value } };
      }

      case "alias":
        return {
          alias: {
            expr: PlanBuilder.toExpression(expr.inner),
            name: [expr.name],
          },
        };

      case "aggregateFunction":
        return {
          unresolvedFunction: {
            functionName: expr.name,
            arguments: expr.arguments.map((e) => PlanBuilder.toExpression(e)),
            isDistinct: false,
          },
        };

      // Binary operators → UnresolvedFunction with infix operator name
      case "gt":
      case "lt":
      case "eq":
      case "neq":
      case "gte":
      case "lte":
      case "and":
      case "or":
      case "add":
      case "subtract":
      case "multiply":
      case "divide": {
        const fnName = OPERATOR_FUNCTION_MAP[expr.type];
        return {
          unresolvedFunction: {
            functionName: fnName,
            arguments: [PlanBuilder.toExpression(expr.left), PlanBuilder.toExpression(expr.right)],
            isDistinct: false,
          },
        };
      }

      case "sortOrder":
        return PlanBuilder.toExpression(expr.inner);
    }
  }
}

/** Maps our expression type names to Spark's internal function names. */
const OPERATOR_FUNCTION_MAP: Record<string, string> = {
  gt: ">",
  lt: "<",
  eq: "=",
  neq: "!=",
  gte: ">=",
  lte: "<=",
  and: "and",
  or: "or",
  add: "+",
  subtract: "-",
  multiply: "*",
  divide: "/",
};
