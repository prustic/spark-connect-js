/**
 * ─── ProtoBuilder ───────────────────────────────────────────────────────────
 *
 * Converts the spark-core LogicalPlan/Expression tree into proper
 * @bufbuild/protobuf messages using the generated Spark Connect schemas.
 *
 * spark-core's PlanBuilder produces plain objects to stay dependency-free.
 * This module bridges the gap by creating typed protobuf messages that
 * can be serialized to binary for the gRPC wire format.
 *
 * @see Spark Connect proto (Relation): sql/connect/common/src/main/protobuf/spark/connect/relations.proto
 * @see Spark Connect proto (Expression): sql/connect/common/src/main/protobuf/spark/connect/expressions.proto
 */

import { create } from "@bufbuild/protobuf";
import type { LogicalPlan, Expression as CoreExpression } from "@spark-js/core";
import {
  type Relation,
  RelationSchema,
  ReadSchema,
  Read_DataSourceSchema,
  SQLSchema,
  FilterSchema,
  ProjectSchema,
  AggregateSchema,
  Aggregate_GroupType,
  LimitSchema,
  type Expression,
  ExpressionSchema,
  Expression_LiteralSchema,
  Expression_UnresolvedAttributeSchema,
  Expression_UnresolvedFunctionSchema,
  Expression_AliasSchema,
} from "@spark-js/connect";

/** Maps our expression type names to Spark's internal function names. */
const OPERATOR_FN: Record<string, string> = {
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

/**
 * Convert a spark-core LogicalPlan tree into a Spark Connect Relation
 * protobuf message, ready for serialization.
 */
export function buildRelation(plan: LogicalPlan): Relation {
  switch (plan.type) {
    case "read":
      return create(RelationSchema, {
        relType: {
          case: "read",
          value: create(ReadSchema, {
            readType: {
              case: "dataSource",
              value: create(Read_DataSourceSchema, {
                format: plan.format,
                paths: [plan.path],
                options: plan.options,
              }),
            },
          }),
        },
      });

    case "sql":
      return create(RelationSchema, {
        relType: {
          case: "sql",
          value: create(SQLSchema, { query: plan.query }),
        },
      });

    case "filter":
      return create(RelationSchema, {
        relType: {
          case: "filter",
          value: create(FilterSchema, {
            input: buildRelation(plan.child),
            condition: buildExpression(plan.condition),
          }),
        },
      });

    case "project":
      return create(RelationSchema, {
        relType: {
          case: "project",
          value: create(ProjectSchema, {
            input: buildRelation(plan.child),
            expressions: plan.expressions.map(buildExpression),
          }),
        },
      });

    case "aggregate":
      return create(RelationSchema, {
        relType: {
          case: "aggregate",
          value: create(AggregateSchema, {
            input: buildRelation(plan.child),
            groupType: Aggregate_GroupType.GROUPBY,
            groupingExpressions: plan.groupingExpressions.map(buildExpression),
            aggregateExpressions: plan.aggregateExpressions.map(buildExpression),
          }),
        },
      });

    case "limit":
      return create(RelationSchema, {
        relType: {
          case: "limit",
          value: create(LimitSchema, {
            input: buildRelation(plan.child),
            limit: plan.limit,
          }),
        },
      });
  }
}

/**
 * Convert a spark-core Expression tree node into a Spark Connect Expression
 * protobuf message.
 */
export function buildExpression(expr: CoreExpression): Expression {
  switch (expr.type) {
    case "unresolvedAttribute":
      return create(ExpressionSchema, {
        exprType: {
          case: "unresolvedAttribute",
          value: create(Expression_UnresolvedAttributeSchema, {
            unparsedIdentifier: expr.name,
          }),
        },
      });

    case "literal": {
      if (expr.value === null) {
        return create(ExpressionSchema, {
          exprType: {
            case: "literal",
            value: create(Expression_LiteralSchema, {
              literalType: { case: undefined, value: undefined },
            }),
          },
        });
      }
      if (typeof expr.value === "string") {
        return create(ExpressionSchema, {
          exprType: {
            case: "literal",
            value: create(Expression_LiteralSchema, {
              literalType: { case: "string", value: expr.value },
            }),
          },
        });
      }
      if (typeof expr.value === "boolean") {
        return create(ExpressionSchema, {
          exprType: {
            case: "literal",
            value: create(Expression_LiteralSchema, {
              literalType: { case: "boolean", value: expr.value },
            }),
          },
        });
      }
      if (typeof expr.value === "bigint") {
        return create(ExpressionSchema, {
          exprType: {
            case: "literal",
            value: create(Expression_LiteralSchema, {
              literalType: { case: "long", value: expr.value },
            }),
          },
        });
      }
      // number → integer if safe integer, otherwise double
      if (Number.isInteger(expr.value) && Number.isSafeInteger(expr.value)) {
        return create(ExpressionSchema, {
          exprType: {
            case: "literal",
            value: create(Expression_LiteralSchema, {
              literalType: { case: "integer", value: expr.value },
            }),
          },
        });
      }
      return create(ExpressionSchema, {
        exprType: {
          case: "literal",
          value: create(Expression_LiteralSchema, {
            literalType: { case: "double", value: expr.value },
          }),
        },
      });
    }

    case "alias":
      return create(ExpressionSchema, {
        exprType: {
          case: "alias",
          value: create(Expression_AliasSchema, {
            expr: buildExpression(expr.inner),
            name: [expr.name],
          }),
        },
      });

    case "aggregateFunction":
      return create(ExpressionSchema, {
        exprType: {
          case: "unresolvedFunction",
          value: create(Expression_UnresolvedFunctionSchema, {
            functionName: expr.name,
            arguments: expr.arguments.map(buildExpression),
            isDistinct: false,
          }),
        },
      });

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
    case "divide":
      return create(ExpressionSchema, {
        exprType: {
          case: "unresolvedFunction",
          value: create(Expression_UnresolvedFunctionSchema, {
            functionName: OPERATOR_FN[expr.type],
            arguments: [buildExpression(expr.left), buildExpression(expr.right)],
            isDistinct: false,
          }),
        },
      });

    default: {
      const _exhaustive: never = expr;
      throw new Error(`Unsupported expression type: ${(_exhaustive as CoreExpression).type}`);
    }
  }
}
