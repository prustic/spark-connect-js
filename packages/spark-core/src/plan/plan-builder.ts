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

      case "catalog": {
        const op = plan.operation;
        switch (op.op) {
          case "listDatabases":
            return { catalog: { listDatabases: { pattern: op.pattern } } };
          case "listTables":
            return { catalog: { listTables: { dbName: op.dbName, pattern: op.pattern } } };
          case "listColumns":
            return { catalog: { listColumns: { tableName: op.tableName, dbName: op.dbName } } };
          case "tableExists":
            return { catalog: { tableExists: { tableName: op.tableName, dbName: op.dbName } } };
          case "databaseExists":
            return { catalog: { databaseExists: { dbName: op.dbName } } };
          case "currentDatabase":
            return { catalog: { currentDatabase: {} } };
          case "setCurrentDatabase":
            return { catalog: { setCurrentDatabase: { dbName: op.dbName } } };
        }
        break;
      }

      case "setOperation": {
        const opMap = { union: "UNION", intersect: "INTERSECT", except: "EXCEPT" };
        return {
          setOp: {
            leftInput: PlanBuilder.toRelation(plan.left),
            rightInput: PlanBuilder.toRelation(plan.right),
            setOpType: opMap[plan.opType],
            isAll: plan.isAll,
            byName: plan.byName,
            allowMissingColumns: plan.allowMissingColumns,
          },
        };
      }

      case "sample":
        return {
          sample: {
            input: PlanBuilder.toRelation(plan.child),
            lowerBound: plan.lowerBound,
            upperBound: plan.upperBound,
            withReplacement: plan.withReplacement,
            seed: plan.seed,
          },
        };

      case "fillNa":
        return {
          fillNa: {
            input: PlanBuilder.toRelation(plan.child),
            cols: plan.cols,
            // Spark NAFill only accepts double, string, or boolean literals
            values: plan.values.map((v) => {
              if (typeof v === "number") return { literal: { double: v } };
              if (typeof v === "string") return { literal: { string: v } };
              return { literal: { boolean: v } };
            }),
          },
        };

      case "dropNa":
        return {
          dropNa: {
            input: PlanBuilder.toRelation(plan.child),
            cols: plan.cols,
            minNonNulls: plan.minNonNulls,
          },
        };

      case "toDF":
        return {
          toDf: {
            input: PlanBuilder.toRelation(plan.child),
            columnNames: plan.columnNames,
          },
        };

      case "describe":
        return {
          describe: {
            input: PlanBuilder.toRelation(plan.child),
            cols: plan.cols,
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

      case "unresolvedFunction":
        return {
          unresolvedFunction: {
            functionName: expr.name,
            arguments: expr.arguments.map((e) => PlanBuilder.toExpression(e)),
            isDistinct: expr.isDistinct ?? false,
          },
        };

      case "cast":
        return {
          cast: {
            expr: PlanBuilder.toExpression(expr.inner),
            typeStr: expr.targetType,
          },
        };

      case "window": {
        const w: Record<string, unknown> = {
          windowFunction: PlanBuilder.toExpression(expr.windowFunction),
          partitionSpec: expr.partitionSpec.map((e) => PlanBuilder.toExpression(e)),
          orderSpec: expr.orderSpec.map((o) => ({
            child: PlanBuilder.toExpression(o.expression),
            direction: o.direction,
            nullOrdering: o.nullOrdering,
          })),
        };
        if (expr.frameSpec) {
          const toBound = (b: import("./logical-plan.js").FrameBoundary) => {
            if (b.type === "currentRow") return { currentRow: true };
            if (b.type === "unbounded") return { unbounded: true };
            return { value: PlanBuilder.toExpression(b.value) };
          };
          w.frameSpec = {
            frameType: expr.frameSpec.frameType === "row" ? "ROW" : "RANGE",
            lower: toBound(expr.frameSpec.lower),
            upper: toBound(expr.frameSpec.upper),
          };
        }
        return { window: w };
      }
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
