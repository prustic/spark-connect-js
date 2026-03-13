/**
 * ProtoBuilder
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
import type {
  LogicalPlan,
  Expression as CoreExpression,
  SortOrder as CoreSortOrder,
  FrameBoundary as CoreFrameBoundary,
} from "@spark-connect-js/core";
import {
  type Relation,
  RelationSchema,
  ReadSchema,
  Read_DataSourceSchema,
  Read_NamedTableSchema,
  LocalRelationSchema,
  SQLSchema,
  FilterSchema,
  ProjectSchema,
  AggregateSchema,
  Aggregate_GroupType,
  LimitSchema,
  SortSchema,
  JoinSchema,
  Join_JoinType,
  DropSchema,
  WithColumnsSchema,
  DeduplicateSchema,
  OffsetSchema,
  type Expression,
  ExpressionSchema,
  Expression_LiteralSchema,
  Expression_UnresolvedAttributeSchema,
  Expression_UnresolvedFunctionSchema,
  Expression_AliasSchema,
  Expression_SortOrderSchema,
  Expression_SortOrder_SortDirection,
  Expression_SortOrder_NullOrdering,
  Expression_CastSchema,
  Expression_ExpressionStringSchema,
  type Catalog,
  CatalogSchema,
  CurrentDatabaseSchema,
  SetCurrentDatabaseSchema,
  ListDatabasesSchema,
  ListTablesSchema,
  ListColumnsSchema,
  TableExistsSchema,
  DatabaseExistsSchema,
  SetOperationSchema,
  SetOperation_SetOpType,
  SampleSchema,
  NAFillSchema,
  NADropSchema,
  ToDFSchema,
  StatDescribeSchema,
  Expression_WindowSchema,
  Expression_Window_WindowFrameSchema,
  Expression_Window_WindowFrame_FrameType,
  Expression_Window_WindowFrame_FrameBoundarySchema,
  RangeSchema,
  WithColumnsRenamedSchema,
  WithColumnsRenamed_RenameSchema,
  SubqueryAliasSchema,
  HintSchema,
  TailSchema,
  RepartitionSchema,
  RepartitionByExpressionSchema,
  StatSummarySchema,
  NAReplaceSchema,
  NAReplace_ReplacementSchema,
  StatCorrSchema,
  StatCovSchema,
  StatCrosstabSchema,
  StatFreqItemsSchema,
  StatApproxQuantileSchema,
  UnpivotSchema,
  Unpivot_ValuesSchema,
  Aggregate_PivotSchema,
} from "@spark-connect-js/connect";

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

    case "readTable":
      return create(RelationSchema, {
        relType: {
          case: "read",
          value: create(ReadSchema, {
            readType: {
              case: "namedTable",
              value: create(Read_NamedTableSchema, {
                unparsedIdentifier: plan.tableName,
                options: plan.options,
              }),
            },
          }),
        },
      });

    case "localRelation":
      return create(RelationSchema, {
        relType: {
          case: "localRelation",
          value: create(LocalRelationSchema, {
            data: plan.data,
            schema: plan.schema,
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

    case "aggregate": {
      const groupTypeMap: Record<string, Aggregate_GroupType> = {
        groupby: Aggregate_GroupType.GROUPBY,
        rollup: Aggregate_GroupType.ROLLUP,
        cube: Aggregate_GroupType.CUBE,
        pivot: Aggregate_GroupType.PIVOT,
      };
      const aggValue = create(AggregateSchema, {
        input: buildRelation(plan.child),
        groupType: groupTypeMap[plan.groupType ?? "groupby"] ?? Aggregate_GroupType.GROUPBY,
        groupingExpressions: plan.groupingExpressions.map(buildExpression),
        aggregateExpressions: plan.aggregateExpressions.map(buildExpression),
      });
      if (plan.pivot) {
        aggValue.pivot = create(Aggregate_PivotSchema, {
          col: buildExpression(plan.pivot.col),
          values: plan.pivot.values.map((v) => buildLiteral(v)),
        });
      }
      return create(RelationSchema, {
        relType: { case: "aggregate", value: aggValue },
      });
    }

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

    case "sort":
      return create(RelationSchema, {
        relType: {
          case: "sort",
          value: create(SortSchema, {
            input: buildRelation(plan.child),
            order: plan.order.map(buildSortOrder),
            isGlobal: plan.isGlobal,
          }),
        },
      });

    case "join": {
      const joinTypeMap: Record<string, Join_JoinType> = {
        inner: Join_JoinType.INNER,
        full_outer: Join_JoinType.FULL_OUTER,
        left_outer: Join_JoinType.LEFT_OUTER,
        right_outer: Join_JoinType.RIGHT_OUTER,
        left_semi: Join_JoinType.LEFT_SEMI,
        left_anti: Join_JoinType.LEFT_ANTI,
        cross: Join_JoinType.CROSS,
      };
      return create(RelationSchema, {
        relType: {
          case: "join",
          value: create(JoinSchema, {
            left: buildRelation(plan.left),
            right: buildRelation(plan.right),
            joinCondition: plan.condition ? buildExpression(plan.condition) : undefined,
            joinType: joinTypeMap[plan.joinType] ?? Join_JoinType.INNER,
          }),
        },
      });
    }

    case "drop":
      return create(RelationSchema, {
        relType: {
          case: "drop",
          value: create(DropSchema, {
            input: buildRelation(plan.child),
            columnNames: plan.columnNames,
          }),
        },
      });

    case "withColumns":
      return create(RelationSchema, {
        relType: {
          case: "withColumns",
          value: create(WithColumnsSchema, {
            input: buildRelation(plan.child),
            aliases: plan.aliases.map((a) =>
              create(Expression_AliasSchema, {
                expr: buildExpression(a.expression),
                name: [a.name],
              }),
            ),
          }),
        },
      });

    case "deduplicate":
      return create(RelationSchema, {
        relType: {
          case: "deduplicate",
          value: create(DeduplicateSchema, {
            input: buildRelation(plan.child),
            columnNames: plan.columnNames ?? [],
            allColumnsAsKeys: plan.allColumnsAsKeys,
          }),
        },
      });

    case "offset":
      return create(RelationSchema, {
        relType: {
          case: "offset",
          value: create(OffsetSchema, {
            input: buildRelation(plan.child),
            offset: plan.offset,
          }),
        },
      });

    case "catalog": {
      const op = plan.operation;
      let catValue: Catalog["catType"];
      switch (op.op) {
        case "currentDatabase":
          catValue = { case: "currentDatabase", value: create(CurrentDatabaseSchema) };
          break;
        case "setCurrentDatabase":
          catValue = {
            case: "setCurrentDatabase",
            value: create(SetCurrentDatabaseSchema, { dbName: op.dbName }),
          };
          break;
        case "listDatabases":
          catValue = {
            case: "listDatabases",
            value: create(ListDatabasesSchema, { pattern: op.pattern }),
          };
          break;
        case "listTables":
          catValue = {
            case: "listTables",
            value: create(ListTablesSchema, { dbName: op.dbName, pattern: op.pattern }),
          };
          break;
        case "listColumns":
          catValue = {
            case: "listColumns",
            value: create(ListColumnsSchema, { tableName: op.tableName, dbName: op.dbName }),
          };
          break;
        case "tableExists":
          catValue = {
            case: "tableExists",
            value: create(TableExistsSchema, { tableName: op.tableName, dbName: op.dbName }),
          };
          break;
        case "databaseExists":
          catValue = {
            case: "databaseExists",
            value: create(DatabaseExistsSchema, { dbName: op.dbName }),
          };
          break;
      }
      const catalog = create(CatalogSchema, { catType: catValue });
      return create(RelationSchema, {
        relType: { case: "catalog", value: catalog },
      });
    }

    case "setOperation": {
      const opTypeMap: Record<string, SetOperation_SetOpType> = {
        union: SetOperation_SetOpType.UNION,
        intersect: SetOperation_SetOpType.INTERSECT,
        except: SetOperation_SetOpType.EXCEPT,
      };
      return create(RelationSchema, {
        relType: {
          case: "setOp",
          value: create(SetOperationSchema, {
            leftInput: buildRelation(plan.left),
            rightInput: buildRelation(plan.right),
            setOpType: opTypeMap[plan.opType] ?? SetOperation_SetOpType.UNION,
            isAll: plan.isAll,
            byName: plan.byName,
            allowMissingColumns: plan.allowMissingColumns,
          }),
        },
      });
    }

    case "sample":
      return create(RelationSchema, {
        relType: {
          case: "sample",
          value: create(SampleSchema, {
            input: buildRelation(plan.child),
            lowerBound: plan.lowerBound,
            upperBound: plan.upperBound,
            withReplacement: plan.withReplacement,
            seed: plan.seed !== undefined ? BigInt(plan.seed) : undefined,
          }),
        },
      });

    case "fillNa":
      return create(RelationSchema, {
        relType: {
          case: "fillNa",
          value: create(NAFillSchema, {
            input: buildRelation(plan.child),
            cols: plan.cols,
            // Spark NAFill only accepts double, string, or boolean literals
            values: plan.values.map((v) => {
              if (typeof v === "number") {
                return create(Expression_LiteralSchema, {
                  literalType: { case: "double", value: v },
                });
              }
              if (typeof v === "string") {
                return create(Expression_LiteralSchema, {
                  literalType: { case: "string", value: v },
                });
              }
              return create(Expression_LiteralSchema, {
                literalType: { case: "boolean", value: v },
              });
            }),
          }),
        },
      });

    case "dropNa":
      return create(RelationSchema, {
        relType: {
          case: "dropNa",
          value: create(NADropSchema, {
            input: buildRelation(plan.child),
            cols: plan.cols,
            minNonNulls: plan.minNonNulls,
          }),
        },
      });

    case "toDF":
      return create(RelationSchema, {
        relType: {
          case: "toDf",
          value: create(ToDFSchema, {
            input: buildRelation(plan.child),
            columnNames: plan.columnNames,
          }),
        },
      });

    case "describe":
      return create(RelationSchema, {
        relType: {
          case: "describe",
          value: create(StatDescribeSchema, {
            input: buildRelation(plan.child),
            cols: plan.cols,
          }),
        },
      });

    case "range":
      return create(RelationSchema, {
        relType: {
          case: "range",
          value: create(RangeSchema, {
            start: BigInt(plan.start),
            end: BigInt(plan.end),
            step: BigInt(plan.step),
            numPartitions: plan.numPartitions,
          }),
        },
      });

    case "withColumnsRenamed":
      return create(RelationSchema, {
        relType: {
          case: "withColumnsRenamed",
          value: create(WithColumnsRenamedSchema, {
            input: buildRelation(plan.child),
            renames: plan.renames.map((r) =>
              create(WithColumnsRenamed_RenameSchema, {
                colName: r.colName,
                newColName: r.newColName,
              }),
            ),
          }),
        },
      });

    case "subqueryAlias":
      return create(RelationSchema, {
        relType: {
          case: "subqueryAlias",
          value: create(SubqueryAliasSchema, {
            input: buildRelation(plan.child),
            alias: plan.alias,
          }),
        },
      });

    case "hint":
      return create(RelationSchema, {
        relType: {
          case: "hint",
          value: create(HintSchema, {
            input: buildRelation(plan.child),
            name: plan.name,
            parameters: plan.parameters.map(buildExpression),
          }),
        },
      });

    case "tail":
      return create(RelationSchema, {
        relType: {
          case: "tail",
          value: create(TailSchema, {
            input: buildRelation(plan.child),
            limit: plan.limit,
          }),
        },
      });

    case "repartition":
      return create(RelationSchema, {
        relType: {
          case: "repartition",
          value: create(RepartitionSchema, {
            input: buildRelation(plan.child),
            numPartitions: plan.numPartitions,
            shuffle: plan.shuffle,
          }),
        },
      });

    case "repartitionByExpression":
      return create(RelationSchema, {
        relType: {
          case: "repartitionByExpression",
          value: create(RepartitionByExpressionSchema, {
            input: buildRelation(plan.child),
            partitionExprs: plan.partitionExprs.map(buildExpression),
            numPartitions: plan.numPartitions,
          }),
        },
      });

    case "summary":
      return create(RelationSchema, {
        relType: {
          case: "summary",
          value: create(StatSummarySchema, {
            input: buildRelation(plan.child),
            statistics: plan.statistics,
          }),
        },
      });

    case "naReplace":
      return create(RelationSchema, {
        relType: {
          case: "replace",
          value: create(NAReplaceSchema, {
            input: buildRelation(plan.child),
            cols: plan.cols,
            replacements: plan.replacements.map((r) =>
              create(NAReplace_ReplacementSchema, {
                oldValue: r.oldValue != null ? buildReplaceLiteral(r.oldValue) : undefined,
                newValue: r.newValue != null ? buildReplaceLiteral(r.newValue) : undefined,
              }),
            ),
          }),
        },
      });

    case "unpivot": {
      const unpivotValue = create(UnpivotSchema, {
        input: buildRelation(plan.child),
        ids: plan.ids.map(buildExpression),
        variableColumnName: plan.variableColumnName,
        valueColumnName: plan.valueColumnName,
      });
      if (plan.values) {
        unpivotValue.values = create(Unpivot_ValuesSchema, {
          values: plan.values.map(buildExpression),
        });
      }
      return create(RelationSchema, {
        relType: { case: "unpivot", value: unpivotValue },
      });
    }

    case "statCorr":
      return create(RelationSchema, {
        relType: {
          case: "corr",
          value: create(StatCorrSchema, {
            input: buildRelation(plan.child),
            col1: plan.col1,
            col2: plan.col2,
            method: plan.method,
          }),
        },
      });

    case "statCov":
      return create(RelationSchema, {
        relType: {
          case: "cov",
          value: create(StatCovSchema, {
            input: buildRelation(plan.child),
            col1: plan.col1,
            col2: plan.col2,
          }),
        },
      });

    case "statCrosstab":
      return create(RelationSchema, {
        relType: {
          case: "crosstab",
          value: create(StatCrosstabSchema, {
            input: buildRelation(plan.child),
            col1: plan.col1,
            col2: plan.col2,
          }),
        },
      });

    case "statFreqItems":
      return create(RelationSchema, {
        relType: {
          case: "freqItems",
          value: create(StatFreqItemsSchema, {
            input: buildRelation(plan.child),
            cols: plan.cols,
            support: plan.support,
          }),
        },
      });

    case "statApproxQuantile":
      return create(RelationSchema, {
        relType: {
          case: "approxQuantile",
          value: create(StatApproxQuantileSchema, {
            input: buildRelation(plan.child),
            cols: plan.cols,
            probabilities: plan.probabilities,
            relativeError: plan.relativeError,
          }),
        },
      });
  }
}

/**
 * Convert a JS primitive to an Expression_Literal proto message.
 */
function buildLiteral(value: string | number | boolean | null) {
  if (value === null) {
    return create(Expression_LiteralSchema, {
      literalType: { case: undefined, value: undefined },
    });
  }
  if (typeof value === "string") {
    return create(Expression_LiteralSchema, {
      literalType: { case: "string", value },
    });
  }
  if (typeof value === "boolean") {
    return create(Expression_LiteralSchema, {
      literalType: { case: "boolean", value },
    });
  }
  if (Number.isInteger(value) && Number.isSafeInteger(value)) {
    return create(Expression_LiteralSchema, {
      literalType: { case: "integer", value },
    });
  }
  return create(Expression_LiteralSchema, {
    literalType: { case: "double", value },
  });
}

/** NAReplace only supports null, bool, double, and string literals. */
function buildReplaceLiteral(value: string | number | boolean) {
  if (typeof value === "string") {
    return create(Expression_LiteralSchema, {
      literalType: { case: "string", value },
    });
  }
  if (typeof value === "boolean") {
    return create(Expression_LiteralSchema, {
      literalType: { case: "boolean", value },
    });
  }
  return create(Expression_LiteralSchema, {
    literalType: { case: "double", value },
  });
}

/**
 * Convert a spark-core SortOrder to a Spark Connect Expression.SortOrder.
 */
function buildSortOrder(order: CoreSortOrder) {
  return create(Expression_SortOrderSchema, {
    child: buildExpression(order.expression),
    direction:
      order.direction === "ascending"
        ? Expression_SortOrder_SortDirection.ASCENDING
        : Expression_SortOrder_SortDirection.DESCENDING,
    nullOrdering:
      order.nullOrdering === "nulls_first"
        ? Expression_SortOrder_NullOrdering.SORT_NULLS_FIRST
        : Expression_SortOrder_NullOrdering.SORT_NULLS_LAST,
  });
}

/**
 * Convert a spark-core FrameBoundary to a Spark Connect WindowFrame.FrameBoundary.
 */
function buildFrameBoundary(boundary: CoreFrameBoundary) {
  switch (boundary.type) {
    case "currentRow":
      return create(Expression_Window_WindowFrame_FrameBoundarySchema, {
        boundary: { case: "currentRow", value: true },
      });
    case "unbounded":
      return create(Expression_Window_WindowFrame_FrameBoundarySchema, {
        boundary: { case: "unbounded", value: true },
      });
    case "value":
      return create(Expression_Window_WindowFrame_FrameBoundarySchema, {
        boundary: { case: "value", value: buildExpression(boundary.value) },
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

    case "sortOrder":
      // sortOrder is a wrapper — build the inner expression
      return buildExpression(expr.inner);

    case "unresolvedFunction":
      return create(ExpressionSchema, {
        exprType: {
          case: "unresolvedFunction",
          value: create(Expression_UnresolvedFunctionSchema, {
            functionName: expr.name,
            arguments: expr.arguments.map(buildExpression),
            isDistinct: expr.isDistinct ?? false,
          }),
        },
      });

    case "expressionString":
      return create(ExpressionSchema, {
        exprType: {
          case: "expressionString",
          value: create(Expression_ExpressionStringSchema, {
            expression: expr.expression,
          }),
        },
      });

    case "cast":
      return create(ExpressionSchema, {
        exprType: {
          case: "cast",
          value: create(Expression_CastSchema, {
            expr: buildExpression(expr.inner),
            castToType: { case: "typeStr", value: expr.targetType },
          }),
        },
      });

    case "window": {
      const windowValue = create(Expression_WindowSchema, {
        windowFunction: buildExpression(expr.windowFunction),
        partitionSpec: expr.partitionSpec.map(buildExpression),
        orderSpec: expr.orderSpec.map(buildSortOrder),
      });
      if (expr.frameSpec) {
        windowValue.frameSpec = create(Expression_Window_WindowFrameSchema, {
          frameType:
            expr.frameSpec.frameType === "row"
              ? Expression_Window_WindowFrame_FrameType.ROW
              : Expression_Window_WindowFrame_FrameType.RANGE,
          lower: buildFrameBoundary(expr.frameSpec.lower),
          upper: buildFrameBoundary(expr.frameSpec.upper),
        });
      }
      return create(ExpressionSchema, {
        exprType: { case: "window", value: windowValue },
      });
    }

    default: {
      const _exhaustive: never = expr;
      throw new Error(`Unsupported expression type: ${(_exhaustive as CoreExpression).type}`);
    }
  }
}
