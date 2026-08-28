import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { MergeAction_ActionType } from "@spark-connect-js/connect";
import type { Expression, LogicalPlan } from "@spark-connect-js/core";
import { buildCommandProto } from "./grpc-transport.js";

const SOURCE_PLAN: LogicalPlan = { type: "sql", query: "SELECT 1 AS id" };

const CONDITION: Expression = { type: "expressionString", expression: "s.id = target.id" };

function mergeAction(
  actionType: Extract<Expression, { type: "mergeAction" }>["actionType"],
  extra: Partial<Extract<Expression, { type: "mergeAction" }>> = {},
): Expression {
  return { type: "mergeAction", actionType, assignments: [], ...extra };
}

describe("buildCommandProto: mergeIntoTableCommand", () => {
  it("maps target, source plan, condition, and the schema evolution flag", () => {
    const cmd = buildCommandProto({
      type: "mergeIntoTableCommand",
      targetTableName: "db.target",
      sourceTablePlan: SOURCE_PLAN,
      mergeCondition: CONDITION,
      matchActions: [mergeAction("updateStar")],
      notMatchedActions: [],
      notMatchedBySourceActions: [],
      withSchemaEvolution: true,
    });

    assert.equal(cmd.commandType.case, "mergeIntoTableCommand");
    if (cmd.commandType.case !== "mergeIntoTableCommand") return;
    const v = cmd.commandType.value;
    assert.equal(v.targetTableName, "db.target");
    assert.notEqual(v.sourceTablePlan, undefined);
    assert.equal(v.mergeCondition?.exprType.case, "expressionString");
    assert.equal(v.withSchemaEvolution, true);
    assert.equal(v.matchActions.length, 1);
    assert.deepEqual(v.notMatchedActions, []);
    assert.deepEqual(v.notMatchedBySourceActions, []);
  });

  it("maps every IR action type to its proto enum value", () => {
    const expectations = [
      ["delete", MergeAction_ActionType.DELETE],
      ["insert", MergeAction_ActionType.INSERT],
      ["insertStar", MergeAction_ActionType.INSERT_STAR],
      ["update", MergeAction_ActionType.UPDATE],
      ["updateStar", MergeAction_ActionType.UPDATE_STAR],
    ] as const;

    for (const [irType, protoEnum] of expectations) {
      const cmd = buildCommandProto({
        type: "mergeIntoTableCommand",
        targetTableName: "t",
        sourceTablePlan: SOURCE_PLAN,
        mergeCondition: CONDITION,
        matchActions: [mergeAction(irType)],
        notMatchedActions: [],
        notMatchedBySourceActions: [],
        withSchemaEvolution: false,
      });
      if (cmd.commandType.case !== "mergeIntoTableCommand") {
        assert.fail("wrong command case");
      }
      const action = cmd.commandType.value.matchActions[0];
      assert.equal(action.exprType.case, "mergeAction");
      if (action.exprType.case !== "mergeAction") return;
      assert.equal(action.exprType.value.actionType, protoEnum);
      assert.deepEqual(action.exprType.value.assignments, []);
      assert.equal(action.exprType.value.condition, undefined);
    }
  });

  it("maps per-action conditions and assignment key/value expressions", () => {
    const cmd = buildCommandProto({
      type: "mergeIntoTableCommand",
      targetTableName: "t",
      sourceTablePlan: SOURCE_PLAN,
      mergeCondition: CONDITION,
      matchActions: [
        mergeAction("update", {
          condition: { type: "expressionString", expression: "target.stale" },
          assignments: [
            {
              key: { type: "expressionString", expression: "val" },
              value: { type: "unresolvedAttribute", name: "s.val" },
            },
          ],
        }),
      ],
      notMatchedActions: [],
      notMatchedBySourceActions: [],
      withSchemaEvolution: false,
    });

    if (cmd.commandType.case !== "mergeIntoTableCommand") {
      assert.fail("wrong command case");
    }
    const action = cmd.commandType.value.matchActions[0];
    if (action.exprType.case !== "mergeAction") {
      assert.fail("wrong expression case");
    }
    const merge = action.exprType.value;
    assert.equal(merge.condition?.exprType.case, "expressionString");
    assert.equal(merge.assignments.length, 1);
    assert.equal(merge.assignments[0].key?.exprType.case, "expressionString");
    assert.equal(merge.assignments[0].value?.exprType.case, "unresolvedAttribute");
  });

  it("defaults missing action lists to empty and the flag to false", () => {
    const cmd = buildCommandProto({
      type: "mergeIntoTableCommand",
      targetTableName: "t",
      sourceTablePlan: SOURCE_PLAN,
      mergeCondition: CONDITION,
    });

    if (cmd.commandType.case !== "mergeIntoTableCommand") {
      assert.fail("wrong command case");
    }
    const v = cmd.commandType.value;
    assert.deepEqual(v.matchActions, []);
    assert.deepEqual(v.notMatchedActions, []);
    assert.deepEqual(v.notMatchedBySourceActions, []);
    assert.equal(v.withSchemaEvolution, false);
  });
});
