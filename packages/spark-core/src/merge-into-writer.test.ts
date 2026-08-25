import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { SparkSession } from "./spark-session.js";
import type { Transport } from "./spark-session.js";
import { MergeIntoWriter } from "./merge-into-writer.js";
import { Column, col, lit } from "./column.js";
import { expr } from "./functions/conditional.js";
import { InvalidInputError } from "./errors.js";
import type { Expression } from "./plan/logical-plan.js";

function mockCommandTransport() {
  const commandCalls: Record<string, unknown>[] = [];
  const transport: Transport & { commandCalls: Record<string, unknown>[] } = {
    commandCalls,
    async *executePlan(): AsyncIterable<Uint8Array> {},
    async executeCommand(_sid: string, cmd: Record<string, unknown>): Promise<void> {
      commandCalls.push(cmd);
    },
  };
  return transport;
}

function makeWriter() {
  const transport = mockCommandTransport();
  const spark = SparkSession.builder()
    .remote("sc://localhost:15002")
    .transport(transport)
    .getOrCreate();
  const df = spark.sql("SELECT 1 AS id");
  return { transport, df, writer: df.mergeInto("target", expr("s.id = target.id")) };
}

type MergeAction = Extract<Expression, { type: "mergeAction" }>;

describe("DataFrame.mergeInto", () => {
  it("returns a MergeIntoWriter", () => {
    const { writer } = makeWriter();
    assert.ok(writer instanceof MergeIntoWriter);
  });

  it("sends a mergeIntoTableCommand with the source plan and condition", async () => {
    const { transport, df, writer } = makeWriter();
    await writer.whenMatched().updateAll().merge();

    const cmd = transport.commandCalls[0];
    assert.equal(cmd.type, "mergeIntoTableCommand");
    assert.equal(cmd.targetTableName, "target");
    assert.equal(cmd.sourceTablePlan, df._plan);
    assert.deepEqual(cmd.mergeCondition, {
      type: "expressionString",
      expression: "s.id = target.id",
    });
    assert.equal(cmd.withSchemaEvolution, false);
    assert.deepEqual(cmd.notMatchedActions, []);
    assert.deepEqual(cmd.notMatchedBySourceActions, []);
  });
});

describe("MergeIntoWriter clauses", () => {
  it("updateAll() queues an updateStar action with no assignments", async () => {
    const { transport, writer } = makeWriter();
    await writer.whenMatched().updateAll().merge();

    const actions = transport.commandCalls[0].matchActions as MergeAction[];
    assert.equal(actions.length, 1);
    assert.equal(actions[0].actionType, "updateStar");
    assert.deepEqual(actions[0].assignments, []);
    assert.equal("condition" in actions[0], false);
  });

  it("update() queues assignments with expression-string keys", async () => {
    const { transport, writer } = makeWriter();
    const value = col("s.val");
    await writer.whenMatched().update({ "address.city": value }).merge();

    const actions = transport.commandCalls[0].matchActions as MergeAction[];
    assert.equal(actions[0].actionType, "update");
    assert.deepEqual(actions[0].assignments, [
      { key: { type: "expressionString", expression: "address.city" }, value: value._expr },
    ]);
  });

  it("threads the clause condition when given and omits it when absent", async () => {
    const { transport, writer } = makeWriter();
    const cond = col("s.val").eq(lit("x"));
    await writer.whenMatched(cond).delete().whenNotMatched().insertAll().merge();

    const matched = transport.commandCalls[0].matchActions as MergeAction[];
    assert.equal(matched[0].actionType, "delete");
    assert.deepEqual(matched[0].condition, cond._expr);
    const notMatched = transport.commandCalls[0].notMatchedActions as MergeAction[];
    assert.equal(notMatched[0].actionType, "insertStar");
    assert.equal("condition" in notMatched[0], false);
  });

  it("insert() lands in notMatchedActions", async () => {
    const { transport, writer } = makeWriter();
    await writer
      .whenNotMatched()
      .insert({ id: col("s.id") })
      .merge();

    const actions = transport.commandCalls[0].notMatchedActions as MergeAction[];
    assert.equal(actions[0].actionType, "insert");
    assert.equal(actions[0].assignments.length, 1);
  });

  it("whenNotMatchedBySource() supports all three actions", async () => {
    const { transport, writer } = makeWriter();
    await writer
      .whenNotMatchedBySource()
      .updateAll()
      .whenNotMatchedBySource()
      .update({ stale: lit(true) })
      .whenNotMatchedBySource()
      .delete()
      .merge();

    const actions = transport.commandCalls[0].notMatchedBySourceActions as MergeAction[];
    assert.deepEqual(
      actions.map((a) => a.actionType),
      ["updateStar", "update", "delete"],
    );
  });

  it("accumulates repeated clauses in call order", async () => {
    const { transport, writer } = makeWriter();
    await writer.whenMatched().delete().whenMatched().updateAll().merge();

    const actions = transport.commandCalls[0].matchActions as MergeAction[];
    assert.deepEqual(
      actions.map((a) => a.actionType),
      ["delete", "updateStar"],
    );
  });

  it("a clause object can be reused, appending one action per call", async () => {
    const { transport, writer } = makeWriter();
    const clause = writer.whenMatched();
    clause.delete();
    clause.delete();
    await writer.merge();

    assert.equal((transport.commandCalls[0].matchActions as MergeAction[]).length, 2);
  });

  it("withSchemaEvolution() sets the flag and returns the writer", async () => {
    const { transport, writer } = makeWriter();
    await writer.withSchemaEvolution().whenNotMatched().insertAll().merge();

    assert.equal(transport.commandCalls[0].withSchemaEvolution, true);
  });
});

describe("MergeIntoWriter validation", () => {
  it("merge() with no clause actions rejects", async () => {
    const { writer } = makeWriter();
    await assert.rejects(
      () => writer.merge(),
      (err: unknown) => {
        if (!(err instanceof InvalidInputError)) return false;
        assert.match(err.message, /mergeInto\("target"\)/);
        assert.match(err.message, /whenMatched\/whenNotMatched\/whenNotMatchedBySource/);
        return true;
      },
    );
  });

  it("update({}) and insert({}) throw synchronously naming the *All alternative", () => {
    const { writer } = makeWriter();
    assert.throws(
      () => writer.whenMatched().update({}),
      (err: unknown) => err instanceof InvalidInputError && /updateAll\(\)/.test(err.message),
    );
    assert.throws(
      () => writer.whenNotMatched().insert({}),
      (err: unknown) => err instanceof InvalidInputError && /insertAll\(\)/.test(err.message),
    );
  });

  it("snapshots assignments at call time and copies action arrays into the command", async () => {
    const { transport, writer } = makeWriter();
    const assignments: Record<string, Column> = { val: col("s.val") };
    const clause = writer.whenMatched();
    clause.update(assignments);
    assignments.other = col("s.other");
    await writer.merge();

    const actions = transport.commandCalls[0].matchActions as MergeAction[];
    assert.equal(actions[0].assignments.length, 1);
    assert.notEqual(transport.commandCalls[0].matchActions, writer._matchedActions);
  });
});
