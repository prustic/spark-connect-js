import { describe, it, after } from "node:test";
import assert from "node:assert/strict";
import { col, lit, expr } from "@spark-connect-js/node";
import { spark, stopSession, tempTable } from "./setup.js";

async function seedTarget(table: string): Promise<void> {
  await spark()
    .sql("SELECT * FROM VALUES (1, 'a'), (2, 'b'), (3, 'c') AS t(id, val)")
    .writeTo(table)
    .using("delta")
    .createOrReplace();
}

function sourceDf() {
  return spark().sql("SELECT * FROM VALUES (2, 'bb'), (4, 'dd') AS s(id, val)").alias("s");
}

async function collectSorted(table: string) {
  const rows = await spark().read.table(table).collect();
  return rows.map((r) => [r["id"], r["val"]]).sort((a, b) => Number(a[0]) - Number(b[0]));
}

describe("DataFrame.mergeInto", () => {
  after(stopSession);

  it("merges with matched update, not-matched insert, and by-source delete", async () => {
    const table = tempTable("merge_three_way");
    await seedTarget(table);

    await sourceDf()
      .mergeInto(table, expr(`s.id = ${table}.id`))
      .whenMatched()
      .update({ val: col("s.val") })
      .whenNotMatched()
      .insertAll()
      .whenNotMatchedBySource()
      .delete()
      .merge();

    assert.deepEqual(await collectSorted(table), [
      [2, "bb"],
      [4, "dd"],
    ]);
  });

  it("applies a clause condition to narrow the action", async () => {
    const table = tempTable("merge_conditional");
    await seedTarget(table);

    await sourceDf()
      .mergeInto(table, expr(`s.id = ${table}.id`))
      .whenMatched(col("s.val").eq(lit("bb")))
      .delete()
      .merge();

    assert.deepEqual(await collectSorted(table), [
      [1, "a"],
      [3, "c"],
    ]);
  });

  it("evolves the target schema when merging a wider source", async () => {
    // This Delta build ignores the plan-level schema-evolution flag (SQL
    // MERGE WITH SCHEMA EVOLUTION behaves the same), so evolution is enabled
    // via the Delta conf. withSchemaEvolution() stays in the chain to
    // exercise the flag through a real server round-trip.
    const table = tempTable("merge_evolve");
    await spark().conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true");
    try {
      await spark()
        .sql("SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(id, val)")
        .writeTo(table)
        .using("delta")
        .createOrReplace();

      await spark()
        .sql("SELECT * FROM VALUES (2, 'bb', 10), (3, 'cc', 20) AS s(id, val, score)")
        .alias("s")
        .mergeInto(table, expr(`s.id = ${table}.id`))
        .withSchemaEvolution()
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .merge();

      const rows = await spark().read.table(table).collect();
      const byId = new Map(rows.map((r) => [Number(r["id"] as bigint), r]));
      assert.equal(rows.length, 3);
      assert.equal(byId.get(1)?.["score"], null);
      assert.equal(byId.get(2)?.["score"], 10);
      assert.equal(byId.get(3)?.["score"], 20);
    } finally {
      await spark().conf.set("spark.databricks.delta.schema.autoMerge.enabled", "false");
    }
  });

  it("merges into an Iceberg table", async () => {
    await spark().sql("CREATE NAMESPACE IF NOT EXISTS iceberg.db").collect();
    const table = `iceberg.db.${tempTable("merge_iceberg")}`;
    await spark()
      .sql("SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(id, val)")
      .writeTo(table)
      .createOrReplace();

    await sourceDf()
      .mergeInto(table, expr(`s.id = ${table}.id`))
      .whenMatched()
      .updateAll()
      .whenNotMatched()
      .insertAll()
      .merge();

    assert.deepEqual(await collectSorted(table), [
      [1, "a"],
      [2, "bb"],
      [4, "dd"],
    ]);
  });
});
