import { describe, it, after } from "node:test";
import assert from "node:assert/strict";
import { ArrowDecoder } from "@spark-connect-js/node";
import { Table, tableFromIPC } from "apache-arrow";
import { spark, stopSession, tempPath } from "./setup.js";

describe("DataFrame expression-level methods", () => {
  after(stopSession);

  const source = () => spark().sql("SELECT 1 AS a, 'x' AS b, 2 AS ab").alias("t");

  it("select('*') and select('t.*') expand every column", async () => {
    const expected = [{ a: 1, b: "x", ab: 2 }];
    assert.deepEqual(await source().select("*").collect(), expected);
    assert.deepEqual(await source().select("t.*").collect(), expected);
  });

  it("toJSON() yields one JSON string per row in a value column", async () => {
    const rows = await source().toJSON().collect();
    assert.equal(rows.length, 1);
    assert.deepEqual(JSON.parse(rows[0].value), { a: 1, b: "x", ab: 2 });
  });

  it("colRegex() selects the columns whose names match", async () => {
    const df = source();
    const rows = await df.select(df.colRegex("`a.*`")).collect();
    assert.deepEqual(rows, [{ a: 1, ab: 2 }]);
  });

  it("withMetadata() is visible through schema()", async () => {
    const schema = await source().withMetadata("a", { unit: "kg" }).schema();
    assert.deepEqual(schema.getField("a")?.metadata, { unit: "kg" });
  });

  it("toArrow() returns IPC streams that decode to the collect() rows", async () => {
    const df = source();
    const decoded = await ArrowDecoder.decode(await df.toArrow());
    assert.deepEqual(decoded, await df.collect());
  });

  it("toArrow() over several batches decodes fully with the documented idiom", async () => {
    const chunks = await spark().range(100_000).toArrow();
    assert.ok(chunks.length > 1, `expected several IPC streams, got ${String(chunks.length)}`);

    const table = new Table(chunks.flatMap((c) => tableFromIPC(c).batches));
    assert.equal(table.numRows, 100_000);
    // Pins apache-arrow's behavior, not ours: if this starts failing after an
    // arrow upgrade, the first-stream warning in the toArrow TSDoc is stale.
    assert.ok(
      tableFromIPC(chunks).numRows < 100_000,
      "tableFromIPC(chunks) now reads every stream; drop the warning from the toArrow TSDoc",
    );
  });

  it("metadataColumn('_metadata') exposes the source file of a parquet read", async () => {
    const path = tempPath("metadata_column");
    await spark().range(3).write.mode("overwrite").parquet(path);

    const df = spark().read.parquet(path);
    const rows = await df
      .select(df.metadataColumn("_metadata").getField("file_path").alias("f"))
      .collect();
    assert.equal(rows.length, 3);
    for (const row of rows) {
      assert.match(String(row["f"]), /metadata_column/);
    }
  });

  it("groupBy().mean() matches avg()", async () => {
    const df = spark().sql("SELECT * FROM VALUES ('x', 2), ('x', 4) AS t(k, v)");
    assert.deepEqual(
      await df.groupBy("k").mean("v").collect(),
      await df.groupBy("k").avg("v").collect(),
    );
  });
});
