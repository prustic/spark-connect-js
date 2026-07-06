import { describe, it, after } from "node:test";
import assert from "node:assert/strict";
import { StructType } from "@spark-connect-js/node";
import { spark, stopSession } from "./setup.js";

describe("DataFrame.schema()", () => {
  after(stopSession);

  it("returns a StructType with DDL type names for nested types", async () => {
    const df = spark().sql(`
      SELECT
        CAST(1 AS BIGINT) AS id,
        CAST(1.5 AS DECIMAL(10,2)) AS amount,
        array('a', 'b') AS tags,
        map('k', CAST(1 AS INT)) AS counts,
        named_struct('x', 1, 'ys', array(CAST(1.0 AS DOUBLE))) AS point,
        TIMESTAMP_NTZ '2026-01-01 00:00:00' AS ts_ntz,
        INTERVAL '1' HOUR AS dur
    `);

    const schema = await df.schema();

    assert.ok(schema instanceof StructType);
    const byName = Object.fromEntries(schema.fields.map((f) => [f.name, f.dataType]));
    assert.equal(byName["id"], "bigint");
    assert.equal(byName["amount"], "decimal(10,2)");
    assert.equal(byName["tags"], "array<string>");
    assert.equal(byName["counts"], "map<string,int>");
    assert.equal(byName["point"], "struct<x:int,ys:array<double>>");
    assert.equal(byName["ts_ntz"], "timestamp_ntz");
    assert.equal(byName["dur"], "interval hour");
  });

  it("round-trips a schema through createDataFrame, nullability included", async () => {
    const source = await spark()
      .sql(
        `
        SELECT * FROM VALUES
          (CAST(1 AS BIGINT), 'a'),
          (CAST(NULL AS BIGINT), NULL)
        AS t(id, name)
      `,
      )
      .schema();

    const df = spark().createDataFrame([{ id: 1n, name: "a" }], source);
    const roundTripped = await df.schema();

    assert.deepStrictEqual(
      roundTripped.fields.map((f) => [f.name, f.dataType, f.nullable]),
      source.fields.map((f) => [f.name, f.dataType, f.nullable]),
    );
  });

  it("accepts a plain DDL string schema on createDataFrame", async () => {
    const df = spark().createDataFrame([{ id: 1n, name: "a" }], "id BIGINT, name STRING");
    const schema = await df.schema();
    assert.deepStrictEqual(
      schema.fields.map((f) => [f.name, f.dataType, f.nullable]),
      [
        ["id", "bigint", true],
        ["name", "string", true],
      ],
    );
  });

  it("round-trips a NOT NULL schema through createDataFrame", async () => {
    // range() yields a non-nullable id, the case the encoder used to lose.
    const source = await spark().range(1).schema();
    assert.equal(source.fields[0].nullable, false);

    const df = spark().createDataFrame([{ id: 1n }, { id: 2n }], source);
    const roundTripped = await df.schema();

    assert.deepStrictEqual(
      roundTripped.fields.map((f) => [f.name, f.dataType, f.nullable]),
      source.fields.map((f) => [f.name, f.dataType, f.nullable]),
    );
    assert.equal(await df.count(), 2n);
  });

  it("columns() and dtypes() agree with schema()", async () => {
    const df = spark().sql("SELECT 1 AS a, 'x' AS b");
    const [schema, columns, dtypes] = await Promise.all([df.schema(), df.columns(), df.dtypes()]);

    assert.deepStrictEqual(columns, schema.fieldNames);
    assert.deepStrictEqual(
      dtypes,
      schema.fields.map((f) => [f.name, f.dataType]),
    );
  });
});
