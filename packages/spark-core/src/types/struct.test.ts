import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { StructType, StructField } from "./struct.js";

describe("StructField", () => {
  it("creates with defaults", () => {
    const f = new StructField("name", "string");
    assert.equal(f.name, "name");
    assert.equal(f.dataType, "string");
    assert.equal(f.nullable, true);
    assert.deepStrictEqual(f.metadata, {});
  });

  it("creates with explicit nullable=false", () => {
    const f = new StructField("id", "integer", false);
    assert.equal(f.nullable, false);
  });

  it("toString returns readable format", () => {
    const f = new StructField("age", "integer", false);
    assert.equal(f.toString(), "StructField(age, integer, false)");
  });
});

describe("StructType", () => {
  it("creates empty by default", () => {
    const st = new StructType();
    assert.equal(st.length, 0);
    assert.deepStrictEqual(st.fieldNames, []);
  });

  it("add() returns a new StructType with the field appended", () => {
    const st = new StructType().add("name", "string").add("age", "integer", false);
    assert.equal(st.length, 2);
    assert.deepStrictEqual(st.fieldNames, ["name", "age"]);
  });

  it("getField() looks up by name", () => {
    const st = new StructType().add("name", "string").add("age", "integer");
    const field = st.getField("age");
    assert.ok(field);
    assert.equal(field.dataType, "integer");
  });

  it("getField() returns undefined for missing", () => {
    const st = new StructType().add("name", "string");
    assert.equal(st.getField("missing"), undefined);
  });

  it("treeString() formats as root tree", () => {
    const st = new StructType().add("name", "string").add("age", "integer", false);
    const expected =
      "root\n |-- name: string (nullable = true)\n |-- age: integer (nullable = false)";
    assert.equal(st.treeString(), expected);
  });

  it("fromProto() parses struct fields from proto response", () => {
    const proto = {
      struct: {
        fields: [
          { name: "id", dataType: { kind: { case: "long" } }, nullable: false },
          { name: "name", dataType: { kind: { case: "string" } }, nullable: true },
        ],
      },
    };
    const st = StructType.fromProto(proto);
    assert.equal(st.length, 2);
    assert.equal(st.fields[0].name, "id");
    assert.equal(st.fields[0].dataType, "long");
    assert.equal(st.fields[0].nullable, false);
    assert.equal(st.fields[1].name, "name");
    assert.equal(st.fields[1].dataType, "string");
  });

  it("fromProto() handles empty/missing struct", () => {
    const st = StructType.fromProto({});
    assert.equal(st.length, 0);
  });
});
