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

  it("toDDL() returns DDL-formatted schema string", () => {
    const st = new StructType().add("name", "string").add("age", "integer", false);
    assert.equal(st.toDDL(), "name string, age integer NOT NULL");
  });

  it("toDDL() returns empty string for empty schema", () => {
    assert.equal(new StructType().toDDL(), "");
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
    assert.equal(st.fields[0].dataType, "bigint");
    assert.equal(st.fields[0].nullable, false);
    assert.equal(st.fields[1].name, "name");
    assert.equal(st.fields[1].dataType, "string");
  });

  it("fromProto() handles empty/missing struct", () => {
    const st = StructType.fromProto({});
    assert.equal(st.length, 0);
  });

  it("fromProto() renders DDL names for renamed primitives", () => {
    const st = StructType.fromProto({
      struct: {
        fields: [
          { name: "a", dataType: { kind: { case: "integer" } } },
          { name: "b", dataType: { kind: { case: "byte" } } },
          { name: "c", dataType: { kind: { case: "short" } } },
          { name: "d", dataType: { kind: { case: "timestampNtz" } } },
          { name: "e", dataType: { kind: { case: "double" } } },
        ],
      },
    });
    assert.deepStrictEqual(
      st.fields.map((f) => f.dataType),
      ["int", "tinyint", "smallint", "timestamp_ntz", "double"],
    );
  });

  it("fromProto() renders decimal precision and scale", () => {
    const st = StructType.fromProto({
      struct: {
        fields: [
          {
            name: "amount",
            dataType: { kind: { case: "decimal", value: { precision: 18, scale: 2 } } },
          },
        ],
      },
    });
    assert.equal(st.fields[0].dataType, "decimal(18,2)");
  });

  it("fromProto() recurses into array, map, and struct", () => {
    const st = StructType.fromProto({
      struct: {
        fields: [
          {
            name: "tags",
            dataType: {
              kind: { case: "array", value: { elementType: { kind: { case: "string" } } } },
            },
          },
          {
            name: "counts",
            dataType: {
              kind: {
                case: "map",
                value: {
                  keyType: { kind: { case: "string" } },
                  valueType: { kind: { case: "long" } },
                },
              },
            },
          },
          {
            name: "point",
            dataType: {
              kind: {
                case: "struct",
                value: {
                  fields: [
                    { name: "x", dataType: { kind: { case: "integer" } } },
                    {
                      name: "ys",
                      dataType: {
                        kind: {
                          case: "array",
                          value: { elementType: { kind: { case: "double" } } },
                        },
                      },
                    },
                  ],
                },
              },
            },
          },
        ],
      },
    });
    assert.deepStrictEqual(
      st.fields.map((f) => f.dataType),
      ["array<string>", "map<string,bigint>", "struct<x:int,ys:array<double>>"],
    );
  });

  it("fromProto() parses JSON-string field metadata", () => {
    const st = StructType.fromProto({
      struct: {
        fields: [
          {
            name: "id",
            dataType: { kind: { case: "long" } },
            metadata: '{"comment":"primary key"}',
          },
        ],
      },
    });
    assert.deepStrictEqual(st.fields[0].metadata, { comment: "primary key" });
  });

  it("fromProto() renders interval types from their start/end fields", () => {
    const st = StructType.fromProto({
      struct: {
        fields: [
          {
            name: "a",
            dataType: {
              kind: { case: "dayTimeInterval", value: { startField: 1, endField: 1 } },
            },
          },
          {
            name: "b",
            dataType: {
              kind: { case: "dayTimeInterval", value: { startField: 0, endField: 2 } },
            },
          },
          { name: "c", dataType: { kind: { case: "dayTimeInterval", value: {} } } },
          {
            name: "d",
            dataType: {
              kind: { case: "yearMonthInterval", value: { startField: 0, endField: 0 } },
            },
          },
          { name: "e", dataType: { kind: { case: "yearMonthInterval", value: {} } } },
        ],
      },
    });
    assert.deepStrictEqual(
      st.fields.map((f) => f.dataType),
      [
        "interval hour",
        "interval day to minute",
        "interval day to second",
        "interval year",
        "interval year to month",
      ],
    );
  });

  it("fromProto() renders a udt as its sql type", () => {
    const st = StructType.fromProto({
      struct: {
        fields: [
          {
            name: "vec",
            dataType: {
              kind: {
                case: "udt",
                value: {
                  sqlType: {
                    kind: {
                      case: "array",
                      value: { elementType: { kind: { case: "double" } } },
                    },
                  },
                },
              },
            },
          },
        ],
      },
    });
    assert.equal(st.fields[0].dataType, "array<double>");
  });

  it("fromProto() renders an unset oneof as unknown", () => {
    const st = StructType.fromProto({
      struct: {
        fields: [{ name: "x", dataType: { kind: { case: undefined } } }],
      },
    });
    assert.equal(st.fields[0].dataType, "unknown");
  });

  it("fromProto() falls back to the unparsed DDL string", () => {
    const st = StructType.fromProto({
      struct: {
        fields: [
          {
            name: "x",
            dataType: { kind: { case: "unparsed", value: { dataTypeString: "geometry" } } },
          },
        ],
      },
    });
    assert.equal(st.fields[0].dataType, "geometry");
  });

  it("constructor defensively copies the fields array", () => {
    const fields = [new StructField("a", "string")];
    const st = new StructType(fields);
    fields.push(new StructField("b", "int")); // mutate original
    assert.equal(st.length, 1); // StructType unaffected
    assert.equal(st.fields[0].name, "a");
  });

  it("StructField defensively copies metadata", () => {
    const meta = { key: "val" };
    const sf = new StructField("x", "string", true, meta);
    meta.key = "changed"; // mutate original
    assert.equal(sf.metadata.key, "val"); // unaffected
  });
});
