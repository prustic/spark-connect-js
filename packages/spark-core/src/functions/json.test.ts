import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  from_json, to_json, get_json_object, json_tuple,
  schema_of_json, json_array_length, json_object_keys,
} from "./json.js";

import type { Column } from "../column.js";

function assertFn(c: Column, name: string, argCount?: number) {
  assert.equal(c._expr.type, "unresolvedFunction");
  if (c._expr.type === "unresolvedFunction") {
    assert.equal(c._expr.name, name);
    if (argCount !== undefined) assert.equal(c._expr.arguments.length, argCount);
  }
}

describe("json functions", () => {
  it("from_json takes col and schema string", () => {
    const r = from_json("data", "struct<name:string>");
    assertFn(r, "from_json", 2);
  });

  it("to_json wraps a column", () => {
    assertFn(to_json("data"), "to_json", 1);
  });

  it("get_json_object takes col and path", () => {
    assertFn(get_json_object("json_col", "$.name"), "get_json_object", 2);
  });

  it("json_tuple takes col and fields", () => {
    const r = json_tuple("json_col", "name", "age");
    assertFn(r, "json_tuple", 3); // col + 2 field literals
  });

  it("schema_of_json takes a JSON string", () => {
    assertFn(schema_of_json('{"name":"spark"}'), "schema_of_json", 1);
  });

  it("json_array_length/json_object_keys", () => {
    assertFn(json_array_length("x"), "json_array_length", 1);
    assertFn(json_object_keys("x"), "json_object_keys", 1);
  });
});
