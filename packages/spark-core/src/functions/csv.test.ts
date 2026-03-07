import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { from_csv, to_csv, schema_of_csv } from "./csv.js";

import type { Column } from "../column.js";

function assertFn(c: Column, name: string, argCount?: number) {
  assert.equal(c._expr.type, "unresolvedFunction");
  if (c._expr.type === "unresolvedFunction") {
    assert.equal(c._expr.name, name);
    if (argCount !== undefined) assert.equal(c._expr.arguments.length, argCount);
  }
}

describe("csv functions", () => {
  it("from_csv takes col and schema", () => {
    assertFn(from_csv("data", "name STRING, age INT"), "from_csv", 2);
  });

  it("to_csv wraps a column", () => {
    assertFn(to_csv("data"), "to_csv", 1);
  });

  it("schema_of_csv takes a CSV string", () => {
    assertFn(schema_of_csv("name,age"), "schema_of_csv", 1);
  });
});
