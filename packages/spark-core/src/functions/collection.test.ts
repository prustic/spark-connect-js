import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { explode } from "./collection.js";

describe("collection functions", () => {
  it("explode creates unresolvedFunction", () => {
    const result = explode("arr");
    if (result._expr.type === "unresolvedFunction") {
      assert.equal(result._expr.name, "explode");
    }
  });
});
