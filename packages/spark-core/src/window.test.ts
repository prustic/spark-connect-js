import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { WindowSpec, Window } from "./window.js";

describe("WindowSpec", () => {
  it("starts empty", () => {
    const spec = new WindowSpec();
    assert.equal(spec._partitionSpec.length, 0);
    assert.equal(spec._orderSpec.length, 0);
    assert.equal(spec._frameSpec, undefined);
  });

  it("partitionBy() adds partition expressions", () => {
    const spec = new WindowSpec().partitionBy("dept", "region");
    assert.equal(spec._partitionSpec.length, 2);
    assert.deepStrictEqual(spec._partitionSpec[0], {
      type: "unresolvedAttribute",
      name: "dept",
    });
  });

  it("orderBy() adds sort orders", () => {
    const spec = new WindowSpec().orderBy("salary");
    assert.equal(spec._orderSpec.length, 1);
    assert.equal(spec._orderSpec[0].direction, "ascending");
  });

  it("rowsBetween() sets row frame", () => {
    const spec = new WindowSpec().rowsBetween(-1, 1);
    assert.ok(spec._frameSpec);
    assert.equal(spec._frameSpec.frameType, "row");
  });

  it("rangeBetween() sets range frame", () => {
    const spec = new WindowSpec().rangeBetween(Window.unboundedPreceding, Window.currentRow);
    assert.ok(spec._frameSpec);
    assert.equal(spec._frameSpec.frameType, "range");
    assert.equal(spec._frameSpec.lower.type, "unbounded");
    assert.equal(spec._frameSpec.upper.type, "currentRow");
  });

  it("is immutable — each method returns a new instance", () => {
    const s1 = new WindowSpec();
    const s2 = s1.partitionBy("a");
    const s3 = s2.orderBy("b");
    assert.notEqual(s1, s2);
    assert.notEqual(s2, s3);
    assert.equal(s1._partitionSpec.length, 0);
    assert.equal(s2._orderSpec.length, 0);
  });
});

describe("Window static factory", () => {
  it("partitionBy() returns a WindowSpec", () => {
    const spec = Window.partitionBy("dept");
    assert.ok(spec instanceof WindowSpec);
    assert.equal(spec._partitionSpec.length, 1);
  });

  it("orderBy() returns a WindowSpec", () => {
    const spec = Window.orderBy("salary");
    assert.ok(spec instanceof WindowSpec);
    assert.equal(spec._orderSpec.length, 1);
  });

  it("rowsBetween() returns a WindowSpec with frame", () => {
    const spec = Window.rowsBetween(-2, 2);
    assert.ok(spec._frameSpec);
    assert.equal(spec._frameSpec.frameType, "row");
  });

  it("exposes boundary constants", () => {
    assert.equal(Window.unboundedPreceding, -2147483648);
    assert.equal(Window.unboundedFollowing, 2147483647);
    assert.equal(Window.currentRow, 0);
  });
});
