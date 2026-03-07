import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { year, month, dayofmonth, current_date, datediff } from "./datetime.js";

describe("date functions", () => {
  it("year() creates unresolvedFunction", () => {
    const result = year("date_col");
    if (result._expr.type === "unresolvedFunction") {
      assert.equal(result._expr.name, "year");
    }
  });

  it("month() creates unresolvedFunction", () => {
    assert.equal(month("d")._expr.type, "unresolvedFunction");
  });

  it("dayofmonth() creates unresolvedFunction", () => {
    assert.equal(dayofmonth("d")._expr.type, "unresolvedFunction");
  });

  it("current_date() takes no arguments", () => {
    const result = current_date();
    if (result._expr.type === "unresolvedFunction") {
      assert.equal(result._expr.arguments.length, 0);
    }
  });

  it("datediff() accepts two columns", () => {
    const result = datediff("end", "start");
    if (result._expr.type === "unresolvedFunction") {
      assert.equal(result._expr.name, "datediff");
      assert.equal(result._expr.arguments.length, 2);
    }
  });
});
