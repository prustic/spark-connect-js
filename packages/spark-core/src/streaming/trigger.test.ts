import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { InvalidInputError } from "../errors.js";
import { Trigger } from "./trigger.js";

describe("Trigger", () => {
  it("processingTime() returns the kind+interval shape", () => {
    assert.deepEqual(Trigger.processingTime("10 seconds"), {
      kind: "processingTime",
      interval: "10 seconds",
    });
  });

  it("processingTime() rejects empty interval", () => {
    assert.throws(() => Trigger.processingTime(""), InvalidInputError);
  });

  it("processingTime() rejects whitespace-only interval", () => {
    assert.throws(() => Trigger.processingTime("   "), InvalidInputError);
  });

  it("continuous() returns the kind+interval shape", () => {
    assert.deepEqual(Trigger.continuous("1 second"), {
      kind: "continuous",
      interval: "1 second",
    });
  });

  it("continuous() rejects empty interval", () => {
    assert.throws(() => Trigger.continuous(""), InvalidInputError);
  });

  it("availableNow() and once() take no arguments", () => {
    assert.deepEqual(Trigger.availableNow(), { kind: "availableNow" });
    assert.deepEqual(Trigger.once(), { kind: "once" });
  });
});
