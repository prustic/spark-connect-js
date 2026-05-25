import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { UDFRegistration } from "./udf-registration.js";
import type { SparkSession } from "./spark-session.js";

function makeRegistration(): { udf: UDFRegistration; calls: Record<string, unknown>[] } {
  const calls: Record<string, unknown>[] = [];
  const session = {
    _executeCommand: async (cmd: Record<string, unknown>) => {
      calls.push(cmd);
    },
  } as unknown as SparkSession;
  return { udf: new UDFRegistration(session), calls };
}

describe("UDFRegistration.registerJavaFunction", () => {
  it("sends a registerFunction command with class name and return type", async () => {
    const { udf, calls } = makeRegistration();
    await udf.registerJavaFunction("my_upper", "com.example.UpperUDF", "STRING");
    assert.equal(calls.length, 1);
    assert.deepStrictEqual(calls[0], {
      type: "registerFunction",
      functionName: "my_upper",
      className: "com.example.UpperUDF",
      aggregate: false,
      returnType: "STRING",
    });
  });

  it("omits returnType when not provided", async () => {
    const { udf, calls } = makeRegistration();
    await udf.registerJavaFunction("my_fn", "com.example.UDF");
    assert.equal(calls.length, 1);
    assert.deepStrictEqual(calls[0], {
      type: "registerFunction",
      functionName: "my_fn",
      className: "com.example.UDF",
      aggregate: false,
    });
  });
});

describe("UDFRegistration.registerJavaUDAF", () => {
  it("sends a registerFunction command with aggregate=true", async () => {
    const { udf, calls } = makeRegistration();
    await udf.registerJavaUDAF("my_agg", "com.example.MyAggregate");
    assert.equal(calls.length, 1);
    assert.deepStrictEqual(calls[0], {
      type: "registerFunction",
      functionName: "my_agg",
      className: "com.example.MyAggregate",
      aggregate: true,
    });
  });
});
