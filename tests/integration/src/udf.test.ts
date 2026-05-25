import { describe, it, after } from "node:test";
import assert from "node:assert/strict";
import { spark, stopSession } from "./setup.js";

describe("UDFRegistration", () => {
  after(stopSession);

  // Deploying a real UDF JAR is out of scope for this suite; a missing class
  // confirms that the registerFunction command was parsed by the server.
  it("registerJavaFunction() reaches the server for a missing class", async () => {
    await assert.rejects(
      spark().udf.registerJavaFunction(
        "spark_js_missing_udf",
        "com.example.spark_js.NoSuchUDF",
        "STRING",
      ),
      /NoSuchUDF|ClassNotFoundException|class not found/i,
    );
  });

  it("registerJavaUDAF() reaches the server for a missing class", async () => {
    await assert.rejects(
      spark().udf.registerJavaUDAF("spark_js_missing_udaf", "com.example.spark_js.NoSuchUDAF"),
      /NoSuchUDAF|ClassNotFoundException|class not found/i,
    );
  });
});
