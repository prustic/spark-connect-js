import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { SparkSession } from "./spark-session.js";
import { InvalidInputError } from "./errors.js";
import { StructType } from "./types/struct.js";
import type { Row } from "./types/row.js";

function makeSession(analyzeResponse: Record<string, unknown>): {
  spark: SparkSession;
  calls: Record<string, unknown>[];
} {
  const calls: Record<string, unknown>[] = [];
  const transport = {
    executePlan: () => {
      throw new Error("not used in this test");
    },
    analyzePlan: async (_sessionId: string, request: Record<string, unknown>) => {
      calls.push(request);
      return analyzeResponse;
    },
  };
  const spark = SparkSession.builder().remote("sc://stub").transport(transport).getOrCreate();
  return { spark, calls };
}

describe("SparkSession.version", () => {
  it("issues an AnalyzePlan(sparkVersion) and returns the response version", async () => {
    const { spark, calls } = makeSession({ type: "sparkVersion", version: "4.0.0" });
    assert.equal(await spark.version(), "4.0.0");
    assert.deepStrictEqual(calls, [{ type: "sparkVersion" }]);
  });
});

describe("SparkSession tags", () => {
  it("addTag stores; getTags reflects insertion order; clearTags removes all", () => {
    const { spark } = makeSession({});
    spark.addTag("etl");
    spark.addTag("daily");
    assert.deepStrictEqual(spark.getTags(), ["etl", "daily"]);
    spark.clearTags();
    assert.deepStrictEqual(spark.getTags(), []);
  });

  it("removeTag removes only that tag", () => {
    const { spark } = makeSession({});
    spark.addTag("a");
    spark.addTag("b");
    spark.removeTag("a");
    assert.deepStrictEqual(spark.getTags(), ["b"]);
  });

  it("addTag rejects empty tags", () => {
    const { spark } = makeSession({});
    assert.throws(() => spark.addTag(""));
  });

  it("addTag rejects tags containing comma", () => {
    const { spark } = makeSession({});
    assert.throws(() => spark.addTag("a,b"));
  });

  it("removeTag is a no-op for tags that aren't set", () => {
    const { spark } = makeSession({});
    spark.removeTag("nope");
    assert.deepStrictEqual(spark.getTags(), []);
  });
});

describe("SparkSession.interrupt*", () => {
  function makeWithInterrupt(): {
    spark: import("./spark-session.js").SparkSession;
    calls: Record<string, unknown>[];
  } {
    const calls: Record<string, unknown>[] = [];
    const transport = {
      executePlan: () => {
        throw new Error("not used");
      },
      interrupt: async (_sessionId: string, request: Record<string, unknown>) => {
        calls.push(request);
        return ["op-1", "op-2"];
      },
    };
    const spark = SparkSession.builder().remote("sc://stub").transport(transport).getOrCreate();
    return { spark, calls };
  }

  it("interruptAll sends { type: 'all' } and returns interrupted IDs", async () => {
    const { spark, calls } = makeWithInterrupt();
    const ids = await spark.interruptAll();
    assert.deepStrictEqual(ids, ["op-1", "op-2"]);
    assert.deepStrictEqual(calls, [{ type: "all" }]);
  });

  it("interruptTag sends { type: 'tag', tag }", async () => {
    const { spark, calls } = makeWithInterrupt();
    await spark.interruptTag("etl");
    assert.deepStrictEqual(calls, [{ type: "tag", tag: "etl" }]);
  });

  it("interruptTag rejects empty tags", async () => {
    const { spark } = makeWithInterrupt();
    await assert.rejects(spark.interruptTag(""));
  });

  it("interruptTag rejects tags containing comma", async () => {
    const { spark } = makeWithInterrupt();
    await assert.rejects(spark.interruptTag("a,b"));
  });

  it("interruptOperation sends { type: 'operationId', operationId }", async () => {
    const { spark, calls } = makeWithInterrupt();
    await spark.interruptOperation("op-xyz");
    assert.deepStrictEqual(calls, [{ type: "operationId", operationId: "op-xyz" }]);
  });
});

describe("SparkSession unsupported-transport paths", () => {
  function bareTransport(): { spark: SparkSession } {
    const spark = SparkSession.builder()
      .remote("sc://stub")
      .transport({
        executePlan: () => {
          throw new Error("not used");
        },
      })
      .getOrCreate();
    return { spark };
  }

  it("interruptAll throws UnsupportedOperationError when transport lacks interrupt", async () => {
    const { spark } = bareTransport();
    await assert.rejects(spark.interruptAll(), /does not support interrupt/);
  });

  it("conf.get throws UnsupportedOperationError when transport lacks config", async () => {
    const { spark } = bareTransport();
    await assert.rejects(spark.conf.get("any.key"), /does not support config/);
  });

  it("interruptOperation rejects empty IDs", async () => {
    const { spark } = bareTransport();
    await assert.rejects(spark.interruptOperation(""));
  });
});

describe("SparkSessionBuilder.sessionId", () => {
  it("rejects non-UUID strings", () => {
    const builder = SparkSession.builder()
      .remote("sc://stub")
      .transport({
        executePlan: () => {
          throw new Error("not used");
        },
      });
    assert.throws(() => builder.sessionId("not-a-uuid"));
  });

  it("accepts a valid UUID", () => {
    const builder = SparkSession.builder()
      .remote("sc://stub")
      .transport({
        executePlan: () => {
          throw new Error("not used");
        },
      });
    builder.sessionId("550e8400-e29b-41d4-a716-446655440000");
  });
});

describe("SparkSession.createDataFrame input validation", () => {
  function newSession(): SparkSession {
    return SparkSession.builder()
      .remote("sc://stub")
      .transport({
        executePlan: () => {
          throw new Error("not used");
        },
      })
      .getOrCreate();
  }

  it("throws InvalidInputError on an empty Uint8Array", () => {
    assert.throws(() => newSession().createDataFrame(new Uint8Array()), InvalidInputError);
  });

  it("rejects Arrow file-format bytes with a message naming the fix", () => {
    // ARROW1\0\0 magic prefix, plus a byte to satisfy the length check.
    const fileMagic = new Uint8Array([0x41, 0x52, 0x52, 0x4f, 0x57, 0x31, 0x00, 0x00, 0x00]);
    assert.throws(
      () => newSession().createDataFrame(fileMagic),
      (err: unknown) => {
        if (!(err instanceof InvalidInputError)) return false;
        assert.match(err.message, /file-format/);
        assert.match(err.message, /streaming/);
        assert.match(err.message, /tableToIPC/);
        return true;
      },
    );
  });

  it("accepts non-file-format bytes without throwing", () => {
    // Streaming format starts with a continuation marker (0xFFFFFFFF), not
    // the ARROW1 magic. Passing something that merely isn't the file magic
    // should reach the plan builder (which does not execute here).
    const notFile = new Uint8Array([0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00, 0x00]);
    assert.doesNotThrow(() => newSession().createDataFrame(notFile));
  });
});

describe("SparkSession.createDataFrame schema forwarding", () => {
  function sessionWithEncoder() {
    const calls: { rows: Row[]; schema: StructType | undefined }[] = [];
    const spark = SparkSession.builder()
      .remote("sc://stub")
      .transport({
        executePlan: () => {
          throw new Error("not used");
        },
      })
      .arrowEncoder((rows, schema) => {
        calls.push({ rows, schema });
        return new Uint8Array([1]);
      })
      .getOrCreate();
    return { spark, calls };
  }

  it("passes a StructType through to the encoder and DDL to the plan", () => {
    const { spark, calls } = sessionWithEncoder();
    const schema = new StructType().add("id", "bigint", false);
    const df = spark.createDataFrame([{ id: 1n }], schema);
    assert.equal(calls[0].schema, schema);
    assert.equal((df._plan as { schema?: string }).schema, "id bigint NOT NULL");
  });

  it("passes a DDL string to the plan but not the encoder", () => {
    const { spark, calls } = sessionWithEncoder();
    const df = spark.createDataFrame([{ id: 1n }], "id BIGINT");
    assert.equal(calls[0].schema, undefined);
    assert.equal((df._plan as { schema?: string }).schema, "id BIGINT");
  });

  it("serializes a StructType to DDL on the Uint8Array overload", () => {
    const { spark } = sessionWithEncoder();
    const notFile = new Uint8Array([0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00, 0x00]);
    const schema = new StructType().add("id", "bigint", false);
    const df = spark.createDataFrame(notFile, schema);
    assert.equal((df._plan as { schema?: string }).schema, "id bigint NOT NULL");
  });

  it("rejects a schema whose field names do not match the row keys", () => {
    const { spark, calls } = sessionWithEncoder();
    const schema = new StructType().add("ID", "bigint", false);
    assert.throws(
      () => spark.createDataFrame([{ id: 1n }], schema),
      (err: unknown) => {
        if (!(err instanceof InvalidInputError)) return false;
        assert.match(err.message, /case-sensitive/);
        assert.match(err.message, /ID/);
        assert.match(err.message, /id/);
        return true;
      },
    );
    assert.equal(calls.length, 0);
  });

  it("rejects a later row whose keys do not match the schema", () => {
    const { spark, calls } = sessionWithEncoder();
    const schema = new StructType().add("id", "bigint", false).add("name", "string");
    assert.throws(
      () =>
        spark.createDataFrame(
          [
            { id: 1n, name: "a" },
            { id: 2n, nmae: "b" },
          ],
          schema,
        ),
      (err: unknown) => {
        if (!(err instanceof InvalidInputError)) return false;
        assert.match(err.message, /row 1/);
        assert.match(err.message, /nmae/);
        assert.match(err.message, /name/);
        return true;
      },
    );
    assert.equal(calls.length, 0);
  });

  it("rejects a null in a NOT NULL column before the encoder runs", () => {
    const { spark, calls } = sessionWithEncoder();
    const schema = new StructType().add("id", "bigint", false);
    assert.throws(
      () => spark.createDataFrame([{ id: 1n }, { id: null }], schema),
      (err: unknown) => {
        if (!(err instanceof InvalidInputError)) return false;
        assert.match(err.message, /column "id"/);
        assert.match(err.message, /NOT NULL/);
        assert.match(err.message, /row 1/);
        return true;
      },
    );
    assert.equal(calls.length, 0);
  });

  it("rejects an empty StructType instead of sending an empty DDL string", () => {
    const { spark } = sessionWithEncoder();
    assert.throws(() => spark.createDataFrame([{ id: 1n }], new StructType()), InvalidInputError);
  });

  it("rejects an empty DDL string like the readers do", () => {
    const { spark } = sessionWithEncoder();
    assert.throws(() => spark.createDataFrame([{ id: 1n }], "  "), InvalidInputError);
  });

  it("rejects a non-StructType schema object with a domain error", () => {
    const { spark } = sessionWithEncoder();
    const ducked = { toDDL: () => "id BIGINT" } as unknown as StructType;
    assert.throws(() => spark.createDataFrame([{ id: 1n }], ducked), InvalidInputError);
  });
});
