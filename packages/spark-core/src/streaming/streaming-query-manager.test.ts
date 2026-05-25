import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { SparkSession } from "../spark-session.js";
import type { Transport } from "../spark-session.js";
import { InvalidInputError, SparkClientError } from "../errors.js";
import { StreamingQueryManager } from "./streaming-query-manager.js";
import { StreamingQuery } from "./streaming-query.js";

interface CapturingTransport extends Transport {
  commands: Record<string, unknown>[];
  setResponses(responses: Record<string, unknown>[]): void;
}

function capturingTransport(): CapturingTransport {
  let pending: Record<string, unknown>[] = [];
  const commands: Record<string, unknown>[] = [];
  return {
    commands,
    setResponses(r) {
      pending = r;
    },
    async *executePlan(): AsyncIterable<Uint8Array> {
      // no-op
    },
    async executeCommandResponses(_session, command) {
      commands.push(command);
      return pending;
    },
  };
}

function makeManager(transport: CapturingTransport): StreamingQueryManager {
  const spark = SparkSession.builder()
    .remote("sc://localhost:15002")
    .transport(transport)
    .getOrCreate();
  return new StreamingQueryManager(spark);
}

describe("StreamingQueryManager", () => {
  it("spark.streams returns a StreamingQueryManager", () => {
    const t = capturingTransport();
    const spark = SparkSession.builder().remote("sc://localhost:15002").transport(t).getOrCreate();
    assert.ok(spark.streams instanceof StreamingQueryManager);
  });

  it("active() issues op=active and reconstructs StreamingQuery handles", async () => {
    const t = capturingTransport();
    t.setResponses([
      {
        type: "streamingQueryManagerCommandResult",
        resultType: "active",
        activeQueries: [
          { id: "id-a", runId: "run-a", name: "q1" },
          { id: "id-b", runId: "run-b", name: "" },
        ],
      },
    ]);
    const mgr = makeManager(t);
    const queries = await mgr.active();
    assert.equal(queries.length, 2);
    assert.ok(queries[0] instanceof StreamingQuery);
    assert.equal(queries[0].id, "id-a");
    assert.equal(queries[0].name, "q1");
    assert.equal(queries[1].name, undefined); // empty-string normalized
    assert.equal(t.commands[0]["type"], "streamingQueryManagerCommand");
    assert.equal(t.commands[0]["op"], "active");
  });

  it("active() returns [] when the server reports no queries", async () => {
    const t = capturingTransport();
    t.setResponses([
      {
        type: "streamingQueryManagerCommandResult",
        resultType: "active",
        activeQueries: [],
      },
    ]);
    const mgr = makeManager(t);
    assert.deepEqual(await mgr.active(), []);
  });

  it("get(id) forwards the id and returns a StreamingQuery", async () => {
    const t = capturingTransport();
    t.setResponses([
      {
        type: "streamingQueryManagerCommandResult",
        resultType: "query",
        query: { id: "id-a", runId: "run-a", name: "q1" },
      },
    ]);
    const mgr = makeManager(t);
    const q = await mgr.get("id-a");
    assert.ok(q instanceof StreamingQuery);
    assert.equal(q.id, "id-a");
    assert.equal(q.name, "q1");
    assert.equal(t.commands[0]["op"], "getQuery");
    assert.equal(t.commands[0]["id"], "id-a");
  });

  it("get(id) returns null when the server has no such query", async () => {
    const t = capturingTransport();
    t.setResponses([{ type: "streamingQueryManagerCommandResult", resultType: "query" }]);
    const mgr = makeManager(t);
    assert.equal(await mgr.get("missing"), null);
  });

  it("get() rejects an empty id", async () => {
    const t = capturingTransport();
    const mgr = makeManager(t);
    await assert.rejects(() => mgr.get(""), InvalidInputError);
    assert.equal(t.commands.length, 0);
  });

  it("awaitAnyTermination() without timeout returns undefined", async () => {
    const t = capturingTransport();
    t.setResponses([{ type: "streamingQueryManagerCommandResult" }]);
    const mgr = makeManager(t);
    assert.equal(await mgr.awaitAnyTermination(), undefined);
    assert.equal(t.commands[0]["op"], "awaitAnyTermination");
    assert.equal(t.commands[0]["timeoutMs"], undefined);
  });

  it("awaitAnyTermination(timeoutMs) forwards the timeout and returns terminated", async () => {
    const t = capturingTransport();
    t.setResponses([
      {
        type: "streamingQueryManagerCommandResult",
        resultType: "awaitAnyTermination",
        terminated: true,
      },
    ]);
    const mgr = makeManager(t);
    assert.equal(await mgr.awaitAnyTermination(5000), true);
    assert.equal(t.commands[0]["timeoutMs"], 5000);
  });

  it("awaitAnyTermination() rejects negative, non-finite, or non-integer timeouts", async () => {
    const t = capturingTransport();
    const mgr = makeManager(t);
    await assert.rejects(() => mgr.awaitAnyTermination(-1), InvalidInputError);
    await assert.rejects(() => mgr.awaitAnyTermination(Number.NaN), InvalidInputError);
    await assert.rejects(
      () => mgr.awaitAnyTermination(Number.POSITIVE_INFINITY),
      InvalidInputError,
    );
    await assert.rejects(() => mgr.awaitAnyTermination(1000.5), InvalidInputError);
  });

  it("resetTerminated() issues the command and resolves", async () => {
    const t = capturingTransport();
    t.setResponses([{ type: "streamingQueryManagerCommandResult" }]);
    const mgr = makeManager(t);
    await mgr.resetTerminated();
    assert.equal(t.commands[0]["op"], "resetTerminated");
  });

  it("throws SparkClientError when the server returns no manager-result", async () => {
    const t = capturingTransport();
    t.setResponses([]);
    const mgr = makeManager(t);
    await assert.rejects(() => mgr.active(), SparkClientError);
  });

  it("addListener rejects null / non-object input", async () => {
    const t = capturingTransport();
    const mgr = makeManager(t);
    // Cast through unknown to bypass the TS interface and exercise the runtime guard.
    await assert.rejects(
      () => mgr.addListener(null as unknown as Parameters<typeof mgr.addListener>[0]),
      InvalidInputError,
    );
    await assert.rejects(
      () => mgr.addListener(undefined as unknown as Parameters<typeof mgr.addListener>[0]),
      InvalidInputError,
    );
    await assert.rejects(
      () => mgr.addListener("nope" as unknown as Parameters<typeof mgr.addListener>[0]),
      InvalidInputError,
    );
  });
});
