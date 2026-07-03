import { describe, it, after } from "node:test";
import assert from "node:assert/strict";
import { Trigger } from "@spark-connect-js/node";
import type {
  StreamingQuery,
  StreamingQueryListener,
  StreamingQueryProgress,
  QueryStartedEvent,
  QueryTerminatedEvent,
} from "@spark-connect-js/node";
import { spark, stopSession } from "./setup.js";

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

async function waitUntil(
  predicate: () => Promise<boolean>,
  timeoutMs: number,
  intervalMs = 200,
): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return true;
    await new Promise((r) => setTimeout(r, intervalMs));
  }
  return false;
}

function startRateMemoryQuery(name: string): Promise<StreamingQuery> {
  return spark()
    .readStream.format("rate")
    .option("rowsPerSecond", "10")
    .option("numPartitions", "1")
    .load()
    .writeStream.format("memory")
    .queryName(name)
    .outputMode("append")
    .trigger(Trigger.processingTime("300 milliseconds"))
    .start();
}

describe("StreamingQueryManager (spark.streams)", () => {
  after(stopSession);

  it("active() returns the running query, then [] after stop", async () => {
    const name = `mgr_active_${Math.random().toString(36).slice(2, 10)}`;
    const query = await startRateMemoryQuery(name);
    try {
      const alive = await spark().streams.active();
      assert.ok(
        alive.some((q) => q.id === query.id),
        `expected active() to include the query we just started (id=${query.id})`,
      );
    } finally {
      await query.stop();
    }
    const ok = await waitUntil(
      async () => !(await spark().streams.active()).some((q) => q.id === query.id),
      5_000,
    );
    assert.ok(ok, "expected the stopped query to drop out of active() within 5s");
  });

  it("get(id) round-trips the same query handle; returns null for unknown id", async () => {
    const name = `mgr_get_${Math.random().toString(36).slice(2, 10)}`;
    const query = await startRateMemoryQuery(name);
    try {
      const fetched = await spark().streams.get(query.id);
      assert.ok(fetched, "expected get(id) to return a query handle");
      assert.equal(fetched.id, query.id);
      assert.match(fetched.runId, UUID_RE);

      const miss = await spark().streams.get("00000000-0000-0000-0000-000000000000");
      assert.equal(miss, null);
    } finally {
      await query.stop();
    }
  });

  it("awaitAnyTermination(timeoutMs) returns false while running, true after stop", async () => {
    // The flag is sticky until reset; previous tests' stops would poison this.
    await spark().streams.resetTerminated();
    const name = `mgr_await_${Math.random().toString(36).slice(2, 10)}`;
    const query = await startRateMemoryQuery(name);
    try {
      assert.equal(await spark().streams.awaitAnyTermination(200), false);
    } finally {
      await query.stop();
    }
    assert.equal(await spark().streams.awaitAnyTermination(2_000), true);
    await spark().streams.resetTerminated();
  });
});

describe("StreamingQueryListener (spark.streams.addListener)", () => {
  after(stopSession);

  it("fires onQueryStarted / onQueryProgress / onQueryTerminated across a query lifecycle", async () => {
    const started: QueryStartedEvent[] = [];
    const progress: StreamingQueryProgress[] = [];
    const terminated: QueryTerminatedEvent[] = [];
    const listener: StreamingQueryListener = {
      onQueryStarted: (e) => {
        started.push(e);
      },
      onQueryProgress: (e) => {
        progress.push(e);
      },
      onQueryTerminated: (e) => {
        terminated.push(e);
      },
    };

    // Register a second listener whose onQueryProgress throws and assert the
    // first listener still receives every event (exception isolation).
    const goodCount = { n: 0 };
    const isolatedListener: StreamingQueryListener = {
      onQueryProgress: () => {
        goodCount.n += 1;
      },
    };
    const throwingListener: StreamingQueryListener = {
      onQueryProgress: () => {
        throw new Error("intentional");
      },
    };

    await spark().streams.addListener(listener);
    await spark().streams.addListener(isolatedListener);
    await spark().streams.addListener(throwingListener);

    let query: StreamingQuery | undefined;
    try {
      query = await startRateMemoryQuery(`mgr_listener_${Math.random().toString(36).slice(2, 10)}`);
      const sawProgress = await waitUntil(async () => progress.length >= 1, 10_000);
      assert.ok(sawProgress, "expected onQueryProgress to fire within 10s");
      // Regression: progress event is unwrapped, batchId is top-level.
      assert.equal(typeof progress[0].batchId, "number");
      assert.ok(goodCount.n >= 1, "isolated listener should have received events too");
      assert.equal(started.length, 1);
      assert.match(started[0].runId, UUID_RE);
    } finally {
      if (query !== undefined) await query.stop();
    }

    const sawTerminated = await waitUntil(async () => terminated.length >= 1, 5_000);
    assert.ok(sawTerminated, "expected onQueryTerminated to fire after stop()");

    await spark().streams.removeListener(listener);
    await spark().streams.removeListener(isolatedListener);
    await spark().streams.removeListener(throwingListener);
  });
});
