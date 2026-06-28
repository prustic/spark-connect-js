import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { SparkSession } from "../spark-session.js";
import type { Transport } from "../spark-session.js";
import { StreamingQueryListenerBase } from "./streaming-query-listener.js";
import type {
  StreamingQueryListener,
  QueryIdleEvent,
  QueryTerminatedEvent,
} from "./streaming-query-listener.js";
import type { StreamingQueryProgress } from "./types.js";

interface ScriptedFrame {
  events?: { eventType: string; eventJson: string }[];
  listenerBusListenerAdded?: boolean;
}

interface ScriptedTransport extends Transport {
  pushFrame(frame: ScriptedFrame): void;
  pushError(err: unknown): void;
  closeStream(): void;
  managerCommands: Record<string, unknown>[];
}

/**
 * Fake Transport that lets a test script the listener-bus subscription
 * stream frame-by-frame. `pushFrame` enqueues a frame the generator yields;
 * `closeStream` ends the iteration; `pushError` makes it throw next.
 */
function scriptedTransport(): ScriptedTransport {
  type Pending =
    | { kind: "frame"; value: Record<string, unknown> }
    | { kind: "error"; err: unknown }
    | { kind: "close" };

  const queue: Pending[] = [];
  const state: { wake: (() => void) | null } = { wake: null };
  const managerCommands: Record<string, unknown>[] = [];

  const wake = (): void => {
    const w = state.wake;
    state.wake = null;
    if (w !== null) w();
  };

  return {
    managerCommands,
    pushFrame(frame) {
      queue.push({
        kind: "frame",
        value: { type: "streamingQueryListenerEventsResult", ...frame },
      });
      wake();
    },
    pushError(err) {
      queue.push({ kind: "error", err });
      wake();
    },
    closeStream() {
      queue.push({ kind: "close" });
      wake();
    },
    async *executePlan(): AsyncIterable<Uint8Array> {
      // no-op
    },
    async executeCommandResponses(_session, command) {
      // Manager non-streaming calls (e.g. removeListenerBusListener) land here.
      managerCommands.push(command);
      return [];
    },
    async *executeCommandStream() {
      while (true) {
        if (queue.length === 0) {
          await new Promise<void>((r) => {
            state.wake = r;
          });
          continue;
        }

        const next = queue.shift()!;
        if (next.kind === "close") return;
        if (next.kind === "error") throw next.err;

        yield next.value;
      }
    },
  };
}

function newSession(transport: Transport): SparkSession {
  return SparkSession.builder().remote("sc://localhost:15002").transport(transport).getOrCreate();
}

describe("StreamingQueryListenerBus error surfacing", () => {
  it("rejects addListener if the transport lacks executeCommandStream (sync throw in _run)", async () => {
    const minimal: Transport = {
      async *executePlan(): AsyncIterable<Uint8Array> {
        // no-op; executeCommandStream is intentionally omitted.
      },
    };
    const spark = SparkSession.builder()
      .remote("sc://localhost:15002")
      .transport(minimal)
      .getOrCreate();
    await assert.rejects(
      () => spark.streams.addListener({ onQueryProgress: () => undefined }),
      /does not support executeCommandStream/,
    );
  });

  it("a second addListener after a sync-throw failure re-invokes the transport (not the stale _driver)", async () => {
    // Without the add()-side cleanup, `this._driver = this._run()` would
    // overwrite _run's catch-side null with the fulfilled Promise. The second
    // add() would see _driver !== null and re-throw the stale rejection
    // without calling the transport. Counting invocations pins that down.
    let invocations = 0;
    const errs = ["first", "second"];
    const transport: Transport = {
      async *executePlan(): AsyncIterable<Uint8Array> {
        // no-op
      },
      // Sync-throw on every invocation, with a different message each time.
      executeCommandStream() {
        const i = invocations++;
        throw new Error(errs[Math.min(i, errs.length - 1)]);
      },
    };
    const spark = SparkSession.builder()
      .remote("sc://localhost:15002")
      .transport(transport)
      .getOrCreate();
    await assert.rejects(
      () => spark.streams.addListener({ onQueryProgress: () => undefined }),
      /first/,
    );
    await assert.rejects(
      () => spark.streams.addListener({ onQueryProgress: () => undefined }),
      /second/,
    );
    assert.equal(invocations, 2, "transport should have been invoked twice");
  });
});

describe("StreamingQueryListenerBase", () => {
  it("implements StreamingQueryListener as an empty class", () => {
    class MyListener extends StreamingQueryListenerBase {
      progress: StreamingQueryProgress[] = [];
      onQueryProgress(e: StreamingQueryProgress) {
        this.progress.push(e);
      }
    }
    const l = new MyListener();
    // Just verifies the type/composition; no runtime behavior asserted here.
    assert.ok(l instanceof StreamingQueryListenerBase);
    assert.equal(typeof l.onQueryProgress, "function");
  });
});

describe("StreamingQueryListenerBus (via spark.streams.addListener)", () => {
  it("resolves addListener after the server's registration ack", async () => {
    const t = scriptedTransport();
    const spark = newSession(t);
    const collected: StreamingQueryProgress[] = [];
    const listener: StreamingQueryListener = {
      onQueryProgress: (e) => {
        collected.push(e);
      },
    };
    // Queue the ack so the addListener resolves.
    t.pushFrame({ listenerBusListenerAdded: true });
    await spark.streams.addListener(listener);
    t.closeStream();
    // Let the driver task drain.
    await new Promise((r) => setTimeout(r, 0));
    assert.equal(collected.length, 0);
  });

  it("fans out events to every registered listener, in registration order", async () => {
    const t = scriptedTransport();
    const spark = newSession(t);
    const a: string[] = [];
    const b: string[] = [];
    t.pushFrame({ listenerBusListenerAdded: true });
    await spark.streams.addListener({
      onQueryProgress: (e) => {
        a.push((e["batchId"] as number | undefined)?.toString() ?? "?");
      },
    });
    await spark.streams.addListener({
      onQueryProgress: (e) => {
        b.push((e["batchId"] as number | undefined)?.toString() ?? "?");
      },
    });
    t.pushFrame({
      events: [
        { eventType: "progress", eventJson: '{"progress":{"batchId":0}}' },
        { eventType: "progress", eventJson: '{"progress":{"batchId":1}}' },
      ],
    });
    // Yield the loop so the driver processes the frame.
    await new Promise((r) => setTimeout(r, 5));
    assert.deepEqual(a, ["0", "1"]);
    assert.deepEqual(b, ["0", "1"]);
    t.closeStream();
    await new Promise((r) => setTimeout(r, 0));
  });

  it("isolates a throwing listener so the other one still receives events", async () => {
    const t = scriptedTransport();
    const spark = newSession(t);
    const good: number[] = [];
    t.pushFrame({ listenerBusListenerAdded: true });
    await spark.streams.addListener({
      onQueryProgress: () => {
        throw new Error("boom");
      },
    });
    await spark.streams.addListener({
      onQueryProgress: (e) => {
        good.push(e["batchId"] as number);
      },
    });
    t.pushFrame({
      events: [
        { eventType: "progress", eventJson: '{"progress":{"batchId":0}}' },
        { eventType: "progress", eventJson: '{"progress":{"batchId":1}}' },
      ],
    });
    await new Promise((r) => setTimeout(r, 5));
    assert.deepEqual(good, [0, 1]);
    t.closeStream();
    await new Promise((r) => setTimeout(r, 0));
  });

  it("decodes idle and terminated events into their typed shapes", async () => {
    const t = scriptedTransport();
    const spark = newSession(t);
    const idle: QueryIdleEvent[] = [];
    const terminated: QueryTerminatedEvent[] = [];
    t.pushFrame({ listenerBusListenerAdded: true });
    await spark.streams.addListener({
      onQueryIdle: (e) => {
        idle.push(e);
      },
      onQueryTerminated: (e) => {
        terminated.push(e);
      },
    });
    t.pushFrame({
      events: [
        {
          eventType: "idle",
          eventJson: JSON.stringify({ id: "id-a", runId: "run-b", timestamp: "2026-01-01" }),
        },
        {
          eventType: "terminated",
          eventJson: JSON.stringify({
            id: "id-a",
            runId: "run-b",
            exception: "boom",
            errorClassOnException: "java.lang.RuntimeException",
          }),
        },
      ],
    });
    await new Promise((r) => setTimeout(r, 5));
    assert.deepEqual(idle, [{ id: "id-a", runId: "run-b", timestamp: "2026-01-01" }]);
    assert.deepEqual(terminated, [
      {
        id: "id-a",
        runId: "run-b",
        exception: "boom",
        errorClassOnException: "java.lang.RuntimeException",
      },
    ]);
    t.closeStream();
    await new Promise((r) => setTimeout(r, 0));
  });

  it("silently drops malformed event JSON", async () => {
    const t = scriptedTransport();
    const spark = newSession(t);
    const progress: StreamingQueryProgress[] = [];
    t.pushFrame({ listenerBusListenerAdded: true });
    await spark.streams.addListener({
      onQueryProgress: (e) => {
        progress.push(e);
      },
    });
    t.pushFrame({
      events: [
        { eventType: "progress", eventJson: "{not-valid-json" },
        { eventType: "progress", eventJson: '{"progress":{"batchId":1}}' },
      ],
    });
    await new Promise((r) => setTimeout(r, 5));
    assert.equal(progress.length, 1, "only the well-formed event should reach the listener");
    assert.equal((progress[0] as { batchId?: number }).batchId, 1);
    t.closeStream();
    await new Promise((r) => setTimeout(r, 0));
  });

  it("unwraps the server's `progress` wrapper so callbacks see a flat StreamingQueryProgress", async () => {
    // Server sends `{"progress": {"batchId": 42, "inputRowsPerSecond": 5}}`;
    // the wrapper must be peeled off (matches PySpark Connect's bus).
    const t = scriptedTransport();
    const spark = newSession(t);
    const seen: StreamingQueryProgress[] = [];
    t.pushFrame({ listenerBusListenerAdded: true });
    await spark.streams.addListener({
      onQueryProgress: (e) => {
        seen.push(e);
      },
    });
    t.pushFrame({
      events: [
        {
          eventType: "progress",
          eventJson: JSON.stringify({ progress: { batchId: 42, inputRowsPerSecond: 5 } }),
        },
      ],
    });
    await new Promise((r) => setTimeout(r, 5));
    assert.equal(seen.length, 1);
    assert.equal((seen[0] as { batchId?: number }).batchId, 42);
    assert.equal((seen[0] as { inputRowsPerSecond?: number }).inputRowsPerSecond, 5);
    assert.equal((seen[0] as Record<string, unknown>)["progress"], undefined);
    t.closeStream();
    await new Promise((r) => setTimeout(r, 0));
  });

  it("removeListener tears down the bus when the last listener is removed", async () => {
    const t = scriptedTransport();
    const spark = newSession(t);
    const listener: StreamingQueryListener = { onQueryProgress: () => undefined };
    t.pushFrame({ listenerBusListenerAdded: true });
    await spark.streams.addListener(listener);
    const removeP = spark.streams.removeListener(listener);
    t.closeStream();
    await removeP;
    const found = t.managerCommands.find((c) => c["op"] === "removeListenerBusListener");
    assert.ok(found, "expected removeListenerBusListener command to be sent");
  });

  it("self-removal from inside a callback does not deadlock", async () => {
    // Regression for the single-event-loop case where bus.remove() awaiting
    // the driver, which is awaiting the user callback, which is awaiting
    // bus.remove(), forms a cycle. The fix: remove() doesn't await the
    // driver; server-close drives teardown.
    const t = scriptedTransport();
    const spark = newSession(t);
    let progressCalled = false;
    const theListener: StreamingQueryListener = {
      onQueryProgress: async () => {
        progressCalled = true;
        await spark.streams.removeListener(theListener);
      },
    };
    t.pushFrame({ listenerBusListenerAdded: true });
    await spark.streams.addListener(theListener);
    t.pushFrame({ events: [{ eventType: "progress", eventJson: '{"progress":{"batchId":0}}' }] });
    t.closeStream();
    // If the deadlock is present this never resolves; node --test times out.
    await new Promise((r) => setTimeout(r, 30));
    assert.ok(progressCalled, "callback must have run");
    assert.ok(
      t.managerCommands.some((c) => c["op"] === "removeListenerBusListener"),
      "remove must have sent the bus-listener teardown command",
    );
  });

  it("clears listeners on non-recoverable stream error (PySpark drop policy)", async () => {
    const t = scriptedTransport();
    const spark = newSession(t);
    const listener: StreamingQueryListener = { onQueryProgress: () => undefined };
    t.pushFrame({ listenerBusListenerAdded: true });
    await spark.streams.addListener(listener);
    t.pushError(new Error("rpc dead"));
    // Yield to let the driver catch the error.
    await new Promise((r) => setTimeout(r, 5));
    // A fresh addListener should re-open the subscription, proving the bus
    // was torn down.
    t.pushFrame({ listenerBusListenerAdded: true });
    await spark.streams.addListener(listener);
    t.closeStream();
    await new Promise((r) => setTimeout(r, 0));
  });
});
