import type { SparkSession } from "../spark-session.js";
import type { StreamingQueryProgress } from "./types.js";

/** Fired when a streaming query is launched. */
export interface QueryStartedEvent {
  id: string;
  runId: string;
  name?: string;
  timestamp: string;
}

/** Fired when a streaming query is idle (no input data is currently available). */
export interface QueryIdleEvent {
  id: string;
  runId: string;
  timestamp: string;
}

/** Fired when a streaming query terminates, either cleanly or with an exception. */
export interface QueryTerminatedEvent {
  id: string;
  runId: string;
  exception?: string;
  errorClassOnException?: string;
}

/**
 * Callback interface for streaming-query lifecycle events. Implement any
 * subset of the callbacks; the bus skips the ones you don't define.
 *
 * @remarks
 * Callbacks dispatch serially per session, so a slow callback delays the
 * next event. `async` callbacks are awaited; queue inside the callback if
 * you need parallel work.
 *
 * @see [Spark source: StreamingQueryListener.scala](https://github.com/apache/spark/blob/master/sql/api/src/main/scala/org/apache/spark/sql/streaming/StreamingQueryListener.scala)
 */
export interface StreamingQueryListener {
  onQueryStarted?(event: QueryStartedEvent): void | Promise<void>;
  onQueryProgress?(event: StreamingQueryProgress): void | Promise<void>;
  onQueryIdle?(event: QueryIdleEvent): void | Promise<void>;
  onQueryTerminated?(event: QueryTerminatedEvent): void | Promise<void>;
}

/**
 * Empty base class implementing {@link StreamingQueryListener} for users who
 * prefer the `class MyListener extends ...` style over object literals.
 */
export class StreamingQueryListenerBase implements StreamingQueryListener {}

/**
 * Owns the session's listener-bus subscription, listener set, and dispatch.
 * Lazy-created on the first `addListener`, torn down on the last `removeListener`.
 *
 * @internal
 */
export class StreamingQueryListenerBus {
  private readonly _session: SparkSession;
  private readonly _listeners: StreamingQueryListener[] = [];
  private _driver: Promise<void> | null = null;
  /** Resolves once the server has acked `addListenerBusListener`. */
  private _registered: Promise<void> | null = null;
  /**
   * Promise chain that serializes every `_dispatch` call so events delivered
   * from the driver and from `dispatchStarted` (called by the writer) reach
   * each listener in strict FIFO order, matching PySpark's single-thread bus.
   */
  private _dispatchChain: Promise<void> = Promise.resolve();

  constructor(session: SparkSession) {
    this._session = session;
  }

  /**
   * Lazy-opens the subscription on the first call and waits for the server's
   * registration ack. On registration failure, resets state so a follow-up
   * `add()` re-opens cleanly.
   */
  async add(listener: StreamingQueryListener): Promise<void> {
    this._listeners.push(listener);
    if (this._driver === null) {
      this._driver = this._run();
    }

    if (this._registered !== null) {
      try {
        await this._registered;
      } catch (err) {
        this._driver = null;
        this._registered = null;
        throw err;
      }
    }
  }

  /**
   * Remove a listener. When the last leaves, sends `removeListenerBusListener`
   * so the server closes the subscription.
   *
   * Does not `await` the driver: a callback calling `removeListener(self)`
   * would otherwise deadlock the event loop.
   */
  async remove(listener: StreamingQueryListener): Promise<void> {
    const idx = this._listeners.indexOf(listener);
    if (idx < 0) return;
    this._listeners.splice(idx, 1);

    if (this._listeners.length === 0 && this._driver !== null) {
      // Null slots before the await so a concurrent add() re-opens cleanly.
      this._driver = null;
      this._registered = null;
      await this._session._executeCommandResponses({
        type: "streamingQueryListenerBusCommand",
        op: "removeListenerBusListener",
      });
    }
  }

  /** Number of currently-registered local listeners. */
  size(): number {
    return this._listeners.length;
  }

  /**
   * Dispatch a `QueryStartedEvent` from outside the driver. Called by
   * `DataStreamWriter._start`, where the started event arrives on the start
   * result rather than the bus.
   */
  async dispatchStarted(event: QueryStartedEvent): Promise<void> {
    await this._dispatch((l) => l.onQueryStarted?.(event));
  }

  private async _run(): Promise<void> {
    // Boxed so the Promise-callback assignments are visible to the outer
    // async scope without TS narrowing to never.
    const reg: { resolve: (() => void) | null; reject: ((e: unknown) => void) | null } = {
      resolve: null,
      reject: null,
    };

    this._registered = new Promise((resolve, reject) => {
      reg.resolve = resolve;
      reg.reject = reject;
    });

    try {
      const frames = this._session._executeCommandStream({
        type: "streamingQueryListenerBusCommand",
        op: "addListenerBusListener",
      });

      for await (const frame of frames) {
        if (frame["type"] !== "streamingQueryListenerEventsResult") continue;

        const payload = frame as {
          events?: { eventType: string; eventJson: string }[];
          listenerBusListenerAdded?: boolean;
        };

        if (payload.listenerBusListenerAdded === true && reg.resolve !== null) {
          const r = reg.resolve;
          reg.resolve = null;
          reg.reject = null;
          r();
        }

        for (const e of payload.events ?? []) {
          await this._dispatchEvent(e.eventType, e.eventJson);
        }
      }

      // Clean server close (usually after our removeListenerBusListener).
      if (reg.resolve !== null) reg.resolve();
    } catch (err) {
      // Non-recoverable drop. Surface to any pending registration awaiter;
      // otherwise clear so a future addListener() re-opens. The sync-throw
      // path is handled in add()'s catch (see add() for the ordering).
      if (reg.reject !== null) reg.reject(err);
      else {
        this._driver = null;
        this._registered = null;
      }

      this._listeners.length = 0;
    }
  }

  private async _dispatchEvent(eventType: string, eventJson: string): Promise<void> {
    if (eventType === "progress") {
      const event = safeParse<StreamingQueryProgress>(eventJson);
      if (event !== undefined) {
        await this._dispatch((l) => l.onQueryProgress?.(event));
      }
      return;
    }

    if (eventType === "idle") {
      const event = safeParse<Partial<QueryIdleEvent>>(eventJson);
      if (event !== undefined) {
        await this._dispatch((l) =>
          l.onQueryIdle?.({
            id: event.id ?? "",
            runId: event.runId ?? "",
            timestamp: event.timestamp ?? "",
          }),
        );
      }
      return;
    }

    if (eventType === "terminated") {
      const event = safeParse<Partial<QueryTerminatedEvent>>(eventJson);
      if (event !== undefined) {
        await this._dispatch((l) =>
          l.onQueryTerminated?.({
            id: event.id ?? "",
            runId: event.runId ?? "",
            ...(event.exception !== undefined && { exception: event.exception }),
            ...(event.errorClassOnException !== undefined && {
              errorClassOnException: event.errorClassOnException,
            }),
          }),
        );
      }
      return;
    }

    // unspecified / unknown: ignore
  }

  /**
   * Append a fan-out task to {@link _dispatchChain} so all dispatches across
   * the bus (driver-side and {@link dispatchStarted}) stay strictly FIFO.
   */
  private _dispatch(
    apply: (listener: StreamingQueryListener) => void | Promise<void>,
  ): Promise<void> {
    const next = this._dispatchChain.then(async () => {
      // Snapshot to tolerate add/remove during dispatch.
      const snapshot = this._listeners.slice();
      for (const listener of snapshot) {
        try {
          await apply(listener);
        } catch {
          // Isolate a throwing callback so the remaining listeners still get
          // the event. User callbacks are user code to instrument.
        }
      }
    });

    // Keep one task's throw from poisoning the chain. Real isolation lives
    // in the per-listener try/catch above.
    this._dispatchChain = next.catch(() => undefined);

    return next;
  }
}

function safeParse<T>(json: string): T | undefined {
  try {
    return JSON.parse(json) as T;
  } catch {
    return undefined;
  }
}
