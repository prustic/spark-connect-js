import type { SparkSession } from "../spark-session.js";
import type { StreamingQueryProgress } from "./types.js";

/**
 * Fired when a streaming query has just been launched. Parsed from the
 * server's `queryStartedEventJson` on `WriteStreamOperationStartResult`.
 */
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
 * Callback interface for receiving streaming-query lifecycle events. Pass an
 * object literal with the callbacks you care about, or `extends`
 * {@link StreamingQueryListenerBase} for a class-based style.
 *
 * @remarks
 * Callbacks are dispatched **serially** per session, so slow callbacks delay
 * subsequent event delivery. For parallel work, queue inside the callback.
 * Callbacks may be `async`; the bus `await`s each return.
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
 * Empty base class implementing {@link StreamingQueryListener}. Provided for
 * users who prefer the PySpark/Scala `class MyListener extends ...` style;
 * users who prefer object literals can implement the interface directly.
 */
export class StreamingQueryListenerBase implements StreamingQueryListener {}

/**
 * Session-scoped listener-bus driver. Owns the long-running
 * `StreamingQueryListenerBusCommand` subscription, the listener set, and the
 * fan-out dispatch. Created lazily on the first
 * {@link StreamingQueryManager.addListener}; torn down on the last
 * {@link StreamingQueryManager.removeListener}.
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
   * Add a listener. Lazy-opens the subscription and waits for the server's
   * registration ack. Catches a sync-throw registration failure (e.g. a
   * transport missing `executeCommandStream`) and resets state so a follow-up
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
   * Remove a listener. When the last listener is removed, sends
   * `removeListenerBusListener` so the server closes the subscription; the
   * driver task completes on its own.
   *
   * Does not `await` the driver: callbacks may invoke `removeListener(self)`,
   * which would deadlock on a single event loop (driver -> dispatch ->
   * callback -> remove -> driver). Server-close drives teardown instead.
   * Side effect: if `addListener` is called again before the server processes
   * the remove, in-flight events from the old subscription briefly dispatch
   * to the new listener. Same race exists in PySpark Connect's bus thread.
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
   * Dispatch a `QueryStartedEvent` from outside the driver (called from
   * `DataStreamWriter._start` when listeners are registered and the server's
   * start result carries `queryStartedEventJson`).
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
      // Server closed the stream cleanly (typically after our removeListenerBusListener).
      if (reg.resolve !== null) reg.resolve();
    } catch (err) {
      // Non-recoverable drop: surface to the pending registration awaiter if
      // any; otherwise clear so a future addListener() re-opens. Live-drop
      // path nulls _driver/_registered here; sync-throw path is handled in
      // add()'s catch (assignment ordering, see add()).
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
      const event = safeParse(eventJson);
      if (event !== undefined) {
        await this._dispatch((l) => l.onQueryProgress?.(event as StreamingQueryProgress));
      }
      return;
    }
    if (eventType === "idle") {
      const event = safeParse(eventJson) as Partial<QueryIdleEvent> | undefined;
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
      const event = safeParse(eventJson) as Partial<QueryTerminatedEvent> | undefined;
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
          // Per-listener exception isolation: a throwing callback must not
          // break delivery to the other listeners. Callbacks are user code;
          // it's the caller's responsibility to instrument them.
        }
      }
    });
    // Swallow the (impossible) chain failure so one task's throw can't poison
    // subsequent dispatches. The per-listener try/catch above is the real
    // exception isolation.
    this._dispatchChain = next.catch(() => undefined);
    return next;
  }
}

function safeParse(json: string): unknown {
  try {
    return JSON.parse(json);
  } catch {
    return undefined;
  }
}
