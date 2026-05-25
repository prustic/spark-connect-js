import { InvalidInputError, SparkClientError } from "../errors.js";
import type { SparkSession } from "../spark-session.js";
import { StreamingQuery } from "./streaming-query.js";
import type { StreamingQueryListener } from "./streaming-query-listener.js";

/**
 * Decoded `StreamingQueryManagerCommandResult` payload as the transport hands
 * it back to core. Only one of `activeQueries` / `query` / `terminated` /
 * `listenerIds` is set, matching the issued command.
 *
 * @internal Boundary contract with the transport command-result decoder.
 */
interface StreamingQueryManagerCommandResultPayload {
  type: "streamingQueryManagerCommandResult";
  resultType?:
    | "active"
    | "query"
    | "awaitAnyTermination"
    | "resetTerminated"
    | "addListener"
    | "removeListener"
    | "listListeners";
  activeQueries?: { id: string; runId: string; name: string }[];
  query?: { id: string; runId: string; name: string };
  terminated?: boolean;
  listenerIds?: string[];
}

/**
 * Manages the streaming queries on a {@link SparkSession}. Obtained via
 * `spark.streams`. Mirrors `pyspark.sql.streaming.StreamingQueryManager` and
 * the canonical Scala `StreamingQueryManager`.
 *
 * @see [Spark source: StreamingQueryManager.scala](https://github.com/apache/spark/blob/master/sql/api/src/main/scala/org/apache/spark/sql/streaming/StreamingQueryManager.scala)
 */
export class StreamingQueryManager {
  private readonly _session: SparkSession;

  /** @internal Constructed by {@link SparkSession.streams}. */
  constructor(session: SparkSession) {
    this._session = session;
  }

  /** All currently-active streaming queries on this session. */
  async active(): Promise<StreamingQuery[]> {
    const { activeQueries = [] } = await this._exec("active");
    return activeQueries.map(
      (q) =>
        new StreamingQuery(this._session, q.id, q.runId, q.name.length === 0 ? undefined : q.name),
    );
  }

  /**
   * Look up a running query by `id` (the persistent identifier, not `runId`).
   * Returns `null` when the server reports no matching query.
   */
  async get(id: string): Promise<StreamingQuery | null> {
    if (id.length === 0) {
      throw new InvalidInputError("StreamingQueryManager.get: id must be non-empty.");
    }
    const { query } = await this._exec("getQuery", { id });
    if (query === undefined) return null;
    return new StreamingQuery(
      this._session,
      query.id,
      query.runId,
      query.name.length === 0 ? undefined : query.name,
    );
  }

  /**
   * Block until any streaming query on this session terminates.
   *
   * - With `timeoutMs`: returns `true` if a query terminated within the
   *   timeout, `false` otherwise.
   * - Without a timeout: blocks until something terminates, returns `undefined`.
   *
   * @remarks
   * The timeout is in **milliseconds**, matching the Scala client and the
   * Spark Connect `timeout_ms` wire field. PySpark's
   * `awaitAnyTermination(timeout)` takes **seconds**; pass `10_000` for the
   * PySpark `10` equivalent.
   *
   * @throws SparkConnectError if a query terminated with an exception.
   */
  async awaitAnyTermination(timeoutMs?: number): Promise<boolean | undefined> {
    if (timeoutMs === undefined) {
      await this._exec("awaitAnyTermination");
      return undefined;
    }
    if (!Number.isInteger(timeoutMs) || timeoutMs < 0) {
      throw new InvalidInputError(
        `StreamingQueryManager.awaitAnyTermination: timeoutMs must be a non-negative integer, got ${String(timeoutMs)}`,
      );
    }
    const { terminated = false } = await this._exec("awaitAnyTermination", { timeoutMs });
    return terminated;
  }

  /** Clear the "any query terminated" flag so the next call blocks again. */
  async resetTerminated(): Promise<void> {
    await this._exec("resetTerminated");
  }

  /**
   * Subscribe a listener to the streaming-query event bus. Lazy-opens a
   * server-side subscription on the first call; subsequent listeners share
   * the same subscription. Resolves once the server acknowledges
   * registration.
   *
   * @remarks Callbacks are dispatched serially per session. On a
   *   non-recoverable subscription drop the bus silently clears all
   *   listeners; the user must `addListener` again to restart.
   */
  async addListener(listener: StreamingQueryListener): Promise<void> {
    if (listener === null || typeof listener !== "object") {
      throw new InvalidInputError(
        "StreamingQueryManager.addListener: listener must be an object implementing StreamingQueryListener.",
      );
    }
    await this._session._getOrCreateListenerBus().add(listener);
  }

  /**
   * Remove a previously-added listener. No-op if it isn't registered. When
   * the last listener is removed, tears down the subscription.
   */
  async removeListener(listener: StreamingQueryListener): Promise<void> {
    const bus = this._session._peekListenerBus();
    if (bus === undefined) return;
    await bus.remove(listener);
  }

  // `listListeners()` is intentionally not exposed. The proto's
  // `StreamingQueryManagerCommand.listListeners` returns IDs of Java
  // listeners registered via `StreamingQueryManagerCommand.addListener`
  // (which carries `listener_payload: bytes`), not the TS listeners we
  // subscribe via `StreamingQueryListenerBusCommand.addListenerBusListener`.
  // A TS client never registers Java listeners, so the result is always
  // empty. PySpark Connect omits it for the same reason.

  private async _exec(
    op: string,
    extra: Record<string, unknown> = {},
  ): Promise<StreamingQueryManagerCommandResultPayload> {
    const responses = await this._session._executeCommandResponses({
      type: "streamingQueryManagerCommand",
      op,
      ...extra,
    });
    const match = responses.find((r) => r["type"] === "streamingQueryManagerCommandResult");
    if (match === undefined) {
      throw new SparkClientError(
        `Spark Connect server returned no streamingQueryManagerCommandResult for op "${op}".`,
      );
    }
    return match as unknown as StreamingQueryManagerCommandResultPayload;
  }
}
