import { InvalidInputError, SparkClientError } from "../errors.js";
import type { SparkSession } from "../spark-session.js";
import type {
  StreamingQueryException,
  StreamingQueryProgress,
  StreamingQueryStatus,
} from "./types.js";

/**
 * Decoded `StreamingQueryCommandResult` payload as the transport hands it
 * back to core. Mirrors the proto's `result_type` oneof: only one of
 * `status` / `recentProgressJson` / `explain` / `exception` / `terminated`
 * is set, matching the issued command.
 *
 * @internal Boundary contract with the transport command-result decoder.
 */
interface StreamingQueryCommandResultPayload {
  type: "streamingQueryCommandResult";
  queryId?: { id: string; runId: string };
  resultType?: "status" | "recentProgress" | "explain" | "exception" | "awaitTermination";
  status?: {
    statusMessage: string;
    isDataAvailable: boolean;
    isTriggerActive: boolean;
    isActive: boolean;
  };
  recentProgressJson?: string[];
  explain?: string;
  exception?: { exceptionMessage?: string; errorClass?: string; stackTrace?: string };
  terminated?: boolean;
}

/**
 * Handle for a streaming query running on the Spark Connect server. Returned
 * by {@link DataStreamWriter.start} and {@link DataStreamWriter.toTable}.
 *
 * Properties (`id`, `runId`, `name`) are cached from the start response.
 * Methods (`status`, `lastProgress`, `stop`, …) each issue a
 * `StreamingQueryCommand` RPC.
 *
 * `isActive` is asynchronous on this client even though PySpark and Scala
 * expose it as a synchronous property. Every other inspection method on this
 * class crosses the wire; a synchronous getter would lie about its source of
 * truth.
 *
 * @see [Spark source: StreamingQuery.scala](https://github.com/apache/spark/blob/master/sql/api/src/main/scala/org/apache/spark/sql/streaming/StreamingQuery.scala)
 */
export class StreamingQuery {
  private readonly _session: SparkSession;
  readonly id: string;
  readonly runId: string;
  readonly name: string | undefined;

  /** @internal Constructed by {@link DataStreamWriter} from the start response. */
  constructor(session: SparkSession, id: string, runId: string, name: string | undefined) {
    this._session = session;
    this.id = id;
    this.runId = runId;
    this.name = name;
  }

  /** Whether the query is still running. */
  async isActive(): Promise<boolean> {
    return (await this.status()).isActive;
  }

  /** Current runtime status (message, data availability, trigger activity). */
  async status(): Promise<StreamingQueryStatus> {
    const { status } = await this._exec("status");
    if (status === undefined) {
      throw new SparkClientError("Spark Connect server did not return a status result.");
    }
    return {
      message: status.statusMessage,
      isDataAvailable: status.isDataAvailable,
      isTriggerActive: status.isTriggerActive,
      isActive: status.isActive,
    };
  }

  /**
   * Most recent progress report, or `null` if no batch has been processed yet.
   */
  async lastProgress(): Promise<StreamingQueryProgress | null> {
    const { recentProgressJson = [] } = await this._exec("lastProgress");
    return recentProgressJson.length === 0
      ? null
      : (JSON.parse(recentProgressJson[recentProgressJson.length - 1]) as StreamingQueryProgress);
  }

  /**
   * Recent progress reports, oldest first. The server keeps a bounded number
   * (default 100); older entries fall off.
   */
  async recentProgress(): Promise<StreamingQueryProgress[]> {
    const { recentProgressJson = [] } = await this._exec("recentProgress");
    return recentProgressJson.map((s) => JSON.parse(s) as StreamingQueryProgress);
  }

  /**
   * The exception that terminated the query, or `null` if the query is
   * still running or stopped cleanly.
   */
  async exception(): Promise<StreamingQueryException | null> {
    const { exception } = await this._exec("exception");
    if (exception === undefined) return null;
    const { exceptionMessage, errorClass, stackTrace } = exception;
    if (exceptionMessage === undefined && errorClass === undefined && stackTrace === undefined) {
      return null;
    }
    return {
      ...(exceptionMessage !== undefined && { message: exceptionMessage }),
      ...(errorClass !== undefined && { errorClass }),
      ...(stackTrace !== undefined && { stackTrace }),
    };
  }

  /**
   * Logical and physical plan as a string. `extended=true` adds the analyzed
   * and optimized plans.
   */
  async explain(extended?: boolean): Promise<string> {
    const { explain = "" } = await this._exec("explain", { extended: extended === true });
    return explain;
  }

  /** Stop the query. Returns after the server confirms termination. */
  async stop(): Promise<void> {
    await this._exec("stop");
  }

  /**
   * Block until all currently-available source data has been processed and
   * committed to the sink. Useful in tests; do not use in production code
   * with unbounded sources.
   */
  async processAllAvailable(): Promise<void> {
    await this._exec("processAllAvailable");
  }

  /**
   * Block until the query terminates.
   *
   * - With `timeoutMs`: returns `true` if the query terminated within the
   *   timeout, `false` otherwise.
   * - Without a timeout: blocks forever and returns `undefined` once the
   *   query terminates.
   *
   * @throws SparkConnectError if the query terminated with an exception.
   */
  async awaitTermination(timeoutMs?: number): Promise<boolean | undefined> {
    if (timeoutMs === undefined) {
      await this._exec("awaitTermination");
      return undefined;
    }
    if (!Number.isInteger(timeoutMs) || timeoutMs < 0) {
      throw new InvalidInputError(
        `StreamingQuery.awaitTermination: timeoutMs must be a non-negative integer, got ${String(timeoutMs)}`,
      );
    }
    const { terminated = false } = await this._exec("awaitTermination", { timeoutMs });
    return terminated;
  }

  private async _exec(
    op: string,
    extra: Record<string, unknown> = {},
  ): Promise<StreamingQueryCommandResultPayload> {
    const responses = await this._session._executeCommandResponses({
      type: "streamingQueryCommand",
      queryId: { id: this.id, runId: this.runId },
      op,
      ...extra,
    });
    const match = responses.find((r) => r["type"] === "streamingQueryCommandResult");
    if (match === undefined) {
      throw new SparkClientError(
        `Spark Connect server returned no streamingQueryCommandResult for op "${op}".`,
      );
    }
    return match as unknown as StreamingQueryCommandResultPayload;
  }
}
