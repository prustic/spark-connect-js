/**
 * RuntimeConfig
 *
 * Reads and writes Spark configuration entries on the connected server.
 * Reachable via {@link SparkSession.conf}.
 *
 * @see Spark source: sql/api/src/main/scala/org/apache/spark/sql/RuntimeConfig.scala
 * @see PySpark: pyspark.sql.conf.RuntimeConfig
 *
 * Each method round-trips one Config RPC. Most entries that are set on the
 * `spark.sql.*` namespace are also reachable via `spark.sql("SET key=value")`,
 * but `RuntimeConfig` is the canonical, typed surface for it.
 */

import type { SparkSession } from "./spark-session.js";

export class RuntimeConfig {
  /** @internal */
  private readonly _session: SparkSession;

  /** @internal */
  constructor(session: SparkSession) {
    this._session = session;
  }

  /**
   * Set a Spark configuration entry.
   *
   * @param key   Configuration key (e.g. `"spark.sql.shuffle.partitions"`)
   * @param value Value as a string. Numbers and booleans must be stringified.
   */
  async set(key: string, value: string): Promise<void> {
    await this._session._config({
      op: "set",
      pairs: [[key, value]],
    });
  }

  /**
   * Read a configuration entry.
   *
   * @returns the entry value, or `undefined` if not set and no default exists
   *   on the server.
   */
  async get(key: string): Promise<string | undefined> {
    const result = await this._session._config({ op: "get", keys: [key] });
    const pairs = result["pairs"] as [string, string | undefined][];
    return pairs[0]?.[1];
  }

  /** Remove a configuration entry. */
  async unset(key: string): Promise<void> {
    await this._session._config({ op: "unset", keys: [key] });
  }

  /**
   * Return all configuration entries currently set on the session.
   *
   * @param prefix Optional key prefix filter (e.g. `"spark.sql"`).
   */
  async getAll(prefix?: string): Promise<Record<string, string>> {
    const result = await this._session._config({
      op: "getAll",
      ...(prefix !== undefined ? { prefix } : {}),
    });
    const pairs = result["pairs"] as [string, string | undefined][];
    const out: Record<string, string> = {};
    for (const [k, v] of pairs) {
      if (v !== undefined) out[k] = v;
    }
    return out;
  }

  /**
   * Return whether a configuration entry can be modified on the running
   * session, or whether it is a static (server-bound) setting.
   */
  async isModifiable(key: string): Promise<boolean> {
    const result = await this._session._config({ op: "isModifiable", keys: [key] });
    const pairs = result["pairs"] as [string, string | undefined][];
    return pairs[0]?.[1] === "true";
  }
}
