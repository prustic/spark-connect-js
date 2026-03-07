/**
 * ─── Catalog ────────────────────────────────────────────────────────────────
 *
 * Provides access to Spark's catalog API for inspecting databases, tables,
 * functions, and columns.
 *
 * @see Spark source: sql/core/src/main/scala/org/apache/spark/sql/catalog/Catalog.scala
 * @see Spark Connect: The catalog operations are sent as Relation.Catalog
 *   protobuf messages, and the server returns the results as DataFrames.
 *
 * Catalog operations are executed via ExecutePlan (not AnalyzePlan) because
 * in Spark Connect, catalog queries are modeled as Relations that return
 * tabular results.
 */

import { DataFrame } from "./data-frame.js";
import type { SparkSession } from "./spark-session.js";
import type { Row } from "./types/row.js";
import type { CatalogOperation } from "./plan/logical-plan.js";

export class Catalog {
  /** @internal */
  private readonly _session: SparkSession;

  /** @internal */
  constructor(session: SparkSession) {
    this._session = session;
  }

  /** List all databases. Returns a DataFrame with database metadata. */
  listDatabases(pattern?: string): DataFrame {
    return this._catalogDF({ op: "listDatabases", pattern });
  }

  /** List all tables in a database. Returns a DataFrame with table metadata. */
  listTables(dbName?: string, pattern?: string): DataFrame {
    return this._catalogDF({ op: "listTables", dbName, pattern });
  }

  /** List all columns of a table. Returns a DataFrame with column metadata. */
  listColumns(tableName: string, dbName?: string): DataFrame {
    return this._catalogDF({ op: "listColumns", tableName, dbName });
  }

  /** Check if a table exists. */
  async tableExists(tableName: string, dbName?: string): Promise<boolean> {
    const rows = await this._collectCatalog({ op: "tableExists", tableName, dbName });
    return rows.length > 0 && rows[0]["exists"] === true;
  }

  /** Check if a database exists. */
  async databaseExists(dbName: string): Promise<boolean> {
    const rows = await this._collectCatalog({ op: "databaseExists", dbName });
    return rows.length > 0 && rows[0]["exists"] === true;
  }

  /** Get the current database name. */
  async currentDatabase(): Promise<string> {
    const rows = await this._collectCatalog({ op: "currentDatabase" });
    // Spark returns a single row with a "result" or first column
    if (rows.length > 0) {
      const firstVal = Object.values(rows[0])[0];
      return typeof firstVal === "string" ? firstVal : "default";
    }
    return "default";
  }

  /** Set the current database. */
  async setCurrentDatabase(dbName: string): Promise<void> {
    await this._collectCatalog({ op: "setCurrentDatabase", dbName });
  }

  /** @internal — create a DataFrame from a catalog operation */
  private _catalogDF(operation: CatalogOperation): DataFrame {
    return DataFrame._fromPlan(this._session, { type: "catalog", operation });
  }

  /** @internal — execute a catalog operation and collect the result */
  private async _collectCatalog(operation: CatalogOperation): Promise<Row[]> {
    return this._catalogDF(operation).collect();
  }
}
