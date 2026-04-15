/**
 * Catalog
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

  /** List all functions in a database. Returns a DataFrame with function metadata. */
  listFunctions(dbName?: string, pattern?: string): DataFrame {
    return this._catalogDF({ op: "listFunctions", dbName, pattern });
  }

  /** List all catalogs. Returns a DataFrame with catalog metadata. */
  listCatalogs(pattern?: string): DataFrame {
    return this._catalogDF({ op: "listCatalogs", pattern });
  }

  /** Get the database with the specified name. Returns a single-row DataFrame. */
  getDatabase(dbName: string): DataFrame {
    return this._catalogDF({ op: "getDatabase", dbName });
  }

  /** Get the table or view with the specified name. Returns a single-row DataFrame. */
  getTable(tableName: string, dbName?: string): DataFrame {
    return this._catalogDF({ op: "getTable", tableName, dbName });
  }

  /** Get the function with the specified name. Returns a single-row DataFrame. */
  getFunction(functionName: string, dbName?: string): DataFrame {
    return this._catalogDF({ op: "getFunction", functionName, dbName });
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

  /** Check if a function exists. */
  async functionExists(functionName: string, dbName?: string): Promise<boolean> {
    const rows = await this._collectCatalog({ op: "functionExists", functionName, dbName });
    return rows.length > 0 && rows[0]["exists"] === true;
  }

  /** Returns true if the table is currently cached in-memory. */
  async isCached(tableName: string): Promise<boolean> {
    const rows = await this._collectCatalog({ op: "isCached", tableName });
    return rows.length > 0 && rows[0]["isCached"] === true;
  }

  /** Drops the local temporary view. Returns true if the view existed. */
  async dropTempView(viewName: string): Promise<boolean> {
    const rows = await this._collectCatalog({ op: "dropTempView", viewName });
    return rows.length > 0 && Object.values(rows[0])[0] === true;
  }

  /** Drops the global temporary view. Returns true if the view existed. */
  async dropGlobalTempView(viewName: string): Promise<boolean> {
    const rows = await this._collectCatalog({ op: "dropGlobalTempView", viewName });
    return rows.length > 0 && Object.values(rows[0])[0] === true;
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

  /** @internal Create a DataFrame from a catalog operation */
  private _catalogDF(operation: CatalogOperation): DataFrame {
    return DataFrame._fromPlan(this._session, { type: "catalog", operation });
  }

  /** @internal Execute a catalog operation and collect the result */
  private async _collectCatalog(operation: CatalogOperation): Promise<Row[]> {
    return this._catalogDF(operation).collect();
  }
}
