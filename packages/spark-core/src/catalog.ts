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
import type { StructType } from "./types/struct.js";
import type { StorageLevel } from "./storage-level.js";

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
    return this._firstValue(rows) === true;
  }

  /** Check if a database exists. */
  async databaseExists(dbName: string): Promise<boolean> {
    const rows = await this._collectCatalog({ op: "databaseExists", dbName });
    return this._firstValue(rows) === true;
  }

  /** Check if a function exists. */
  async functionExists(functionName: string, dbName?: string): Promise<boolean> {
    const rows = await this._collectCatalog({ op: "functionExists", functionName, dbName });
    return this._firstValue(rows) === true;
  }

  /** Returns true if the table is currently cached in-memory. */
  async isCached(tableName: string): Promise<boolean> {
    const rows = await this._collectCatalog({ op: "isCached", tableName });
    return this._firstValue(rows) === true;
  }

  /** Drops the local temporary view. Returns true if the view existed. */
  async dropTempView(viewName: string): Promise<boolean> {
    const rows = await this._collectCatalog({ op: "dropTempView", viewName });
    return this._firstValue(rows) === true;
  }

  /** Drops the global temporary view. Returns true if the view existed. */
  async dropGlobalTempView(viewName: string): Promise<boolean> {
    const rows = await this._collectCatalog({ op: "dropGlobalTempView", viewName });
    return this._firstValue(rows) === true;
  }

  /** Get the current database name. */
  async currentDatabase(): Promise<string> {
    const rows = await this._collectCatalog({ op: "currentDatabase" });
    return this._firstValue(rows) as string;
  }

  /** Set the current database. */
  async setCurrentDatabase(dbName: string): Promise<void> {
    await this._collectCatalog({ op: "setCurrentDatabase", dbName });
  }

  /** Get the current default catalog name. */
  async currentCatalog(): Promise<string> {
    const rows = await this._collectCatalog({ op: "currentCatalog" });
    return this._firstValue(rows) as string;
  }

  /** Set the current default catalog. */
  async setCurrentCatalog(catalogName: string): Promise<void> {
    await this._collectCatalog({ op: "setCurrentCatalog", catalogName });
  }

  /** Cache the specified table in-memory with an optional storage level. */
  async cacheTable(tableName: string, storageLevel?: StorageLevel): Promise<void> {
    await this._collectCatalog({ op: "cacheTable", tableName, storageLevel });
  }

  /** Remove the specified table from the in-memory cache. */
  async uncacheTable(tableName: string): Promise<void> {
    await this._collectCatalog({ op: "uncacheTable", tableName });
  }

  /** Remove all cached tables from the in-memory cache. */
  async clearCache(): Promise<void> {
    await this._collectCatalog({ op: "clearCache" });
  }

  /** Invalidate and refresh all cached data and metadata for the given table. */
  async refreshTable(tableName: string): Promise<void> {
    await this._collectCatalog({ op: "refreshTable", tableName });
  }

  /** Invalidate and refresh cached data for any DataFrame containing the given path. */
  async refreshByPath(path: string): Promise<void> {
    await this._collectCatalog({ op: "refreshByPath", path });
  }

  /** Recover all partitions of the given table and update the catalog. */
  async recoverPartitions(tableName: string): Promise<void> {
    await this._collectCatalog({ op: "recoverPartitions", tableName });
  }

  /**
   * Create a table based on the dataset in a data source.
   *
   * When `path` is specified, an external table is created from the data at
   * the given path. Otherwise a managed table is created.
   *
   * Returns a DataFrame associated with the new table.
   */
  createTable(
    tableName: string,
    options?: {
      path?: string;
      source?: string;
      description?: string;
      schema?: StructType;
      options?: Record<string, string>;
    },
  ): DataFrame {
    return this._catalogDF({
      op: "createTable",
      tableName,
      path: options?.path,
      source: options?.source,
      description: options?.description,
      schema: options?.schema?.toDDL(),
      options: options?.options,
    });
  }

  /**
   * Create an external table based on the dataset in a data source.
   *
   * Returns a DataFrame associated with the external table.
   */
  createExternalTable(
    tableName: string,
    options?: {
      path?: string;
      source?: string;
      schema?: StructType;
      options?: Record<string, string>;
    },
  ): DataFrame {
    return this._catalogDF({
      op: "createExternalTable",
      tableName,
      path: options?.path,
      source: options?.source,
      schema: options?.schema?.toDDL(),
      options: options?.options,
    });
  }

  /** @internal Create a DataFrame from a catalog operation */
  private _catalogDF(operation: CatalogOperation): DataFrame {
    return DataFrame._fromPlan(this._session, { type: "catalog", operation });
  }

  /** @internal Execute a catalog operation and collect the result */
  private async _collectCatalog(operation: CatalogOperation): Promise<Row[]> {
    return this._catalogDF(operation).collect();
  }

  /**
   * @internal Extract the first column value from the first row.
   * Spark Connect catalog operations return single-column DataFrames
   * with varying column names — this avoids hardcoding column names.
   */
  private _firstValue(rows: Row[]): unknown {
    if (rows.length === 0) return undefined;
    return Object.values(rows[0])[0];
  }
}
