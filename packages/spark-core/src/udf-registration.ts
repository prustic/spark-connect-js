/**
 * UDFRegistration
 *
 * Surface for registering user-defined functions with the Spark Connect server.
 * Reachable via `SparkSession.udf`.
 *
 * @see Spark source: sql/core/src/main/scala/org/apache/spark/sql/UDFRegistration.scala
 * @see PySpark: pyspark.sql.UDFRegistration
 *
 * UDF registration is sent as a `Command.registerFunction` proto carrying a
 * `CommonInlineUserDefinedFunction`. From a TypeScript client we cannot
 * synthesise Python pickles or Scala-serialised closures, so only the Java
 * variants are wired here. The referenced class must already be on the
 * Spark Connect server's classpath.
 */

import type { SparkSession } from "./spark-session.js";

export class UDFRegistration {
  /** @internal */
  private readonly _session: SparkSession;

  /** @internal */
  constructor(session: SparkSession) {
    this._session = session;
  }

  /**
   * Register a Java UDF as a SQL function.
   *
   * The class must implement one of `org.apache.spark.sql.api.java.UDF1`
   * through `UDF22` and must already be on the server's classpath.
   *
   * @param name           SQL function name to register under
   * @param javaClassName  Fully-qualified Java class name implementing the UDF
   * @param returnType     Optional Spark SQL DDL type string for the return
   *                       value (e.g. "INT", "STRING", "ARRAY<INT>"). Required
   *                       when the return type cannot be inferred.
   *
   * @example
   *   await spark.udf.registerJavaFunction(
   *     "my_upper",
   *     "com.example.UpperUDF",
   *     "STRING",
   *   );
   *   const df = spark.sql("SELECT my_upper(name) FROM people");
   */
  async registerJavaFunction(
    name: string,
    javaClassName: string,
    returnType?: string,
  ): Promise<void> {
    await this._session._executeCommand({
      type: "registerFunction",
      functionName: name,
      className: javaClassName,
      aggregate: false,
      ...(returnType !== undefined ? { returnType } : {}),
    });
  }

  /**
   * Register a Java UDAF (user-defined aggregate function) as a SQL function.
   *
   * The class must extend `org.apache.spark.sql.expressions.UserDefinedAggregateFunction`
   * and must already be on the server's classpath.
   */
  async registerJavaUDAF(name: string, javaClassName: string): Promise<void> {
    await this._session._executeCommand({
      type: "registerFunction",
      functionName: name,
      className: javaClassName,
      aggregate: true,
    });
  }
}
