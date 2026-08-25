import type { DataFrame } from "./data-frame.js";
import { Column } from "./column.js";
import { expr } from "./functions/conditional.js";
import type { Expression } from "./plan/logical-plan.js";
import { InvalidInputError } from "./errors.js";

type MergeActionType = "delete" | "insert" | "insertStar" | "update" | "updateStar";

// Fail fast on anything that is not a Column or SQL string: a bad condition
// would otherwise surface as an opaque TypeError (or worse, a clause
// condition silently dropped) during serialization in merge().
function toCondition(condition: Column | string | undefined, where: string): Column | undefined {
  if (condition === undefined) {
    return undefined;
  }
  if (typeof condition === "string") {
    return expr(condition);
  }
  if (condition instanceof Column) {
    return condition;
  }

  throw new InvalidInputError(`${where} condition must be a Column or a SQL string.`);
}

// Built eagerly at clause-method call time so a later mutation of the caller's
// assignments record cannot change the queued action. Keys are parsed as SQL
// expression strings (parity with PySpark/Scala `expr(k)`), which allows
// nested-field targets like "address.city".
function buildMergeAction(
  actionType: MergeActionType,
  method: "update" | "insert",
  condition: Column | undefined,
  assignments?: Record<string, Column>,
): Expression {
  let entries: { key: Expression; value: Expression }[] = [];
  if (assignments !== undefined) {
    const pairs = Object.entries(assignments);
    if (pairs.length === 0) {
      const all = method === "update" ? "updateAll" : "insertAll";
      throw new InvalidInputError(
        `${method}() requires at least one assignment; use ${all}() to ${method} all columns.`,
      );
    }
    entries = pairs.map(([key, value]) => {
      // No string sugar for values: in SQL, SET val = 's.val' assigns a
      // literal, so a bare string is ambiguous. Require an explicit Column.
      if (!(value instanceof Column)) {
        throw new InvalidInputError(
          `${method}() assignment for "${key}" must be a Column; ` +
            `wrap the value with col(), expr(), or lit().`,
        );
      }

      return {
        key: { type: "expressionString", expression: key } as Expression,
        value: value._expr,
      };
    });
  }

  return {
    type: "mergeAction",
    actionType,
    ...(condition !== undefined && { condition: condition._expr }),
    assignments: entries,
  };
}

/**
 * Merges a source {@link DataFrame} into a target table with fine-grained
 * matched / not-matched / not-matched-by-source clauses.
 *
 * Obtained via `df.mergeInto(table, condition)`, where the DataFrame is the
 * merge source. Clause builders return this writer, so clauses chain; call
 * {@link merge} to execute. At least one clause action is required.
 *
 * The target table is addressed by name in conditions and assignment values,
 * so alias the source to keep references unambiguous.
 *
 * @example Upsert with delete of rows gone from the source
 * ```ts
 * await source.alias("s")
 *   .mergeInto("target", expr("s.id = target.id"))
 *   .whenMatched().updateAll()
 *   .whenNotMatched().insertAll()
 *   .whenNotMatchedBySource().delete()
 *   .merge();
 * ```
 *
 * @see [Spark source: MergeIntoWriter.scala](https://github.com/apache/spark/blob/master/sql/api/src/main/scala/org/apache/spark/sql/MergeIntoWriter.scala)
 * @see [PySpark source: merge.py](https://github.com/apache/spark/blob/master/python/pyspark/sql/merge.py)
 */
export class MergeIntoWriter {
  private readonly _df: DataFrame;
  private readonly _tableName: string;
  private readonly _condition: Column;
  private _schemaEvolution = false;
  /** @internal Appended to by the When* clause classes. */
  readonly _matchedActions: Expression[] = [];
  /** @internal */
  readonly _notMatchedActions: Expression[] = [];
  /** @internal */
  readonly _notMatchedBySourceActions: Expression[] = [];

  /** @internal Obtained via `DataFrame.mergeInto`. */
  constructor(df: DataFrame, tableName: string, condition: Column | string) {
    this._df = df;
    this._tableName = tableName;
    const cond = toCondition(condition, "mergeInto()");
    if (cond === undefined) {
      throw new InvalidInputError("mergeInto() requires a merge condition.");
    }
    this._condition = cond;
  }

  /**
   * Clause for rows where the merge condition matches a target row.
   * An optional extra condition narrows which matched rows the action applies to.
   */
  whenMatched(condition?: Column | string): WhenMatched {
    return new WhenMatched(this, toCondition(condition, "whenMatched()"));
  }

  /**
   * Clause for source rows with no matching target row.
   * An optional extra condition narrows which rows are inserted.
   */
  whenNotMatched(condition?: Column | string): WhenNotMatched {
    return new WhenNotMatched(this, toCondition(condition, "whenNotMatched()"));
  }

  /**
   * Clause for target rows with no matching source row.
   * An optional extra condition narrows which target rows the action applies to.
   */
  whenNotMatchedBySource(condition?: Column | string): WhenNotMatchedBySource {
    return new WhenNotMatchedBySource(this, toCondition(condition, "whenNotMatchedBySource()"));
  }

  /**
   * Enable automatic schema evolution for this merge.
   *
   * Depends on server and table-provider support: a provider that does not
   * honor plan-level evolution ignores the flag silently. Delta additionally
   * gates evolution behind `spark.databricks.delta.schema.autoMerge.enabled`.
   */
  withSchemaEvolution(): this {
    this._schemaEvolution = true;
    return this;
  }

  /**
   * Execute the merge.
   *
   * The writer stays usable: calling `merge()` again re-executes the same
   * merge against the table's current state.
   *
   * @throws `InvalidInputError` when no clause action has been defined.
   */
  async merge(): Promise<void> {
    if (
      this._matchedActions.length === 0 &&
      this._notMatchedActions.length === 0 &&
      this._notMatchedBySourceActions.length === 0
    ) {
      throw new InvalidInputError(
        `mergeInto("${this._tableName}") needs at least one whenMatched/whenNotMatched/` +
          `whenNotMatchedBySource clause before merge().`,
      );
    }

    await this._df._session._executeCommand({
      type: "mergeIntoTableCommand",
      targetTableName: this._tableName,
      sourceTablePlan: this._df._plan,
      mergeCondition: this._condition._expr,
      matchActions: [...this._matchedActions],
      notMatchedActions: [...this._notMatchedActions],
      notMatchedBySourceActions: [...this._notMatchedBySourceActions],
      withSchemaEvolution: this._schemaEvolution,
    });
  }
}

/**
 * Actions for a {@link MergeIntoWriter.whenMatched} clause.
 * Each action appends to the writer and returns it for chaining.
 */
export class WhenMatched {
  private readonly _writer: MergeIntoWriter;
  private readonly _condition: Column | undefined;

  /** @internal */
  constructor(writer: MergeIntoWriter, condition: Column | undefined) {
    this._writer = writer;
    this._condition = condition;
  }

  /** Update all columns of the matched target row from the source row. */
  updateAll(): MergeIntoWriter {
    this._writer._matchedActions.push(buildMergeAction("updateStar", "update", this._condition));
    return this._writer;
  }

  /**
   * Update the given columns of the matched target row.
   *
   * @param assignments - Map of target column (a SQL expression string, so
   * nested fields like `"address.city"` work) to the value to assign.
   * @throws `InvalidInputError` when the map is empty.
   */
  update(assignments: Record<string, Column>): MergeIntoWriter {
    this._writer._matchedActions.push(
      buildMergeAction("update", "update", this._condition, assignments),
    );
    return this._writer;
  }

  /** Delete the matched target row. */
  delete(): MergeIntoWriter {
    this._writer._matchedActions.push(buildMergeAction("delete", "update", this._condition));
    return this._writer;
  }
}

/**
 * Actions for a {@link MergeIntoWriter.whenNotMatched} clause.
 * Each action appends to the writer and returns it for chaining.
 */
export class WhenNotMatched {
  private readonly _writer: MergeIntoWriter;
  private readonly _condition: Column | undefined;

  /** @internal */
  constructor(writer: MergeIntoWriter, condition: Column | undefined) {
    this._writer = writer;
    this._condition = condition;
  }

  /** Insert the source row with all its columns. */
  insertAll(): MergeIntoWriter {
    this._writer._notMatchedActions.push(buildMergeAction("insertStar", "insert", this._condition));
    return this._writer;
  }

  /**
   * Insert a row built from the given column assignments.
   *
   * @param assignments - Map of target column to the value to insert.
   * @throws `InvalidInputError` when the map is empty.
   */
  insert(assignments: Record<string, Column>): MergeIntoWriter {
    this._writer._notMatchedActions.push(
      buildMergeAction("insert", "insert", this._condition, assignments),
    );
    return this._writer;
  }
}

/**
 * Actions for a {@link MergeIntoWriter.whenNotMatchedBySource} clause.
 * Each action appends to the writer and returns it for chaining.
 */
export class WhenNotMatchedBySource {
  private readonly _writer: MergeIntoWriter;
  private readonly _condition: Column | undefined;

  /** @internal */
  constructor(writer: MergeIntoWriter, condition: Column | undefined) {
    this._writer = writer;
    this._condition = condition;
  }

  /**
   * Update all columns of the unmatched target row.
   *
   * Present for parity with the PySpark and Scala clients; Spark's analyzer
   * rejects UPDATE_STAR in a not-matched-by-source clause, since there is no
   * source row to update from.
   */
  updateAll(): MergeIntoWriter {
    this._writer._notMatchedBySourceActions.push(
      buildMergeAction("updateStar", "update", this._condition),
    );
    return this._writer;
  }

  /**
   * Update the given columns of the unmatched target row.
   *
   * @param assignments - Map of target column to the value to assign. Values
   * can only reference the target, since there is no matching source row.
   * @throws `InvalidInputError` when the map is empty.
   */
  update(assignments: Record<string, Column>): MergeIntoWriter {
    this._writer._notMatchedBySourceActions.push(
      buildMergeAction("update", "update", this._condition, assignments),
    );
    return this._writer;
  }

  /** Delete the unmatched target row. */
  delete(): MergeIntoWriter {
    this._writer._notMatchedBySourceActions.push(
      buildMergeAction("delete", "update", this._condition),
    );
    return this._writer;
  }
}
