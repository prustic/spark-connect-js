import type { DataFrame } from "./data-frame.js";
import type { Column } from "./column.js";
import type { Expression } from "./plan/logical-plan.js";
import { InvalidInputError } from "./errors.js";

type MergeActionType = "delete" | "insert" | "insertStar" | "update" | "updateStar";

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
    entries = pairs.map(([key, value]) => ({
      key: { type: "expressionString", expression: key },
      value: value._expr,
    }));
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
  constructor(df: DataFrame, tableName: string, condition: Column) {
    this._df = df;
    this._tableName = tableName;
    this._condition = condition;
  }

  /**
   * Clause for rows where the merge condition matches a target row.
   * An optional extra condition narrows which matched rows the action applies to.
   */
  whenMatched(condition?: Column): WhenMatched {
    return new WhenMatched(this, condition);
  }

  /**
   * Clause for source rows with no matching target row.
   * An optional extra condition narrows which rows are inserted.
   */
  whenNotMatched(condition?: Column): WhenNotMatched {
    return new WhenNotMatched(this, condition);
  }

  /**
   * Clause for target rows with no matching source row.
   * An optional extra condition narrows which target rows the action applies to.
   */
  whenNotMatchedBySource(condition?: Column): WhenNotMatchedBySource {
    return new WhenNotMatchedBySource(this, condition);
  }

  /** Enable automatic schema evolution for this merge. */
  withSchemaEvolution(): this {
    this._schemaEvolution = true;
    return this;
  }

  /**
   * Execute the merge.
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
