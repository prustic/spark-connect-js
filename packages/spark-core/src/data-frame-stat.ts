import { DataFrame } from "./data-frame.js";
import { Column, col as _col } from "./column.js";
import type { Row } from "./types/row.js";
import { InvalidInputError } from "./errors.js";

/**
 * Statistical and approximate-query operations on a {@link DataFrame}.
 *
 * Obtained via {@link DataFrame.stat}. Mirrors `DataFrameStatFunctions` in
 * the JVM Spark API.
 *
 * @example
 * ```ts
 * const c = await df.stat.corr("height", "weight");
 * const quantiles = await df.stat.approxQuantile("latency", [0.5, 0.95, 0.99], 0.01);
 * ```
 *
 * @see [Spark source: DataFrameStatFunctions.scala](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/DataFrameStatFunctions.scala)
 */
export class DataFrameStat<R extends Row = Row> {
  private readonly _df: DataFrame<R>;

  /** @internal */
  constructor(df: DataFrame<R>) {
    this._df = df;
  }

  /** Compute Pearson correlation between two columns. */
  corr(col1: string, col2: string, method = "pearson"): DataFrame {
    return DataFrame._fromPlan(this._df._session, {
      type: "statCorr",
      child: this._df._plan,
      col1,
      col2,
      method,
    });
  }

  /** Compute sample covariance between two columns. */
  cov(col1: string, col2: string): DataFrame {
    return DataFrame._fromPlan(this._df._session, {
      type: "statCov",
      child: this._df._plan,
      col1,
      col2,
    });
  }

  /** Compute a pair-wise frequency table (contingency table). */
  crosstab(col1: string, col2: string): DataFrame {
    return DataFrame._fromPlan(this._df._session, {
      type: "statCrosstab",
      child: this._df._plan,
      col1,
      col2,
    });
  }

  /** Find frequent items in the given columns. */
  freqItems(cols: string[], support?: number): DataFrame {
    return DataFrame._fromPlan(this._df._session, {
      type: "statFreqItems",
      child: this._df._plan,
      cols,
      support,
    });
  }

  /** Compute approximate quantiles for numerical columns. */
  approxQuantile(cols: string[], probabilities: number[], relativeError: number): DataFrame {
    return DataFrame._fromPlan(this._df._session, {
      type: "statApproxQuantile",
      child: this._df._plan,
      cols,
      probabilities,
      relativeError,
    });
  }

  /**
   * Stratified sample without replacement, taking `fractions[stratum]` of the
   * rows in each stratum. Strata absent from the map are dropped.
   *
   * @param col - The column defining the strata.
   * @param fractions - Sampling fraction per stratum. Object keys are always
   * strings, so a numeric or boolean stratum passed that way matches nothing
   * and silently samples no rows. Use a `Map` to keep the stratum's own type.
   * @param seed - Optional seed; a random one is used when omitted.
   *
   * @see [Spark source: DataFrameStatFunctions.scala](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/DataFrameStatFunctions.scala)
   */
  sampleBy(
    col: Column | string,
    fractions: Record<string, number> | Map<string | number | boolean | bigint | null, number>,
    seed?: number,
  ): DataFrame<R> {
    const entries = fractions instanceof Map ? [...fractions.entries()] : Object.entries(fractions);
    if (entries.length === 0) {
      throw new InvalidInputError("sampleBy() requires at least one stratum fraction.");
    }
    // The proto marks seed optional, but the server reads an absent seed as
    // zero, making repeated samples identical (SPARK-48184), so one is always
    // sent. A non-integer would otherwise fail as a raw RangeError in BigInt().
    if (seed !== undefined && !Number.isSafeInteger(seed)) {
      throw new InvalidInputError("sampleBy() seed must be a safe integer.");
    }
    for (const [stratum, fraction] of entries) {
      if (!Number.isFinite(fraction) || fraction < 0 || fraction > 1) {
        throw new InvalidInputError(
          `sampleBy() fraction for stratum ${String(stratum)} must be between 0 and 1.`,
        );
      }
    }

    return DataFrame._fromPlan<R>(this._df._session, {
      type: "statSampleBy",
      child: this._df._plan,
      col: (typeof col === "string" ? _col(col) : col)._expr,
      fractions: entries.map(([stratum, fraction]) => ({ stratum, fraction })),
      seed: seed ?? Math.floor(Math.random() * 2 ** 31),
    });
  }
}
