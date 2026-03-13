import { DataFrame } from "./data-frame.js";

/**
 * Statistic functions for DataFrames.
 * Accessed via `df.stat`.
 */
export class DataFrameStat {
  private readonly _df: DataFrame;

  /** @internal */
  constructor(df: DataFrame) {
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
}
