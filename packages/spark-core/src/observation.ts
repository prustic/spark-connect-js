import { InvalidInputError } from "./errors.js";
import type { Row } from "./types/row.js";

/**
 * Named observation attached to a DataFrame via `df.observe(observation, ...)`.
 *
 * Metrics are computed server-side alongside the observed query and delivered
 * with the action's response stream, so reading them costs no second pass.
 *
 * @remarks
 * Mirrors PySpark's `Observation`, except that `get` does not block. Awaiting
 * the action already guarantees the metrics have arrived.
 *
 * @see [PySpark source: observation.py](https://github.com/apache/spark/blob/master/python/pyspark/sql/observation.py)
 *
 * @example
 *   const obs = new Observation("stats");
 *   const rows = await df.observe(obs, count("*").alias("rows")).collect();
 *   obs.get; // { rows: 42n }
 */
export class Observation {
  readonly name: string;
  private _metrics: Row | undefined;
  private _registered = false;

  constructor(name: string) {
    if (typeof name !== "string" || name.length === 0) {
      throw new InvalidInputError("Observation requires a non-empty name.");
    }
    this.name = name;
  }

  /**
   * The observed metrics row. Throws when no action has completed on the
   * observed DataFrame yet. An action must drain its result stream for
   * metrics to arrive; breaking out of `toLocalIterator()` early skips them.
   */
  get get(): Row {
    if (this._metrics === undefined) {
      throw new InvalidInputError(
        `Observation "${this.name}" has no metrics yet. ` +
          "Run an action on the observed DataFrame first.",
      );
    }
    return this._metrics;
  }

  /** @internal Marks single-use; `DataFrame.observe` rejects reuse. */
  _register(): void {
    if (this._registered) {
      throw new InvalidInputError(
        `Observation "${this.name}" has already been used. ` +
          "Create a new Observation per observed DataFrame.",
      );
    }
    this._registered = true;
  }

  /** @internal Set by the session when the transport delivers metrics. */
  _record(metrics: Row): void {
    this._metrics = metrics;
  }
}
