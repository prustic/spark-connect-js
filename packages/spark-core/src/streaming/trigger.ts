import { InvalidInputError } from "../errors.js";

/**
 * Trigger policy for a streaming query, set via {@link DataStreamWriter.trigger}.
 *
 * Mirrors Spark's `org.apache.spark.sql.streaming.Trigger`. Each variant maps
 * to one case of the `trigger` oneof on `WriteStreamOperationStart`.
 *
 * @see [Spark source: Trigger.scala](https://github.com/apache/spark/blob/master/sql/api/src/main/scala/org/apache/spark/sql/streaming/Trigger.scala)
 *
 * @example
 * ```ts
 * df.writeStream.trigger(Trigger.processingTime("10 seconds")).start();
 * df.writeStream.trigger(Trigger.availableNow()).start();
 * ```
 */
export type Trigger =
  | { kind: "processingTime"; interval: string }
  | { kind: "availableNow" }
  | { kind: "once" }
  | { kind: "continuous"; interval: string };

/** Factory helpers for {@link Trigger}. */
export const Trigger = {
  /** Run a micro-batch every `interval` (e.g. `"10 seconds"`, `"1 minute"`). */
  processingTime(interval: string): Trigger {
    requireNonEmptyInterval(interval, "Trigger.processingTime");
    return { kind: "processingTime", interval };
  },

  /**
   * Process all currently-available data in one or more micro-batches and stop.
   * Preferred over {@link Trigger.once} for new code (Spark 3.3+).
   */
  availableNow(): Trigger {
    return { kind: "availableNow" };
  },

  /**
   * Process all currently-available data in a single batch and stop. Marked
   * deprecated upstream; use {@link Trigger.availableNow} instead.
   */
  once(): Trigger {
    return { kind: "once" };
  },

  /**
   * Continuous-mode processing with the given checkpoint interval
   * (e.g. `"1 second"`). Experimental in Spark; limited sink support.
   */
  continuous(interval: string): Trigger {
    requireNonEmptyInterval(interval, "Trigger.continuous");
    return { kind: "continuous", interval };
  },
};

function requireNonEmptyInterval(interval: string, where: string): void {
  if (interval.trim().length === 0) {
    throw new InvalidInputError(`${where}: interval must be a non-empty string.`);
  }
}
