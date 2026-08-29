import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { totalInputRows, normalizeProgress, type StreamingQueryProgress } from "./types.js";

// Captured from a live Spark 4.0 Connect server: windowed + watermarked rate
// query in append mode, so eventTime, stateOperators, sources, and sink are
// all populated. Guards the interface against drift from the real payload.
const CAPTURED_PROGRESS_JSON = `{
  "id": "eeef24cf-df9c-4590-b449-772a027aa14b",
  "runId": "eebdebf4-bc6f-44db-a776-caf3874506f8",
  "name": "progress_capture",
  "timestamp": "2026-07-05T16:43:59.367Z",
  "batchId": 1,
  "batchDuration": 1333,
  "durationMs": {
    "triggerExecution": 1333,
    "queryPlanning": 5,
    "getBatch": 0,
    "commitOffsets": 13,
    "latestOffset": 0,
    "addBatch": 1302,
    "walCommit": 12
  },
  "eventTime": {
    "min": "2026-07-05T16:43:57.756Z",
    "avg": "2026-07-05T16:43:58.246Z",
    "watermark": "1970-01-01T00:00:00.000Z",
    "max": "2026-07-05T16:43:58.736Z"
  },
  "stateOperators": [
    {
      "operatorName": "stateStoreSave",
      "numRowsTotal": 2,
      "numRowsUpdated": 2,
      "allUpdatesTimeMs": 70,
      "numRowsRemoved": 0,
      "allRemovalsTimeMs": 268,
      "commitTimeMs": 9722,
      "memoryUsedBytes": 85200,
      "numRowsDroppedByWatermark": 0,
      "numShufflePartitions": 200,
      "numStateStoreInstances": 200,
      "customMetrics": {
        "stateOnCurrentVersionSizeBytes": 24400,
        "loadedMapCacheHitCount": 400,
        "loadedMapCacheMissCount": 0
      }
    }
  ],
  "sources": [
    {
      "description": "RateStreamV2[rowsPerSecond=50, rampUpTimeSeconds=0, numPartitions=1",
      "startOffset": "0",
      "endOffset": "1",
      "latestOffset": "1",
      "numInputRows": 50,
      "inputRowsPerSecond": 31.36762860727729,
      "processedRowsPerSecond": 37.50937734433609,
      "metrics": {}
    }
  ],
  "sink": {
    "description": "MemorySink",
    "numOutputRows": 0,
    "metrics": {}
  },
  "observedMetrics": {}
}`;

describe("StreamingQueryProgress shape", () => {
  const progress = JSON.parse(CAPTURED_PROGRESS_JSON) as StreamingQueryProgress;

  it("typed access reaches every named top-level field in the captured payload", () => {
    assert.equal(progress.id, "eeef24cf-df9c-4590-b449-772a027aa14b");
    assert.equal(progress.name, "progress_capture");
    assert.equal(progress.batchId, 1);
    assert.equal(progress.batchDuration, 1333);
    assert.equal(progress.durationMs?.triggerExecution, 1333);
    assert.equal(progress.eventTime?.watermark, "1970-01-01T00:00:00.000Z");
    assert.deepStrictEqual(progress.observedMetrics, {});
  });

  it("top-level row counts are absent on Connect; sources carry them", () => {
    assert.equal(progress.numInputRows, undefined);
    assert.equal(progress.inputRowsPerSecond, undefined);

    const source = progress.sources?.[0];
    assert.ok(source);
    assert.equal(source.numInputRows, 50);
    assert.equal(typeof source.inputRowsPerSecond, "number");
    assert.equal(source.startOffset, "0");
    assert.deepStrictEqual(source.metrics, {});
  });

  it("state operator fields are all numeric with numeric custom metrics", () => {
    const op = progress.stateOperators?.[0];
    assert.ok(op);
    assert.equal(op.operatorName, "stateStoreSave");
    assert.equal(op.numRowsTotal, 2);
    assert.equal(op.numRowsDroppedByWatermark, 0);
    assert.equal(op.numStateStoreInstances, 200);
    assert.equal(op.customMetrics["loadedMapCacheHitCount"], 400);
  });

  it("sink progress carries description, row count, and metrics", () => {
    assert.equal(progress.sink?.description, "MemorySink");
    assert.equal(progress.sink?.numOutputRows, 0);
  });

  it("totalInputRows sums the per-source counts", () => {
    assert.equal(totalInputRows(progress), 50);
    assert.equal(totalInputRows({}), 0);
  });

  it("index signature admits unknown fields without weakening named ones", () => {
    const extended = { ...progress, futureField: { anything: true } };
    assert.equal(extended.batchId, 1);
    assert.equal(typeof extended["futureField"], "object");
  });
});

// Listener-bus events use a different server-side serializer. Captured from
// the first onQueryProgress of the same live query shape.
const CAPTURED_BUS_EVENT_JSON = `{
  "id": "48fede23-191b-4c6c-b497-b8b62d7b4e58",
  "runId": "f0922819-e8da-4dfd-a1c9-8a82373a5d84",
  "name": "bus_capture",
  "timestamp": "2026-07-05T17:04:37.497Z",
  "batchId": 0,
  "batchDuration": 1244,
  "numInputRows": 0,
  "inputRowsPerSecond": 0,
  "processedRowsPerSecond": 0,
  "durationMs": {
    "addBatch": 1184,
    "commitOffsets": 11,
    "getBatch": 0,
    "latestOffset": 0,
    "queryPlanning": 37,
    "triggerExecution": 1244,
    "walCommit": 12
  },
  "eventTime": {
    "watermark": "1970-01-01T00:00:00.000Z"
  },
  "stateOperators": [
    {
      "operatorName": "stateStoreSave",
      "numRowsTotal": 0,
      "numRowsUpdated": 0,
      "allUpdatesTimeMs": 27,
      "numRowsRemoved": 0,
      "allRemovalsTimeMs": 188,
      "commitTimeMs": 6438,
      "memoryUsedBytes": 48000,
      "numRowsDroppedByWatermark": 0,
      "numShufflePartitions": 200,
      "numStateStoreInstances": 200,
      "customMetrics": {
        "loadedMapCacheHitCount": 0,
        "loadedMapCacheMissCount": 0,
        "stateOnCurrentVersionSizeBytes": 19200
      }
    }
  ],
  "sources": [
    {
      "description": "RateStreamV2[rowsPerSecond=50, rampUpTimeSeconds=0, numPartitions=1",
      "startOffset": null,
      "endOffset": 0,
      "latestOffset": 0,
      "numInputRows": 0,
      "inputRowsPerSecond": 0,
      "processedRowsPerSecond": 0
    }
  ],
  "sink": {
    "description": "MemorySink",
    "numOutputRows": 0
  }
}`;

describe("StreamingQueryProgress shape, listener-bus path", () => {
  const progress = JSON.parse(CAPTURED_BUS_EVENT_JSON) as StreamingQueryProgress;

  it("batch-0 offsets arrive null and numeric", () => {
    const source = progress.sources?.[0];
    assert.ok(source);
    assert.equal(source.startOffset, null);
    assert.equal(source.endOffset, 0);
    assert.equal(source.latestOffset, 0);
  });

  it("empty metrics maps are omitted", () => {
    assert.equal(progress.sources?.[0].metrics, undefined);
    assert.equal(progress.sink?.metrics, undefined);
  });

  it("top-level row counts are present on this path", () => {
    assert.equal(progress.numInputRows, 0);
    assert.equal(progress.inputRowsPerSecond, 0);
    assert.equal(totalInputRows(progress), 0);
  });

  it("eventTime carries only the watermark before any data", () => {
    assert.deepStrictEqual(progress.eventTime, { watermark: "1970-01-01T00:00:00.000Z" });
  });
});

describe("normalizeProgress observed metrics", () => {
  it("reshapes the server's {values, schema} wrapper into a Row", () => {
    const progress = normalizeProgress({
      observedMetrics: {
        m: {
          values: [20, "x"],
          schema: { type: "struct", fields: [{ name: "cnt" }, { name: "label" }] },
        },
      },
    });

    assert.deepStrictEqual(progress.observedMetrics, { m: { cnt: 20, label: "x" } });
  });

  it("leaves a payload that is already row-shaped untouched", () => {
    const progress = normalizeProgress({
      observedMetrics: { m: { cnt: 20 } },
    });

    assert.deepStrictEqual(progress.observedMetrics, { m: { cnt: 20 } });
  });

  it("falls back to positional names when the schema is absent", () => {
    const progress = normalizeProgress({
      observedMetrics: { m: { values: [1, 2] } },
    });

    assert.deepStrictEqual(progress.observedMetrics, { m: { col_0: 1, col_1: 2 } });
  });

  it("passes through a progress report with no observed metrics", () => {
    const progress = normalizeProgress({ id: "q" });
    assert.equal(progress.observedMetrics, undefined);
  });
});
