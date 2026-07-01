import { describe, it } from "node:test";
import assert from "node:assert/strict";
import * as grpc from "@grpc/grpc-js";
import { create } from "@bufbuild/protobuf";
import { ExecutePlanResponseSchema, type ExecutePlanResponse } from "@spark-connect-js/connect";
import { iterateWithReattach } from "./reattach.js";
import { type RetryPolicy } from "./retry.js";

const fastPolicy: RetryPolicy = {
  maxRetries: 3,
  initialBackoffMs: 1,
  maxBackoffMs: 1,
  backoffMultiplier: 1,
  jitterMs: 0,
  maxConsecutiveNoProgressReattaches: 0,
};
const noSleep = (): Promise<void> => Promise.resolve();

function arrowBatch(responseId: string, bytes: number[]): ExecutePlanResponse {
  return create(ExecutePlanResponseSchema, {
    responseId,
    responseType: {
      case: "arrowBatch",
      value: { data: new Uint8Array(bytes), rowCount: BigInt(bytes.length) },
    },
  });
}

function resultComplete(responseId: string): ExecutePlanResponse {
  return create(ExecutePlanResponseSchema, {
    responseId,
    responseType: { case: "resultComplete", value: {} },
  });
}

async function* gen(...items: (ExecutePlanResponse | Error)[]): AsyncIterable<ExecutePlanResponse> {
  for (const item of items) {
    if (item instanceof Error) throw item;
    yield item;
  }
}

function transientError(message = "stream lost"): Error & { code: number } {
  const e = new Error(message) as Error & { code: number };
  e.code = grpc.status.UNAVAILABLE;
  return e;
}

async function collect(it: AsyncIterable<Uint8Array>): Promise<Uint8Array[]> {
  const out: Uint8Array[] = [];
  for await (const b of it) out.push(b);
  return out;
}

describe("iterateWithReattach", () => {
  it("yields arrow batches and stops on resultComplete (no reattach needed)", async () => {
    const reattachCalls: (string | undefined)[] = [];
    const out = await collect(
      iterateWithReattach({
        initial: () =>
          gen(arrowBatch("r1", [1, 2]), arrowBatch("r2", [3, 4]), resultComplete("r3")),
        reattach: (id) => {
          reattachCalls.push(id);
          return gen();
        },
        retryPolicy: fastPolicy,
        sleep: noSleep,
      }),
    );
    assert.equal(out.length, 2);
    assert.deepStrictEqual(Array.from(out[0]), [1, 2]);
    assert.deepStrictEqual(Array.from(out[1]), [3, 4]);
    assert.equal(reattachCalls.length, 0);
  });

  it("reattaches after a retryable mid-stream failure and resumes from lastResponseId", async () => {
    const reattachCalls: (string | undefined)[] = [];
    const out = await collect(
      iterateWithReattach({
        initial: () => gen(arrowBatch("r1", [1]), arrowBatch("r2", [2]), transientError("drop")),
        reattach: (id) => {
          reattachCalls.push(id);
          return gen(arrowBatch("r3", [3]), resultComplete("r4"));
        },
        retryPolicy: fastPolicy,
        sleep: noSleep,
      }),
    );
    assert.deepStrictEqual(
      out.map((u) => Array.from(u)),
      [[1], [2], [3]],
    );
    // Reattach was called once with the lastResponseId we had at the drop.
    assert.deepStrictEqual(reattachCalls, ["r2"]);
  });

  it("reattaches multiple times if reattach itself drops", async () => {
    const reattachCalls: (string | undefined)[] = [];
    const out = await collect(
      iterateWithReattach({
        initial: () => gen(arrowBatch("r1", [1]), transientError()),
        reattach: (id) => {
          reattachCalls.push(id);
          if (reattachCalls.length === 1) {
            return gen(arrowBatch("r2", [2]), transientError());
          }
          return gen(arrowBatch("r3", [3]), resultComplete("r4"));
        },
        retryPolicy: fastPolicy,
        sleep: noSleep,
      }),
    );
    assert.deepStrictEqual(
      out.map((u) => Array.from(u)),
      [[1], [2], [3]],
    );
    assert.deepStrictEqual(reattachCalls, ["r1", "r2"]);
  });

  it("throws non-retryable errors immediately, no reattach attempt", async () => {
    let reattachCalled = false;
    await assert.rejects(
      collect(
        iterateWithReattach({
          initial: () => {
            const e = new Error("bad request") as Error & { code: number };
            e.code = grpc.status.INVALID_ARGUMENT;
            return gen(arrowBatch("r1", [1]), e);
          },
          reattach: () => {
            reattachCalled = true;
            return gen();
          },
          retryPolicy: fastPolicy,
          sleep: noSleep,
        }),
      ),
    );
    assert.equal(reattachCalled, false);
  });

  it("gives up after maxRetries reattach attempts", async () => {
    let reattachCalls = 0;
    await assert.rejects(
      collect(
        iterateWithReattach({
          initial: () => gen(transientError()),
          reattach: () => {
            reattachCalls++;
            return gen(transientError());
          },
          retryPolicy: fastPolicy,
          sleep: noSleep,
        }),
      ),
    );
    assert.equal(reattachCalls, fastPolicy.maxRetries);
  });

  it("invokes onResponse for every response across initial and reattach streams", async () => {
    const seen: string[] = [];
    await collect(
      iterateWithReattach({
        initial: () => gen(arrowBatch("r1", [1]), arrowBatch("r2", [2]), transientError()),
        reattach: () => gen(arrowBatch("r3", [3]), resultComplete("r4")),
        retryPolicy: fastPolicy,
        sleep: noSleep,
        onResponse: (response) => {
          seen.push(response.responseId);
        },
      }),
    );
    assert.deepStrictEqual(seen, ["r1", "r2", "r3", "r4"]);
  });

  it("reattach receives undefined lastResponseId when no response was seen", async () => {
    const reattachCalls: (string | undefined)[] = [];
    await collect(
      iterateWithReattach({
        initial: () => gen(transientError()),
        reattach: (id) => {
          reattachCalls.push(id);
          return gen(resultComplete("r1"));
        },
        retryPolicy: fastPolicy,
        sleep: noSleep,
      }),
    );
    assert.deepStrictEqual(reattachCalls, [undefined]);
  });

  it("bails with REATTACH_NO_PROGRESS after N consecutive zero-yield reattaches", async () => {
    const capPolicy: RetryPolicy = { ...fastPolicy, maxConsecutiveNoProgressReattaches: 3 };
    await assert.rejects(
      collect(
        iterateWithReattach({
          initial: () => gen(transientError()),
          reattach: () => gen(transientError()),
          retryPolicy: capPolicy,
          sleep: noSleep,
        }),
      ),
      (err: unknown) => {
        if (!(err instanceof Error)) return false;
        assert.match(err.message, /no progress/i);
        assert.match(err.message, /3 consecutive stream attempts/);
        return true;
      },
    );
  });

  it("resets the no-progress counter when a stream yields a batch before failing", async () => {
    const capPolicy: RetryPolicy = { ...fastPolicy, maxConsecutiveNoProgressReattaches: 3 };
    const batches = await collect(
      iterateWithReattach({
        initial: () => gen(transientError()),
        reattach: (() => {
          let n = 0;
          return () => {
            n++;
            if (n === 1) return gen(transientError());
            if (n === 2) return gen(arrowBatch("r1", [1]), transientError());
            return gen(resultComplete("r2"));
          };
        })(),
        retryPolicy: capPolicy,
        sleep: noSleep,
      }),
    );
    assert.deepStrictEqual(
      batches.map((b) => Array.from(b)),
      [[1]],
    );
  });

  it("does not enforce the no-progress ceiling when maxConsecutiveNoProgressReattaches is 0", async () => {
    let calls = 0;
    const disabledPolicy: RetryPolicy = {
      ...fastPolicy,
      maxRetries: 10,
      maxConsecutiveNoProgressReattaches: 0,
    };
    const batches = await collect(
      iterateWithReattach({
        initial: () => gen(transientError()),
        reattach: () => {
          calls++;
          if (calls >= 5) return gen(resultComplete("done"));
          return gen(transientError());
        },
        retryPolicy: disabledPolicy,
        sleep: noSleep,
      }),
    );
    assert.equal(batches.length, 0);
    assert.equal(calls, 5);
  });

  it("resets the retry attempt counter (and its backoff) when a stream yields a batch", async () => {
    const growingPolicy: RetryPolicy = {
      maxRetries: 10,
      initialBackoffMs: 10,
      maxBackoffMs: 10_000,
      backoffMultiplier: 2,
      jitterMs: 0,
      maxConsecutiveNoProgressReattaches: 0,
    };
    const sleeps: number[] = [];
    const recordSleep = (ms: number): Promise<void> => {
      sleeps.push(ms);
      return Promise.resolve();
    };
    let call = 0;
    await collect(
      iterateWithReattach({
        initial: () => gen(arrowBatch("r1", [1]), transientError()),
        reattach: () => {
          call++;
          if (call === 1) return gen(arrowBatch("r2", [2]), transientError());
          if (call === 2) return gen(arrowBatch("r3", [3]), transientError());
          return gen(resultComplete("r4"));
        },
        retryPolicy: growingPolicy,
        sleep: recordSleep,
      }),
    );
    assert.deepStrictEqual(sleeps, [10, 10, 10]);
  });
});
