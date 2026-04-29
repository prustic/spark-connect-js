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
});
