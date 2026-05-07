import { describe, it, after } from "node:test";
import assert from "node:assert/strict";
import { connect, type SparkSession } from "@spark-connect-js/node";
import { startTcpProxy, type TcpProxy } from "./helpers/tcp-proxy.js";

const UPSTREAM_REMOTE = process.env["SPARK_REMOTE"] ?? "sc://localhost:15002";

function upstreamPort(remote: string): number {
  const match = /^sc:\/\/[^:]+:(\d+)/.exec(remote);
  if (!match) throw new Error(`unrecognised SPARK_REMOTE: ${remote}`);
  return Number.parseInt(match[1], 10);
}

describe("reattach survives a mid-query connection drop", () => {
  let proxy: TcpProxy | undefined;
  let session: SparkSession | undefined;

  after(async () => {
    await session?.stop();
    await proxy?.close();
  });

  it("collects all rows after a TCP RST mid-stream", async () => {
    proxy = await startTcpProxy(upstreamPort(UPSTREAM_REMOTE));
    session = connect(`sc://127.0.0.1:${String(proxy.port)}`);

    // Single-partition range so the server emits a deterministic stream of
    // ArrowBatch responses on one TCP connection. 50k int64 rows is far
    // larger than the 4KB drop threshold, so the proxy is guaranteed to RST
    // the connection mid-stream regardless of JIT/cache warmup.
    const N = 50_000;
    const dropped = proxy.dropAfterUpstreamBytes(4096);
    const rows = await session.range(0, N, 1, 1).collect();

    // Both promises must complete: the drop fired (reattach was actually
    // exercised) AND the resumed stream delivered every row.
    await dropped;
    assert.equal(rows.length, N);
  });
});
