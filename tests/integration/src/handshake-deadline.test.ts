import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { SparkSession, GrpcTransport, SparkConnectError } from "@spark-connect-js/node";
import { ArrowDecoder } from "@spark-connect-js/node";

describe("handshake deadline", () => {
  it("surfaces CONNECTION_TIMEOUT through waitForReady when the endpoint is unreachable", async () => {
    // 127.0.0.1:1 refuses immediately, so TCP handles close cleanly. The
    // deadline value itself is covered by the unit test.
    const transport = new GrpcTransport({
      host: "127.0.0.1",
      port: 1,
      handshakeTimeoutMs: 500,
    });
    const dead = SparkSession.builder()
      .remote("sc://127.0.0.1:1")
      .transport(transport)
      .arrowDecoder((chunks) => ArrowDecoder.decode(chunks))
      .getOrCreate();

    let caught: unknown;
    try {
      await dead.range(1).collect();
    } catch (err) {
      caught = err;
    }

    try {
      await dead.stop();
    } catch {
      // stop() awaits the same cached handshake rejection.
    }
    transport.close();

    if (!(caught instanceof SparkConnectError)) {
      throw new Error(`expected SparkConnectError, got ${String(caught)}`);
    }
    assert.equal(caught.errorClass, "CONNECTION_TIMEOUT");
  });
});
