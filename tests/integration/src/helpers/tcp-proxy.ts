import * as net from "node:net";

/**
 * In-process TCP forwarding proxy used by the reattach integration test.
 *
 * Listens on a random local port and forwards each accepted connection to
 * `upstreamPort` on localhost. Exposes two ways to drop the connection:
 *
 *   - `dropAll()` immediately destroys all open sockets.
 *   - `dropAfterUpstreamBytes(n)` arms a one-shot drop that fires the first
 *     time cumulative upstream→client bytes (across all connections) cross
 *     the threshold. This is the deterministic trigger the reattach test
 *     uses: by counting bytes rather than wall-clock time, the drop is
 *     guaranteed to fire mid-stream regardless of JIT/cache warmup.
 *
 * `socket.destroy()` causes a TCP RST, which gRPC surfaces as UNAVAILABLE
 * — fast enough that the test does not have to wait for the 30s gRPC
 * keepalive timeout to notice the drop.
 */
export interface TcpProxy {
  port: number;
  /** Drop all currently open sockets (forces a TCP RST on each side). */
  dropAll: () => void;
  /**
   * Arm a one-shot drop after `n` cumulative bytes have flowed
   * upstream→client. Returns a promise that resolves when the drop fires,
   * or rejects on `close()` if it never fired.
   */
  dropAfterUpstreamBytes: (n: number) => Promise<void>;
  close: () => Promise<void>;
}

export function startTcpProxy(upstreamPort: number): Promise<TcpProxy> {
  const sockets = new Set<net.Socket>();
  let upstreamBytes = 0;
  let armed: { threshold: number; resolve: () => void; reject: (e: Error) => void } | null = null;

  const dropAll = (): void => {
    for (const s of sockets) s.destroy();
    sockets.clear();
  };

  const server = net.createServer((client) => {
    const upstream = net.connect(upstreamPort, "127.0.0.1");
    sockets.add(client);
    sockets.add(upstream);

    upstream.on("data", (chunk: Buffer) => {
      upstreamBytes += chunk.length;
      if (armed && upstreamBytes >= armed.threshold) {
        const { resolve } = armed;
        armed = null;
        dropAll();
        resolve();
      }
    });
    upstream.pipe(client);
    client.pipe(upstream);

    const cleanup = (s: net.Socket) => () => sockets.delete(s);
    client.on("close", cleanup(client));
    upstream.on("close", cleanup(upstream));
    client.on("error", () => undefined);
    upstream.on("error", () => undefined);
  });

  return new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const addr = server.address();
      if (addr === null || typeof addr === "string") {
        reject(new Error(`unexpected proxy listen address: ${String(addr)}`));
        return;
      }
      resolve({
        port: addr.port,
        dropAll,
        dropAfterUpstreamBytes: (n) =>
          new Promise<void>((res, rej) => {
            armed = { threshold: n, resolve: res, reject: rej };
          }),
        close: () =>
          new Promise<void>((r, rj) => {
            if (armed) {
              armed.reject(new Error("proxy closed before drop fired"));
              armed = null;
            }
            dropAll();
            server.close((err) => (err ? rj(err) : r()));
          }),
      });
    });
  });
}
