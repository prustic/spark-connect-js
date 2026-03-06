/**
 * ─── SparkProcessManager ────────────────────────────────────────────────────
 *
 * Optional utility for spawning and managing a local Spark Connect server
 * process from Node.js.  Useful for local development and testing.
 *
 * @see Spark Connect server entry: connector/connect/server/src/main/scala/org/apache/spark/sql/connect/service/SparkConnectServer.scala
 * @see Spark submit: core/src/main/scala/org/apache/spark/deploy/SparkSubmit.scala
 *
 * ─── How Spark Connect Server Starts ────────────────────────────────────────
 *
 * The Spark Connect server is a JVM process:
 *   $SPARK_HOME/sbin/start-connect-server.sh \
 *     --packages org.apache.spark:spark-connect_2.13:3.5.0 \
 *     --conf spark.connect.grpc.binding.port=15002
 *
 * Or via spark-submit:
 *   spark-submit --class org.apache.spark.sql.connect.service.SparkConnectServer \
 *     --packages ... spark-connect-server.jar
 *
 * The server opens a gRPC listener (default :15002) and manages Spark sessions
 * on behalf of remote clients.  Each client session gets its own isolated
 * SparkSession on the JVM side.
 *
 * ─── child_process Considerations ───────────────────────────────────────────
 *
 *   • spawn() vs exec(): We use spawn() because exec() buffers ALL stdout/
 *     stderr in memory — the Spark server can produce GB of logs.
 *
 *   • Signal handling: JVM needs SIGTERM for graceful shutdown (runs shutdown
 *     hooks, flushes data).  If it doesn't exit within a timeout, escalate
 *     to SIGKILL.  Never send SIGKILL first — it skips JVM shutdown hooks
 *     and can corrupt shuffle files / temp directories.
 *
 *   • Detached mode: If the Node process exits, should the Spark server
 *     keep running?  We default to attached (killed on parent exit) but
 *     support detached mode via options.
 *
 *   • Health check: After spawning, we poll the gRPC port until the server
 *     accepts connections (or timeout).  The JVM takes several seconds to
 *     start — NEVER assume it's ready immediately after spawn.
 */

import { spawn, type ChildProcess } from "node:child_process";
import { once } from "node:events";

export interface SparkProcessOptions {
  /** Path to spark-submit or start-connect-server.sh */
  sparkHome: string;
  /** gRPC port (default: 15002) */
  port?: number;
  /** Additional Spark --conf key=value pairs */
  conf?: Record<string, string>;
  /** Additional --packages dependencies */
  packages?: string[];
  /** Detach the process so it survives Node.js exit */
  detached?: boolean;
  /** How long to wait for the server to accept connections (ms) */
  startupTimeoutMs?: number;
}

export class SparkProcessManager {
  private process: ChildProcess | null = null;
  private readonly options: Required<SparkProcessOptions>;

  constructor(options: SparkProcessOptions) {
    this.options = {
      port: 15002,
      conf: {},
      packages: [],
      detached: false,
      startupTimeoutMs: 60_000,
      ...options,
    };
  }

  /**
   * Spawn the Spark Connect server and wait until it's accepting gRPC
   * connections.
   *
   * @throws If the server fails to start within the configured timeout
   */
  async start(): Promise<void> {
    if (this.process) {
      throw new Error("Spark Connect server is already running.");
    }

    const args = this._buildArgs();
    const sparkSubmit = `${this.options.sparkHome}/bin/spark-submit`;

    this.process = spawn(sparkSubmit, args, {
      stdio: ["ignore", "pipe", "pipe"],
      detached: this.options.detached,
    });

    // Log server output for debugging (pipe to Node's stdout/stderr)
    this.process.stdout?.pipe(process.stdout);
    this.process.stderr?.pipe(process.stderr);

    // Wait for the gRPC port to become available
    await this._waitForReady();
  }

  /**
   * Gracefully stop the Spark Connect server.
   *
   * Sends SIGTERM first, allowing the JVM to run shutdown hooks (flush
   * shuffle data, clean temp dirs, deregister from cluster manager).
   * Escalates to SIGKILL after 10 seconds if the process hasn't exited.
   */
  async stop(): Promise<void> {
    if (!this.process) return;

    const proc = this.process;
    this.process = null;

    // SIGTERM → graceful JVM shutdown
    proc.kill("SIGTERM");

    // Race: either the process exits or we force-kill after 10s
    const exitPromise = once(proc, "exit");
    const timeout = new Promise<void>((resolve) => {
      const timer = setTimeout(() => {
        proc.kill("SIGKILL");
        resolve();
      }, 10_000);
      // Don't hold the event loop open for the timer
      timer.unref();
    });

    await Promise.race([exitPromise, timeout]);
  }

  /** Build spark-submit CLI arguments for the Connect server. */
  private _buildArgs(): string[] {
    const args: string[] = ["--class", "org.apache.spark.sql.connect.service.SparkConnectServer"];

    // Add packages (Spark Connect jar + user packages)
    const packages = ["org.apache.spark:spark-connect_2.13:3.5.0", ...this.options.packages];
    args.push("--packages", packages.join(","));

    // Add --conf entries
    const conf: Record<string, string> = {
      "spark.connect.grpc.binding.port": String(this.options.port),
      ...this.options.conf,
    };
    for (const [key, value] of Object.entries(conf)) {
      args.push("--conf", `${key}=${value}`);
    }

    return args;
  }

  /**
   * Poll the gRPC port until the server is accepting connections.
   * Uses Node's net module to attempt raw TCP connections.
   */
  private async _waitForReady(): Promise<void> {
    const { createConnection } = await import("node:net");
    const start = Date.now();

    while (Date.now() - start < this.options.startupTimeoutMs) {
      const isReady = await new Promise<boolean>((resolve) => {
        const socket = createConnection({ port: this.options.port, host: "127.0.0.1" }, () => {
          socket.destroy();
          resolve(true);
        });
        socket.on("error", () => {
          socket.destroy();
          resolve(false);
        });
        // Don't wait more than 1s per attempt
        socket.setTimeout(1000, () => {
          socket.destroy();
          resolve(false);
        });
      });

      if (isReady) return;

      // Wait 500ms before retrying
      await new Promise((r) => setTimeout(r, 500));
    }

    throw new Error(`Spark Connect server did not start within ${this.options.startupTimeoutMs}ms`);
  }
}
