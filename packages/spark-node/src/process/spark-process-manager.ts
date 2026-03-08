/**
 * Spawns and manages a local Spark Connect server process.
 * Useful for local development and testing.
 *
 * @see connector/connect/server/src/main/scala/org/apache/spark/sql/connect/service/SparkConnectServer.scala
 * @see core/src/main/scala/org/apache/spark/deploy/SparkSubmit.scala
 *
 * child_process notes:
 *   - spawn() over exec() — the server can produce GB of logs.
 *   - SIGTERM first for graceful JVM shutdown; SIGKILL only after timeout.
 *   - After spawning, poll the gRPC port before returning — JVM startup
 *     takes several seconds.
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

  /** Spawn the server and wait until it accepts gRPC connections. */
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

    // Log server output for debugging
    this.process.stdout?.on("data", (chunk: Buffer) => process.stdout.write(chunk));
    this.process.stderr?.on("data", (chunk: Buffer) => process.stderr.write(chunk));

    // Wait for the gRPC port to become available
    await this._waitForReady();
  }

  /** Stop the server. Sends SIGTERM first, escalates to SIGKILL after 10s. */
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
