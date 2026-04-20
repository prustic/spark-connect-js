import { randomUUID } from "node:crypto";
import { connect, type SparkSession } from "@spark-connect-js/node";

const SPARK_REMOTE = process.env["SPARK_REMOTE"] ?? "sc://localhost:15002";
const RUN_ID = randomUUID().slice(0, 8);

let session: SparkSession | undefined;

export function spark(): SparkSession {
  session ??= connect(SPARK_REMOTE);
  return session;
}

export async function stopSession(): Promise<void> {
  if (session) {
    await session.stop();
    session = undefined;
  }
}

/** Server-side temp path unique to this test run. */
export function tempPath(name: string): string {
  return `/tmp/spark_test_${RUN_ID}_${name}`;
}

/** Table name unique to this test run. */
export function tempTable(name: string): string {
  return `test_${RUN_ID}_${name}`;
}
