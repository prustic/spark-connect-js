import { connect, type SparkSession } from "@spark-connect-js/node";

const SPARK_REMOTE = process.env["SPARK_REMOTE"] ?? "sc://localhost:15002";

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
