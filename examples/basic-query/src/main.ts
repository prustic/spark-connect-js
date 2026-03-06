/**
 * basic-query — Minimal Spark Connect example
 *
 * Prerequisites:
 *   docker compose up -d        # start Spark Connect on port 15002
 *
 * Run:
 *   pnpm build && node dist/main.js
 */

import { connect, col, lit } from "@spark-js/node";

const SPARK_REMOTE = process.env["SPARK_REMOTE"] ?? "sc://localhost:15002";

async function main(): Promise<void> {
  const spark = connect(SPARK_REMOTE);

  // ── 1. Simple SQL query ─────────────────────────────────────────────────
  const range = spark.sql("SELECT id, id * 2 AS doubled FROM range(10)");
  await range.show();

  // ── 2. DataFrame API: filter + select ───────────────────────────────────
  const filtered = range.filter(col("id").gt(lit(5))).select("id", "doubled");
  const rows = await filtered.collect();
  console.log(`Collected ${String(rows.length)} rows:`);
  console.log(JSON.stringify(rows, null, 2));
}

main().catch((err: unknown) => {
  console.error(err);
  process.exit(1);
});
