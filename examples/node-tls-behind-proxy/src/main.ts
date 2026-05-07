import { connect } from "@spark-connect-js/node";

const SPARK_REMOTE = process.env["SPARK_REMOTE"] ?? "sc://localhost:8443/;use_ssl=true";

const session = connect(SPARK_REMOTE);

console.log(`Connecting to ${SPARK_REMOTE}`);

const count = await session.range(0, 1_000_000, 1, 1).count();
console.log(`Counted ${count.toLocaleString()} rows over TLS.`);

const version = await session.version();
console.log(`Server Spark version: ${version}`);

await session.stop();
