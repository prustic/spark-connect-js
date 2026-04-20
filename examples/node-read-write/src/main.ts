import { connect, col } from "@spark-connect-js/node";

const SPARK_REMOTE = process.env["SPARK_REMOTE"] ?? "sc://localhost:15002";
const session = connect(SPARK_REMOTE);
const tmpDir = "/tmp/spark-read-write-example";

// One dataset, every format Spark supports.
const sensors = session.sql(`
  SELECT * FROM VALUES
    (1, 'temperature', 23.4, '2025-06-01'),
    (2, 'humidity',    55.1, '2025-06-01'),
    (3, 'temperature', 24.8, '2025-06-01'),
    (4, 'pressure',   1013.2, '2025-06-01'),
    (5, 'humidity',    52.3, '2025-06-02'),
    (6, 'temperature', 22.1, '2025-06-02'),
    (7, 'pressure',   1015.0, '2025-06-02'),
    (8, 'temperature', 25.5, '2025-06-02')
  AS readings(id, sensor_type, value, date)
`);

const sorted = col("id").asc();

// CSV: write with header, read back with an explicit DDL schema
await sensors.write.mode("overwrite").option("header", "true").csv(`${tmpDir}/csv`);
const fromCsv = await session.read
  .schema("id INT, sensor_type STRING, value DOUBLE, date STRING")
  .option("header", "true")
  .csv(`${tmpDir}/csv`)
  .sort(sorted)
  .collect();
console.log("CSV roundtrip (with DDL schema):");
console.table(fromCsv);

// JSON
await sensors.write.mode("overwrite").json(`${tmpDir}/json`);
const fromJson = await session.read.json(`${tmpDir}/json`).sort(sorted).collect();
console.log("JSON roundtrip:");
console.table(fromJson);

// Parquet (columnar, compressed by default)
await sensors.write.mode("overwrite").parquet(`${tmpDir}/parquet`);
const fromParquet = await session.read.parquet(`${tmpDir}/parquet`).sort(sorted).collect();
console.log("Parquet roundtrip:");
console.table(fromParquet);

// ORC (columnar, common in Hive ecosystems)
await sensors.write.mode("overwrite").orc(`${tmpDir}/orc`);
const fromOrc = await session.read.orc(`${tmpDir}/orc`).sort(sorted).collect();
console.log("ORC roundtrip:");
console.table(fromOrc);

// Text (requires a single string column named "value")
const textDf = sensors.selectExpr("concat(sensor_type, ':', cast(value as string)) as value");
await textDf.write.mode("overwrite").text(`${tmpDir}/text`);
const fromText = await session.read.text(`${tmpDir}/text`).collect();
console.log("Text roundtrip:");
console.table(fromText);

// Bucketed table: hash-distribute rows across 4 buckets by sensor_type
await sensors.write.mode("overwrite").bucketBy(4, "sensor_type").saveAsTable("sensor_bucketed");
const bucketed = await session.read.table("sensor_bucketed").sort(sorted).collect();
console.log("Bucketed table (4 buckets on sensor_type):");
console.table(bucketed);

// insertInto: append new readings to the existing table
const newReadings = session.sql(`
  SELECT * FROM VALUES
    (9,  'temperature', 21.0, '2025-06-03'),
    (10, 'humidity',    58.7, '2025-06-03')
  AS readings(id, sensor_type, value, date)
`);
await newReadings.write.insertInto("sensor_bucketed");
const afterInsert = await session.read.table("sensor_bucketed").sort(sorted).collect();
console.log("After insertInto (2 new readings appended):");
console.table(afterInsert);

await session.stop();
console.log("\nDone.");
