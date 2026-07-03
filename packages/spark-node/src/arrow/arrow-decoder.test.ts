import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { ArrowDecoder } from "./arrow-decoder.js";
import {
  tableFromArrays,
  tableToIPC,
  Table,
  RecordBatch,
  Vector,
  Field,
  Schema,
  Struct,
  Int,
  Utf8,
  Decimal,
  DateDay,
  DateMillisecond,
  TimestampMillisecond,
  TimestampMicrosecond,
  Map_,
  List,
  makeBuilder,
  makeData,
  type DataType,
} from "apache-arrow";

// apache-arrow's makeBuilder is parameterised on a Type literal that we
// cannot recover from a runtime DataType value, so its return type leaks as
// any without an explicit generic argument. Pin the generic to DataType once
// here so the rest of the file works with concrete Vector values.
function buildVector(type: DataType, values: readonly unknown[]): Vector {
  const builder = makeBuilder<DataType>({ type, nullValues: [null] });
  for (const v of values) builder.append(v);
  return builder.finish().toVector();
}

/** Build an IPC stream from a column-builder map. */
function makeIpc(fields: Field<DataType>[], values: unknown[][]): Uint8Array {
  const vectors: Vector[] = fields.map((f, i) => buildVector(f.type, values[i]));

  const data = makeData({
    type: new Struct(fields as never),
    length: values[0]?.length ?? 0,
    children: vectors.map((v) => v.data[0]) as never,
    nullCount: 0,
  });
  const batch = new RecordBatch(new Schema(fields), data);
  const table = new Table([batch]);
  return tableToIPC(table, "stream");
}

describe("ArrowDecoder.decode() — basic types", () => {
  it("returns empty array for no chunks", async () => {
    assert.deepStrictEqual(await ArrowDecoder.decode([]), []);
  });

  it("decodes integers, strings, booleans, floats together", async () => {
    const chunk = tableToIPC(
      tableFromArrays({ id: [1, 2], name: ["alice", "bob"], active: [true, false] }),
      "stream",
    );
    const rows = await ArrowDecoder.decode([chunk]);
    assert.equal(rows.length, 2);
    assert.equal(rows[0]["id"], 1);
    assert.equal(rows[0]["name"], "alice");
    assert.equal(rows[0]["active"], true);
  });

  it("preserves nulls", async () => {
    const chunk = makeIpc([new Field("x", new Int(true, 32), true)], [[1, null, 3]]);
    const rows = await ArrowDecoder.decode([chunk]);
    assert.equal(rows[0]["x"], 1);
    assert.equal(rows[1]["x"], null);
    assert.equal(rows[2]["x"], 3);
  });
});

describe("ArrowDecoder.decode() — Long (Int64)", () => {
  it("always returns bigint, regardless of magnitude", async () => {
    const chunk = makeIpc(
      [new Field("v", new Int(true, 64), true)],
      [[100n, BigInt(Number.MAX_SAFE_INTEGER), BigInt(Number.MAX_SAFE_INTEGER) + 1n]],
    );
    const rows = await ArrowDecoder.decode([chunk]);
    assert.equal(typeof rows[0]["v"], "bigint");
    assert.equal(rows[0]["v"], 100n);
    assert.equal(typeof rows[1]["v"], "bigint");
    assert.equal(rows[1]["v"], BigInt(Number.MAX_SAFE_INTEGER));
    assert.equal(typeof rows[2]["v"], "bigint");
    assert.equal(rows[2]["v"], BigInt(Number.MAX_SAFE_INTEGER) + 1n);
  });

  it("returns null for null values", async () => {
    const chunk = makeIpc([new Field("v", new Int(true, 64), true)], [[null]]);
    const rows = await ArrowDecoder.decode([chunk]);
    assert.equal(rows[0]["v"], null);
  });

  it("smaller ints (Int32) return number unchanged", async () => {
    const chunk = makeIpc([new Field("v", new Int(true, 32), true)], [[42]]);
    const rows = await ArrowDecoder.decode([chunk]);
    assert.equal(typeof rows[0]["v"], "number");
    assert.equal(rows[0]["v"], 42);
  });
});

describe("ArrowDecoder.decode() — Decimal", () => {
  function dec128(scale: number, precision: number, unscaledWords: Uint32Array[]): Uint8Array {
    const f = new Field("d", new Decimal(scale, precision, 128), true);
    return makeIpc([f], [unscaledWords]);
  }

  it("formats positive value at scale 2 as fixed-point string", async () => {
    // unscaled 150 = 1.50 at scale 2
    const chunk = dec128(2, 10, [new Uint32Array([150, 0, 0, 0])]);
    const rows = await ArrowDecoder.decode([chunk]);
    assert.equal(rows[0]["d"], "1.50");
  });

  it("formats negative value at scale 4", async () => {
    // unscaled -123456789 = -12345.6789 at scale 4
    // -123456789 as two's complement 128-bit:
    const neg = -123_456_789n;
    const mask = (1n << 128n) - 1n;
    const twos = (neg + (1n << 128n)) & mask;
    const u32 = new Uint32Array(4);
    for (let i = 0; i < 4; i++) {
      u32[i] = Number((twos >> BigInt(i * 32)) & 0xffffffffn);
    }
    const chunk = dec128(4, 18, [u32]);
    const rows = await ArrowDecoder.decode([chunk]);
    assert.equal(rows[0]["d"], "-12345.6789");
  });

  it("pads when unscaled value is smaller than scale (0.000001)", async () => {
    // unscaled 1000 at scale 9 = 0.000001000
    const chunk = dec128(9, 20, [new Uint32Array([1000, 0, 0, 0])]);
    const rows = await ArrowDecoder.decode([chunk]);
    assert.equal(rows[0]["d"], "0.000001000");
  });

  it("scale 0 returns the integer digits as a string", async () => {
    const chunk = dec128(0, 10, [new Uint32Array([42, 0, 0, 0])]);
    const rows = await ArrowDecoder.decode([chunk]);
    assert.equal(rows[0]["d"], "42");
  });

  it("preserves null", async () => {
    const f = new Field("d", new Decimal(2, 10, 128), true);
    const chunk = makeIpc([f], [[null]]);
    const rows = await ArrowDecoder.decode([chunk]);
    assert.equal(rows[0]["d"], null);
  });
});

describe("ArrowDecoder.decode() — Date and Timestamp", () => {
  it("DateDay decodes to a JS Date", async () => {
    const chunk = makeIpc([new Field("d", new DateDay(), true)], [[new Date("2026-06-29")]]);
    const rows = await ArrowDecoder.decode([chunk]);
    assert.ok(rows[0]["d"] instanceof Date);
    assert.equal(rows[0]["d"].toISOString(), "2026-06-29T00:00:00.000Z");
  });

  it("DateMillisecond decodes to a JS Date", async () => {
    const chunk = makeIpc(
      [new Field("d", new DateMillisecond(), true)],
      [[new Date("2026-06-29")]],
    );
    const rows = await ArrowDecoder.decode([chunk]);
    assert.ok(rows[0]["d"] instanceof Date);
  });

  it("TimestampMillisecond decodes to a JS Date", async () => {
    const chunk = makeIpc(
      [new Field("t", new TimestampMillisecond(), true)],
      [[new Date("2026-06-29T13:45:06.123Z")]],
    );
    const rows = await ArrowDecoder.decode([chunk]);
    assert.ok(rows[0]["t"] instanceof Date);
    assert.equal(rows[0]["t"].toISOString(), "2026-06-29T13:45:06.123Z");
  });

  it("TimestampMicrosecond decodes to a JS Date (sub-ms precision lost)", async () => {
    const chunk = makeIpc(
      [new Field("t", new TimestampMicrosecond(), true)],
      [[new Date("2026-06-29T13:45:06.123Z")]],
    );
    const rows = await ArrowDecoder.decode([chunk]);
    assert.ok(rows[0]["t"] instanceof Date);
  });
});

describe("ArrowDecoder.decode() — Map", () => {
  it("decodes Map<int32, utf8> to a JS Map with non-string keys preserved", async () => {
    const mapType = new Map_(
      new Field(
        "entries",
        new Struct([
          new Field("key", new Int(true, 32), false),
          new Field("value", new Utf8(), true),
        ] as never),
        false,
      ),
      false,
    );
    const chunk = makeIpc(
      [new Field("m", mapType, true)],
      [
        [
          new Map<number, string>([
            [1, "a"],
            [2, "b"],
          ]),
        ],
      ],
    );
    const rows = await ArrowDecoder.decode([chunk]);
    const m = rows[0]["m"] as Map<unknown, unknown>;
    assert.ok(m instanceof Map);
    assert.equal(m.get(1), "a");
    assert.equal(m.get(2), "b");
    // The non-string key stayed a number, not stringified
    assert.equal(typeof [...m.keys()][0], "number");
  });

  it("decodes Map<int64, utf8>", async () => {
    const mapType = new Map_(
      new Field(
        "entries",
        new Struct([
          new Field("key", new Int(true, 64), false),
          new Field("value", new Utf8(), true),
        ] as never),
        false,
      ),
      false,
    );
    const chunk = makeIpc(
      [new Field("m", mapType, true)],
      [[new Map<bigint, string>([[1n, "a"]])]],
    );
    const rows = await ArrowDecoder.decode([chunk]);
    const m = rows[0]["m"] as Map<unknown, unknown>;
    assert.equal(typeof [...m.keys()][0], "bigint");
    assert.equal(m.get(1n), "a");
  });
});

describe("ArrowDecoder.decode() — Struct", () => {
  it("decodes a struct to a plain object", async () => {
    const structType = new Struct([
      new Field("id", new Int(true, 32), true),
      new Field("name", new Utf8(), true),
    ]);
    const chunk = makeIpc([new Field("s", structType, true)], [[{ id: 1, name: "alice" }]]);
    const rows = await ArrowDecoder.decode([chunk]);
    assert.deepEqual(rows[0]["s"], { id: 1, name: "alice" });
  });

  it("recurses into nested types (long inside struct)", async () => {
    const structType = new Struct([
      new Field("id", new Int(true, 64), true),
      new Field("name", new Utf8(), true),
    ]);
    const chunk = makeIpc(
      [new Field("s", structType, true)],
      [[{ id: 9_000_000_000n, name: "x" }]],
    );
    const rows = await ArrowDecoder.decode([chunk]);
    const s = rows[0]["s"] as Record<string, unknown>;
    assert.equal(typeof s["id"], "bigint");
    assert.equal(s["id"], 9_000_000_000n);
  });
});

describe("ArrowDecoder.decode() — List", () => {
  it("decodes a list of int32 to an array", async () => {
    const listType = new List(new Field("item", new Int(true, 32), true));
    const chunk = makeIpc([new Field("xs", listType, true)], [[[1, 2, 3]]]);
    const rows = await ArrowDecoder.decode([chunk]);
    assert.deepEqual(rows[0]["xs"], [1, 2, 3]);
  });

  it("recurses into a list of int64", async () => {
    const listType = new List(new Field("item", new Int(true, 64), true));
    const chunk = makeIpc([new Field("xs", listType, true)], [[[100n, 9_000_000_000n]]]);
    const rows = await ArrowDecoder.decode([chunk]);
    const xs = rows[0]["xs"] as unknown[];
    assert.equal(typeof xs[0], "bigint");
    assert.equal(xs[1], 9_000_000_000n);
  });
});
