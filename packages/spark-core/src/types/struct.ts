/**
 * Mirrors Spark's StructType and StructField for schema inspection.
 *
 * @see [Spark source: StructType.scala](https://github.com/apache/spark/blob/master/sql/api/src/main/scala/org/apache/spark/sql/types/StructType.scala)
 * @see [Spark source: StructField.scala](https://github.com/apache/spark/blob/master/sql/api/src/main/scala/org/apache/spark/sql/types/StructField.scala)
 */
export class StructField {
  readonly name: string;
  readonly dataType: string;
  readonly nullable: boolean;
  readonly metadata: Record<string, unknown>;

  constructor(
    name: string,
    dataType: string,
    nullable = true,
    metadata: Record<string, unknown> = {},
  ) {
    this.name = name;
    this.dataType = dataType;
    this.nullable = nullable;
    this.metadata = { ...metadata };
  }

  toString(): string {
    return `StructField(${this.name}, ${this.dataType}, ${String(this.nullable)})`;
  }
}

/**
 * Mirrors Spark's StructType and StructField for schema inspection. Returned
 * by `DataFrame.schema()`; accepted anywhere a DDL schema string goes via
 * {@link StructType.toDDL}.
 *
 * @see [Spark source: StructType.scala](https://github.com/apache/spark/blob/master/sql/api/src/main/scala/org/apache/spark/sql/types/StructType.scala)
 * @see [Spark source: StructField.scala](https://github.com/apache/spark/blob/master/sql/api/src/main/scala/org/apache/spark/sql/types/StructField.scala)
 */
export class StructType {
  readonly fields: readonly StructField[];

  constructor(fields: StructField[] = []) {
    this.fields = [...fields];
  }

  /** Add a field and return a new StructType. */
  add(name: string, dataType: string, nullable = true): StructType {
    return new StructType([...this.fields, new StructField(name, dataType, nullable)]);
  }

  /** Get field names. */
  get fieldNames(): string[] {
    return this.fields.map((f) => f.name);
  }

  /** Look up a field by name. */
  getField(name: string): StructField | undefined {
    return this.fields.find((f) => f.name === name);
  }

  /** Number of top-level fields. */
  get length(): number {
    return this.fields.length;
  }

  /**
   * Pretty-print the schema in Spark's tree format:
   *   root
   *    |-- name: string (nullable = true)
   *    |-- age: integer (nullable = false)
   */
  treeString(): string {
    const lines = ["root"];
    for (const field of this.fields) {
      lines.push(` |-- ${field.name}: ${field.dataType} (nullable = ${String(field.nullable)})`);
    }
    return lines.join("\n");
  }

  toString(): string {
    return this.treeString();
  }

  /**
   * Return a DDL-formatted schema string, e.g. `"name string, age integer"`.
   * Field names that are not plain identifiers are back-tick quoted.
   *
   * Compatible with `DataFrameReader.schema()`. Non-nullable fields render as
   * `NOT NULL`, which `createDataFrame(rows, ddl)` rejects because encoded rows
   * are always nullable. Pass the {@link StructType} there instead.
   */
  toDDL(): string {
    return this.fields
      .map((f) => `${quoteIfNeeded(f.name)} ${f.dataType}${f.nullable ? "" : " NOT NULL"}`)
      .join(", ");
  }

  /**
   * Build a StructType from a Spark Connect DataType proto (schema response).
   * The proto is a plain object with `struct.fields` array.
   */
  static fromProto(proto: Record<string, unknown>): StructType {
    // Shape 1: { struct: { fields: [...] } }
    const struct = proto as { struct?: { fields?: ProtoField[] } };
    if (struct.struct?.fields) {
      return new StructType(struct.struct.fields.map(parseProtoField));
    }
    // Shape 2: { kind: { case: "struct", value: { fields: [...] } } } (protobuf oneof)
    const kind = proto as { kind?: { case?: string; value?: { fields?: ProtoField[] } } };
    if (kind.kind?.case === "struct" && kind.kind.value?.fields) {
      return new StructType(kind.kind.value.fields.map(parseProtoField));
    }
    // Shape 3: top-level { fields: [...] }
    const top = proto as { fields?: ProtoField[] };
    if (top.fields) {
      return new StructType(top.fields.map(parseProtoField));
    }
    return new StructType();
  }
}

interface ProtoField {
  name?: string;
  dataType?: Record<string, unknown>;
  nullable?: boolean;
  metadata?: string | Record<string, unknown>;
}

function parseProtoField(field: ProtoField): StructField {
  return new StructField(
    field.name ?? "",
    resolveProtoDataType(field.dataType),
    field.nullable ?? true,
    parseFieldMetadata(field.metadata),
  );
}

// The wire carries StructField.metadata as a JSON string.
function parseFieldMetadata(metadata: string | Record<string, unknown> | undefined) {
  if (typeof metadata !== "string") {
    return metadata ?? {};
  }
  try {
    const parsed: unknown = JSON.parse(metadata);
    return typeof parsed === "object" && parsed !== null ? (parsed as Record<string, unknown>) : {};
  } catch {
    return {};
  }
}

/** Proto case names whose DDL spelling differs from the case name itself. */
const DDL_NAMES: Record<string, string> = {
  integer: "int",
  long: "bigint",
  byte: "tinyint",
  short: "smallint",
  timestampNtz: "timestamp_ntz",
  calendarInterval: "interval",
  null: "void",
};

const DAY_TIME_UNITS = ["day", "hour", "minute", "second"];
const YEAR_MONTH_UNITS = ["year", "month"];

interface ProtoIntervalValue {
  startField?: number;
  endField?: number;
}

/**
 * Render an interval type honoring its start/end fields, like PySpark's
 * `simpleString()`: `interval hour`, `interval day to minute`. Both fields
 * unset means the type's full range.
 */
function resolveInterval(units: string[], value: ProtoIntervalValue | undefined): string {
  const fullRange = `interval ${units[0]} to ${units[units.length - 1]}`;

  if (value?.startField === undefined) {
    return fullRange;
  }

  const start = units[value.startField];
  const end = units[value.endField ?? value.startField];
  if (start === undefined || end === undefined) {
    return fullRange;
  }

  return start === end ? `interval ${start}` : `interval ${start} to ${end}`;
}

/**
 * Render a Spark Connect `DataType` proto as its DDL simple string, recursing
 * into nested types: `decimal(10,2)`, `array<string>`, `map<string,int>`,
 * `struct<a:int,b:array<string>>`.
 */
function resolveProtoDataType(dt: Record<string, unknown> | undefined): string {
  const resolved = unwrapKind(dt);
  if (!resolved) return "unknown";
  const { name, value } = resolved;

  switch (name) {
    case "decimal": {
      const v = value as { precision?: number; scale?: number };
      return `decimal(${String(v?.precision ?? 10)},${String(v?.scale ?? 0)})`;
    }
    case "char":
    case "varchar": {
      const v = value as { length?: number };
      return v?.length !== undefined ? `${name}(${String(v.length)})` : name;
    }
    case "array": {
      const v = value as { elementType?: Record<string, unknown> };
      return `array<${resolveProtoDataType(v?.elementType)}>`;
    }
    case "map": {
      const v = value as {
        keyType?: Record<string, unknown>;
        valueType?: Record<string, unknown>;
      };
      return `map<${resolveProtoDataType(v?.keyType)},${resolveProtoDataType(v?.valueType)}>`;
    }
    case "struct": {
      const v = value as { fields?: ProtoField[] };
      const inner = (v?.fields ?? [])
        .map((f) => `${f.name ?? ""}:${resolveProtoDataType(f.dataType)}`)
        .join(",");
      return `struct<${inner}>`;
    }
    case "dayTimeInterval":
      return resolveInterval(DAY_TIME_UNITS, value as ProtoIntervalValue);
    case "yearMonthInterval":
      return resolveInterval(YEAR_MONTH_UNITS, value as ProtoIntervalValue);
    case "udt": {
      const v = value as { sqlType?: Record<string, unknown> };
      return v?.sqlType ? resolveProtoDataType(v.sqlType) : "udt";
    }
    case "unparsed": {
      const v = value as { dataTypeString?: string };
      return v?.dataTypeString ?? "unknown";
    }
    default:
      return DDL_NAMES[name] ?? name;
  }
}

/**
 * Accept both the protobuf-oneof wire shape `{ kind: { case, value } }` and a
 * plain-JSON shape `{ decimal: {...} }` from non-protobuf transports.
 */
function unwrapKind(
  dt: Record<string, unknown> | undefined,
): { name: string; value: unknown } | undefined {
  if (!dt) return undefined;

  const kind = (dt.kind ?? dt) as Record<string, unknown>;
  if (typeof kind !== "object" || kind === null) return undefined;

  // An unset oneof arrives as `{ case: undefined }`; the `case` key must not
  // fall through to the plain-JSON loop below.
  if ("case" in kind) {
    const oneof = kind as { case?: string; value?: unknown };
    return typeof oneof.case === "string" ? { name: oneof.case, value: oneof.value } : undefined;
  }

  for (const key of Object.keys(kind)) {
    if (key !== "$typeName" && key !== "$unknown") {
      return { name: key, value: kind[key] };
    }
  }
  return undefined;
}

// Mirrors Spark 4.x QuotingUtils.quoteIfNeeded. SPARK-47300 added the rule in
// 4.0, so 3.5 leaves a leading-digit name bare.
const VALID_IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/;

function needsQuoting(name: string): boolean {
  return !VALID_IDENTIFIER.test(name);
}

/** @internal */
export function quoteIfNeeded(name: string): string {
  return needsQuoting(name) ? `\`${name.replaceAll("`", "``")}\`` : name;
}
