/**
 * ─── StructType / StructField ───────────────────────────────────────────────
 *
 * Mirrors Spark's StructType and StructField for schema inspection.
 *
 * @see Spark source (StructType): sql/api/src/main/scala/org/apache/spark/sql/types/StructType.scala
 * @see Spark source (StructField): sql/api/src/main/scala/org/apache/spark/sql/types/StructField.scala
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
    this.metadata = metadata;
  }

  toString(): string {
    return `StructField(${this.name}, ${this.dataType}, ${String(this.nullable)})`;
  }
}

export class StructType {
  readonly fields: StructField[];

  constructor(fields: StructField[] = []) {
    this.fields = fields;
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
  metadata?: Record<string, unknown>;
}

function parseProtoField(field: ProtoField): StructField {
  return new StructField(
    field.name ?? "",
    resolveProtoDataType(field.dataType),
    field.nullable ?? true,
    (field.metadata as Record<string, unknown>) ?? {},
  );
}

function resolveProtoDataType(dt: Record<string, unknown> | undefined): string {
  if (!dt) return "unknown";
  const kind = dt.kind ?? dt;
  if (typeof kind === "object" && kind !== null) {
    const obj = kind as { case?: string; value?: unknown };
    if (obj.case) {
      // Handle nested struct/array/map
      if (obj.case === "struct") return "struct";
      if (obj.case === "array") return "array";
      if (obj.case === "map") return "map";
      // Primitive types: the case name IS the type
      return obj.case;
    }
    // Fallback: look at keys
    for (const key of Object.keys(kind)) {
      if (key !== "$typeName" && key !== "$unknown") return key;
    }
  }
  return "unknown";
}
