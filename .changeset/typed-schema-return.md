---
"@spark-connect-js/core": minor
"@spark-connect-js/node": minor
---

`DataFrame.schema()` returns a `StructType` instead of the raw protobuf schema object. Nested types render as DDL simple strings per field (`decimal(10,2)`, `array<string>`, `map<string,int>`, `struct<a:int,b:string>`), `columns()`, `dtypes()`, and `printSchema()` share the same typed path, and the unused `Schema` and `FieldDescriptor` type exports are removed. Code that read the raw proto shape should use `schema.fields` (or `schema.toDDL()` where a DDL string is needed).
