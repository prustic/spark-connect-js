import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  struct, array, array_contains, array_distinct, array_union, array_intersect,
  array_except, array_join, array_sort, sort_array, array_max, array_min,
  array_position, array_remove, array_repeat, array_compact,
  arrays_overlap, arrays_zip, flatten, sequence, element_at, get, slice,
  explode, explode_outer, posexplode, posexplode_outer, inline, inline_outer,
  stack, size, create_map, map_keys, map_values, map_entries,
  map_from_entries, map_from_arrays, map_concat,
} from "./collection.js";

import type { Column } from "../column.js";

function assertFn(c: Column, name: string, argCount?: number) {
  assert.equal(c._expr.type, "unresolvedFunction");
  if (c._expr.type === "unresolvedFunction") {
    assert.equal(c._expr.name, name);
    if (argCount !== undefined) assert.equal(c._expr.arguments.length, argCount);
  }
}

describe("collection functions", () => {
  it("struct/array work with variadic args", () => {
    assertFn(struct("a", "b"), "struct", 2);
    assertFn(array("a", "b"), "array", 2);
  });

  it("array_contains takes col and value", () => {
    const r = array_contains("arr", "val");
    assert.equal(r._expr.type, "unresolvedFunction");
  });

  it("array_distinct/array_compact/flatten", () => {
    assertFn(array_distinct("arr"), "array_distinct", 1);
    assertFn(array_compact("arr"), "array_compact", 1);
    assertFn(flatten("arr"), "flatten", 1);
  });

  it("array set operations: union/intersect/except", () => {
    assertFn(array_union("a", "b"), "array_union", 2);
    assertFn(array_intersect("a", "b"), "array_intersect", 2);
    assertFn(array_except("a", "b"), "array_except", 2);
  });

  it("array_join with separator", () => {
    const r = array_join("arr", ",");
    assert.equal(r._expr.type, "unresolvedFunction");
  });

  it("array_sort/sort_array", () => {
    assertFn(array_sort("arr"), "array_sort", 1);
    assertFn(sort_array("arr"), "sort_array", 2); // includes default asc=true
  });

  it("array_max/array_min", () => {
    assertFn(array_max("arr"), "array_max", 1);
    assertFn(array_min("arr"), "array_min", 1);
  });

  it("array_position/array_remove take col + value", () => {
    const r1 = array_position("arr", "val");
    assert.equal(r1._expr.type, "unresolvedFunction");
    const r2 = array_remove("arr", "val");
    assert.equal(r2._expr.type, "unresolvedFunction");
  });

  it("array_repeat takes col and count", () => {
    const r = array_repeat("x", 3);
    assert.equal(r._expr.type, "unresolvedFunction");
  });

  it("arrays_overlap/arrays_zip", () => {
    assertFn(arrays_overlap("a", "b"), "arrays_overlap", 2);
    assertFn(arrays_zip("a", "b"), "arrays_zip", 2);
  });

  it("sequence takes start/stop", () => {
    assertFn(sequence("a", "b"), "sequence", 2);
  });

  it("element_at/get/slice", () => {
    const r1 = element_at("arr", "idx");
    assert.equal(r1._expr.type, "unresolvedFunction");
    const r2 = get("arr", 0);
    assert.equal(r2._expr.type, "unresolvedFunction");
    const r3 = slice("arr", 1, 3);
    assert.equal(r3._expr.type, "unresolvedFunction");
  });

  it("explode/explode_outer/posexplode/posexplode_outer", () => {
    assertFn(explode("arr"), "explode", 1);
    assertFn(explode_outer("arr"), "explode_outer", 1);
    assertFn(posexplode("arr"), "posexplode", 1);
    assertFn(posexplode_outer("arr"), "posexplode_outer", 1);
  });

  it("inline/inline_outer/stack", () => {
    assertFn(inline("arr"), "inline", 1);
    assertFn(inline_outer("arr"), "inline_outer", 1);
  });

  it("stack takes n + columns", () => {
    const r = stack(2, "a", "b");
    assert.equal(r._expr.type, "unresolvedFunction");
  });

  it("size creates unresolvedFunction", () => {
    assertFn(size("arr"), "size", 1);
  });

  it("create_map/map_keys/map_values/map_entries/map_from_entries", () => {
    assertFn(create_map("k", "v"), "map", 2);
    assertFn(map_keys("m"), "map_keys", 1);
    assertFn(map_values("m"), "map_values", 1);
    assertFn(map_entries("m"), "map_entries", 1);
    assertFn(map_from_entries("m"), "map_from_entries", 1);
  });

  it("map_from_arrays/map_concat", () => {
    assertFn(map_from_arrays("k", "v"), "map_from_arrays", 2);
    assertFn(map_concat("m1", "m2"), "map_concat", 2);
  });
});
