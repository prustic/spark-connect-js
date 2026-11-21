import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  abs,
  round,
  bround,
  ceil,
  floor,
  sqrt,
  cbrt,
  pow,
  log,
  log2,
  log10,
  log1p,
  exp,
  expm1,
  sin,
  cos,
  tan,
  asin,
  acos,
  atan,
  atan2,
  sinh,
  cosh,
  tanh,
  signum,
  sign,
  degrees,
  radians,
  factorial,
  greatest,
  least,
  hex,
  unhex,
  bin,
  rand,
  randn,
  pmod,
  rint,
  hypot,
  negate,
  negative,
  positive,
  e,
  pi,
  ceiling,
  power,
  ln,
  cot,
  csc,
  sec,
  asinh,
  acosh,
  atanh,
  conv,
  width_bucket,
  random,
  uniform,
  try_add,
  try_subtract,
  try_multiply,
  try_divide,
  try_mod,
} from "./math.js";

import { col, lit, type Column } from "../column.js";

// Helper: assert unresolvedFunction with correct name
function assertFn(col: Column, name: string) {
  assert.equal(col._expr.type, "unresolvedFunction");
  if (col._expr.type === "unresolvedFunction") {
    assert.equal(col._expr.name, name);
  }
}

describe("math functions", () => {
  it("abs() creates unresolvedFunction", () => {
    assertFn(abs("value"), "abs");
  });

  it("round() includes scale parameter", () => {
    const result = round("value", 2);
    if (result._expr.type === "unresolvedFunction") {
      assert.equal(result._expr.name, "round");
      assert.equal(result._expr.arguments.length, 2);
    }
  });

  it("bround() includes scale parameter", () => {
    const result = bround("value", 2);
    if (result._expr.type === "unresolvedFunction") {
      assert.equal(result._expr.name, "bround");
      assert.equal(result._expr.arguments.length, 2);
    }
  });

  it("ceil/floor/sqrt/cbrt work", () => {
    assertFn(ceil("x"), "ceil");
    assertFn(floor("x"), "floor");
    assertFn(sqrt("x"), "sqrt");
    assertFn(cbrt("x"), "cbrt");
  });

  it("pow() uses 'power' function name", () => {
    const result = pow("x", 2);
    if (result._expr.type === "unresolvedFunction") {
      assert.equal(result._expr.name, "power");
    }
  });

  it("pow() with a numeric literal exponent wraps it in lit()", () => {
    const result = pow("x", 3);
    if (result._expr.type === "unresolvedFunction") {
      const exponentExpr = result._expr.arguments[1];
      assert.equal(exponentExpr.type, "literal");
      if (exponentExpr.type === "literal") {
        assert.equal(exponentExpr.value, 3);
      }
    }
  });

  it("pow() with a Column exponent forwards its expression as-is", () => {
    const exponent = col("exp");
    const result = pow("base", exponent);
    if (result._expr.type === "unresolvedFunction") {
      const exponentExpr = result._expr.arguments[1];
      assert.equal(exponentExpr.type, "unresolvedAttribute");
      if (exponentExpr.type === "unresolvedAttribute") {
        assert.equal(exponentExpr.name, "exp");
      }
    }
  });

  it("log/log2/log10/log1p work", () => {
    assertFn(log("x"), "ln");
    assertFn(log2("x"), "log2");
    assertFn(log10("x"), "log10");
    assertFn(log1p("x"), "log1p");
  });

  it("exp/expm1 work", () => {
    assertFn(exp("x"), "exp");
    assertFn(expm1("x"), "expm1");
  });

  it("trig functions work", () => {
    assertFn(sin("x"), "sin");
    assertFn(cos("x"), "cos");
    assertFn(tan("x"), "tan");
    assertFn(asin("x"), "asin");
    assertFn(acos("x"), "acos");
    assertFn(atan("x"), "atan");
    assertFn(sinh("x"), "sinh");
    assertFn(cosh("x"), "cosh");
    assertFn(tanh("x"), "tanh");
  });

  it("atan2 takes two args", () => {
    const result = atan2("y", "x");
    if (result._expr.type === "unresolvedFunction") {
      assert.equal(result._expr.name, "atan2");
      assert.equal(result._expr.arguments.length, 2);
    }
  });

  it("signum and sign are the same", () => {
    assertFn(signum("x"), "signum");
    assertFn(sign("x"), "signum");
  });

  it("degrees/radians work", () => {
    assertFn(degrees("x"), "degrees");
    assertFn(radians("x"), "radians");
  });

  it("factorial works", () => {
    assertFn(factorial("x"), "factorial");
  });

  it("greatest/least handle variadic args", () => {
    const g = greatest("a", "b", "c");
    if (g._expr.type === "unresolvedFunction") {
      assert.equal(g._expr.name, "greatest");
      assert.equal(g._expr.arguments.length, 3);
    }
    const l = least("a", "b", "c");
    if (l._expr.type === "unresolvedFunction") {
      assert.equal(l._expr.name, "least");
      assert.equal(l._expr.arguments.length, 3);
    }
  });

  it("hex/unhex/bin work", () => {
    assertFn(hex("x"), "hex");
    assertFn(unhex("x"), "unhex");
    assertFn(bin("x"), "bin");
  });

  it("rand/randn work without seed", () => {
    assertFn(rand(), "rand");
    assertFn(randn(), "randn");
  });

  it("rand/randn work with seed", () => {
    const r = rand(42);
    if (r._expr.type === "unresolvedFunction") {
      assert.equal(r._expr.arguments.length, 1);
    }
  });

  it("pmod takes two args", () => {
    const result = pmod("x", "y");
    if (result._expr.type === "unresolvedFunction") {
      assert.equal(result._expr.name, "pmod");
      assert.equal(result._expr.arguments.length, 2);
    }
  });

  it("rint/hypot work", () => {
    assertFn(rint("x"), "rint");
    const h = hypot("a", "b");
    if (h._expr.type === "unresolvedFunction") {
      assert.equal(h._expr.name, "hypot");
      assert.equal(h._expr.arguments.length, 2);
    }
  });

  it("negate/negative/positive work", () => {
    assertFn(negate("x"), "negative");
    assertFn(negative("x"), "negative");
    assertFn(positive("x"), "positive");
  });

  it("e() and pi() are zero-arg", () => {
    assertFn(e(), "e");
    assertFn(pi(), "pi");
  });
});

describe("math functions added for PySpark parity", () => {
  it("send the function name PySpark sends", () => {
    const cases: [Column, string, number][] = [
      [ln("x"), "ln", 1],
      [cot("x"), "cot", 1],
      [csc("x"), "csc", 1],
      [sec("x"), "sec", 1],
      [asinh("x"), "asinh", 1],
      [acosh("x"), "acosh", 1],
      [atanh("x"), "atanh", 1],
      [ceiling("x"), "ceiling", 1],
      [try_add("a", "b"), "try_add", 2],
      [try_subtract("a", "b"), "try_subtract", 2],
      [try_multiply("a", "b"), "try_multiply", 2],
      [try_divide("a", "b"), "try_divide", 2],
      [try_mod("a", "b"), "try_mod", 2],
    ];
    for (const [result, name, argCount] of cases) {
      assert.ok(result._expr.type === "unresolvedFunction", name);
      assert.equal(result._expr.name, name);
      assert.equal(result._expr.arguments.length, argCount, name);
    }
  });

  it("power and random are the same functions as pow and rand, as in PySpark", () => {
    assert.equal(power, pow);
    assert.equal(random, rand);
  });

  it("ceil, ceiling, and floor take an optional scale", () => {
    for (const [result, name] of [
      [ceil("x", 2), "ceil"],
      [ceiling("x", 2), "ceiling"],
      [floor("x", lit(2)), "floor"],
    ] as const) {
      assert.ok(result._expr.type === "unresolvedFunction");
      assert.equal(result._expr.name, name);
      assert.deepStrictEqual(result._expr.arguments[1], { type: "literal", value: 2 });
    }
  });

  it("conv takes its bases as literals", () => {
    const result = conv("x", 16, 2);
    assert.ok(result._expr.type === "unresolvedFunction");
    assert.deepStrictEqual(result._expr.arguments.slice(1), [
      { type: "literal", value: 16 },
      { type: "literal", value: 2 },
    ]);
  });

  it("width_bucket reads a number as a literal and a string as a column", () => {
    const literal = width_bucket("v", "lo", "hi", 5);
    const named = width_bucket("v", "lo", "hi", "n");
    assert.ok(literal._expr.type === "unresolvedFunction");
    assert.ok(named._expr.type === "unresolvedFunction");
    assert.deepStrictEqual(literal._expr.arguments[3], { type: "literal", value: 5 });
    assert.deepStrictEqual(named._expr.arguments[3], { type: "unresolvedAttribute", name: "n" });
  });

  it("picks a non-negative 63-bit seed on the client when none is given", () => {
    for (const result of [rand(), randn(), random(), uniform(0, 10)]) {
      assert.ok(result._expr.type === "unresolvedFunction");
      const seed = result._expr.arguments.at(-1);
      assert.ok(seed?.type === "literal" && typeof seed.value === "bigint");
      assert.ok(seed.value >= 0n && seed.value < 2n ** 63n);
    }
  });

  it("passes a given seed through", () => {
    const result = uniform(0, 10, 42);
    assert.ok(result._expr.type === "unresolvedFunction");
    assert.deepStrictEqual(result._expr.arguments, [
      { type: "literal", value: 0 },
      { type: "literal", value: 10 },
      { type: "literal", value: 42 },
    ]);
  });
});
