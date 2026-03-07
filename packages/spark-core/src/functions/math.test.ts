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
} from "./math.js";

import type { Column } from "../column.js";

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
