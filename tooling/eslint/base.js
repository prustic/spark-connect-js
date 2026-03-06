import eslint from "@eslint/js";
import tseslint from "typescript-eslint";

/**
 * Shared ESLint flat config for all spark-js packages.
 *
 * Uses typescript-eslint v8+ with type-aware linting.  This catches real
 * bugs that plain TypeScript misses: floating promises, unsafe `any` usage,
 * and incorrect async patterns — all critical when building a gRPC streaming
 * client that juggles Arrow buffers and async iterables.
 */
export default tseslint.config(
  eslint.configs.recommended,
  ...tseslint.configs.recommendedTypeChecked,
  {
    languageOptions: {
      parserOptions: {
        projectService: true,
      },
    },
    rules: {
      // Allow unused vars prefixed with _ (common for destructuring discards)
      "@typescript-eslint/no-unused-vars": [
        "error",
        { argsIgnorePattern: "^_", varsIgnorePattern: "^_" },
      ],
      // We use explicit any in Transport/gRPC boundary code intentionally
      "@typescript-eslint/no-explicit-any": "warn",
      // Require awaiting floating promises — critical for gRPC stream safety
      "@typescript-eslint/no-floating-promises": "error",
      // Disallow .then() when async/await is available
      "@typescript-eslint/no-misused-promises": "error",
    },
  },
  {
    // node:test's describe/it/test return promises handled by the runner
    files: ["**/*.test.ts", "**/*.test.js"],
    rules: {
      "@typescript-eslint/no-floating-promises": "off",
      "@typescript-eslint/require-await": "off",
      "require-yield": "off",
    },
  },
  {
    ignores: ["dist/", "coverage/", "*.config.*"],
  },
);
