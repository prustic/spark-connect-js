# Contributing to spark-connect-js

For development setup, see the [README](README.md).

## Ways to Contribute

- **Report bugs**: open an issue with reproduction steps
- **Suggest features**: open an issue describing the use case
- **Submit a PR**: bug fixes, new functions, documentation improvements
- **Improve docs**: typos, unclear explanations, missing examples

## Opening Issues

- Search existing issues first to avoid duplicates.
- For bugs, include: what you expected, what happened, Node.js version, Spark version, and a minimal reproduction.
- For feature requests, describe the use case and how PySpark handles it (if applicable).

## Pull Requests

1. Fork the repo and create a branch from `main`.
2. Make your changes with tests.
3. Run `pnpm blt` to verify build, lint, and tests all pass.
4. Add a changeset: `pnpm changeset` (describe what changed and select the affected packages).
5. Open a PR against `main`.

Keep PRs focused. One concern per PR. If a PR touches multiple unrelated things, split it up.

## Commit Messages

Use [Conventional Commits](https://www.conventionalcommits.org/):

```
feat: add DataFrame.unpivot() method
fix: handle null columns in groupBy aggregation
chore: update @grpc/grpc-js to 1.13
docs: add streaming example
```

Scope is optional: `feat(core): ...`, `fix(node): ...`

## Code Guidelines

- TypeScript strict mode
- ESLint + Prettier enforced in CI
- Zero runtime dependencies in `@spark-connect-js/core`
- Tests live alongside source as `*.test.ts` files
- Run `pnpm format:fix` before committing

## License

By contributing, you agree that your contributions will be licensed under [Apache-2.0](LICENSE).
