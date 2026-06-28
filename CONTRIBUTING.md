# Contributing to spark-connect-js

Thanks for considering a contribution. spark-connect-js is a TypeScript client for Apache Spark Connect; PRs that improve fidelity with Spark, fix bugs, fill in API gaps, or sharpen the docs are all welcome.

For development setup (clone, install, build, run tests, add a function, add a DataFrame method), see the [Contributing](https://prustic.github.io/spark-connect-js/contributing/) page on the docs site. Below is the policy and process side: what to expect when you open an issue or send a PR.

## Ways to contribute

- Report bugs by opening an issue with a reproduction.
- Suggest features by opening an issue with the use case and (where relevant) how PySpark handles the same thing.
- Send a PR for bug fixes, missing methods, doc improvements, or runnable examples.

For anything bigger than a small fix, an issue first to align on the approach saves cycles for both sides.

## Opening issues

Search existing issues to avoid duplicates. For bugs, include what you expected, what actually happened, your Node.js version, the Spark server version, and a minimal reproduction (a single SQL string or a 10-line script is ideal). For feature requests, describe the use case and what the PySpark equivalent looks like.

Security vulnerabilities don't go in public issues. See [SECURITY.md](SECURITY.md) for the disclosure process.

## Pull requests

1. Fork and branch from `main`.
2. Make your changes with tests. Bug fixes need a regression test; new methods need plan-shape unit tests at minimum, and an integration test against a real Spark Connect server when behavior is non-trivial.
3. Run the full build, lint, and test pipeline before pushing; that's what CI runs.
4. Open the PR against `main` with a description that explains what changed and why.

Keep PRs focused. One concern per PR; split unrelated changes.

## Commit messages

[Conventional Commits](https://www.conventionalcommits.org/):

```text
feat: add DataFrame.unpivot()
fix: handle null columns in groupBy aggregation
chore: bump @grpc/grpc-js to 1.13
docs: add streaming example
```

Scope is optional but helps readers (`feat(core):`, `fix(node):`).

## License

By contributing, you agree that your contributions are licensed under [Apache-2.0](LICENSE).
