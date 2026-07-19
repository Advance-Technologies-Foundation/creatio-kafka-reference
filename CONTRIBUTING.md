# Contributing

Contributions should improve the Kafka reference without turning it into a collection of unrelated examples.

1. Create a focused branch and explain the Creatio and Kafka versions used for validation.
2. Keep infrastructure and credentials outside the repository. Update only the committed example configuration.
3. Build `MainSolution.slnx` and run the unit tests for the framework you changed.
4. When runtime behavior changes, run the E2E test against a disposable Creatio environment and Kafka topics.
5. Update the relevant document and `.codex/workspace-diary.md` with reusable findings.

Pull requests should state the exact commands run and clearly distinguish automated proof from manual validation.
