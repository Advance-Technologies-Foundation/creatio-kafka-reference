# Agent instructions

This is a public reference implementation for Kafka integration with Creatio. Keep it complete, installable, and
reviewable as one clio workspace.

## Scope

- Keep the repository focused on Kafka. Do not add Google Pub/Sub, virtual entities, or transport-comparison code.
- Preserve the three verification layers: unit tests, black-box E2E tests, and the standalone console.
- Never commit credentials, tokens, private broker addresses, client names, or local environment configuration.
- Use neutral placeholder infrastructure values in committed files.

## Build and test

Restore Creatio references into the ignored `.application` directory, then run:

```powershell
dotnet build MainSolution.slnx -c dev-n8
dotnet test tests/AtfKafkaReference/AtfKafkaReference.Tests.csproj -c dev-n8
```

E2E tests require the environment variables documented in `docs/kafka-webservice-e2e-tests.md` and must never
hard-code their values.

## Change rules

- Treat `.application` as read-only dependency input.
- Add or update tests for behavior changes.
- Keep public APIs documented with XML comments.
- Keep `CLAUDE.md` as the single pointer to this file so all coding agents receive the same rules.
- Append significant decisions and discoveries to `.codex/workspace-diary.md`.
