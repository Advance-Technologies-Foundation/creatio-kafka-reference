# Creatio Kafka reference

This repository is a complete, installable Creatio workspace showing a Kafka integration from package startup to
end-to-end verification. It is a reference implementation: copy the patterns into a production-owned package and
review the security, availability, naming, and operational settings for your environment.

## What it demonstrates

- a package-level application container and explicit dependency boundaries;
- a bounded set of long-lived Kafka consumers started and stopped with the Creatio application lifecycle;
- correlated request/reply messages and a separate HTTP-to-Kafka notification endpoint;
- version-pinned `librdkafka` delivery as a package artifact, followed by immutable host-runtime provisioning;
- fast unit tests, environment-driven end-to-end tests, and a standalone round-trip/load console.

The repository intentionally contains no Google Pub/Sub, HTTP-versus-Kafka comparison, or virtual-entity example.

## Layout

| Path | Purpose |
| --- | --- |
| `packages/AtfKafkaReference` | Installable Creatio package |
| `tests/AtfKafkaReference` | Unit tests for package behavior |
| `tests/AtfKafkaReference.IntegrationTests` | Black-box Creatio/Kafka E2E tests |
| `tools/KafkaRoundTrip.Console` | Kafka request/reply and controlled-load client |
| `docs` | Architecture, native-runtime, test, and load guidance |

## Prerequisites

- .NET SDK 8 or later;
- [clio](https://github.com/Advance-Technologies-Foundation/clio);
- a Creatio development environment and its build references restored into `.application`;
- Kafka with SASL/SCRAM-SHA-512 over TLS, request/reply/notification topics, and matching credentials.

## Start here

1. Restore workspace references for your Creatio environment with clio.
2. Configure the `Kafka*` system settings described in [docs/kafka-round-trip.md](docs/kafka-round-trip.md).
3. Build with `dotnet build MainSolution.slnx -c dev-n8`.
4. Deploy using the workflow appropriate for your Creatio environment (FSM or database mode).
5. Copy `tools/KafkaRoundTrip.Console/appsettings.dev.example.json` to `appsettings.dev.json`, configure local
   credentials, and run the console.

```powershell
dotnet test tests/AtfKafkaReference/AtfKafkaReference.Tests.csproj -c dev-n8
dotnet run --project tools/KafkaRoundTrip.Console -- "Hello from Kafka"
```

Secrets are never committed. The sample broker, user, and topic values are placeholders.

## Production boundary

The stable package GUID is part of this reference repository. If you fork this project into a separately owned
production solution, assign new package/application identities before deployment. Do not install the reference and
a derivative retaining the same identifiers into the same Creatio environment.

See [CONTRIBUTING.md](CONTRIBUTING.md) for validation and contribution rules.
