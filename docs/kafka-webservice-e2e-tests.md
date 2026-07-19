# Kafka web-service E2E tests

`AtfKafkaReference.IntegrationTests` treats Creatio and Kafka as black boxes. It authenticates through Creatio,
calls `KafkaMessageService.Publish`, and consumes the correlated notification from Kafka. A second scenario proves
that blank input is rejected with HTTP 400 without accepting a Kafka publish.

## Required environment variables

| Variable | Required | Meaning |
| --- | ---: | --- |
| `CREATIO_URL` | yes | absolute Creatio application URL |
| `CREATIO_IS_NETCORE` | yes | `true` for modern Creatio, otherwise `false` |
| `CREATIO_ACCESS_TOKEN` | one auth mode | bearer token |
| `CREATIO_USERNAME` and `CREATIO_PASSWORD` | one auth mode | cookie authentication |
| `KAFKA_PASSWORD` | yes | Kafka SASL password |
| `KAFKA_BOOTSTRAP_SERVERS` | no | broker list; committed fallback is a placeholder |
| `KAFKA_USERNAME` | no | SASL username |
| `KAFKA_NOTIFICATION_TOPIC` | no | notification topic |
| `KAFKA_TIMEOUT_SECONDS` | no | positive timeout; defaults to 30 |

Configure exactly one Creatio authentication mode. Store values in CI secrets or the local process environment,
never in committed files.

```powershell
dotnet test tests/AtfKafkaReference.IntegrationTests/AtfKafkaReference.IntegrationTests.csproj -c Release
```

These scenarios are environment tests, not CI-safe unit tests. Use disposable topic/group identities and confirm the
package has been built, deployed, and restarted before interpreting a failure.
