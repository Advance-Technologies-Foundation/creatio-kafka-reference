# Kafka round trip

The central example is a correlated asynchronous request/reply:

1. `KafkaRoundTrip.Console` publishes a `KafkaRequest` to the configured request topic.
2. `KafkaAppEventListener` has already started one or more `KafkaWorker` consumers.
3. A worker deserializes and validates the request, then `KafkaMessageHandler` reads the current Creatio contact.
4. The worker publishes a `KafkaReply` with the same correlation ID and commits the consumed record.
5. The console consumes the matching reply and ignores unrelated records.

`KafkaMessageService.Publish` demonstrates the inverse boundary: an authenticated Creatio REST call publishes a
`KafkaNotification`, which the E2E test consumes and correlates by `messageId`.

## Configuration

The package binds these Creatio system settings:

| Code | Required | Example purpose |
| --- | ---: | --- |
| `KafkaBootstrapServers` | yes | Kafka broker endpoints |
| `KafkaUsername` | yes | SASL username |
| `KafkaPassword` | yes | SASL password; deliberately has no committed value binding |
| `KafkaRequestTopic` | yes | console-to-Creatio requests |
| `KafkaReplyTopic` | yes | Creatio-to-console replies |
| `KafkaConsumerGroup` | yes | Creatio worker group |
| `KafkaNotificationTopic` | yes | REST-to-Kafka notifications |
| `KafkaNativeLibraryPath` | no | explicit native-library override |
| `KafkaWorkerCount` | no | bounded consumer count, from 1 through 32 |

Committed values are placeholders. Assign environment-specific topics and groups before deployment; environments
must not share a request topic and consumer group unless cross-environment work distribution is intentional.

The worker remains disabled while `KafkaPassword` is empty. Restart the Creatio application after changing cached
settings such as worker count or topics.

## Build and run

```powershell
dotnet build MainSolution.slnx -c dev-n8
Copy-Item tools/KafkaRoundTrip.Console/appsettings.dev.example.json `
  tools/KafkaRoundTrip.Console/appsettings.dev.json
dotnet run --project tools/KafkaRoundTrip.Console -- "Hello from Kafka"
```

The client and package both use SASL/SCRAM-SHA-512 over TLS. Adapt the client-config construction if your approved
Kafka deployment uses a different security protocol; do not silently downgrade transport security.

## Lifecycle

`KafkaAppEventListener` delegates startup and shutdown to the singleton `IKafkaWorkerHost`. Each configured worker
owns an independent Kafka consumer/producer loop. Shutdown cancels all loops and joins their threads. The maintenance
web service provides an explicit operational stop boundary, but stopping managed clients does not unload a native
module that the worker process has already loaded.
