# Workspace diary

## 2026-07-19 - Extract standalone Kafka reference
Context: Split the Kafka implementation out of a multi-concept workshop into a public reference workspace.
Decision: Use the standalone `AtfKafkaReference` package identity and retain only Kafka lifecycle, native-runtime,
web-service, testing, and console concerns.
Discovery: The source implementation's package-owned native ZIP plus immutable `conf/native/kafka/<version>/<rid>`
runtime is the key boundary that permits managed package updates while `librdkafka` is loaded.
Files: packages/AtfKafkaReference, tests/AtfKafkaReference, tests/AtfKafkaReference.IntegrationTests,
tools/KafkaRoundTrip.Console, docs
Impact: Kafka can be reviewed, installed, validated, and maintained independently of unrelated examples.

## 2026-07-19 - Secure and validate the maintenance boundary
Context: Publication review identified that stopping integration workers is a privileged host operation.
Decision: Require `CanManageSolution`, return stable 403/500 responses, log full failures only server-side, and add
portable plus optional-reference CI validation.
Discovery: The endpoint can use Creatio's existing `DBSecurityEngine.GetCanExecuteOperation` contract while tests
substitute the security engine explicitly for authorized and unauthorized callers.
Files: packages/AtfKafkaReference/Files/src/cs/EntryPoints/WebServices/IntegrationMaintenanceService.cs,
tests/AtfKafkaReference/EntryPoints/WebServices/IntegrationMaintenanceServiceTests.cs,
.github/workflows/validate.yml, docs/kafka-round-trip.md
Impact: Unprivileged callers cannot stop Kafka workers and responses no longer disclose internal exception details.
