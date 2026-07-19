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
