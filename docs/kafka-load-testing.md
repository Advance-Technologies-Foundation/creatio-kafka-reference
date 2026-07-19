# Kafka load testing

The console includes a controlled request/reply load mode:

```powershell
# --load <requests-per-second> <duration-seconds> <drain-seconds>
dotnet run --project tools/KafkaRoundTrip.Console -c Release -- --load 50 60 30
```

It creates a unique consumer group, publishes correlation-tagged requests at the requested rate, collects matching
replies through the drain window, and reports completed count plus p50/p95 round-trip latency.

## Valid experiment rules

- Give each Creatio environment distinct request/reply topics and a distinct consumer group.
- Ensure request-topic partitions are at least the configured `KafkaWorkerCount` before comparing worker counts.
- Start from zero consumer lag and verify the expected number of active consumers.
- Change one variable at a time, recycle Creatio after changing cached package settings, and warm the application.
- Record offered requests, in-window completions, drain completions, p50/p95, final lag, and Creatio/Kafka resource
  samples. A broker that accepts all requests does not prove the downstream consumer remains within its latency SLO.

The harness is intentionally transparent rather than a replacement for a production load-testing platform. Review
its pacing and measurement semantics before using results for capacity decisions.
