using System;
using NUnit.Framework;

namespace AtfKafkaReference.IntegrationTests.Infrastructure;

public sealed class KafkaTestSettings {
	private KafkaTestSettings(string bootstrapServers, string username, string password,
			string notificationTopic, TimeSpan timeout) {
		BootstrapServers = bootstrapServers;
		Username = username;
		Password = password;
		NotificationTopic = notificationTopic;
		Timeout = timeout;
	}

	public string BootstrapServers { get; }
	public string Username { get; }
	public string Password { get; }
	public string NotificationTopic { get; }
	public TimeSpan Timeout { get; }

	public static KafkaTestSettings Load() {
		string Read(string name, string fallback = null) =>
			TestContext.Parameters.Get(name, Environment.GetEnvironmentVariable(name) ?? fallback);
		string password = Read("KAFKA_PASSWORD");
		if (string.IsNullOrWhiteSpace(password)) {
			throw new InvalidOperationException("Set KAFKA_PASSWORD for the Kafka E2E tests.");
		}
		if (!int.TryParse(Read("KAFKA_TIMEOUT_SECONDS", "30"), out int timeoutSeconds) || timeoutSeconds <= 0) {
			throw new InvalidOperationException("Set KAFKA_TIMEOUT_SECONDS to a positive integer.");
		}
		return new KafkaTestSettings(
			Read("KAFKA_BOOTSTRAP_SERVERS", "kafka.example.com:9094"),
			Read("KAFKA_USERNAME", "kafka-user"),
			password,
			Read("KAFKA_NOTIFICATION_TOPIC", "atf-kafka-reference.notifications"),
			TimeSpan.FromSeconds(timeoutSeconds));
	}
}
