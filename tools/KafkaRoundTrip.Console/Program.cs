using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json;
using AtfKafkaReference.Kafka;
using Confluent.Kafka;

namespace KafkaRoundTrip.ConsoleApp;

internal static class Program {
	private static async Task<int> Main(string[] args) {
		try {
			KafkaConsoleSettings settings = LoadSettings();
			if (args.Length > 0 && args[0].Equals("--load", StringComparison.OrdinalIgnoreCase)) {
				int rate = ReadPositiveInteger(args, 1, 50, "rate");
				int durationSeconds = ReadPositiveInteger(args, 2, 60, "duration");
				int drainSeconds = ReadPositiveInteger(args, 3, 30, "drain");
				return await RunLoadAsync(settings, rate, durationSeconds, drainSeconds);
			}

			string message = args.Length == 0 ? "Hello from the Kafka reference" : string.Join(" ", args);
			return await RunRoundTripAsync(settings, message);
		} catch (Exception exception) {
			await Console.Error.WriteLineAsync(exception.Message);
			return 1;
		}
	}

	private static KafkaConsoleSettings LoadSettings() {
		string path = Path.Combine(AppContext.BaseDirectory, "appsettings.dev.json");
		if (!File.Exists(path)) {
			path = Path.Combine(Directory.GetCurrentDirectory(), "appsettings.dev.json");
		}
		if (!File.Exists(path)) {
			throw new FileNotFoundException(
				"Copy appsettings.dev.example.json to appsettings.dev.json and configure Kafka credentials.", path);
		}
		KafkaConsoleConfiguration? configuration = JsonSerializer.Deserialize<KafkaConsoleConfiguration>(
			File.ReadAllText(path), new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
		if (configuration?.Kafka == null || string.IsNullOrWhiteSpace(configuration.Kafka.Password)) {
			throw new InvalidOperationException("Kafka.Password is required in appsettings.dev.json.");
		}
		return configuration.Kafka;
	}

	private static async Task<int> RunRoundTripAsync(KafkaConsoleSettings settings, string message) {
		Guid correlationId = Guid.NewGuid();
		using IConsumer<string, string> consumer = CreateConsumer(settings,
			"atf-kafka-reference.console." + correlationId.ToString("N"));
		consumer.Subscribe(settings.ReplyTopic);
		WaitForAssignment(consumer, settings.Timeout);

		using IProducer<string, string> producer = CreateProducer(settings);
		var request = new KafkaRequest {
			CorrelationId = correlationId,
			Message = message,
			SentAtUtc = DateTime.UtcNow
		};
		await producer.ProduceAsync(settings.RequestTopic, new Message<string, string> {
			Key = correlationId.ToString("D"),
			Value = JsonSerializer.Serialize(request)
		});
		Console.WriteLine($"Sent [{correlationId}]: {message}");

		DateTime deadline = DateTime.UtcNow.Add(settings.Timeout);
		while (DateTime.UtcNow < deadline) {
			ConsumeResult<string, string>? consumed = consumer.Consume(TimeSpan.FromMilliseconds(250));
			if (consumed == null) {
				continue;
			}
			KafkaReply? reply = JsonSerializer.Deserialize<KafkaReply>(consumed.Message.Value);
			if (reply?.CorrelationId == correlationId) {
				Console.WriteLine($"Received [{reply.CorrelationId}]: {reply.Message}");
				return 0;
			}
		}
		await Console.Error.WriteLineAsync("No correlated Kafka reply was received before the timeout.");
		return 2;
	}

	private static async Task<int> RunLoadAsync(KafkaConsoleSettings settings, int ratePerSecond,
			int durationSeconds, int drainSeconds) {
		Guid runId = Guid.NewGuid();
		using IConsumer<string, string> consumer = CreateConsumer(settings,
			"atf-kafka-reference.load." + runId.ToString("N"));
		consumer.Subscribe(settings.ReplyTopic);
		WaitForAssignment(consumer, settings.Timeout);
		using IProducer<string, string> producer = CreateProducer(settings);

		var sentAt = new ConcurrentDictionary<Guid, long>();
		var latencies = new ConcurrentBag<double>();
		using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(durationSeconds + drainSeconds));
		Task collector = Task.Run(() => CollectReplies(consumer, sentAt, latencies, cancellation.Token));
		int requested = ratePerSecond * durationSeconds;
		long intervalTicks = Stopwatch.Frequency / ratePerSecond;
		var stopwatch = Stopwatch.StartNew();

		for (int index = 0; index < requested; index++) {
			long target = index * intervalTicks;
			while (stopwatch.ElapsedTicks < target) {
				await Task.Delay(1);
			}
			Guid correlationId = Guid.NewGuid();
			sentAt[correlationId] = Stopwatch.GetTimestamp();
			await producer.ProduceAsync(settings.RequestTopic, new Message<string, string> {
				Key = correlationId.ToString("D"),
				Value = JsonSerializer.Serialize(new KafkaRequest {
					CorrelationId = correlationId,
					Message = $"Load {runId:N} #{index}",
					SentAtUtc = DateTime.UtcNow
				})
			});
		}
		producer.Flush(TimeSpan.FromSeconds(10));
		await Task.Delay(TimeSpan.FromSeconds(drainSeconds));
		cancellation.Cancel();
		await collector;

		double[] ordered = latencies.OrderBy(value => value).ToArray();
		Console.WriteLine($"Requested: {requested}; received: {ordered.Length}; " +
			$"p50: {Percentile(ordered, 0.50):F1} ms; p95: {Percentile(ordered, 0.95):F1} ms");
		return ordered.Length == requested ? 0 : 3;
	}

	private static void CollectReplies(IConsumer<string, string> consumer,
			ConcurrentDictionary<Guid, long> sentAt, ConcurrentBag<double> latencies,
			CancellationToken cancellationToken) {
		while (!cancellationToken.IsCancellationRequested) {
			ConsumeResult<string, string>? consumed = consumer.Consume(TimeSpan.FromMilliseconds(100));
			if (consumed == null) {
				continue;
			}
			KafkaReply? reply = JsonSerializer.Deserialize<KafkaReply>(consumed.Message.Value);
			if (reply != null && sentAt.TryRemove(reply.CorrelationId, out long start)) {
				latencies.Add((Stopwatch.GetTimestamp() - start) * 1000d / Stopwatch.Frequency);
			}
		}
	}

	private static IProducer<string, string> CreateProducer(KafkaConsoleSettings settings) =>
		new ProducerBuilder<string, string>(CreateClientConfig<ProducerConfig>(settings)).Build();

	private static IConsumer<string, string> CreateConsumer(KafkaConsoleSettings settings, string groupId) =>
		new ConsumerBuilder<string, string>(CreateClientConfig(new ConsumerConfig {
			GroupId = groupId,
			AutoOffsetReset = AutoOffsetReset.Latest,
			EnableAutoCommit = false
		}, settings)).Build();

	private static ProducerConfig CreateClientConfig<ProducerConfig>(KafkaConsoleSettings settings)
			where ProducerConfig : ClientConfig, new() => CreateClientConfig(new ProducerConfig(), settings);

	private static T CreateClientConfig<T>(T config, KafkaConsoleSettings settings) where T : ClientConfig {
		config.BootstrapServers = settings.BootstrapServers;
		config.SecurityProtocol = SecurityProtocol.SaslSsl;
		config.SaslMechanism = SaslMechanism.ScramSha512;
		config.SaslUsername = settings.Username;
		config.SaslPassword = settings.Password;
		config.SslCaCertificateStores = "Root";
		return config;
	}

	private static void WaitForAssignment(IConsumer<string, string> consumer, TimeSpan timeout) {
		DateTime deadline = DateTime.UtcNow.Add(timeout);
		while (consumer.Assignment.Count == 0 && DateTime.UtcNow < deadline) {
			consumer.Consume(TimeSpan.FromMilliseconds(250));
		}
		if (consumer.Assignment.Count == 0) {
			throw new TimeoutException("Kafka consumer did not receive a partition assignment.");
		}
	}

	private static int ReadPositiveInteger(string[] args, int index, int fallback, string name) {
		if (args.Length <= index) {
			return fallback;
		}
		if (!int.TryParse(args[index], out int value) || value <= 0) {
			throw new ArgumentException($"{name} must be a positive integer.");
		}
		return value;
	}

	private static double Percentile(double[] values, double percentile) {
		if (values.Length == 0) {
			return 0;
		}
		return values[(int)Math.Ceiling(percentile * values.Length) - 1];
	}
}

internal sealed class KafkaConsoleConfiguration {
	public KafkaConsoleSettings? Kafka { get; init; }
}

internal sealed class KafkaConsoleSettings {
	public string BootstrapServers { get; init; } = "kafka.example.com:9094";
	public string Username { get; init; } = "kafka-user";
	public string Password { get; init; } = string.Empty;
	public string RequestTopic { get; init; } = "atf-kafka-reference.requests";
	public string ReplyTopic { get; init; } = "atf-kafka-reference.replies";
	public string NotificationTopic { get; init; } = "atf-kafka-reference.notifications";
	public int TimeoutSeconds { get; init; } = 30;
	public TimeSpan Timeout => TimeSpan.FromSeconds(TimeoutSeconds);
}
