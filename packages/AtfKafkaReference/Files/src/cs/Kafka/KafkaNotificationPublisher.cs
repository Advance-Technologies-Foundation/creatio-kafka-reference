using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using AtfKafkaReference.Kafka;
using ErrorOr;

namespace AtfKafkaReference.Kafka {

	internal interface IKafkaNotificationPublisher {
		Task<ErrorOr<KafkaNotification>> PublishAsync(string message, CancellationToken cancellationToken);
	}

	internal sealed class KafkaNotificationPublisher : IKafkaNotificationPublisher, IDisposable {

		private readonly KafkaOptions _options;
		private readonly IKafkaClientFactory _clientFactory;
		private readonly Lazy<IProducer<string, string>> _producer;
		private int _isDisposed;

		public KafkaNotificationPublisher(KafkaOptions options, IKafkaClientFactory clientFactory) {
			_options = options;
			_clientFactory = clientFactory;
			_producer = new Lazy<IProducer<string, string>>(CreateProducer);
		}

		public async Task<ErrorOr<KafkaNotification>> PublishAsync(string message,
				CancellationToken cancellationToken) {
			KafkaNotification notification = new KafkaNotification {
				MessageId = Guid.NewGuid(),
				Message = message,
				SentAtUtc = DateTime.UtcNow
			};
			try {
				await _producer.Value.ProduceAsync(_options.NotificationTopic, new Message<string, string> {
					Key = notification.MessageId.ToString("D"),
					Value = JsonSerializer.Serialize(notification)
				}, cancellationToken).ConfigureAwait(false);
				return notification;
			} catch (ProduceException<string, string> exception) {
				return ErrorOr.Error.Failure("Kafka.PublishFailed", exception.Error.Reason);
			}
		}

		private IProducer<string, string> CreateProducer() {
			return _clientFactory.CreateProducer(new ProducerConfig {
				BootstrapServers = _options.BootstrapServers,
				SecurityProtocol = SecurityProtocol.SaslSsl,
				SaslMechanism = SaslMechanism.ScramSha512,
				SaslUsername = _options.Username,
				SaslPassword = _options.Password,
				SslCaCertificateStores = "Root",
				EnableIdempotence = true,
				Acks = Acks.All
			}, _options.NativeLibraryPath);
		}

		public void Dispose() {
			if (Interlocked.Exchange(ref _isDisposed, 1) != 0) {
				return;
			}
			if (_producer.IsValueCreated) {
				_producer.Value.Flush(TimeSpan.FromSeconds(5));
				_producer.Value.Dispose();
			}
		}
	}
}
