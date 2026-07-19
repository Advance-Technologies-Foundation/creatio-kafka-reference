using System;
using System.Text.Json;
using System.Threading;
using Common.Logging;
using Confluent.Kafka;
using AtfKafkaReference.Kafka;

namespace AtfKafkaReference.Kafka {

	internal interface IKafkaWorker {
		void Run(CancellationToken cancellationToken);
	}

	internal sealed class KafkaWorker : IKafkaWorker {

		private readonly KafkaOptions _options;
		private readonly IKafkaMessageHandler _handler;
		private readonly IKafkaClientFactory _clientFactory;
		private readonly ILog _log;

		public KafkaWorker(KafkaOptions options, IKafkaMessageHandler handler, IKafkaClientFactory clientFactory,
				ILog log) {
			_options = options;
			_handler = handler;
			_clientFactory = clientFactory;
			_log = log;
		}

		public void Run(CancellationToken cancellationToken) {
			ConsumerConfig consumerConfig = CreateConsumerConfig();
			ProducerConfig producerConfig = CreateProducerConfig();

			using (IConsumer<string, string> consumer =
					_clientFactory.CreateConsumer(consumerConfig, _options.NativeLibraryPath))
			using (IProducer<string, string> producer =
					_clientFactory.CreateProducer(producerConfig, _options.NativeLibraryPath)) {
				consumer.Subscribe(_options.RequestTopic);
				_log.InfoFormat("Kafka listener started. Request topic: {0}; reply topic: {1}",
					_options.RequestTopic, _options.ReplyTopic);

				try {
					while (!cancellationToken.IsCancellationRequested) {
						ConsumeResult<string, string> consumed;
						try {
							consumed = consumer.Consume(cancellationToken);
						} catch (ConsumeException exception) {
							_log.Error("Kafka consume failed; the listener will continue.", exception);
							continue;
						}

						try {
							if (TryHandle(consumed.Message.Value, out KafkaReply reply)) {
								string json = JsonSerializer.Serialize(reply);
								producer.ProduceAsync(_options.ReplyTopic, new Message<string, string> {
									Key = reply.CorrelationId.ToString("D"),
									Value = json
								}).GetAwaiter().GetResult();
								consumer.Commit(consumed);
								_log.InfoFormat("Kafka request {0} processed.", reply.CorrelationId);
							}
						} catch (Exception exception) {
							_log.Error("Kafka request processing failed; the listener will continue.", exception);
						}
					}
				} catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) {
					// Normal application shutdown.
				} finally {
					consumer.Close();
					producer.Flush(TimeSpan.FromSeconds(5));
					_log.Info("Kafka listener stopped.");
				}
			}
		}

		internal bool TryHandle(string json, out KafkaReply reply) {
			reply = null;
			try {
				KafkaRequest request = JsonSerializer.Deserialize<KafkaRequest>(json);
				reply = _handler.Handle(request);
				if (reply == null) {
					_log.Warn("Ignoring an invalid Kafka request.");
					return false;
				}
				return true;
			} catch (JsonException exception) {
				_log.Error("Ignoring a Kafka request with invalid JSON.", exception);
				return false;
			}
		}

		private ConsumerConfig CreateConsumerConfig() {
			return new ConsumerConfig(CreateClientConfig()) {
				GroupId = _options.ConsumerGroup,
				AutoOffsetReset = AutoOffsetReset.Earliest,
				EnableAutoCommit = false
			};
		}

		private ProducerConfig CreateProducerConfig() {
			return new ProducerConfig(CreateClientConfig()) {
				EnableIdempotence = true,
				Acks = Acks.All
			};
		}

		private ClientConfig CreateClientConfig() {
			return new ClientConfig {
				BootstrapServers = _options.BootstrapServers,
				SecurityProtocol = SecurityProtocol.SaslSsl,
				SaslMechanism = SaslMechanism.ScramSha512,
				SaslUsername = _options.Username,
				SaslPassword = _options.Password,
				SslCaCertificateStores = "Root"
			};
		}
	}
}
