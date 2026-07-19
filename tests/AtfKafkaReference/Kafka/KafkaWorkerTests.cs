using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Common.Logging;
using Confluent.Kafka;
using AtfKafkaReference.Kafka;
using FluentAssertions;
using NSubstitute;
using NUnit.Framework;

namespace AtfKafkaReference.Tests.Kafka {

	[TestFixture(Category = "UnitTests")]
	public class KafkaWorkerTests {
		private IKafkaClientFactory _clientFactory;
		private IKafkaMessageHandler _handler;
		private ILog _log;
		private KafkaOptions _options;

		[SetUp]
		public void SetUp() {
			_clientFactory = Substitute.For<IKafkaClientFactory>();
			_handler = Substitute.For<IKafkaMessageHandler>();
			_log = Substitute.For<ILog>();
			_options = new KafkaOptions {
				BootstrapServers = "broker:9094",
				Username = "user",
				Password = "password",
				RequestTopic = "requests",
				ReplyTopic = "replies",
				ConsumerGroup = "creatio",
				NativeLibraryPath = "native-path"
			};
		}

		[Test]
		[Description("Consumes a valid request, publishes its reply, and commits the consumed offset.")]
		public void Run_WhenRequestIsValid_ShouldPublishReplyAndCommit() {
			// Arrange
			var consumer = Substitute.For<IConsumer<string, string>>();
			var producer = Substitute.For<IProducer<string, string>>();
			var cancellation = new CancellationTokenSource();
			Guid correlationId = Guid.NewGuid();
			KafkaRequest request = new KafkaRequest { CorrelationId = correlationId, Message = "Hello" };
			KafkaReply reply = new KafkaReply { CorrelationId = correlationId, Message = "Reply" };
			var consumed = new ConsumeResult<string, string> {
				Message = new Message<string, string> { Value = JsonSerializer.Serialize(request) }
			};
			consumer.Consume(Arg.Any<CancellationToken>()).Returns(
				_ => consumed,
				_ => {
					cancellation.Cancel();
					throw new OperationCanceledException(cancellation.Token);
				});
			producer.ProduceAsync("replies", Arg.Any<Message<string, string>>(), Arg.Any<CancellationToken>())
				.Returns(Task.FromResult(new DeliveryResult<string, string>()));
			_handler.Handle(Arg.Any<KafkaRequest>()).Returns(reply);
			_clientFactory.CreateConsumer(Arg.Any<ConsumerConfig>(), "native-path").Returns(consumer);
			_clientFactory.CreateProducer(Arg.Any<ProducerConfig>(), "native-path").Returns(producer);
			var sut = new KafkaWorker(_options, _handler, _clientFactory, _log);

			// Act
			sut.Run(cancellation.Token);

			// Assert
			consumer.Received(1).Subscribe("requests");
			producer.Received(1).ProduceAsync("replies",
				Arg.Is<Message<string, string>>(message => message.Key == correlationId.ToString("D") &&
					message.Value.Contains("Reply")), Arg.Any<CancellationToken>());
			consumer.Received(1).Commit(consumed);
			consumer.Received(1).Close();
		}

		[Test]
		[Description("Returns false and logs an error when a Kafka record is not valid JSON.")]
		public void TryHandle_WhenJsonIsMalformed_ShouldReturnFalse() {
			// Arrange
			var sut = new KafkaWorker(_options, _handler, _clientFactory, _log);

			// Act
			bool handled = sut.TryHandle("not-json", out KafkaReply reply);

			// Assert
			handled.Should().BeFalse(because: "malformed transport input must not be published");
			reply.Should().BeNull(because: "no business reply exists for malformed JSON");
			_log.Received(1).Error(Arg.Any<string>(), Arg.Any<JsonException>());
		}

		[Test]
		[Description("Returns false and logs a warning when the message handler rejects a deserialized request.")]
		public void TryHandle_WhenHandlerRejectsRequest_ShouldReturnFalse() {
			// Arrange
			_handler.Handle(Arg.Any<KafkaRequest>()).Returns((KafkaReply)null);
			var sut = new KafkaWorker(_options, _handler, _clientFactory, _log);

			// Act
			bool handled = sut.TryHandle(JsonSerializer.Serialize(new KafkaRequest()), out KafkaReply reply);

			// Assert
			handled.Should().BeFalse(because: "a rejected business request must not be published");
			reply.Should().BeNull(because: "the handler deliberately produced no reply");
			_log.Received(1).Warn("Ignoring an invalid Kafka request.");
		}
	}
}
