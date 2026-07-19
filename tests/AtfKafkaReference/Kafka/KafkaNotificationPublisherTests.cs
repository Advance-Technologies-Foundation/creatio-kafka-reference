using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using AtfKafkaReference.Kafka;
using FluentAssertions;
using NSubstitute;
using NUnit.Framework;

namespace AtfKafkaReference.Tests.Kafka {

	[TestFixture(Category = "UnitTests")]
	public class KafkaNotificationPublisherTests {
		[Test]
		[Description("Returns the generated notification after Kafka acknowledges the publish.")]
		public async Task PublishAsync_WhenKafkaAcknowledges_ShouldReturnNotification() {
			// Arrange
			var producer = Substitute.For<IProducer<string, string>>();
			producer.ProduceAsync("notifications", Arg.Any<Message<string, string>>(), Arg.Any<CancellationToken>())
				.Returns(Task.FromResult(new DeliveryResult<string, string>()));
			IKafkaClientFactory factory = Substitute.For<IKafkaClientFactory>();
			factory.CreateProducer(Arg.Any<ProducerConfig>(), "native-path").Returns(producer);
			var options = new KafkaOptions {
				BootstrapServers = "broker:9094", Username = "user", Password = "password",
				NotificationTopic = "notifications", NativeLibraryPath = "native-path"
			};
			var sut = new KafkaNotificationPublisher(options, factory);

			// Act
			var result = await sut.PublishAsync("Hello", CancellationToken.None);

			// Assert
			result.IsError.Should().BeFalse(because: "Kafka acknowledged the notification");
			result.Value.Message.Should().Be("Hello", because: "the notification preserves the posted text");
			result.Value.MessageId.Should().NotBeEmpty(because: "the HTTP caller needs a correlation ID");
			await producer.Received(1).ProduceAsync("notifications",
				Arg.Is<Message<string, string>>(message => message.Key == result.Value.MessageId.ToString("D") &&
					message.Value.Contains("Hello")), Arg.Any<CancellationToken>());
		}

		[Test]
		[Description("Maps a Kafka produce exception to an error value instead of throwing.")]
		public async Task PublishAsync_WhenKafkaRejects_ShouldReturnFailure() {
			// Arrange
			var producer = Substitute.For<IProducer<string, string>>();
			producer.ProduceAsync("notifications", Arg.Any<Message<string, string>>(), Arg.Any<CancellationToken>())
				.Returns(Task.FromException<DeliveryResult<string, string>>(
					new ProduceException<string, string>(new Error(ErrorCode.Local_MsgTimedOut),
						new DeliveryResult<string, string>())));
			IKafkaClientFactory factory = Substitute.For<IKafkaClientFactory>();
			factory.CreateProducer(Arg.Any<ProducerConfig>(), Arg.Any<string>()).Returns(producer);
			var sut = new KafkaNotificationPublisher(new KafkaOptions { NotificationTopic = "notifications" }, factory);

			// Act
			var result = await sut.PublishAsync("Hello", CancellationToken.None);

			// Assert
			result.IsError.Should().BeTrue(because: "Kafka rejected the publish operation");
			result.FirstError.Code.Should().Be("Kafka.PublishFailed",
				because: "the service maps a stable transport error code");
		}

		[Test]
		[Description("Disposes a producer only after the lazy producer has been created.")]
		public async Task Dispose_WhenProducerWasCreated_ShouldFlushAndDisposeIt() {
			// Arrange
			var producer = Substitute.For<IProducer<string, string>>();
			producer.ProduceAsync(Arg.Any<string>(), Arg.Any<Message<string, string>>(), Arg.Any<CancellationToken>())
				.Returns(Task.FromResult(new DeliveryResult<string, string>()));
			IKafkaClientFactory factory = Substitute.For<IKafkaClientFactory>();
			factory.CreateProducer(Arg.Any<ProducerConfig>(), Arg.Any<string>()).Returns(producer);
			var sut = new KafkaNotificationPublisher(new KafkaOptions { NotificationTopic = "notifications" }, factory);
			await sut.PublishAsync("Hello", CancellationToken.None);

			// Act
			sut.Dispose();

			// Assert
			producer.Received(1).Flush(TimeSpan.FromSeconds(5));
			producer.Received(1).Dispose();
		}
	}
}
