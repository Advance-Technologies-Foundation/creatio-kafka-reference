using System;
using AtfKafkaReference.Kafka;
using FluentAssertions;
using NSubstitute;
using NUnit.Framework;

namespace AtfKafkaReference.Tests.Kafka {

	[TestFixture]
	public class KafkaMessageHandlerTests {

		[Test]
		public void Handle_ShouldCreateCorrelatedReplyWithCurrentContactName() {
			IContactNameProvider contactNameProvider = Substitute.For<IContactNameProvider>();
			contactNameProvider.GetCurrentContactName().Returns("Supervisor");
			KafkaMessageHandler sut = new KafkaMessageHandler(contactNameProvider);
			Guid correlationId = Guid.NewGuid();

			KafkaReply result = sut.Handle(new KafkaRequest {
				CorrelationId = correlationId,
				Message = "Hello Kafka",
				SentAtUtc = DateTime.UtcNow
			});

			result.Should().NotBeNull();
			result.CorrelationId.Should().Be(correlationId);
			result.ContactName.Should().Be("Supervisor");
			result.Message.Should().Be("I see your message Hello Kafka by Supervisor");
			result.SentAtUtc.Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromSeconds(2));
		}

		[TestCase(null)]
		[TestCase("")]
		[TestCase("   ")]
		public void Handle_ShouldReturnNull_WhenMessageIsMissing(string message) {
			IContactNameProvider contactNameProvider = Substitute.For<IContactNameProvider>();
			KafkaMessageHandler sut = new KafkaMessageHandler(contactNameProvider);

			KafkaReply result = sut.Handle(new KafkaRequest {
				CorrelationId = Guid.NewGuid(),
				Message = message
			});

			result.Should().BeNull();
			contactNameProvider.DidNotReceive().GetCurrentContactName();
		}

		[Test]
		public void Handle_ShouldReturnNull_WhenCorrelationIdIsMissing() {
			IContactNameProvider contactNameProvider = Substitute.For<IContactNameProvider>();
			KafkaMessageHandler sut = new KafkaMessageHandler(contactNameProvider);

			KafkaReply result = sut.Handle(new KafkaRequest { Message = "Hello" });

			result.Should().BeNull();
		}
	}
}
