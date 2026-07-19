using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using AtfKafkaReference.Kafka;
using ErrorOr;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using NUnit.Framework;
using Terrasoft.Core.ServiceModelContract;
using Terrasoft.Web.Http.Abstractions;
using AtfKafkaReference.EntryPoints.WebServices;

namespace AtfKafkaReference.Tests.EntryPoints.WebServices {

	[TestFixture(Category = "UnitTests")]
	public class KafkaMessageServiceTests : BaseComposableAppTestFixture {

		private IKafkaNotificationPublisher _publisher;
		private HttpResponse _response;
		private KafkaMessageService _sut;

		[SetUp]
		protected override void SetUp() {
			base.SetUp();
			AtfKafkaReference.AtfKafkaReferenceApp.Instance.Reset();
			_publisher = Substitute.For<IKafkaNotificationPublisher>();
			AtfKafkaReference.AtfKafkaReferenceApp.InjectedServices = new List<Func<IServiceCollection, IServiceCollection>> {
				services => services.AddSingleton(_publisher)
			};
			HttpContext context = Substitute.For<HttpContext>();
			_response = Substitute.For<HttpResponse>();
			context.Response.Returns(_response);
			IHttpContextAccessor accessor = CustomSetupHttpContextAccessor(context, UserConnection);
			_sut = new KafkaMessageService { HttpContextAccessor = accessor };
		}

		[Description("Publishes a valid message and returns HTTP 202 with its Kafka message id.")]
		[Test]
		public void Publish_WhenMessageIsValid_ShouldReturnAccepted() {
			// Arrange
			Guid messageId = Guid.NewGuid();
			_publisher.PublishAsync("Hello console", Arg.Any<CancellationToken>())
				.Returns(Task.FromResult<ErrorOr<KafkaNotification>>(new KafkaNotification {
					MessageId = messageId,
					Message = "Hello console"
				}));

			// Act
			PublishKafkaMessageResponse result = _sut.Publish(
				new PublishKafkaMessageRequest { Message = "Hello console" });

			// Assert
			result.Success.Should().BeTrue(because: "Kafka acknowledged the notification");
			result.MessageId.Should().Be(messageId, because: "the caller needs the Kafka correlation id");
			_response.StatusCode.Should().Be(202, because: "publishing is an accepted asynchronous operation");
			_publisher.Received(1).PublishAsync("Hello console", Arg.Any<CancellationToken>());
		}

		[Description("Rejects an empty message with HTTP 400 without invoking Kafka.")]
		[Test]
		public void Publish_WhenMessageIsEmpty_ShouldReturnBadRequest() {
			// Arrange
			PublishKafkaMessageRequest request = new PublishKafkaMessageRequest { Message = " " };

			// Act
			PublishKafkaMessageResponse result = _sut.Publish(request);

			// Assert
			result.Success.Should().BeFalse(because: "blank text is not a valid notification");
			result.Error.Should().Be("Message is required.", because: "the API should explain validation failure");
			_response.StatusCode.Should().Be(400, because: "invalid input is a bad request");
			_publisher.DidNotReceiveWithAnyArgs().PublishAsync(default, default);
		}

		[Description("Maps a Kafka publish failure to HTTP 502 and an error response value.")]
		[Test]
		public void Publish_WhenKafkaFails_ShouldReturnBadGateway() {
			// Arrange
			_publisher.PublishAsync("Hello console", Arg.Any<CancellationToken>())
				.Returns(Task.FromResult<ErrorOr<KafkaNotification>>(
					Error.Failure("Kafka.PublishFailed", "Broker unavailable")));

			// Act
			PublishKafkaMessageResponse result = _sut.Publish(
				new PublishKafkaMessageRequest { Message = "Hello console" });

			// Assert
			result.Success.Should().BeFalse(because: "Kafka did not accept the notification");
			result.Error.Should().Be("Broker unavailable", because: "the broker failure should be surfaced");
			_response.StatusCode.Should().Be(502, because: "Kafka is an upstream dependency");
		}
	}
}
